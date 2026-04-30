use crate::{
    CapSet, Failure, FailureKind, ForceResult, GraphTrace, Hash, HostFn, Node, Outcome, Store,
    Value,
};
use std::collections::BTreeSet;
use std::path::Path;

pub struct Runtime {
    store: Store,
    caps: CapSet,
}

impl Runtime {
    pub fn new(store_path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Self::with_caps(store_path, CapSet::new())
    }

    pub fn with_caps(store_path: impl AsRef<Path>, caps: CapSet) -> anyhow::Result<Self> {
        Ok(Self {
            store: Store::open(store_path)?,
            caps,
        })
    }

    pub fn register(&mut self, name: impl Into<String>, func: HostFn) {
        self.caps.insert(name, func);
    }

    pub fn int(&self, value: i64) -> anyhow::Result<Hash> {
        self.value(Value::Int(value))
    }

    pub fn text(&self, value: impl Into<String>) -> anyhow::Result<Hash> {
        self.value(Value::Text(value.into()))
    }

    pub fn value(&self, value: Value) -> anyhow::Result<Hash> {
        self.store.put_value(&value)?;
        self.store.put_node(&Node::Value(value))
    }

    pub fn call(&self, function: impl Into<String>, args: Vec<Hash>) -> anyhow::Result<Hash> {
        self.store.put_node(&Node::Apply {
            function: function.into(),
            args,
        })
    }

    pub fn thunk(&self, target: Hash) -> anyhow::Result<Hash> {
        self.store.put_node(&Node::Thunk { target })
    }

    pub fn force(&self, node: Hash) -> anyhow::Result<ForceResult> {
        if let Some(outcome) = self.store.get_outcome(&node)? {
            if let Some(failure) = self.validate_authority(&node)? {
                return Ok(ForceResult {
                    outcome: Outcome::Failure(failure),
                    cache_hit: false,
                });
            }

            return Ok(ForceResult {
                outcome,
                cache_hit: true,
            });
        }

        let outcome = self.eval(&node, &mut GraphTrace::default())?;
        if outcome.is_cacheable() {
            self.store.put_outcome(&node, &outcome)?;
        }
        Ok(ForceResult {
            outcome,
            cache_hit: false,
        })
    }

    pub fn get_node(&self, hash: &Hash) -> anyhow::Result<Option<Node>> {
        self.store.get_node(hash)
    }

    pub fn get_outcome(&self, hash: &Hash) -> anyhow::Result<Option<Outcome>> {
        self.store.get_outcome(hash)
    }

    pub fn force_value(&self, node: Hash) -> anyhow::Result<(Value, bool)> {
        let forced = self.force(node)?;
        match forced.outcome {
            Outcome::Success(value_hash) => {
                let value = self.get_value(&value_hash)?;
                Ok((value, forced.cache_hit))
            }
            Outcome::Failure(failure) => Err(anyhow::anyhow!("{failure:?}")),
        }
    }

    pub fn get_value(&self, hash: &Hash) -> anyhow::Result<Value> {
        self.store
            .get_value(hash)?
            .ok_or_else(|| anyhow::anyhow!("missing value {hash}"))
    }

    fn eval(&self, node_hash: &Hash, trace: &mut GraphTrace) -> anyhow::Result<Outcome> {
        if trace.contains(node_hash) {
            return Ok(Outcome::Failure(Failure::new(
                node_hash.clone(),
                FailureKind::Cycle(node_hash.clone()),
                trace.clone(),
            )));
        }

        trace.push(node_hash.clone());

        let Some(node) = self.store.get_node(node_hash)? else {
            let failure = Failure::new(
                node_hash.clone(),
                FailureKind::MissingObject(node_hash.clone()),
                trace.clone(),
            );
            trace.pop();
            return Ok(Outcome::Failure(failure));
        };

        let outcome = match node {
            Node::Value(value) => {
                let value_hash = self.store.put_value(&value)?;
                Outcome::Success(value_hash)
            }
            Node::Thunk { target } => self.force_dependency(&target, trace)?,
            Node::Apply { function, args } => {
                self.eval_apply(node_hash, &function, &args, trace)?
            }
            Node::HostCall {
                capability, args, ..
            } => self.eval_apply(node_hash, &capability, &args, trace)?,
        };

        trace.pop();
        Ok(outcome)
    }

    fn eval_apply(
        &self,
        node_hash: &Hash,
        function: &str,
        args: &[Hash],
        trace: &mut GraphTrace,
    ) -> anyhow::Result<Outcome> {
        let Some(cap) = self.caps.get(function) else {
            return Ok(Outcome::Failure(Failure::new(
                node_hash.clone(),
                FailureKind::UnknownCapability(function.to_owned()),
                trace.clone(),
            )));
        };

        let mut values = Vec::with_capacity(args.len());
        for arg in args {
            match self.force_dependency(arg, trace)? {
                Outcome::Success(value_hash) => {
                    let Some(value) = self.store.get_value(&value_hash)? else {
                        return Ok(Outcome::Failure(Failure::new(
                            node_hash.clone(),
                            FailureKind::MissingObject(value_hash),
                            trace.clone(),
                        )));
                    };
                    values.push(value);
                }
                Outcome::Failure(failure) => return Ok(Outcome::Failure(failure)),
            }
        }

        match cap.call(&values) {
            Ok(value) => {
                let value_hash = self.store.put_value(&value)?;
                Ok(Outcome::Success(value_hash))
            }
            Err(kind) => Ok(Outcome::Failure(Failure::new(
                node_hash.clone(),
                kind,
                trace.clone(),
            ))),
        }
    }

    fn force_dependency(&self, node: &Hash, trace: &mut GraphTrace) -> anyhow::Result<Outcome> {
        if let Some(outcome) = self.store.get_outcome(node)? {
            if let Some(failure) = self.validate_authority_with_trace(node, trace.clone())? {
                return Ok(Outcome::Failure(failure));
            }
            return Ok(self.with_current_trace(outcome, trace));
        }

        let outcome = self.eval(node, trace)?;
        if outcome.is_cacheable() {
            self.store.put_outcome(node, &outcome)?;
        }
        Ok(outcome)
    }

    fn with_current_trace(&self, outcome: Outcome, trace: &GraphTrace) -> Outcome {
        let Outcome::Failure(mut failure) = outcome else {
            return outcome;
        };

        if trace.hashes().is_empty() {
            return Outcome::Failure(failure);
        }

        let mut dependency_path = trace.hashes().to_vec();
        for hash in failure.trace.hashes() {
            if dependency_path.last() != Some(hash) {
                dependency_path.push(hash.clone());
            }
        }
        failure.trace = GraphTrace::new(dependency_path);
        Outcome::Failure(failure)
    }

    fn validate_authority(&self, node: &Hash) -> anyhow::Result<Option<Failure>> {
        self.validate_authority_with_trace(node, GraphTrace::default())
    }

    fn validate_authority_with_trace(
        &self,
        node: &Hash,
        mut trace: GraphTrace,
    ) -> anyhow::Result<Option<Failure>> {
        self.validate_authority_inner(node, &mut trace, &mut BTreeSet::new())
    }

    fn validate_authority_inner(
        &self,
        node_hash: &Hash,
        trace: &mut GraphTrace,
        visited: &mut BTreeSet<Hash>,
    ) -> anyhow::Result<Option<Failure>> {
        if !visited.insert(node_hash.clone()) {
            return Ok(None);
        }

        trace.push(node_hash.clone());
        let Some(node) = self.store.get_node(node_hash)? else {
            trace.pop();
            return Ok(None);
        };

        let failure = match node {
            Node::Value(_) => None,
            Node::Thunk { target } => self.validate_authority_inner(&target, trace, visited)?,
            Node::Apply { function, args } => {
                if self.caps.get(&function).is_none() {
                    Some(Failure::new(
                        node_hash.clone(),
                        FailureKind::UnknownCapability(function),
                        trace.clone(),
                    ))
                } else {
                    self.validate_args_authority(&args, trace, visited)?
                }
            }
            Node::HostCall {
                capability, args, ..
            } => {
                if self.caps.get(&capability).is_none() {
                    Some(Failure::new(
                        node_hash.clone(),
                        FailureKind::UnknownCapability(capability),
                        trace.clone(),
                    ))
                } else {
                    self.validate_args_authority(&args, trace, visited)?
                }
            }
        };

        trace.pop();
        Ok(failure)
    }

    fn validate_args_authority(
        &self,
        args: &[Hash],
        trace: &mut GraphTrace,
        visited: &mut BTreeSet<Hash>,
    ) -> anyhow::Result<Option<Failure>> {
        for arg in args {
            if let Some(failure) = self.validate_authority_inner(arg, trace, visited)? {
                return Ok(Some(failure));
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn add_ints(args: &[Value]) -> Result<Value, FailureKind> {
        match args {
            [Value::Int(a), Value::Int(b)] => Ok(Value::Int(a + b)),
            _ => Err(FailureKind::TypeError("+ expects two ints".to_owned())),
        }
    }

    fn temp_store() -> anyhow::Result<PathBuf> {
        Ok(std::env::temp_dir().join(format!(
            "r2-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        )))
    }

    #[test]
    fn forces_pure_node_and_reuses_cached_outcome() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let mut rt = Runtime::new(&temp)?;
        rt.register("+", HostFn::pure(add_ints));

        let a = rt.int(20)?;
        let b = rt.int(22)?;
        let sum_expr = rt.call("+", vec![a, b])?;
        let sum = rt.thunk(sum_expr)?;

        let first = rt.force(sum.clone())?;
        assert!(!first.cache_hit);
        let Outcome::Success(value_hash) = first.outcome else {
            panic!("sum should succeed");
        };
        assert_eq!(rt.get_value(&value_hash)?, Value::Int(42));

        let second = rt.force(sum)?;
        assert!(second.cache_hit);

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn fails_when_capability_is_not_registered() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let rt = Runtime::new(&temp)?;
        let a = rt.int(1)?;
        let b = rt.int(2)?;
        let sum_expr = rt.call("+", vec![a, b])?;
        let sum = rt.thunk(sum_expr.clone())?;

        let forced = rt.force(sum.clone())?;
        let Outcome::Failure(failure) = forced.outcome else {
            panic!("unknown capability should fail");
        };
        assert_eq!(failure.kind, FailureKind::UnknownCapability("+".to_owned()));
        assert_eq!(failure.trace.hashes(), &[sum, sum_expr]);

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn reuses_cached_outcome_across_runtime_instances() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let sum = {
            let mut rt = Runtime::new(&temp)?;
            rt.register("+", HostFn::pure(add_ints));

            let a = rt.int(20)?;
            let b = rt.int(22)?;
            let sum_expr = rt.call("+", vec![a, b])?;
            let sum = rt.thunk(sum_expr)?;

            let first = rt.force(sum.clone())?;
            assert!(!first.cache_hit);
            sum
        };

        let mut rt = Runtime::new(&temp)?;
        rt.register("+", HostFn::pure(add_ints));

        let second = rt.force(sum)?;
        assert!(second.cache_hit);
        let Outcome::Success(value_hash) = second.outcome else {
            panic!("cached sum should succeed");
        };
        assert_eq!(rt.get_value(&value_hash)?, Value::Int(42));

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn can_inspect_graph_and_cached_outcome() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let mut rt = Runtime::new(&temp)?;
        rt.register("+", HostFn::pure(add_ints));

        let a = rt.int(20)?;
        let b = rt.int(22)?;
        let sum_expr = rt.call("+", vec![a, b])?;
        let sum = rt.thunk(sum_expr.clone())?;

        assert_eq!(
            rt.get_node(&sum)?,
            Some(Node::Thunk {
                target: sum_expr.clone()
            })
        );
        assert_eq!(rt.get_outcome(&sum)?, None);

        let forced = rt.force(sum.clone())?;
        assert!(!forced.cache_hit);
        assert_eq!(rt.get_outcome(&sum)?, Some(forced.outcome));

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn rejects_object_kind_mismatch() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let rt = Runtime::new(&temp)?;
        let value_node = rt.int(42)?;
        let Outcome::Success(value_hash) = rt.force(value_node)?.outcome else {
            panic!("value node should force");
        };

        let err = rt.get_node(&value_hash).unwrap_err();
        assert!(err.to_string().contains("expected Node"));

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn cached_success_still_requires_current_capability() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let sum = {
            let mut rt = Runtime::new(&temp)?;
            rt.register("+", HostFn::pure(add_ints));

            let a = rt.int(20)?;
            let b = rt.int(22)?;
            let sum_expr = rt.call("+", vec![a, b])?;
            let sum = rt.thunk(sum_expr)?;

            let first = rt.force(sum.clone())?;
            assert!(!first.cache_hit);
            let Outcome::Success(_) = first.outcome else {
                panic!("sum should succeed");
            };
            sum
        };

        let rt = Runtime::new(&temp)?;
        let forced = rt.force(sum)?;
        assert!(!forced.cache_hit);
        let Outcome::Failure(failure) = forced.outcome else {
            panic!("missing capability should block cached success");
        };
        assert_eq!(failure.kind, FailureKind::UnknownCapability("+".to_owned()));

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }
}
