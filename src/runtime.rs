use crate::{CapSet, Failure, FailureKind, ForceResult, Hash, HostFn, Node, Outcome, Store, Value};
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

    pub fn force(&self, node: Hash) -> anyhow::Result<ForceResult> {
        if let Some(outcome) = self.store.get_outcome(&node)? {
            return Ok(ForceResult {
                outcome,
                cache_hit: true,
            });
        }

        let outcome = self.eval(&node, &mut Vec::new())?;
        self.store.put_outcome(&node, &outcome)?;
        Ok(ForceResult {
            outcome,
            cache_hit: false,
        })
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

    fn eval(&self, node_hash: &Hash, path: &mut Vec<Hash>) -> anyhow::Result<Outcome> {
        path.push(node_hash.clone());

        let Some(node) = self.store.get_node(node_hash)? else {
            let failure = Failure::new(
                node_hash.clone(),
                FailureKind::MissingObject(node_hash.clone()),
                path.clone(),
            );
            path.pop();
            return Ok(Outcome::Failure(failure));
        };

        let outcome = match node {
            Node::Value(value) => {
                let value_hash = self.store.put_value(&value)?;
                Outcome::Success(value_hash)
            }
            Node::Apply { function, args } => self.eval_apply(node_hash, &function, &args, path)?,
            Node::HostCall {
                capability, args, ..
            } => self.eval_apply(node_hash, &capability, &args, path)?,
        };

        path.pop();
        Ok(outcome)
    }

    fn eval_apply(
        &self,
        node_hash: &Hash,
        function: &str,
        args: &[Hash],
        path: &mut Vec<Hash>,
    ) -> anyhow::Result<Outcome> {
        let Some(cap) = self.caps.get(function) else {
            return Ok(Outcome::Failure(Failure::new(
                node_hash.clone(),
                FailureKind::UnknownCapability(function.to_owned()),
                path.clone(),
            )));
        };

        let mut values = Vec::with_capacity(args.len());
        for arg in args {
            match self.force_dependency(arg, path)? {
                Outcome::Success(value_hash) => {
                    let Some(value) = self.store.get_value(&value_hash)? else {
                        return Ok(Outcome::Failure(Failure::new(
                            node_hash.clone(),
                            FailureKind::MissingObject(value_hash),
                            path.clone(),
                        )));
                    };
                    values.push(value);
                }
                Outcome::Failure(mut failure) => {
                    failure.dependency_path = path.clone();
                    return Ok(Outcome::Failure(failure));
                }
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
                path.clone(),
            ))),
        }
    }

    fn force_dependency(&self, node: &Hash, path: &mut Vec<Hash>) -> anyhow::Result<Outcome> {
        if let Some(outcome) = self.store.get_outcome(node)? {
            return Ok(outcome);
        }

        let outcome = self.eval(node, path)?;
        self.store.put_outcome(node, &outcome)?;
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn add_ints(args: &[Value]) -> Result<Value, FailureKind> {
        match args {
            [Value::Int(a), Value::Int(b)] => Ok(Value::Int(a + b)),
            _ => Err(FailureKind::TypeError("+ expects two ints".to_owned())),
        }
    }

    #[test]
    fn forces_pure_node_and_reuses_cached_outcome() -> anyhow::Result<()> {
        let temp = std::env::temp_dir().join(format!(
            "r2-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        ));

        let mut rt = Runtime::new(&temp)?;
        rt.register("+", HostFn::pure(add_ints));

        let a = rt.int(20)?;
        let b = rt.int(22)?;
        let sum = rt.call("+", vec![a, b])?;

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
        let temp = std::env::temp_dir().join(format!(
            "r2-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        ));

        let rt = Runtime::new(&temp)?;
        let a = rt.int(1)?;
        let b = rt.int(2)?;
        let sum = rt.call("+", vec![a, b])?;

        let forced = rt.force(sum)?;
        let Outcome::Failure(failure) = forced.outcome else {
            panic!("unknown capability should fail");
        };
        assert_eq!(failure.kind, FailureKind::UnknownCapability("+".to_owned()));

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }
}
