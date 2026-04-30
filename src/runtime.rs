use crate::{
    CapSet, EffectKind, Failure, FailureKind, ForceResult, GcPlan, GraphTrace, Hash, HostFn, Node,
    Outcome, Store, StoreStats, Value,
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

    pub fn has_capability(&self, name: &str) -> bool {
        self.caps.contains(name)
    }

    pub fn capability_effect(&self, name: &str) -> Option<EffectKind> {
        self.caps.effect(name)
    }

    pub fn capability(&self, name: &str) -> Option<crate::Capability> {
        self.caps.capability(name)
    }

    pub fn capabilities(&self) -> Vec<crate::Capability> {
        self.caps.capabilities()
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

    pub fn host_call(
        &self,
        capability: impl Into<String>,
        args: Vec<Hash>,
        effect: EffectKind,
    ) -> anyhow::Result<Hash> {
        self.store.put_node(&Node::HostCall {
            capability: capability.into(),
            args,
            effect,
        })
    }

    pub fn thunk(&self, target: Hash) -> anyhow::Result<Hash> {
        self.store.put_node(&Node::Thunk { target })
    }

    pub fn force(&self, node: Hash) -> anyhow::Result<ForceResult> {
        if self.node_is_cacheable(&node, &mut BTreeSet::new())?
            && let Some(outcome) = self.store.get_outcome(&node)?
        {
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
        if self.should_cache(&node, &outcome)? {
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

    pub fn pin(&self, name: impl Into<String>, hash: Hash) -> anyhow::Result<()> {
        self.store.pin(name, hash)
    }

    pub fn unpin(&self, name: &str) -> anyhow::Result<Option<Hash>> {
        self.store.unpin(name)
    }

    pub fn resolve_pin(&self, name: &str) -> anyhow::Result<Option<Hash>> {
        self.store.resolve_pin(name)
    }

    pub fn pins(&self) -> anyhow::Result<std::collections::BTreeMap<String, Hash>> {
        self.store.pins()
    }

    pub fn alias(&self, name: impl Into<String>, hash: Hash) -> anyhow::Result<()> {
        self.store.alias(name, hash)
    }

    pub fn unalias(&self, name: &str) -> anyhow::Result<Option<Hash>> {
        self.store.unalias(name)
    }

    pub fn resolve_alias(&self, name: &str) -> anyhow::Result<Option<Hash>> {
        self.store.resolve_alias(name)
    }

    pub fn aliases(&self) -> anyhow::Result<std::collections::BTreeMap<String, Hash>> {
        self.store.aliases()
    }

    pub fn store_stats(&self) -> anyhow::Result<StoreStats> {
        self.store.stats()
    }

    pub fn gc_plan(&self) -> anyhow::Result<GcPlan> {
        self.store.gc_plan()
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
                self.eval_apply(node_hash, &function, &args, EffectKind::Pure, trace)?
            }
            Node::HostCall {
                capability,
                args,
                effect,
            } => self.eval_apply(node_hash, &capability, &args, effect, trace)?,
        };

        trace.pop();
        Ok(outcome)
    }

    fn eval_apply(
        &self,
        node_hash: &Hash,
        function: &str,
        args: &[Hash],
        requested_effect: EffectKind,
        trace: &mut GraphTrace,
    ) -> anyhow::Result<Outcome> {
        let Some(cap) = self.caps.get(function) else {
            return Ok(Outcome::Failure(Failure::new(
                node_hash.clone(),
                FailureKind::UnknownCapability(function.to_owned()),
                trace.clone(),
            )));
        };

        let actual_effect = cap.effect();
        if actual_effect != requested_effect {
            return Ok(Outcome::Failure(Failure::new(
                node_hash.clone(),
                FailureKind::EffectMismatch {
                    capability: function.to_owned(),
                    requested: requested_effect,
                    actual: actual_effect,
                },
                trace.clone(),
            )));
        }

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
        if self.node_is_cacheable(node, &mut BTreeSet::new())?
            && let Some(outcome) = self.store.get_outcome(node)?
        {
            if let Some(failure) = self.validate_authority_with_trace(node, trace.clone())? {
                return Ok(Outcome::Failure(failure));
            }
            return Ok(self.with_current_trace(outcome, trace));
        }

        let outcome = self.eval(node, trace)?;
        if self.should_cache(node, &outcome)? {
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
                if let Some(cap) = self.caps.get(&function) {
                    let actual_effect = cap.effect();
                    if actual_effect != EffectKind::Pure {
                        Some(Failure::new(
                            node_hash.clone(),
                            FailureKind::EffectMismatch {
                                capability: function,
                                requested: EffectKind::Pure,
                                actual: actual_effect,
                            },
                            trace.clone(),
                        ))
                    } else {
                        self.validate_args_authority(&args, trace, visited)?
                    }
                } else {
                    Some(Failure::new(
                        node_hash.clone(),
                        FailureKind::UnknownCapability(function),
                        trace.clone(),
                    ))
                }
            }
            Node::HostCall {
                capability,
                args,
                effect,
            } => {
                if let Some(cap) = self.caps.get(&capability) {
                    let actual_effect = cap.effect();
                    if actual_effect != effect {
                        Some(Failure::new(
                            node_hash.clone(),
                            FailureKind::EffectMismatch {
                                capability,
                                requested: effect,
                                actual: actual_effect,
                            },
                            trace.clone(),
                        ))
                    } else {
                        self.validate_args_authority(&args, trace, visited)?
                    }
                } else {
                    Some(Failure::new(
                        node_hash.clone(),
                        FailureKind::UnknownCapability(capability),
                        trace.clone(),
                    ))
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

    fn should_cache(&self, node: &Hash, outcome: &Outcome) -> anyhow::Result<bool> {
        Ok(outcome.is_cacheable() && self.node_is_cacheable(node, &mut BTreeSet::new())?)
    }

    fn node_is_cacheable(
        &self,
        node_hash: &Hash,
        visited: &mut BTreeSet<Hash>,
    ) -> anyhow::Result<bool> {
        if !visited.insert(node_hash.clone()) {
            return Ok(true);
        }

        let Some(node) = self.store.get_node(node_hash)? else {
            return Ok(false);
        };

        match node {
            Node::Value(_) => Ok(true),
            Node::Thunk { target } => self.node_is_cacheable(&target, visited),
            Node::Apply { args, .. } => self.args_are_cacheable(&args, visited),
            Node::HostCall { effect, args, .. } => {
                if effect == EffectKind::Live {
                    Ok(false)
                } else {
                    self.args_are_cacheable(&args, visited)
                }
            }
        }
    }

    fn args_are_cacheable(
        &self,
        args: &[Hash],
        visited: &mut BTreeSet<Hash>,
    ) -> anyhow::Result<bool> {
        for arg in args {
            if !self.node_is_cacheable(arg, visited)? {
                return Ok(false);
            }
        }

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, Ordering};

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
    fn exposes_capability_metadata() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let mut rt = Runtime::new(&temp)?;
        rt.register("+", HostFn::pure(add_ints));
        rt.register("clock", HostFn::live(|_| Ok(Value::Int(1))));

        assert!(rt.has_capability("+"));
        assert!(!rt.has_capability("network"));
        assert_eq!(rt.capability_effect("+"), Some(EffectKind::Pure));
        assert_eq!(rt.capability_effect("clock"), Some(EffectKind::Live));
        assert_eq!(rt.capability_effect("network"), None);
        assert_eq!(
            rt.capability("+"),
            Some(crate::Capability {
                name: "+".to_owned(),
                effect: EffectKind::Pure,
            })
        );

        let caps = rt.capabilities();
        assert_eq!(caps.len(), 2);
        assert_eq!(caps[0].name, "+");
        assert_eq!(caps[1].name, "clock");

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
    fn pins_persist_across_runtime_instances() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let sum = {
            let mut rt = Runtime::new(&temp)?;
            rt.register("+", HostFn::pure(add_ints));

            let a = rt.int(20)?;
            let b = rt.int(22)?;
            let sum_expr = rt.call("+", vec![a, b])?;
            let sum = rt.thunk(sum_expr)?;
            rt.pin("demo.sum", sum.clone())?;
            sum
        };

        let rt = Runtime::new(&temp)?;
        assert_eq!(rt.resolve_pin("demo.sum")?, Some(sum.clone()));
        assert_eq!(rt.resolve_pin("missing")?, None);
        assert_eq!(rt.pins()?.get("demo.sum"), Some(&sum));

        let removed = rt.unpin("demo.sum")?;
        assert_eq!(removed, Some(sum));
        assert_eq!(rt.resolve_pin("demo.sum")?, None);

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn aliases_persist_and_are_distinct_from_pins() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let (first_sum, second_sum) = {
            let mut rt = Runtime::new(&temp)?;
            rt.register("+", HostFn::pure(add_ints));

            let a = rt.int(20)?;
            let b = rt.int(22)?;
            let first_expr = rt.call("+", vec![a.clone(), b.clone()])?;
            let first_sum = rt.thunk(first_expr)?;

            let c = rt.int(1)?;
            let second_expr = rt.call("+", vec![first_sum.clone(), c])?;
            let second_sum = rt.thunk(second_expr)?;

            rt.alias("demo.sum", first_sum.clone())?;
            assert_eq!(rt.resolve_alias("demo.sum")?, Some(first_sum.clone()));
            assert_eq!(rt.resolve_pin("demo.sum")?, None);

            rt.alias("demo.sum", second_sum.clone())?;
            (first_sum, second_sum)
        };

        let rt = Runtime::new(&temp)?;
        assert_eq!(rt.resolve_alias("demo.sum")?, Some(second_sum.clone()));
        assert_eq!(rt.aliases()?.get("demo.sum"), Some(&second_sum));
        assert_eq!(rt.resolve_pin("demo.sum")?, None);

        let removed = rt.unalias("demo.sum")?;
        assert_eq!(removed.as_ref(), Some(&second_sum));
        assert_eq!(rt.resolve_alias("demo.sum")?, None);
        assert_eq!(rt.resolve_pin("demo.sum")?, None);
        assert_ne!(first_sum, second_sum);

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn reports_store_stats() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let mut rt = Runtime::new(&temp)?;
        rt.register("+", HostFn::pure(add_ints));

        let a = rt.int(20)?;
        let b = rt.int(22)?;
        let sum_expr = rt.call("+", vec![a, b])?;
        let sum = rt.thunk(sum_expr)?;
        rt.force(sum.clone())?;
        rt.pin("demo.sum", sum)?;
        let alias_target = rt.int(7)?;
        rt.alias("demo.seven", alias_target)?;

        let stats = rt.store_stats()?;
        assert!(stats.object_count >= 5);
        assert!(stats.outcome_count >= 1);
        assert_eq!(stats.root_count, 1);
        assert_eq!(stats.alias_count, 1);
        assert!(stats.total_bytes > 0);

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

    #[test]
    fn pure_apply_rejects_live_capability() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let mut rt = Runtime::new(&temp)?;
        rt.register("clock", HostFn::live(|_| Ok(Value::Int(1))));

        let clock = rt.call("clock", vec![])?;
        let forced = rt.force(clock)?;
        let Outcome::Failure(failure) = forced.outcome else {
            panic!("pure apply to live cap should fail");
        };
        assert_eq!(
            failure.kind,
            FailureKind::EffectMismatch {
                capability: "clock".to_owned(),
                requested: EffectKind::Pure,
                actual: EffectKind::Live,
            }
        );
        assert_eq!(rt.get_outcome(&failure.node)?, None);

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn live_host_call_is_not_cached() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let counter = Arc::new(AtomicI64::new(0));
        let counter_for_cap = Arc::clone(&counter);
        let mut rt = Runtime::new(&temp)?;
        rt.register(
            "next",
            HostFn::live(move |_| {
                let next = counter_for_cap.fetch_add(1, Ordering::SeqCst) + 1;
                Ok(Value::Int(next))
            }),
        );

        let next = rt.host_call("next", vec![], EffectKind::Live)?;
        let first = rt.force(next.clone())?;
        assert!(!first.cache_hit);
        let Outcome::Success(first_hash) = first.outcome else {
            panic!("live call should succeed");
        };
        assert_eq!(rt.get_value(&first_hash)?, Value::Int(1));
        assert_eq!(rt.get_outcome(&next)?, None);

        let second = rt.force(next)?;
        assert!(!second.cache_hit);
        let Outcome::Success(second_hash) = second.outcome else {
            panic!("live call should succeed again");
        };
        assert_eq!(rt.get_value(&second_hash)?, Value::Int(2));
        assert_eq!(counter.load(Ordering::SeqCst), 2);

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn host_call_rejects_wrong_declared_effect() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let mut rt = Runtime::new(&temp)?;
        rt.register("+", HostFn::pure(add_ints));

        let a = rt.int(1)?;
        let b = rt.int(2)?;
        let sum = rt.host_call("+", vec![a, b], EffectKind::Live)?;
        let forced = rt.force(sum)?;
        let Outcome::Failure(failure) = forced.outcome else {
            panic!("wrong host call effect should fail");
        };
        assert_eq!(
            failure.kind,
            FailureKind::EffectMismatch {
                capability: "+".to_owned(),
                requested: EffectKind::Live,
                actual: EffectKind::Pure,
            }
        );

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn gc_plan_traces_reachable_graph_from_pins() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let mut rt = Runtime::new(&temp)?;
        rt.register("+", HostFn::pure(add_ints));

        let a = rt.int(20)?;
        let b = rt.int(22)?;
        let sum_expr = rt.call("+", vec![a.clone(), b.clone()])?;
        let sum = rt.thunk(sum_expr.clone())?;
        let forced = rt.force(sum.clone())?;
        let Outcome::Success(value_hash) = forced.outcome else {
            panic!("sum should succeed");
        };
        rt.pin("demo.sum", sum.clone())?;

        let plan = rt.gc_plan()?;
        assert_eq!(plan.roots.get("demo.sum"), Some(&sum));
        assert!(plan.reachable_objects.contains(&sum));
        assert!(plan.reachable_objects.contains(&sum_expr));
        assert!(plan.reachable_objects.contains(&a));
        assert!(plan.reachable_objects.contains(&b));
        assert!(plan.reachable_objects.contains(&value_hash));
        assert!(plan.reachable_outcomes.contains(&sum));
        assert!(!plan.unreachable_objects.contains(&sum));
        assert!(!plan.unreachable_outcomes.contains(&sum));

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn gc_plan_does_not_treat_aliases_as_roots() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let rt = Runtime::new(&temp)?;
        let value = rt.int(7)?;
        rt.alias("demo.seven", value.clone())?;

        let plan = rt.gc_plan()?;
        assert_eq!(plan.roots.get("demo.seven"), None);
        assert!(!plan.reachable_objects.contains(&value));
        assert!(plan.unreachable_objects.contains(&value));

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }
}
