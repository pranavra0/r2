use crate::{EffectKind, FailureKind, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

pub type HostResult = Result<Value, FailureKind>;
type HostImpl = dyn Fn(&[Value]) -> HostResult + Send + Sync + 'static;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Capability {
    pub name: String,
    pub effect: EffectKind,
}

#[derive(Clone)]
pub struct HostFn {
    effect: EffectKind,
    func: Arc<HostImpl>,
}

impl HostFn {
    pub fn pure(func: impl Fn(&[Value]) -> HostResult + Send + Sync + 'static) -> Self {
        Self {
            effect: EffectKind::Pure,
            func: Arc::new(func),
        }
    }

    pub fn hermetic(func: impl Fn(&[Value]) -> HostResult + Send + Sync + 'static) -> Self {
        Self {
            effect: EffectKind::Hermetic,
            func: Arc::new(func),
        }
    }

    pub fn live(func: impl Fn(&[Value]) -> HostResult + Send + Sync + 'static) -> Self {
        Self {
            effect: EffectKind::Live,
            func: Arc::new(func),
        }
    }

    pub fn call(&self, args: &[Value]) -> HostResult {
        (self.func)(args)
    }

    pub fn effect(&self) -> EffectKind {
        self.effect.clone()
    }
}

#[derive(Clone, Default)]
pub struct CapSet {
    funcs: BTreeMap<String, HostFn>,
}

impl CapSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, name: impl Into<String>, func: HostFn) -> Option<HostFn> {
        self.funcs.insert(name.into(), func)
    }

    pub fn get(&self, name: &str) -> Option<&HostFn> {
        self.funcs.get(name)
    }

    pub fn contains(&self, name: &str) -> bool {
        self.funcs.contains_key(name)
    }

    pub fn effect(&self, name: &str) -> Option<EffectKind> {
        self.funcs.get(name).map(HostFn::effect)
    }

    pub fn capability(&self, name: &str) -> Option<Capability> {
        self.funcs.get(name).map(|func| Capability {
            name: name.to_owned(),
            effect: func.effect(),
        })
    }

    pub fn capabilities(&self) -> Vec<Capability> {
        self.funcs
            .iter()
            .map(|(name, func)| Capability {
                name: name.clone(),
                effect: func.effect(),
            })
            .collect()
    }
}
