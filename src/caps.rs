use crate::{EffectKind, FailureKind, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

pub type HostResult = Result<Value, FailureKind>;
type HostImpl = dyn Fn(&[Value]) -> HostResult + Send + Sync + 'static;

#[derive(Clone)]
pub struct HostFn {
    pub effect: EffectKind,
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
}

#[derive(Clone, Default)]
pub struct CapSet {
    funcs: BTreeMap<String, HostFn>,
}

impl CapSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, name: impl Into<String>, func: HostFn) {
        self.funcs.insert(name.into(), func);
    }

    pub fn get(&self, name: &str) -> Option<&HostFn> {
        self.funcs.get(name)
    }
}
