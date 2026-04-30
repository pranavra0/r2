use crate::{EffectKind, GraphTrace, Hash};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum FailureKind {
    #[error("unknown capability: {0}")]
    UnknownCapability(String),
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    #[error("effect mismatch for {capability}: requested {requested:?}, capability is {actual:?}")]
    EffectMismatch {
        capability: String,
        requested: EffectKind,
        actual: EffectKind,
    },
    #[error("type error: {0}")]
    TypeError(String),
    #[error("missing object: {0}")]
    MissingObject(Hash),
    #[error("cycle while forcing: {0}")]
    Cycle(Hash),
    #[error("host function failed: {0}")]
    Host(String),
    #[error("action failed: {program} exited with {status}")]
    ActionFailed {
        program: String,
        status: String,
        stdout: String,
        stderr: String,
    },
    #[error("action output missing: {0}")]
    MissingActionOutput(String),
}

impl FailureKind {
    pub fn is_cacheable(&self) -> bool {
        !matches!(
            self,
            Self::UnknownCapability(_) | Self::PermissionDenied(_) | Self::EffectMismatch { .. }
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Failure {
    pub node: Hash,
    pub kind: FailureKind,
    pub trace: GraphTrace,
}

impl Failure {
    pub fn new(node: Hash, kind: FailureKind, trace: GraphTrace) -> Self {
        Self { node, kind, trace }
    }
}
