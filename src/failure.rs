use crate::Hash;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum FailureKind {
    #[error("unknown capability: {0}")]
    UnknownCapability(String),
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    #[error("type error: {0}")]
    TypeError(String),
    #[error("missing object: {0}")]
    MissingObject(Hash),
    #[error("host function failed: {0}")]
    Host(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Failure {
    pub node: Hash,
    pub kind: FailureKind,
    pub dependency_path: Vec<Hash>,
}

impl Failure {
    pub fn new(node: Hash, kind: FailureKind, dependency_path: Vec<Hash>) -> Self {
        Self {
            node,
            kind,
            dependency_path,
        }
    }
}
