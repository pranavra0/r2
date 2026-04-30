use crate::{Failure, Hash};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    Success(Hash),
    Failure(Failure),
}

impl Outcome {
    pub fn is_cacheable(&self) -> bool {
        match self {
            Self::Success(_) => true,
            Self::Failure(failure) => failure.kind.is_cacheable(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForceResult {
    pub outcome: Outcome,
    pub cache_hit: bool,
}
