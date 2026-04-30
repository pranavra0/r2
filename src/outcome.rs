use crate::{CellId, Failure, Hash};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    Success(Hash),
    Failure(Failure),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachedOutcome {
    pub outcome: Outcome,
    pub observed_cells: BTreeMap<CellId, u64>,
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
