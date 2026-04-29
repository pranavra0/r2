use crate::{Failure, Hash};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    Success(Hash),
    Failure(Failure),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForceResult {
    pub outcome: Outcome,
    pub cache_hit: bool,
}
