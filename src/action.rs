use crate::Hash;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionSpec {
    pub program: String,
    pub tool: Hash,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub platform: String,
    pub inputs: Vec<ActionInput>,
    pub outputs: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionInput {
    pub path: String,
    pub hash: Hash,
}
