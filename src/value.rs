use crate::Hash;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tree {
    pub entries: BTreeMap<String, TreeEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TreeEntry {
    Blob(Hash),
    Tree(Hash),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    Int(i64),
    Text(String),
    Bytes(Vec<u8>),
    Blob(Vec<u8>),
    Tree(Tree),
    Tuple(Vec<Hash>),
    Artifact(Hash),
    ActionResult {
        outputs: Hash,
        stdout: Hash,
        stderr: Hash,
    },
}
