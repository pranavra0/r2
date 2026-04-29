use crate::Hash;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    Int(i64),
    Text(String),
    Bytes(Vec<u8>),
    Tuple(Vec<Hash>),
    Artifact(Hash),
}
