use crate::{Hash, Value};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EffectKind {
    Pure,
    Hermetic,
    Live,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Node {
    Value(Value),
    Apply {
        function: String,
        args: Vec<Hash>,
    },
    HostCall {
        capability: String,
        args: Vec<Hash>,
        effect: EffectKind,
    },
}
