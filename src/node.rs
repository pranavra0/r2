use crate::{ActionSpec, Hash, Value};
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
    Thunk {
        target: Hash,
    },
    Apply {
        function: String,
        args: Vec<Hash>,
    },
    HostCall {
        capability: String,
        args: Vec<Hash>,
        effect: EffectKind,
    },
    Action(ActionSpec),
}
