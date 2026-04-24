use std::fmt;

use crate::{Symbol, Term};

pub const FORCE_OP: &str = "thunk.force";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ThunkError {
    WrongArgumentCount { expected: usize, found: usize },
    NotAThunk,
    WrongArity { expected: usize, found: usize },
    UncacheableResult,
}

impl fmt::Display for ThunkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongArgumentCount { expected, found } => {
                write!(f, "thunk.force expected {expected} argument, found {found}")
            }
            Self::NotAThunk => f.write_str("thunk.force expected a zero-argument closure"),
            Self::WrongArity { expected, found } => {
                write!(f, "thunk expected arity {expected}, found {found}")
            }
            Self::UncacheableResult => f.write_str("thunk result could not be reified for caching"),
        }
    }
}

impl std::error::Error for ThunkError {}

pub fn delay(body: Term) -> Term {
    Term::lambda(
        0,
        Term::Perform {
            op: Symbol::from(FORCE_OP),
            args: vec![Term::lambda(0, body)],
        },
    )
}

pub fn force(thunk: Term) -> Term {
    Term::Apply {
        callee: Box::new(thunk),
        args: Vec::new(),
    }
}
