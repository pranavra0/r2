use std::fmt;

use crate::{Symbol, Term};

pub const FORCE_OP: &str = "thunk.force";
pub const FORCE_ALL_OP: &str = "thunk.force_all";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ThunkError {
    WrongArgumentCount { expected: usize, found: usize },
    NotAThunk,
    WrongArity { expected: usize, found: usize },
    UncacheableResult,
    InvalidCacheEntry,
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
            Self::InvalidCacheEntry => {
                f.write_str("thunk cache entry did not point to a cacheable result")
            }
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

pub fn force_all(thunks: impl IntoIterator<Item = Term>) -> Term {
    Term::Perform {
        op: Symbol::from(FORCE_ALL_OP),
        args: thunks.into_iter().collect(),
    }
}
