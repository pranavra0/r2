pub mod data;
pub mod eval;

pub use data::{Canonical, Digest, Lambda, Ref, Symbol, Term, Value, VarIndex};
pub use eval::{Continuation, EvalError, EvalResult, RuntimeValue, Yielded, eval};
