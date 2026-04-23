use std::collections::BTreeMap;
use std::fmt;

use crate::Canonical;
use crate::data::{Digest, Ref, Symbol, Term, Value};
use crate::eval::{EvalError, EvalResult, Reified, RuntimeValue, Yielded, eval};
use crate::host::{HostError, HostHandler};
use crate::store::{MemoryStore, ObjectStore, StoreError, Stored};
use crate::thunk::{self, ThunkError};

#[derive(Debug)]
pub enum RuntimeError {
    Eval(EvalError),
    Store(StoreError),
    Host(HostError),
    Thunk(ThunkError),
    UnhandledEffect { op: Symbol },
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Eval(error) => error.fmt(f),
            Self::Store(error) => error.fmt(f),
            Self::Host(error) => error.fmt(f),
            Self::Thunk(error) => error.fmt(f),
            Self::UnhandledEffect { op } => write!(f, "unhandled effect {op}"),
        }
    }
}

impl std::error::Error for RuntimeError {}

impl From<EvalError> for RuntimeError {
    fn from(value: EvalError) -> Self {
        Self::Eval(value)
    }
}

impl From<StoreError> for RuntimeError {
    fn from(value: StoreError) -> Self {
        Self::Store(value)
    }
}

impl From<HostError> for RuntimeError {
    fn from(value: HostError) -> Self {
        Self::Host(value)
    }
}

impl From<ThunkError> for RuntimeError {
    fn from(value: ThunkError) -> Self {
        Self::Thunk(value)
    }
}

#[derive(Clone, Debug)]
pub struct Runtime<S = MemoryStore> {
    store: S,
    memo: BTreeMap<Digest, Reified>,
    thunk_cache: BTreeMap<Digest, Reified>,
}

impl Runtime<MemoryStore> {
    pub fn new() -> Self {
        Self::with_store(MemoryStore::new())
    }
}

impl Default for Runtime<MemoryStore> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Runtime<S> {
    pub fn with_store(store: S) -> Self {
        Self {
            store,
            memo: BTreeMap::new(),
            thunk_cache: BTreeMap::new(),
        }
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }

    pub fn memo_len(&self) -> usize {
        self.memo.len()
    }

    pub fn thunk_cache_len(&self) -> usize {
        self.thunk_cache.len()
    }
}

impl<S: ObjectStore> Runtime<S> {
    pub fn intern_term(&mut self, term: Term) -> Result<Ref, RuntimeError> {
        self.store.put(Stored::term(term)).map_err(Into::into)
    }

    pub fn intern_value(&mut self, value: Value) -> Result<Ref, RuntimeError> {
        self.store.put(Stored::value(value)).map_err(Into::into)
    }

    pub fn load(&self, reference: &Ref) -> Result<Stored, RuntimeError> {
        self.store.load(reference).map_err(Into::into)
    }

    pub fn eval(&mut self, term: Term) -> Result<EvalResult, RuntimeError> {
        let digest = term.is_closed().then(|| term.digest());

        if let Some(reified) = digest.and_then(|digest| self.memo.get(&digest).cloned()) {
            return Ok(EvalResult::Done(reified.into_runtime()));
        }

        let result = eval(term.clone())?;

        if let (Some(digest), EvalResult::Done(value)) = (&digest, &result)
            && let Some(reified) = value.reify()
        {
            self.memo.insert(*digest, reified);
        }

        Ok(result)
    }

    pub fn eval_ref(&mut self, reference: &Ref) -> Result<EvalResult, RuntimeError> {
        match self.load(reference)? {
            Stored::Term(term) => self.eval(term),
            Stored::Value(value) => Ok(EvalResult::Done(RuntimeValue::Data(value))),
        }
    }

    pub fn run<H: HostHandler>(
        &mut self,
        term: Term,
        host: &mut H,
    ) -> Result<RuntimeValue, RuntimeError> {
        let result = self.eval(term)?;
        self.drive(result, host)
    }

    pub fn run_ref<H: HostHandler>(
        &mut self,
        reference: &Ref,
        host: &mut H,
    ) -> Result<RuntimeValue, RuntimeError> {
        let result = self.eval_ref(reference)?;
        self.drive(result, host)
    }

    fn drive<H: HostHandler>(
        &mut self,
        mut result: EvalResult,
        host: &mut H,
    ) -> Result<RuntimeValue, RuntimeError> {
        loop {
            match result {
                EvalResult::Done(value) => return Ok(value),
                EvalResult::Yielded(yielded) => {
                    if let Some(next) = self.handle_builtin(yielded.clone(), host)? {
                        result = next;
                    } else if let Some(next) =
                        host.handle(&yielded.op, yielded.args, yielded.continuation)?
                    {
                        result = next;
                    } else {
                        return Err(RuntimeError::UnhandledEffect { op: yielded.op });
                    }
                }
            }
        }
    }

    fn handle_builtin<H: HostHandler>(
        &mut self,
        yielded: Yielded,
        host: &mut H,
    ) -> Result<Option<EvalResult>, RuntimeError> {
        if yielded.op.as_str() == thunk::FORCE_OP {
            return Ok(Some(self.handle_thunk_force(yielded, host)?));
        }

        Ok(None)
    }

    fn handle_thunk_force<H: HostHandler>(
        &mut self,
        yielded: Yielded,
        host: &mut H,
    ) -> Result<EvalResult, RuntimeError> {
        let value = self.force_thunk_args(yielded.args, host)?;
        yielded.continuation.resume(value).map_err(Into::into)
    }

    fn force_thunk_args<H: HostHandler>(
        &mut self,
        args: Vec<RuntimeValue>,
        host: &mut H,
    ) -> Result<RuntimeValue, RuntimeError> {
        let mut args = args.into_iter();
        let thunk = args.next().ok_or(ThunkError::WrongArgumentCount {
            expected: 1,
            found: 0,
        })?;

        if args.next().is_some() {
            return Err(ThunkError::WrongArgumentCount {
                expected: 1,
                found: 2,
            }
            .into());
        }

        self.force_thunk(thunk, host)
    }

    fn force_thunk<H: HostHandler>(
        &mut self,
        thunk_value: RuntimeValue,
        host: &mut H,
    ) -> Result<RuntimeValue, RuntimeError> {
        let (key, term) = reified_thunk_term(&thunk_value)?;

        if let Some(reified) = self.thunk_cache.get(&key).cloned() {
            return Ok(reified.into_runtime());
        }

        let value = self.run(term, host)?;
        let reified = value.reify().ok_or(ThunkError::UncacheableResult)?.clone();

        self.persist_reified(&reified)?;
        self.thunk_cache.insert(key, reified.clone());
        Ok(reified.into_runtime())
    }

    fn persist_reified(&mut self, reified: &Reified) -> Result<(), RuntimeError> {
        match reified {
            Reified::Value(value) => {
                self.intern_value(value.clone())?;
            }
            Reified::Lambda(lambda) => {
                self.intern_term(Term::Lambda(lambda.clone()))?;
            }
            Reified::Ref(_) => {}
        }

        Ok(())
    }
}

fn reified_thunk_term(thunk_value: &RuntimeValue) -> Result<(Digest, Term), ThunkError> {
    match thunk_value.reify() {
        Some(Reified::Lambda(lambda)) => {
            if lambda.parameters != 0 {
                return Err(ThunkError::WrongArity {
                    expected: 0,
                    found: usize::from(lambda.parameters),
                });
            }

            let term = Term::Apply {
                callee: Box::new(Term::Lambda(lambda)),
                args: Vec::new(),
            };
            let key = term.digest();
            Ok((key, term))
        }
        Some(Reified::Value(_)) | Some(Reified::Ref(_)) | None => Err(ThunkError::NotAThunk),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use super::*;
    use crate::host::Host;

    fn counting_host(counter: Rc<RefCell<usize>>) -> Host {
        let mut host = Host::new();
        host.register("count.tick", move |_args, continuation| {
            *counter.borrow_mut() += 1;
            continuation
                .resume(RuntimeValue::Data(Value::Integer(1)))
                .map_err(Into::into)
        });
        host
    }

    #[test]
    fn evaluates_stored_terms_by_ref() {
        let mut runtime = Runtime::new();
        let reference = runtime
            .intern_term(Term::Apply {
                callee: Box::new(Term::lambda(1, Term::var(0))),
                args: vec![Term::Value(Value::Integer(7))],
            })
            .expect("term should store");

        let result = runtime.eval_ref(&reference).expect("ref should evaluate");

        match result {
            EvalResult::Done(RuntimeValue::Data(Value::Integer(value))) => assert_eq!(value, 7),
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn evaluates_stored_values_by_ref() {
        let mut runtime = Runtime::new();
        let reference = runtime
            .intern_value(Value::Symbol(Symbol::from("ok")))
            .expect("value should store");

        let result = runtime.eval_ref(&reference).expect("ref should evaluate");

        match result {
            EvalResult::Done(RuntimeValue::Data(Value::Symbol(symbol))) => {
                assert_eq!(symbol, Symbol::from("ok"))
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn memoizes_closed_pure_results() {
        let mut runtime = Runtime::new();
        let term = Term::Apply {
            callee: Box::new(Term::lambda(1, Term::var(0))),
            args: vec![Term::Value(Value::Integer(9))],
        };

        let first = runtime
            .eval(term.clone())
            .expect("first eval should succeed");
        let second = runtime.eval(term).expect("second eval should succeed");

        assert_eq!(runtime.memo_len(), 1);
        assert!(matches!(
            first,
            EvalResult::Done(RuntimeValue::Data(Value::Integer(9)))
        ));
        assert!(matches!(
            second,
            EvalResult::Done(RuntimeValue::Data(Value::Integer(9)))
        ));
    }

    #[test]
    fn memoizes_closed_closure_results() {
        let mut runtime = Runtime::new();
        let term = Term::Apply {
            callee: Box::new(Term::lambda(1, Term::lambda(1, Term::var(1)))),
            args: vec![Term::Value(Value::Integer(4))],
        };

        let first = runtime
            .eval(term.clone())
            .expect("first eval should succeed");
        let second = runtime.eval(term).expect("second eval should succeed");

        assert_eq!(runtime.memo_len(), 1);
        assert!(matches!(first, EvalResult::Done(RuntimeValue::Closure(_))));
        assert!(matches!(second, EvalResult::Done(RuntimeValue::Closure(_))));
    }

    #[test]
    fn yielded_effects_are_not_memoized() {
        let mut runtime = Runtime::new();
        let term = Term::Perform {
            op: Symbol::from("fs.read"),
            args: vec![Term::Value(Value::Symbol(Symbol::from("x")))],
        };

        let result = runtime.eval(term).expect("evaluation should succeed");

        assert!(matches!(result, EvalResult::Yielded(_)));
        assert_eq!(runtime.memo_len(), 0);
    }

    #[test]
    fn thunk_force_computes_once_and_then_uses_cache() {
        let counter = Rc::new(RefCell::new(0));
        let mut host = counting_host(counter.clone());
        let mut runtime = Runtime::new();
        let thunk = thunk::delay(Term::Perform {
            op: Symbol::from("count.tick"),
            args: Vec::new(),
        });
        let program = Term::Apply {
            callee: Box::new(Term::lambda(
                1,
                Term::Apply {
                    callee: Box::new(Term::lambda(1, thunk::force(Term::var(1)))),
                    args: vec![thunk::force(Term::var(0))],
                },
            )),
            args: vec![thunk],
        };

        let result = runtime.run(program, &mut host).expect("program should run");

        match result {
            RuntimeValue::Data(Value::Integer(value)) => assert_eq!(value, 1),
            other => panic!("unexpected result: {other:?}"),
        }
        assert_eq!(*counter.borrow(), 1);
        assert_eq!(runtime.thunk_cache_len(), 1);
    }

    #[test]
    fn nested_thunks_share_their_inner_cache() {
        let counter = Rc::new(RefCell::new(0));
        let mut host = counting_host(counter.clone());
        let mut runtime = Runtime::new();
        let program = thunk::force(thunk::delay(Term::Apply {
            callee: Box::new(Term::lambda(
                1,
                Term::Apply {
                    callee: Box::new(Term::lambda(1, thunk::force(Term::var(1)))),
                    args: vec![thunk::force(Term::var(0))],
                },
            )),
            args: vec![thunk::delay(Term::Perform {
                op: Symbol::from("count.tick"),
                args: Vec::new(),
            })],
        }));

        let result = runtime.run(program, &mut host).expect("program should run");

        match result {
            RuntimeValue::Data(Value::Integer(value)) => assert_eq!(value, 1),
            other => panic!("unexpected result: {other:?}"),
        }
        assert_eq!(*counter.borrow(), 1);
        assert_eq!(runtime.thunk_cache_len(), 2);
    }
}
