use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::rc::Rc;

use crate::data::{Lambda, Ref, Symbol, Term, Value, VarIndex};

type Env = Vec<RuntimeValue>;

#[derive(Clone)]
pub enum RuntimeValue {
    Data(Value),
    Closure(Closure),
    Continuation(Continuation),
    Ref(Ref),
}

impl RuntimeValue {
    pub fn as_data(&self) -> Option<&Value> {
        match self {
            Self::Data(value) => Some(value),
            _ => None,
        }
    }

    pub fn reify(&self) -> Option<Reified> {
        match self {
            Self::Data(value) => Some(Reified::Value(value.clone())),
            Self::Closure(closure) => Some(Reified::Lambda(closure.reify()?)),
            Self::Continuation(_) => None,
            Self::Ref(reference) => Some(Reified::Ref(reference.clone())),
        }
    }
}

impl fmt::Debug for RuntimeValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Data(value) => f.debug_tuple("Data").field(value).finish(),
            Self::Closure(closure) => closure.fmt(f),
            Self::Continuation(continuation) => continuation.fmt(f),
            Self::Ref(reference) => f.debug_tuple("Ref").field(reference).finish(),
        }
    }
}

#[derive(Clone)]
pub struct Closure {
    lambda: Lambda,
    env: Env,
}

impl Closure {
    fn new(lambda: Lambda, env: Env) -> Self {
        Self { lambda, env }
    }

    fn reify(&self) -> Option<Lambda> {
        let captured = self
            .env
            .iter()
            .rev()
            .map(|value| value.reify_term())
            .collect::<Option<Vec<_>>>()?;
        let body = close_term(
            &self.lambda.body,
            u32::from(self.lambda.parameters),
            &captured,
        )?;
        Some(Lambda::new(self.lambda.parameters, body))
    }
}

impl fmt::Debug for Closure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Closure")
            .field("parameters", &self.lambda.parameters)
            .field("env_len", &self.env.len())
            .finish()
    }
}

#[derive(Clone)]
pub struct Continuation {
    state: Rc<RefCell<Option<CapturedContinuation>>>,
}

impl Continuation {
    fn new(scopes: Vec<Scope>) -> Self {
        Self {
            state: Rc::new(RefCell::new(Some(CapturedContinuation { scopes }))),
        }
    }

    pub fn resume(&self, value: RuntimeValue) -> Result<EvalResult, EvalError> {
        let captured = self.take()?;
        Machine::resume(captured.scopes, value)
    }

    fn take(&self) -> Result<CapturedContinuation, EvalError> {
        self.state
            .borrow_mut()
            .take()
            .ok_or(EvalError::ContinuationAlreadyResumed)
    }
}

impl fmt::Debug for Continuation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = if self.state.borrow().is_some() {
            "fresh"
        } else {
            "consumed"
        };
        f.debug_struct("Continuation")
            .field("state", &state)
            .finish()
    }
}

#[derive(Clone, Debug)]
struct CapturedContinuation {
    scopes: Vec<Scope>,
}

#[derive(Clone, Debug)]
struct Scope {
    handlers: BTreeMap<Symbol, HandlerBinding>,
    frames: Vec<Frame>,
}

impl Scope {
    fn root() -> Self {
        Self {
            handlers: BTreeMap::new(),
            frames: Vec::new(),
        }
    }
}

#[derive(Clone, Debug)]
struct HandlerBinding {
    term: Term,
    env: Env,
}

#[derive(Clone, Debug)]
enum Frame {
    ApplyCallee {
        args: Vec<Term>,
        env: Env,
    },
    ApplyArgs {
        callee: RuntimeValue,
        args: Vec<Term>,
        next_index: usize,
        evaluated: Vec<RuntimeValue>,
        env: Env,
    },
    ApplyReady {
        args: Vec<RuntimeValue>,
    },
    PerformArgs {
        op: Symbol,
        args: Vec<Term>,
        next_index: usize,
        evaluated: Vec<RuntimeValue>,
        env: Env,
    },
}

#[derive(Clone, Debug)]
enum Control {
    Term(Term, Env),
    Value(RuntimeValue),
}

#[derive(Clone, Debug)]
pub enum EvalResult {
    Done(RuntimeValue),
    Yielded(Yielded),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reified {
    Value(Value),
    Lambda(Lambda),
    Ref(Ref),
}

impl Reified {
    pub fn into_runtime(self) -> RuntimeValue {
        match self {
            Self::Value(value) => RuntimeValue::Data(value),
            Self::Lambda(lambda) => RuntimeValue::Closure(Closure::new(lambda, Vec::new())),
            Self::Ref(reference) => RuntimeValue::Ref(reference),
        }
    }

    pub fn into_term(self) -> Term {
        match self {
            Self::Value(value) => Term::Value(value),
            Self::Lambda(lambda) => Term::Lambda(lambda),
            Self::Ref(reference) => Term::Ref(reference),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Yielded {
    pub op: Symbol,
    pub args: Vec<RuntimeValue>,
    pub continuation: Continuation,
}

#[derive(Clone, Debug)]
pub enum EvalError {
    UnboundVariable { index: VarIndex, env_len: usize },
    NonCallable(RuntimeValue),
    WrongArity { expected: usize, found: usize },
    ContinuationAlreadyResumed,
    EmptyScopeStack,
}

impl fmt::Display for EvalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnboundVariable { index, env_len } => {
                write!(
                    f,
                    "unbound variable {} in environment of size {}",
                    index.get(),
                    env_len
                )
            }
            Self::NonCallable(value) => {
                write!(f, "attempted to apply non-callable value {value:?}")
            }
            Self::WrongArity { expected, found } => {
                write!(f, "wrong arity: expected {expected}, found {found}")
            }
            Self::ContinuationAlreadyResumed => {
                f.write_str("continuations are one-shot and may only be resumed once")
            }
            Self::EmptyScopeStack => {
                f.write_str("evaluator reached an impossible empty scope stack")
            }
        }
    }
}

impl std::error::Error for EvalError {}

pub fn eval(term: Term) -> Result<EvalResult, EvalError> {
    Machine::start(term).run()
}

#[derive(Clone, Debug)]
struct Machine {
    control: Control,
    scopes: Vec<Scope>,
}

impl Machine {
    fn start(term: Term) -> Self {
        Self {
            control: Control::Term(term, Vec::new()),
            scopes: vec![Scope::root()],
        }
    }

    fn resume(scopes: Vec<Scope>, value: RuntimeValue) -> Result<EvalResult, EvalError> {
        Self {
            control: Control::Value(value),
            scopes,
        }
        .run()
    }

    fn run(mut self) -> Result<EvalResult, EvalError> {
        loop {
            match self.control.clone() {
                Control::Term(term, env) => {
                    if let Some(result) = self.step_term(term, env)? {
                        return Ok(result);
                    }
                }
                Control::Value(value) => {
                    if let Some(frame) = self.pop_frame()? {
                        if let Some(result) = self.step_frame(frame, value)? {
                            return Ok(result);
                        }
                    } else if self.scopes.len() > 1 {
                        self.scopes.pop();
                        self.control = Control::Value(value);
                    } else {
                        return Ok(EvalResult::Done(value));
                    }
                }
            }
        }
    }

    fn step_term(&mut self, term: Term, env: Env) -> Result<Option<EvalResult>, EvalError> {
        match term {
            Term::Var(index) => {
                let value = lookup_var(&env, index)?;
                self.control = Control::Value(value);
            }
            Term::Value(value) => {
                self.control = Control::Value(RuntimeValue::Data(value));
            }
            Term::Lambda(lambda) => {
                self.control = Control::Value(RuntimeValue::Closure(Closure::new(lambda, env)));
            }
            Term::Apply { callee, args } => {
                self.push_frame(Frame::ApplyCallee {
                    args,
                    env: env.clone(),
                })?;
                self.control = Control::Term(*callee, env);
            }
            Term::Perform { op, args } => {
                if args.is_empty() {
                    return self.handle_perform(op, Vec::new());
                } else {
                    self.push_frame(Frame::PerformArgs {
                        op,
                        args: args.clone(),
                        next_index: 1,
                        evaluated: Vec::new(),
                        env: env.clone(),
                    })?;
                    self.control = Control::Term(args[0].clone(), env);
                }
            }
            Term::Handle { body, handlers } => {
                let handlers = handlers
                    .into_iter()
                    .map(|(op, term)| {
                        (
                            op,
                            HandlerBinding {
                                term,
                                env: env.clone(),
                            },
                        )
                    })
                    .collect();

                self.scopes.push(Scope {
                    handlers,
                    frames: Vec::new(),
                });
                self.control = Control::Term(*body, env);
            }
            Term::Ref(reference) => {
                self.control = Control::Value(RuntimeValue::Ref(reference));
            }
        }

        Ok(None)
    }

    fn step_frame(
        &mut self,
        frame: Frame,
        value: RuntimeValue,
    ) -> Result<Option<EvalResult>, EvalError> {
        match frame {
            Frame::ApplyCallee { args, env } => {
                if args.is_empty() {
                    self.apply_callable(value, Vec::new())?;
                } else {
                    self.push_frame(Frame::ApplyArgs {
                        callee: value,
                        args: args.clone(),
                        next_index: 1,
                        evaluated: Vec::new(),
                        env: env.clone(),
                    })?;
                    self.control = Control::Term(args[0].clone(), env);
                }
            }
            Frame::ApplyArgs {
                callee,
                args,
                next_index,
                mut evaluated,
                env,
            } => {
                evaluated.push(value);

                if next_index < args.len() {
                    self.push_frame(Frame::ApplyArgs {
                        callee,
                        args: args.clone(),
                        next_index: next_index + 1,
                        evaluated,
                        env: env.clone(),
                    })?;
                    self.control = Control::Term(args[next_index].clone(), env);
                } else {
                    self.apply_callable(callee, evaluated)?;
                }
            }
            Frame::ApplyReady { args } => {
                self.apply_callable(value, args)?;
            }
            Frame::PerformArgs {
                op,
                args,
                next_index,
                mut evaluated,
                env,
            } => {
                evaluated.push(value);

                if next_index < args.len() {
                    self.push_frame(Frame::PerformArgs {
                        op,
                        args: args.clone(),
                        next_index: next_index + 1,
                        evaluated,
                        env: env.clone(),
                    })?;
                    self.control = Control::Term(args[next_index].clone(), env);
                } else {
                    return self.handle_perform(op, evaluated);
                }
            }
        }

        Ok(None)
    }

    fn apply_callable(
        &mut self,
        callee: RuntimeValue,
        args: Vec<RuntimeValue>,
    ) -> Result<(), EvalError> {
        match callee {
            RuntimeValue::Closure(closure) => {
                let expected = usize::from(closure.lambda.parameters);
                let found = args.len();
                if expected != found {
                    return Err(EvalError::WrongArity { expected, found });
                }

                let mut env = closure.env;
                env.extend(args);
                self.control = Control::Term(*closure.lambda.body, env);
                Ok(())
            }
            RuntimeValue::Continuation(continuation) => {
                if args.len() != 1 {
                    return Err(EvalError::WrongArity {
                        expected: 1,
                        found: args.len(),
                    });
                }

                let captured = continuation.take()?;
                self.scopes.extend(captured.scopes);
                self.control = Control::Value(args.into_iter().next().expect("checked len"));
                Ok(())
            }
            RuntimeValue::Data(_) | RuntimeValue::Ref(_) => Err(EvalError::NonCallable(callee)),
        }
    }

    fn handle_perform(
        &mut self,
        op: Symbol,
        args: Vec<RuntimeValue>,
    ) -> Result<Option<EvalResult>, EvalError> {
        if let Some(handler_index) = self.find_handler(&op) {
            let handler = self.scopes[handler_index]
                .handlers
                .get(&op)
                .cloned()
                .expect("find_handler only returns installed handlers");

            let continuation = Continuation::new(self.capture_scopes(handler_index)?);
            self.scopes.truncate(handler_index + 1);
            self.current_scope_mut()?.frames.clear();

            let mut handler_args = args;
            handler_args.push(RuntimeValue::Continuation(continuation));

            self.push_frame(Frame::ApplyReady { args: handler_args })?;
            self.control = Control::Term(handler.term, handler.env);
            Ok(None)
        } else {
            let continuation = Continuation::new(self.capture_scopes(0)?);
            Ok(Some(EvalResult::Yielded(Yielded {
                op,
                args,
                continuation,
            })))
        }
    }

    fn find_handler(&self, op: &Symbol) -> Option<usize> {
        self.scopes
            .iter()
            .enumerate()
            .rev()
            .find_map(|(index, scope)| scope.handlers.contains_key(op).then_some(index))
    }

    fn capture_scopes(&self, from_index: usize) -> Result<Vec<Scope>, EvalError> {
        if self.scopes.is_empty() || from_index >= self.scopes.len() {
            return Err(EvalError::EmptyScopeStack);
        }

        Ok(self.scopes[from_index..].to_vec())
    }

    fn push_frame(&mut self, frame: Frame) -> Result<(), EvalError> {
        self.current_scope_mut()?.frames.push(frame);
        Ok(())
    }

    fn pop_frame(&mut self) -> Result<Option<Frame>, EvalError> {
        Ok(self.current_scope_mut()?.frames.pop())
    }

    fn current_scope_mut(&mut self) -> Result<&mut Scope, EvalError> {
        self.scopes.last_mut().ok_or(EvalError::EmptyScopeStack)
    }
}

fn lookup_var(env: &Env, index: VarIndex) -> Result<RuntimeValue, EvalError> {
    let offset =
        usize::try_from(index.get()).expect("u32 always fits into usize on supported targets");
    let env_index = env
        .len()
        .checked_sub(offset + 1)
        .ok_or(EvalError::UnboundVariable {
            index,
            env_len: env.len(),
        })?;

    Ok(env[env_index].clone())
}

fn close_term(term: &Term, depth: u32, captured: &[Term]) -> Option<Term> {
    match term {
        Term::Var(index) => {
            if index.get() < depth {
                Some(Term::Var(*index))
            } else {
                let capture_index = usize::try_from(index.get() - depth)
                    .expect("u32 always fits into usize on supported targets");
                captured.get(capture_index).cloned()
            }
        }
        Term::Value(value) => Some(Term::Value(value.clone())),
        Term::Lambda(lambda) => Some(Term::Lambda(Lambda::new(
            lambda.parameters,
            close_term(
                &lambda.body,
                depth.saturating_add(u32::from(lambda.parameters)),
                captured,
            )?,
        ))),
        Term::Apply { callee, args } => Some(Term::Apply {
            callee: Box::new(close_term(callee, depth, captured)?),
            args: args
                .iter()
                .map(|arg| close_term(arg, depth, captured))
                .collect::<Option<Vec<_>>>()?,
        }),
        Term::Perform { op, args } => Some(Term::Perform {
            op: op.clone(),
            args: args
                .iter()
                .map(|arg| close_term(arg, depth, captured))
                .collect::<Option<Vec<_>>>()?,
        }),
        Term::Handle { body, handlers } => Some(Term::Handle {
            body: Box::new(close_term(body, depth, captured)?),
            handlers: handlers
                .iter()
                .map(|(op, handler)| Some((op.clone(), close_term(handler, depth, captured)?)))
                .collect::<Option<BTreeMap<_, _>>>()?,
        }),
        Term::Ref(reference) => Some(Term::Ref(reference.clone())),
    }
}

impl RuntimeValue {
    fn reify_term(&self) -> Option<Term> {
        Some(self.reify()?.into_term())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::Symbol;

    fn data(value: i64) -> RuntimeValue {
        RuntimeValue::Data(Value::Integer(value))
    }

    #[test]
    fn applies_identity_lambda() {
        let term = Term::Apply {
            callee: Box::new(Term::lambda(1, Term::var(0))),
            args: vec![Term::Value(Value::Integer(7))],
        };

        let result = eval(term).expect("evaluation should succeed");

        match result {
            EvalResult::Done(RuntimeValue::Data(Value::Integer(value))) => assert_eq!(value, 7),
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn closures_capture_outer_environment() {
        let term = Term::Apply {
            callee: Box::new(Term::Apply {
                callee: Box::new(Term::lambda(1, Term::lambda(1, Term::var(1)))),
                args: vec![Term::Value(Value::Integer(11))],
            }),
            args: vec![Term::Value(Value::Integer(99))],
        };

        let result = eval(term).expect("evaluation should succeed");

        match result {
            EvalResult::Done(RuntimeValue::Data(Value::Integer(value))) => assert_eq!(value, 11),
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn unhandled_perform_yields_and_can_resume() {
        let term = Term::Apply {
            callee: Box::new(Term::lambda(1, Term::var(0))),
            args: vec![Term::Perform {
                op: Symbol::from("fs.read"),
                args: vec![Term::Value(Value::Symbol(Symbol::from("config.toml")))],
            }],
        };

        let yielded = match eval(term).expect("evaluation should succeed") {
            EvalResult::Yielded(yielded) => yielded,
            other => panic!("expected yielded operation, got {other:?}"),
        };

        assert_eq!(yielded.op, Symbol::from("fs.read"));
        assert_eq!(
            yielded.args[0].as_data(),
            Some(&Value::Symbol(Symbol::from("config.toml")))
        );

        let resumed = yielded
            .continuation
            .resume(RuntimeValue::Data(Value::Bytes(b"hello".to_vec())))
            .expect("resumption should succeed");

        match resumed {
            EvalResult::Done(RuntimeValue::Data(Value::Bytes(bytes))) => {
                assert_eq!(bytes, b"hello")
            }
            other => panic!("unexpected resumed result: {other:?}"),
        }
    }

    #[test]
    fn handlers_resume_their_captured_continuations() {
        let mut handlers = BTreeMap::new();
        handlers.insert(
            Symbol::from("ask"),
            Term::lambda(
                2,
                Term::Apply {
                    callee: Box::new(Term::var(0)),
                    args: vec![Term::var(1)],
                },
            ),
        );

        let term = Term::Handle {
            body: Box::new(Term::Apply {
                callee: Box::new(Term::lambda(1, Term::var(0))),
                args: vec![Term::Perform {
                    op: Symbol::from("ask"),
                    args: vec![Term::Value(Value::Integer(41))],
                }],
            }),
            handlers,
        };

        let result = eval(term).expect("evaluation should succeed");

        match result {
            EvalResult::Done(RuntimeValue::Data(Value::Integer(value))) => assert_eq!(value, 41),
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn handler_bindings_capture_lexical_environment() {
        let mut handlers = BTreeMap::new();
        handlers.insert(
            Symbol::from("ask"),
            Term::lambda(
                1,
                Term::Apply {
                    callee: Box::new(Term::var(0)),
                    args: vec![Term::var(1)],
                },
            ),
        );

        let term = Term::Apply {
            callee: Box::new(Term::lambda(
                1,
                Term::Handle {
                    body: Box::new(Term::Perform {
                        op: Symbol::from("ask"),
                        args: Vec::new(),
                    }),
                    handlers,
                },
            )),
            args: vec![Term::Value(Value::Integer(9))],
        };

        let result = eval(term).expect("evaluation should succeed");

        match result {
            EvalResult::Done(RuntimeValue::Data(Value::Integer(value))) => assert_eq!(value, 9),
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn continuations_are_one_shot() {
        let yielded = match eval(Term::Perform {
            op: Symbol::from("yield"),
            args: Vec::new(),
        })
        .expect("evaluation should succeed")
        {
            EvalResult::Yielded(yielded) => yielded,
            other => panic!("expected yielded operation, got {other:?}"),
        };

        let first = yielded
            .continuation
            .resume(data(1))
            .expect("first resumption should succeed");

        match first {
            EvalResult::Done(RuntimeValue::Data(Value::Integer(value))) => assert_eq!(value, 1),
            other => panic!("unexpected resumed result: {other:?}"),
        }

        let second = yielded.continuation.resume(data(2));
        assert!(matches!(second, Err(EvalError::ContinuationAlreadyResumed)));
    }

    #[test]
    fn reifies_closed_closures() {
        let result = eval(Term::Apply {
            callee: Box::new(Term::lambda(1, Term::lambda(1, Term::var(1)))),
            args: vec![Term::Value(Value::Integer(11))],
        })
        .expect("evaluation should succeed");

        let value = match result {
            EvalResult::Done(value) => value,
            other => panic!("expected final value, got {other:?}"),
        };

        let reified = value.reify().expect("closure should be reifiable");
        assert_eq!(
            reified,
            Reified::Lambda(Lambda::new(1, Term::Value(Value::Integer(11))))
        );
    }
}
