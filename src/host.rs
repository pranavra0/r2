use std::collections::BTreeMap;
use std::fmt;
use std::io::Write;
use std::io::{self, ErrorKind};
use std::path::PathBuf;
use std::process::{Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::effects::process;
use crate::{
    CancellationToken, Canonical, Continuation, Digest, EvalError, EvalResult, RuntimeValue,
    Symbol, Value,
};

type Handler = dyn FnMut(Vec<RuntimeValue>, Continuation) -> Result<EvalResult, HostError> + Send;
pub type ConcurrentHandler =
    dyn Fn(Vec<RuntimeValue>, Continuation) -> Result<EvalResult, HostError> + Send + Sync;
type ProcessCache = Arc<Mutex<BTreeMap<Digest, Value>>>;
type HostTraceEvents = Arc<Mutex<Vec<HostTraceEvent>>>;

const FS_READ_OP: &str = "fs.read";
const FS_WRITE_OP: &str = "fs.write";
const PROCESS_SPAWN_OP: &str = "process.spawn";
const SERVICE_SUPERVISE_OP: &str = "service.supervise";
const CLOCK_NOW_OP: &str = "clock.now";
const CLOCK_SLEEP_OP: &str = "clock.sleep";
const MATH_ADD_OP: &str = "math.add";
const MATH_SUB_OP: &str = "math.sub";
const MATH_MUL_OP: &str = "math.mul";
const MATH_DIV_OP: &str = "math.div";
const MATH_REM_OP: &str = "math.rem";
const MATH_EQ_OP: &str = "math.eq";
const MATH_NE_OP: &str = "math.ne";
const MATH_LT_OP: &str = "math.lt";
const MATH_LE_OP: &str = "math.le";
const MATH_GT_OP: &str = "math.gt";
const MATH_GE_OP: &str = "math.ge";
const ENV_MODE_CLEAR: &str = "clear";
const ENV_MODE_INHERIT: &str = "inherit";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostEffectCaching {
    Deny,
    Allow,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostEffectProvenance {
    Ambient,
    Declared,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HostEffectPolicy {
    caching: HostEffectCaching,
    provenance: HostEffectProvenance,
    sandbox: bool,
}

impl HostEffectPolicy {
    pub const fn new(caching: HostEffectCaching, provenance: HostEffectProvenance) -> Self {
        Self::with_sandbox(caching, provenance, false)
    }

    pub const fn with_sandbox(
        caching: HostEffectCaching,
        provenance: HostEffectProvenance,
        sandbox: bool,
    ) -> Self {
        Self {
            caching,
            provenance,
            sandbox,
        }
    }

    pub const fn volatile() -> Self {
        Self::new(HostEffectCaching::Deny, HostEffectProvenance::Ambient)
    }

    pub const fn stable() -> Self {
        Self::new(HostEffectCaching::Allow, HostEffectProvenance::Ambient)
    }

    pub const fn declared() -> Self {
        Self::new(HostEffectCaching::Deny, HostEffectProvenance::Declared)
    }

    pub const fn hermetic() -> Self {
        Self::with_sandbox(
            HostEffectCaching::Allow,
            HostEffectProvenance::Declared,
            true,
        )
    }

    pub const fn caching(self) -> HostEffectCaching {
        self.caching
    }

    pub const fn provenance(self) -> HostEffectProvenance {
        self.provenance
    }

    pub const fn allows_thunk_cache(self) -> bool {
        matches!(self.caching, HostEffectCaching::Allow)
    }

    pub const fn sandbox(self) -> bool {
        self.sandbox
    }
}

impl Default for HostEffectPolicy {
    fn default() -> Self {
        Self::volatile()
    }
}

impl fmt::Display for HostEffectPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.caching, self.provenance) {
            (HostEffectCaching::Deny, HostEffectProvenance::Ambient) => f.write_str("volatile"),
            (HostEffectCaching::Allow, HostEffectProvenance::Ambient) => f.write_str("stable"),
            (HostEffectCaching::Deny, HostEffectProvenance::Declared) => f.write_str("declared"),
            (HostEffectCaching::Allow, HostEffectProvenance::Declared) => f.write_str("hermetic"),
        }
    }
}

pub trait HostHandler {
    fn handle(
        &mut self,
        op: &Symbol,
        args: Vec<RuntimeValue>,
        continuation: Continuation,
    ) -> Result<Option<EvalResult>, HostError>;

    fn effect_policy(&self, _op: &Symbol) -> HostEffectPolicy {
        HostEffectPolicy::volatile()
    }

    fn concurrent_handler(&self, _op: &Symbol) -> Option<Arc<ConcurrentHandler>> {
        None
    }

    fn drain_trace_events(&mut self) -> Vec<HostTraceEvent> {
        Vec::new()
    }
}

pub(crate) fn materialize_cached_effect_outputs(value: &Value) -> Result<(), String> {
    let Value::Tagged { tag, fields } = value else {
        return Ok(());
    };
    if *tag != Symbol::from("ok") {
        return Ok(());
    }
    let [Value::Record(record)] = fields.as_slice() else {
        return Ok(());
    };
    if !record.contains_key(&Symbol::from("output_files")) {
        return Ok(());
    }

    materialize_cached_outputs(value)
}

pub(crate) fn verify_cached_effect_inputs(value: &Value) -> Result<bool, String> {
    let Value::Tagged { tag, fields } = value else {
        return Ok(true);
    };
    if *tag != Symbol::from("ok") {
        return Ok(true);
    }
    let [Value::Record(record)] = fields.as_slice() else {
        return Ok(true);
    };
    let Some(Value::Bytes(stored_digest_bytes)) = record.get(&Symbol::from("input_digest")) else {
        return Ok(true);
    };
    let stored_digest = Digest::new(
        stored_digest_bytes
            .as_slice()
            .try_into()
            .map_err(|_| "invalid input_digest length".to_string())?,
    );

    let Value::List(declared_inputs) =
        required_record_field(record, "declared_inputs", PROCESS_SPAWN_OP)?
    else {
        return Err("process.spawn result declared_inputs must be a list".to_string());
    };
    let paths = declared_inputs
        .iter()
        .map(|v| match v {
            Value::Bytes(bytes) => Ok(bytes.clone()),
            _ => Err("declared_inputs must contain bytes".to_string()),
        })
        .collect::<Result<Vec<_>, _>>()?;

    let current_inputs_value = match declared_input_files_cache_value(&paths) {
        Ok(value) => value,
        Err(_) => return Ok(false),
    };
    let current_digest = current_inputs_value.digest();

    Ok(current_digest == stored_digest)
}

struct RegisteredHandler {
    handler: Box<Handler>,
    concurrent_handler: Option<Arc<ConcurrentHandler>>,
    policy: HostEffectPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HostTraceEvent {
    ServiceSpawn {
        iteration: u32,
    },
    ServiceExit {
        iteration: u32,
        status: Value,
    },
    ServiceRestart {
        next_iteration: u32,
    },
    ServiceStop {
        restart_count: u32,
        final_status: Value,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProcessSpawnRequest {
    argv: Vec<Vec<u8>>,
    env: BTreeMap<Vec<u8>, Vec<u8>>,
    cwd: Option<Vec<u8>>,
    stdin: Vec<u8>,
    env_mode: EnvMode,
    declared_inputs: Vec<Vec<u8>>,
    declared_outputs: Vec<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EnvMode {
    Clear,
    Inherit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ServiceRestartMode {
    Never,
    Always,
    OnFailure,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ServiceRestartPolicy {
    mode: ServiceRestartMode,
    max_restarts: Option<u32>,
    delay_nanos: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ServiceRestartDecision {
    Stop,
    Restart { delay_nanos: i64 },
}

pub struct Host {
    handlers: BTreeMap<Symbol, RegisteredHandler>,
    trace_events: HostTraceEvents,
    cancellation: CancellationToken,
}

impl Host {
    pub fn new() -> Self {
        Self {
            handlers: BTreeMap::new(),
            trace_events: Arc::new(Mutex::new(Vec::new())),
            cancellation: CancellationToken::new(),
        }
    }

    pub fn with_cancellation(cancellation: CancellationToken) -> Self {
        Self {
            handlers: BTreeMap::new(),
            trace_events: Arc::new(Mutex::new(Vec::new())),
            cancellation,
        }
    }

    pub fn register<F>(&mut self, op: impl Into<Symbol>, handler: F)
    where
        F: FnMut(Vec<RuntimeValue>, Continuation) -> Result<EvalResult, HostError> + Send + 'static,
    {
        self.register_with_policy(op, HostEffectPolicy::volatile(), handler);
    }

    pub fn register_stable<F>(&mut self, op: impl Into<Symbol>, handler: F)
    where
        F: FnMut(Vec<RuntimeValue>, Continuation) -> Result<EvalResult, HostError> + Send + 'static,
    {
        self.register_with_policy(op, HostEffectPolicy::stable(), handler);
    }

    pub fn register_declared<F>(&mut self, op: impl Into<Symbol>, handler: F)
    where
        F: FnMut(Vec<RuntimeValue>, Continuation) -> Result<EvalResult, HostError> + Send + 'static,
    {
        self.register_with_policy(op, HostEffectPolicy::declared(), handler);
    }

    pub fn register_hermetic<F>(&mut self, op: impl Into<Symbol>, handler: F)
    where
        F: FnMut(Vec<RuntimeValue>, Continuation) -> Result<EvalResult, HostError> + Send + 'static,
    {
        self.register_with_policy(op, HostEffectPolicy::hermetic(), handler);
    }

    pub fn register_with_policy<F>(
        &mut self,
        op: impl Into<Symbol>,
        policy: HostEffectPolicy,
        handler: F,
    ) where
        F: FnMut(Vec<RuntimeValue>, Continuation) -> Result<EvalResult, HostError> + Send + 'static,
    {
        self.handlers.insert(
            op.into(),
            RegisteredHandler {
                handler: Box::new(handler),
                concurrent_handler: None,
                policy,
            },
        );
    }

    pub fn register_concurrent_with_policy<F>(
        &mut self,
        op: impl Into<Symbol>,
        policy: HostEffectPolicy,
        handler: F,
    ) where
        F: Fn(Vec<RuntimeValue>, Continuation) -> Result<EvalResult, HostError>
            + Send
            + Sync
            + 'static,
    {
        let handler = Arc::new(handler);
        self.handlers.insert(
            op.into(),
            RegisteredHandler {
                handler: Box::new({
                    let handler = Arc::clone(&handler);
                    move |args, continuation| handler(args, continuation)
                }),
                concurrent_handler: Some(handler),
                policy,
            },
        );
    }

    pub fn install_fs_read(&mut self) {
        self.register(FS_READ_OP, fs_read_handler);
    }

    pub fn install_fs_write(&mut self) {
        self.register(FS_WRITE_OP, fs_write_handler);
    }

    pub fn install_process_spawn(&mut self) {
        self.register_declared(PROCESS_SPAWN_OP, process_spawn_handler);
    }

    pub fn install_hermetic_process_spawn(&mut self) {
        let cache = Arc::new(Mutex::new(BTreeMap::new()));
        let cancellation = self.cancellation.clone();
        self.register_concurrent_with_policy(
            PROCESS_SPAWN_OP,
            HostEffectPolicy::hermetic(),
            move |args, continuation| {
                hermetic_process_spawn_handler(
                    args,
                    continuation,
                    Arc::clone(&cache),
                    &cancellation,
                )
            },
        );
    }

    pub fn install_service_supervise(&mut self) {
        let trace_events = Arc::clone(&self.trace_events);
        let cancellation = self.cancellation.clone();
        self.register(SERVICE_SUPERVISE_OP, move |args, continuation| {
            service_supervise_handler(args, continuation, Arc::clone(&trace_events), &cancellation)
        });
    }

    pub fn install_clock(&mut self) {
        self.install_clock_now();
        self.install_clock_sleep();
    }

    pub fn install_clock_now(&mut self) {
        self.register(CLOCK_NOW_OP, clock_now_handler);
    }

    pub fn install_clock_sleep(&mut self) {
        let cancellation = self.cancellation.clone();
        self.register(CLOCK_SLEEP_OP, move |args, continuation| {
            clock_sleep_handler(args, continuation, &cancellation)
        });
    }

    pub fn install_math(&mut self) {
        self.register_stable(MATH_ADD_OP, |args, continuation| {
            math_integer_handler(args, continuation, MATH_ADD_OP, |left, right| {
                left.checked_add(right)
                    .ok_or_else(|| "math.add overflowed i64".to_string())
            })
        });
        self.register_stable(MATH_SUB_OP, |args, continuation| {
            math_integer_handler(args, continuation, MATH_SUB_OP, |left, right| {
                left.checked_sub(right)
                    .ok_or_else(|| "math.sub overflowed i64".to_string())
            })
        });
        self.register_stable(MATH_MUL_OP, |args, continuation| {
            math_integer_handler(args, continuation, MATH_MUL_OP, |left, right| {
                left.checked_mul(right)
                    .ok_or_else(|| "math.mul overflowed i64".to_string())
            })
        });
        self.register_stable(MATH_DIV_OP, |args, continuation| {
            math_integer_handler(args, continuation, MATH_DIV_OP, |left, right| {
                if right == 0 {
                    Err("math.div cannot divide by zero".to_string())
                } else {
                    left.checked_div(right)
                        .ok_or_else(|| "math.div overflowed i64".to_string())
                }
            })
        });
        self.register_stable(MATH_REM_OP, |args, continuation| {
            math_integer_handler(args, continuation, MATH_REM_OP, |left, right| {
                if right == 0 {
                    Err("math.rem cannot divide by zero".to_string())
                } else {
                    left.checked_rem(right)
                        .ok_or_else(|| "math.rem overflowed i64".to_string())
                }
            })
        });
        self.register_stable(MATH_EQ_OP, |args, continuation| {
            math_compare_handler(args, continuation, MATH_EQ_OP, |left, right| left == right)
        });
        self.register_stable(MATH_NE_OP, |args, continuation| {
            math_compare_handler(args, continuation, MATH_NE_OP, |left, right| left != right)
        });
        self.register_stable(MATH_LT_OP, |args, continuation| {
            math_compare_handler(args, continuation, MATH_LT_OP, |left, right| left < right)
        });
        self.register_stable(MATH_LE_OP, |args, continuation| {
            math_compare_handler(args, continuation, MATH_LE_OP, |left, right| left <= right)
        });
        self.register_stable(MATH_GT_OP, |args, continuation| {
            math_compare_handler(args, continuation, MATH_GT_OP, |left, right| left > right)
        });
        self.register_stable(MATH_GE_OP, |args, continuation| {
            math_compare_handler(args, continuation, MATH_GE_OP, |left, right| left >= right)
        });
    }
}

impl Default for Host {
    fn default() -> Self {
        Self::new()
    }
}

impl HostHandler for Host {
    fn handle(
        &mut self,
        op: &Symbol,
        args: Vec<RuntimeValue>,
        continuation: Continuation,
    ) -> Result<Option<EvalResult>, HostError> {
        if let Some(handler) = self.handlers.get_mut(op) {
            return (handler.handler)(args, continuation).map(Some);
        }

        Ok(None)
    }

    fn effect_policy(&self, op: &Symbol) -> HostEffectPolicy {
        self.handlers
            .get(op)
            .map(|handler| handler.policy)
            .unwrap_or_else(HostEffectPolicy::volatile)
    }

    fn concurrent_handler(&self, op: &Symbol) -> Option<Arc<ConcurrentHandler>> {
        self.handlers
            .get(op)
            .and_then(|handler| handler.concurrent_handler.clone())
    }

    fn drain_trace_events(&mut self) -> Vec<HostTraceEvent> {
        self.trace_events
            .lock()
            .expect("host trace event mutex should not be poisoned")
            .drain(..)
            .collect()
    }
}

impl HostHandler for () {
    fn handle(
        &mut self,
        _op: &Symbol,
        _args: Vec<RuntimeValue>,
        _continuation: Continuation,
    ) -> Result<Option<EvalResult>, HostError> {
        Ok(None)
    }
}

#[derive(Debug)]
pub enum HostError {
    Resume(EvalError),
}

impl fmt::Display for HostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Resume(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for HostError {}

impl From<EvalError> for HostError {
    fn from(value: EvalError) -> Self {
        Self::Resume(value)
    }
}

fn fs_read_handler(
    args: Vec<RuntimeValue>,
    continuation: Continuation,
) -> Result<EvalResult, HostError> {
    let value = match parse_fs_read_args(args.as_slice()) {
        Ok(path) => RuntimeValue::Data(fs_read_result_value(path)),
        Err(message) => error_value(message),
    };

    continuation.resume(value).map_err(Into::into)
}

fn fs_write_handler(
    args: Vec<RuntimeValue>,
    continuation: Continuation,
) -> Result<EvalResult, HostError> {
    let value = match parse_fs_write_args(args.as_slice()) {
        Ok((path, contents)) => RuntimeValue::Data(fs_write_result_value(path, contents)),
        Err(message) => error_value(message),
    };

    continuation.resume(value).map_err(Into::into)
}

fn parse_fs_read_args(args: &[RuntimeValue]) -> Result<Vec<u8>, String> {
    match args {
        [RuntimeValue::Data(Value::Record(request))] => parse_fs_read_request(request),
        [RuntimeValue::Data(Value::Bytes(path))] => Ok(path.clone()),
        _ => Err(
            "fs.read expected either one record request argument or one bytes path argument"
                .to_string(),
        ),
    }
}

fn parse_fs_write_args(args: &[RuntimeValue]) -> Result<(Vec<u8>, Vec<u8>), String> {
    match args {
        [RuntimeValue::Data(Value::Record(request))] => parse_fs_write_request(request),
        [
            RuntimeValue::Data(Value::Bytes(path)),
            RuntimeValue::Data(Value::Bytes(contents)),
        ] => Ok((path.clone(), contents.clone())),
        _ => Err(
            "fs.write expected either one record request argument or a bytes path and bytes contents"
                .to_string(),
        ),
    }
}

fn parse_fs_read_request(request: &BTreeMap<Symbol, Value>) -> Result<Vec<u8>, String> {
    parse_bytes(
        required_record_field(request, "path", FS_READ_OP)?,
        "fs.read field `path` must be bytes",
    )
}

fn parse_fs_write_request(request: &BTreeMap<Symbol, Value>) -> Result<(Vec<u8>, Vec<u8>), String> {
    Ok((
        parse_bytes(
            required_record_field(request, "path", FS_WRITE_OP)?,
            "fs.write field `path` must be bytes",
        )?,
        parse_bytes(
            required_record_field(request, "contents", FS_WRITE_OP)?,
            "fs.write field `contents` must be bytes",
        )?,
    ))
}

fn fs_read_result_value(path: Vec<u8>) -> Value {
    match std::fs::read(path_from_bytes(path)) {
        Ok(contents) => ok_record_value([("contents", Value::Bytes(contents))]),
        Err(error) => io_error_value(error),
    }
}

fn fs_write_result_value(path: Vec<u8>, contents: Vec<u8>) -> Value {
    match std::fs::write(path_from_bytes(path), &contents) {
        Ok(()) => ok_record_value([(
            "written",
            Value::Integer(i64::try_from(contents.len()).expect("byte length should fit into i64")),
        )]),
        Err(error) => io_error_value(error),
    }
}

fn process_spawn_handler(
    args: Vec<RuntimeValue>,
    continuation: Continuation,
) -> Result<EvalResult, HostError> {
    let value = match args.as_slice() {
        [RuntimeValue::Data(Value::Record(request))] => match run_process_request(request) {
            Ok(value) => value,
            Err(message) => error_value(message),
        },
        _ => error_value("process.spawn expected one record request argument"),
    };

    continuation.resume(value).map_err(Into::into)
}

fn hermetic_process_spawn_handler(
    args: Vec<RuntimeValue>,
    continuation: Continuation,
    cache: ProcessCache,
    cancellation: &CancellationToken,
) -> Result<EvalResult, HostError> {
    let value = match args.as_slice() {
        [RuntimeValue::Data(Value::Record(request))] => {
            match run_hermetic_process_request(request, cache, cancellation) {
                Ok(value) => value,
                Err(message) => error_value(message),
            }
        }
        _ => error_value("process.spawn expected one record request argument"),
    };

    continuation.resume(value).map_err(Into::into)
}

fn service_supervise_handler(
    args: Vec<RuntimeValue>,
    continuation: Continuation,
    trace_events: HostTraceEvents,
    cancellation: &CancellationToken,
) -> Result<EvalResult, HostError> {
    let value = match args.as_slice() {
        [RuntimeValue::Data(Value::Record(request))] => {
            match run_service_supervise(request, trace_events, cancellation) {
                Ok(value) => RuntimeValue::Data(value),
                Err(message) => error_value(message),
            }
        }
        _ => error_value("service.supervise expected one record service spec argument"),
    };

    continuation.resume(value).map_err(Into::into)
}

fn clock_now_handler(
    args: Vec<RuntimeValue>,
    continuation: Continuation,
) -> Result<EvalResult, HostError> {
    let value = match args.as_slice() {
        [RuntimeValue::Data(Value::Record(request))] => {
            match parse_empty_clock_request(request, CLOCK_NOW_OP) {
                Ok(()) => match unix_time_nanos() {
                    Ok(unix_nanos) => RuntimeValue::Data(ok_record_value([(
                        "unix_nanos",
                        Value::Integer(unix_nanos),
                    )])),
                    Err(message) => error_value(message),
                },
                Err(message) => error_value(message),
            }
        }
        _ => error_value("clock.now expected one record request argument"),
    };

    continuation.resume(value).map_err(Into::into)
}

fn clock_sleep_handler(
    args: Vec<RuntimeValue>,
    continuation: Continuation,
    cancellation: &CancellationToken,
) -> Result<EvalResult, HostError> {
    let value = match args.as_slice() {
        [RuntimeValue::Data(Value::Record(request))] => match parse_sleep_request(request) {
            Ok(duration_nanos) => match sleep_with_cancellation(duration_nanos, cancellation) {
                Ok(()) => RuntimeValue::Data(ok_record_value([(
                    "duration_nanos",
                    Value::Integer(duration_nanos),
                )])),
                Err(message) => error_value(message),
            },
            Err(message) => error_value(message),
        },
        _ => error_value("clock.sleep expected one record request argument"),
    };

    continuation.resume(value).map_err(Into::into)
}

fn math_integer_handler(
    args: Vec<RuntimeValue>,
    continuation: Continuation,
    op: &str,
    operation: impl FnOnce(i64, i64) -> Result<i64, String>,
) -> Result<EvalResult, HostError> {
    let value = match parse_math_integer_args(args.as_slice(), op).and_then(|(left, right)| {
        operation(left, right).map(|value| RuntimeValue::Data(Value::Integer(value)))
    }) {
        Ok(value) => value,
        Err(message) => error_value(message),
    };

    continuation.resume(value).map_err(Into::into)
}

fn math_compare_handler(
    args: Vec<RuntimeValue>,
    continuation: Continuation,
    op: &str,
    operation: impl FnOnce(i64, i64) -> bool,
) -> Result<EvalResult, HostError> {
    let value = match parse_math_integer_args(args.as_slice(), op) {
        Ok((left, right)) => RuntimeValue::Data(boolean_value(operation(left, right))),
        Err(message) => error_value(message),
    };

    continuation.resume(value).map_err(Into::into)
}

fn parse_math_integer_args(args: &[RuntimeValue], op: &str) -> Result<(i64, i64), String> {
    match args {
        [RuntimeValue::Data(left), RuntimeValue::Data(right)] => Ok((
            parse_integer(left, &format!("{op} left argument must be an integer"))?,
            parse_integer(right, &format!("{op} right argument must be an integer"))?,
        )),
        _ => Err(format!("{op} expected two integer arguments")),
    }
}

fn run_process_request(request: &BTreeMap<Symbol, Value>) -> Result<RuntimeValue, String> {
    let request = parse_process_request(request)?;
    validate_declared_inputs(&request)?;
    let output = execute_process(&request, &CancellationToken::new())?;
    Ok(RuntimeValue::Data(process_result_value(request, output)))
}

fn run_hermetic_process_request(
    request: &BTreeMap<Symbol, Value>,
    cache: ProcessCache,
    cancellation: &CancellationToken,
) -> Result<RuntimeValue, String> {
    check_cancelled(cancellation)?;
    let request = parse_process_request(request)?;
    validate_declared_inputs(&request)?;
    let cache_key = process_cache_key(&request)?;

    if let Some(value) = cache
        .lock()
        .expect("process cache mutex should not be poisoned")
        .get(&cache_key)
        .cloned()
    {
        materialize_cached_outputs(&value)?;
        return Ok(RuntimeValue::Data(value));
    }

    enforce_process_sandbox(&request)?;
    let output = execute_process(&request, cancellation)?;
    let declared_inputs_value = declared_input_files_cache_value(&request.declared_inputs)?;
    let input_digest = declared_inputs_value.digest();
    let value = hermetic_process_result_value(request, output, input_digest);
    if declared_outputs_complete(&value) {
        cache
            .lock()
            .expect("process cache mutex should not be poisoned")
            .insert(cache_key, value.clone());
    }

    Ok(RuntimeValue::Data(value))
}

fn run_service_supervise(
    request: &BTreeMap<Symbol, Value>,
    trace_events: HostTraceEvents,
    cancellation: &CancellationToken,
) -> Result<Value, String> {
    let (spawn_request, restart_policy) = parse_service_spec(request)?;
    let mut spawn_count = 0_u32;
    let mut restart_count = 0_u32;

    loop {
        check_cancelled(cancellation)?;
        spawn_count = spawn_count
            .checked_add(1)
            .ok_or_else(|| "service.supervise spawn count overflowed u32".to_string())?;
        trace_events
            .lock()
            .expect("host trace event mutex should not be poisoned")
            .push(HostTraceEvent::ServiceSpawn {
                iteration: spawn_count,
            });

        validate_declared_inputs(&spawn_request)?;
        enforce_process_sandbox(&spawn_request)?;
        let output = execute_process(&spawn_request, cancellation)?;
        let status = process_status_from_exit_status(output.status);
        let status_value = process_status_value(&status);
        trace_events
            .lock()
            .expect("host trace event mutex should not be poisoned")
            .push(HostTraceEvent::ServiceExit {
                iteration: spawn_count,
                status: status_value.clone(),
            });

        match service_restart_decision(restart_policy, &status, spawn_count)? {
            ServiceRestartDecision::Stop => {
                trace_events
                    .lock()
                    .expect("host trace event mutex should not be poisoned")
                    .push(HostTraceEvent::ServiceStop {
                        restart_count,
                        final_status: status_value.clone(),
                    });
                return Ok(service_supervise_result_value(status_value, restart_count));
            }
            ServiceRestartDecision::Restart { delay_nanos } => {
                restart_count = restart_count
                    .checked_add(1)
                    .ok_or_else(|| "service.supervise restart count overflowed u32".to_string())?;
                if delay_nanos > 0 {
                    sleep_with_cancellation(delay_nanos, cancellation)?;
                }
                trace_events
                    .lock()
                    .expect("host trace event mutex should not be poisoned")
                    .push(HostTraceEvent::ServiceRestart {
                        next_iteration: spawn_count + 1,
                    });
            }
        }
    }
}

fn enforce_process_sandbox(request: &ProcessSpawnRequest) -> Result<(), String> {
    #[cfg(feature = "sandbox")]
    {
        enforce_declared_path_sandbox(request)
    }

    #[cfg(not(feature = "sandbox"))]
    {
        let _ = request;
        Ok(())
    }
}

fn parse_process_request(request: &BTreeMap<Symbol, Value>) -> Result<ProcessSpawnRequest, String> {
    let argv = parse_bytes_list(
        required_field(request, "argv")?,
        "process.spawn field `argv` must be a list of bytes",
    )?;
    if argv.is_empty() {
        return Err("process.spawn field `argv` must contain at least one element".to_string());
    }

    let env = match request.get(&Symbol::from("env")) {
        Some(value) => parse_env_record(value)?,
        None => BTreeMap::new(),
    };
    let cwd = optional_bytes_field(request, "cwd")?;
    let stdin = match request.get(&Symbol::from("stdin")) {
        Some(value) => parse_bytes(value, "process.spawn field `stdin` must be bytes")?,
        None => Vec::new(),
    };
    let env_mode = parse_env_mode(required_field(request, "env_mode")?)?;
    let declared_inputs = parse_bytes_list(
        required_field(request, "declared_inputs")?,
        "process.spawn field `declared_inputs` must be a list of bytes",
    )?;
    let declared_outputs = parse_bytes_list(
        required_field(request, "declared_outputs")?,
        "process.spawn field `declared_outputs` must be a list of bytes",
    )?;

    Ok(ProcessSpawnRequest {
        argv,
        env,
        cwd,
        stdin,
        env_mode,
        declared_inputs,
        declared_outputs,
    })
}

fn parse_service_spec(
    request: &BTreeMap<Symbol, Value>,
) -> Result<(ProcessSpawnRequest, ServiceRestartPolicy), String> {
    let spawn_request = parse_process_request(request)?;
    let restart_policy = match request.get(&Symbol::from("restart_policy")) {
        Some(Value::Record(record)) => parse_service_restart_policy(record)?,
        Some(_) => {
            return Err("service.supervise field `restart_policy` must be a record".to_string());
        }
        None => ServiceRestartPolicy {
            mode: ServiceRestartMode::Never,
            max_restarts: None,
            delay_nanos: 0,
        },
    };

    Ok((spawn_request, restart_policy))
}

fn parse_service_restart_policy(
    record: &BTreeMap<Symbol, Value>,
) -> Result<ServiceRestartPolicy, String> {
    let mode = match required_record_field(record, "mode", SERVICE_SUPERVISE_OP)? {
        Value::Bytes(bytes) if bytes == b"never" => ServiceRestartMode::Never,
        Value::Bytes(bytes) if bytes == b"always" => ServiceRestartMode::Always,
        Value::Bytes(bytes) if bytes == b"on_failure" => ServiceRestartMode::OnFailure,
        Value::Symbol(symbol) if symbol.as_str() == "never" => ServiceRestartMode::Never,
        Value::Symbol(symbol) if symbol.as_str() == "always" => ServiceRestartMode::Always,
        Value::Symbol(symbol) if symbol.as_str() == "on_failure" => ServiceRestartMode::OnFailure,
        _ => {
            return Err(
                "service.supervise restart_policy.mode must be never, always, or on_failure"
                    .to_string(),
            );
        }
    };
    let max_restarts = record
        .get(&Symbol::from("max_restarts"))
        .map(|value| {
            let value = parse_integer(
                value,
                "service.supervise restart_policy.max_restarts must be an integer",
            )?;
            u32::try_from(value).map_err(|_| {
                "service.supervise restart_policy.max_restarts must fit u32".to_string()
            })
        })
        .transpose()?;
    let delay_nanos = record
        .get(&Symbol::from("delay_nanos"))
        .map(|value| {
            parse_integer(
                value,
                "service.supervise restart_policy.delay_nanos must be an integer",
            )
        })
        .transpose()?
        .unwrap_or(0);
    if delay_nanos < 0 {
        return Err(
            "service.supervise restart_policy.delay_nanos must be non-negative".to_string(),
        );
    }

    Ok(ServiceRestartPolicy {
        mode,
        max_restarts,
        delay_nanos,
    })
}

fn parse_empty_clock_request(request: &BTreeMap<Symbol, Value>, op: &str) -> Result<(), String> {
    if request.is_empty() {
        Ok(())
    } else {
        Err(format!("{op} request must be an empty record"))
    }
}

fn parse_sleep_request(request: &BTreeMap<Symbol, Value>) -> Result<i64, String> {
    let duration_nanos = parse_integer(
        required_record_field(request, "duration_nanos", CLOCK_SLEEP_OP)?,
        "clock.sleep field `duration_nanos` must be an integer",
    )?;
    if duration_nanos < 0 {
        return Err("clock.sleep field `duration_nanos` must be non-negative".to_string());
    }
    Ok(duration_nanos)
}

fn execute_process(
    request: &ProcessSpawnRequest,
    cancellation: &CancellationToken,
) -> Result<std::process::Output, String> {
    check_cancelled(cancellation)?;
    let mut argv = request.argv.iter();
    let executable = argv
        .next()
        .expect("process request should always have a non-empty argv");
    let mut command = Command::new(path_from_bytes(executable.clone()));
    for arg in argv {
        command.arg(os_string_from_bytes(arg.clone()));
    }

    if request.env_mode == EnvMode::Clear {
        command.env_clear();
    }
    for (key, value) in &request.env {
        command.env(
            os_string_from_bytes(key.clone()),
            os_string_from_bytes(value.clone()),
        );
    }
    if let Some(cwd) = &request.cwd {
        command.current_dir(path_from_bytes(cwd.clone()));
    }

    command.stdin(Stdio::piped());
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());

    let mut child = command
        .spawn()
        .map_err(|error| format!("process.spawn failed to start child: {error}"))?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(&request.stdin)
            .map_err(|error| format!("process.spawn failed to write stdin: {error}"))?;
    }

    loop {
        check_cancelled_child(cancellation, &mut child)?;
        match child
            .try_wait()
            .map_err(|error| format!("process.spawn failed to wait for child: {error}"))?
        {
            Some(_) => {
                return child.wait_with_output().map_err(|error| {
                    format!("process.spawn failed to collect child output: {error}")
                });
            }
            None => thread::sleep(Duration::from_millis(10)),
        }
    }
}

fn check_cancelled(cancellation: &CancellationToken) -> Result<(), String> {
    if cancellation.is_cancelled() {
        Err("cancelled".to_string())
    } else {
        Ok(())
    }
}

fn check_cancelled_child(
    cancellation: &CancellationToken,
    child: &mut std::process::Child,
) -> Result<(), String> {
    if !cancellation.is_cancelled() {
        return Ok(());
    }
    let _ = child.kill();
    let _ = child.wait();
    Err("cancelled".to_string())
}

fn sleep_with_cancellation(
    delay_nanos: i64,
    cancellation: &CancellationToken,
) -> Result<(), String> {
    let total = u64::try_from(delay_nanos)
        .map_err(|_| "service.supervise delay overflowed u64".to_string())?;
    let mut remaining = Duration::from_nanos(total);
    while remaining > Duration::ZERO {
        check_cancelled(cancellation)?;
        let step = remaining.min(Duration::from_millis(10));
        thread::sleep(step);
        remaining = remaining.saturating_sub(step);
    }
    Ok(())
}

fn process_result_value(request: ProcessSpawnRequest, output: std::process::Output) -> Value {
    let output_files = declared_output_files_value(&request.declared_outputs);
    let mut record = BTreeMap::new();
    record.insert(Symbol::from("status"), exit_status_value(output.status));
    record.insert(Symbol::from("stdout"), Value::Bytes(output.stdout));
    record.insert(Symbol::from("stderr"), Value::Bytes(output.stderr));
    record.insert(
        Symbol::from("declared_inputs"),
        bytes_list_value(request.declared_inputs),
    );
    record.insert(
        Symbol::from("declared_outputs"),
        bytes_list_value(request.declared_outputs),
    );
    record.insert(Symbol::from("output_files"), output_files);

    Value::Tagged {
        tag: Symbol::from("ok"),
        fields: vec![Value::Record(record)],
    }
}

fn hermetic_process_result_value(
    request: ProcessSpawnRequest,
    output: std::process::Output,
    input_digest: Digest,
) -> Value {
    let mut value = process_result_value(request, output);
    if let Value::Tagged { tag: _, fields } = &mut value
        && let [Value::Record(record)] = fields.as_mut_slice()
    {
        record.insert(
            Symbol::from("input_digest"),
            Value::Bytes(input_digest.as_bytes().to_vec()),
        );
    }
    value
}

fn validate_declared_inputs(request: &ProcessSpawnRequest) -> Result<(), String> {
    for path in &request.declared_inputs {
        let path_buf = path_from_bytes(path.clone());
        std::fs::metadata(&path_buf).map_err(|error| {
            format!(
                "process.spawn declared input {} was not accessible: {error}",
                path_buf.display()
            )
        })?;
    }

    Ok(())
}

#[cfg(feature = "sandbox")]
fn enforce_declared_path_sandbox(request: &ProcessSpawnRequest) -> Result<(), String> {
    let allowed_paths = request
        .declared_inputs
        .iter()
        .chain(request.declared_outputs.iter())
        .map(|path| path_from_bytes(path.clone()))
        .collect::<Vec<_>>();

    for argument in request.argv.iter().skip(1) {
        for path in absolute_paths_in_argument(argument) {
            if !is_declared_sandbox_path(&path, &allowed_paths) {
                return Err(format!(
                    "permission_denied: process.spawn sandbox denied access to undeclared path {}",
                    path.display()
                ));
            }
        }
    }

    if let Some(cwd) = &request.cwd {
        let cwd = path_from_bytes(cwd.clone());
        if cwd.is_absolute() && !is_declared_sandbox_path(&cwd, &allowed_paths) {
            return Err(format!(
                "permission_denied: process.spawn sandbox denied cwd {}",
                cwd.display()
            ));
        }
    }

    Ok(())
}

#[cfg(feature = "sandbox")]
fn absolute_paths_in_argument(argument: &[u8]) -> Vec<PathBuf> {
    let text = String::from_utf8_lossy(argument);
    let bytes = text.as_bytes();
    let mut paths = Vec::new();
    let mut index = 0;

    while index < bytes.len() {
        if bytes[index] != b'/' || (index > 0 && !is_shell_path_boundary(bytes[index - 1])) {
            index += 1;
            continue;
        }

        let start = index;
        while index < bytes.len() && !is_shell_path_boundary(bytes[index]) {
            index += 1;
        }
        paths.push(PathBuf::from(&text[start..index]));
    }

    paths
}

#[cfg(feature = "sandbox")]
fn is_shell_path_boundary(byte: u8) -> bool {
    matches!(
        byte,
        b'\0'
            | b' '
            | b'\t'
            | b'\n'
            | b'\r'
            | b'"'
            | b'\''
            | b'`'
            | b';'
            | b'|'
            | b'&'
            | b'<'
            | b'>'
            | b'('
            | b')'
    )
}

#[cfg(feature = "sandbox")]
fn is_declared_sandbox_path(path: &std::path::Path, allowed_paths: &[PathBuf]) -> bool {
    allowed_paths
        .iter()
        .any(|allowed| path == allowed || path.starts_with(allowed))
}

fn process_cache_key(request: &ProcessSpawnRequest) -> Result<Digest, String> {
    let mut record = BTreeMap::new();
    record.insert(Symbol::from("argv"), bytes_list_value(request.argv.clone()));
    record.insert(Symbol::from("env"), env_cache_value(&request.env));
    record.insert(
        Symbol::from("cwd"),
        optional_bytes_value(request.cwd.clone()),
    );
    record.insert(Symbol::from("stdin"), Value::Bytes(request.stdin.clone()));
    record.insert(
        Symbol::from("env_mode"),
        Value::Bytes(env_mode_bytes(request.env_mode)),
    );
    record.insert(
        Symbol::from("declared_inputs"),
        declared_input_files_cache_value(&request.declared_inputs)?,
    );
    record.insert(
        Symbol::from("declared_outputs"),
        bytes_list_value(request.declared_outputs.clone()),
    );

    Ok(Value::Record(record).digest())
}

fn env_cache_value(env: &BTreeMap<Vec<u8>, Vec<u8>>) -> Value {
    Value::List(
        env.iter()
            .map(|(key, value)| {
                Value::Record(BTreeMap::from([
                    (Symbol::from("key"), Value::Bytes(key.clone())),
                    (Symbol::from("value"), Value::Bytes(value.clone())),
                ]))
            })
            .collect(),
    )
}

fn optional_bytes_value(value: Option<Vec<u8>>) -> Value {
    match value {
        Some(bytes) => Value::Tagged {
            tag: Symbol::from("some"),
            fields: vec![Value::Bytes(bytes)],
        },
        None => Value::Tagged {
            tag: Symbol::from("none"),
            fields: Vec::new(),
        },
    }
}

fn env_mode_bytes(mode: EnvMode) -> Vec<u8> {
    match mode {
        EnvMode::Clear => ENV_MODE_CLEAR.as_bytes().to_vec(),
        EnvMode::Inherit => ENV_MODE_INHERIT.as_bytes().to_vec(),
    }
}

fn declared_input_files_cache_value(paths: &[Vec<u8>]) -> Result<Value, String> {
    paths
        .iter()
        .map(|path| {
            let path_buf = path_from_bytes(path.clone());
            let contents = std::fs::read(&path_buf).map_err(|error| {
                format!(
                    "process.spawn declared input {} was not readable: {error}",
                    path_buf.display()
                )
            })?;
            Ok(Value::Record(BTreeMap::from([
                (Symbol::from("path"), Value::Bytes(path.clone())),
                (Symbol::from("contents"), Value::Bytes(contents)),
            ])))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Value::List)
}

fn declared_outputs_complete(value: &Value) -> bool {
    output_file_records(value)
        .map(|files| {
            files.iter().all(|file| match file {
                Value::Record(file) => {
                    matches!(
                        file.get(&Symbol::from("contents")),
                        Some(Value::Tagged { tag, fields })
                            if *tag == Symbol::from("ok")
                                && matches!(fields.as_slice(), [Value::Bytes(_)])
                    )
                }
                _ => false,
            })
        })
        .unwrap_or(false)
}

fn materialize_cached_outputs(value: &Value) -> Result<(), String> {
    for file in output_file_records(value)? {
        let Value::Record(file) = file else {
            return Err(
                "process.spawn cached result field `output_files` must contain records".to_string(),
            );
        };
        let path = parse_bytes(
            required_record_field(file, "path", PROCESS_SPAWN_OP)?,
            "process.spawn cached output field `path` must be bytes",
        )?;
        let contents = match required_record_field(file, "contents", PROCESS_SPAWN_OP)? {
            Value::Tagged { tag, fields }
                if *tag == Symbol::from("ok") && matches!(fields.as_slice(), [Value::Bytes(_)]) =>
            {
                match fields.as_slice() {
                    [Value::Bytes(contents)] => contents.clone(),
                    _ => unreachable!("matches! checked field shape"),
                }
            }
            _ => {
                return Err(
                    "process.spawn cached result did not contain complete declared outputs"
                        .to_string(),
                );
            }
        };

        let path = path_from_bytes(path);
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).map_err(|error| {
                format!(
                    "process.spawn failed to create output directory {}: {error}",
                    parent.display()
                )
            })?;
        }
        std::fs::write(&path, contents).map_err(|error| {
            format!(
                "process.spawn failed to materialize cached output {}: {error}",
                path.display()
            )
        })?;
        apply_cached_output_mode(file, &path)?;
    }

    Ok(())
}

#[cfg(unix)]
fn apply_cached_output_mode(
    record: &BTreeMap<Symbol, Value>,
    path: &std::path::Path,
) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;

    let Some(Value::Integer(mode)) = record.get(&Symbol::from("mode")) else {
        return Ok(());
    };
    let mode = u32::try_from(*mode).map_err(|_| {
        format!(
            "process.spawn cached output {} had invalid mode {mode}",
            path.display()
        )
    })?;
    let permissions = std::fs::Permissions::from_mode(mode);
    std::fs::set_permissions(path, permissions).map_err(|error| {
        format!(
            "process.spawn failed to restore cached output mode {}: {error}",
            path.display()
        )
    })
}

#[cfg(not(unix))]
fn apply_cached_output_mode(
    _record: &BTreeMap<Symbol, Value>,
    _path: &std::path::Path,
) -> Result<(), String> {
    Ok(())
}

fn output_file_records(value: &Value) -> Result<&[Value], String> {
    let Value::Tagged { tag, fields } = value else {
        return Err("process.spawn cached result must be tagged".to_string());
    };
    if *tag != Symbol::from("ok") {
        return Err("process.spawn cached result must be ok".to_string());
    }
    let [Value::Record(record)] = fields.as_slice() else {
        return Err("process.spawn cached result must contain one record".to_string());
    };
    let Value::List(files) = required_record_field(record, "output_files", PROCESS_SPAWN_OP)?
    else {
        return Err("process.spawn cached result field `output_files` must be a list".to_string());
    };
    Ok(files.as_slice())
}

fn exit_status_value(status: ExitStatus) -> Value {
    process_status_value(&process_status_from_exit_status(status))
}

fn process_status_from_exit_status(status: ExitStatus) -> process::ProcessStatus {
    match status.code() {
        Some(code) => process::ProcessStatus::ExitCode(i64::from(code)),
        None => {
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;

                if let Some(signal) = status.signal() {
                    return process::ProcessStatus::Signal(i64::from(signal));
                }
            }

            process::ProcessStatus::Unknown
        }
    }
}

fn process_status_value(status: &process::ProcessStatus) -> Value {
    match status {
        process::ProcessStatus::ExitCode(code) => Value::Tagged {
            tag: Symbol::from("exit_code"),
            fields: vec![Value::Integer(*code)],
        },
        process::ProcessStatus::Signal(signal) => Value::Tagged {
            tag: Symbol::from("signal"),
            fields: vec![Value::Integer(*signal)],
        },
        process::ProcessStatus::Unknown => Value::Tagged {
            tag: Symbol::from("unknown_status"),
            fields: Vec::new(),
        },
    }
}

fn service_restart_decision(
    policy: ServiceRestartPolicy,
    status: &process::ProcessStatus,
    spawn_count: u32,
) -> Result<ServiceRestartDecision, String> {
    let should_restart = match policy.mode {
        ServiceRestartMode::Never => false,
        ServiceRestartMode::Always => true,
        ServiceRestartMode::OnFailure => !matches!(status, process::ProcessStatus::ExitCode(0)),
    };

    if !should_restart {
        return Ok(ServiceRestartDecision::Stop);
    }

    if let Some(max_restarts) = policy.max_restarts
        && spawn_count >= max_restarts
    {
        return Ok(ServiceRestartDecision::Stop);
    }

    Ok(ServiceRestartDecision::Restart {
        delay_nanos: policy.delay_nanos,
    })
}

fn service_supervise_result_value(final_status: Value, restart_count: u32) -> Value {
    ok_record_value([
        ("final_status", final_status),
        ("restart_count", Value::Integer(i64::from(restart_count))),
    ])
}

fn bytes_list_value(items: Vec<Vec<u8>>) -> Value {
    Value::List(items.into_iter().map(Value::Bytes).collect())
}

fn declared_output_files_value(paths: &[Vec<u8>]) -> Value {
    Value::List(
        paths
            .iter()
            .cloned()
            .map(declared_output_file_value)
            .collect(),
    )
}

fn declared_output_file_value(path: Vec<u8>) -> Value {
    let path_buf = path_from_bytes(path.clone());
    let mut record = BTreeMap::new();
    record.insert(Symbol::from("path"), Value::Bytes(path.clone()));
    record.insert(
        Symbol::from("contents"),
        read_declared_output_value(&path_buf),
    );
    insert_declared_output_mode(&mut record, &path_buf);
    Value::Record(record)
}

fn read_declared_output_value(path: &std::path::Path) -> Value {
    match std::fs::read(path) {
        Ok(bytes) => Value::Tagged {
            tag: Symbol::from("ok"),
            fields: vec![Value::Bytes(bytes)],
        },
        Err(error) => Value::Tagged {
            tag: Symbol::from("error"),
            fields: vec![Value::Bytes(error.to_string().into_bytes())],
        },
    }
}

#[cfg(unix)]
fn insert_declared_output_mode(record: &mut BTreeMap<Symbol, Value>, path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;

    if let Ok(metadata) = std::fs::metadata(path) {
        record.insert(
            Symbol::from("mode"),
            Value::Integer(i64::from(metadata.permissions().mode())),
        );
    }
}

#[cfg(not(unix))]
fn insert_declared_output_mode(_record: &mut BTreeMap<Symbol, Value>, _path: &std::path::Path) {}

fn required_field<'a>(
    request: &'a BTreeMap<Symbol, Value>,
    name: &str,
) -> Result<&'a Value, String> {
    request
        .get(&Symbol::from(name))
        .ok_or_else(|| format!("process.spawn request is missing required field `{name}`"))
}

fn required_record_field<'a>(
    request: &'a BTreeMap<Symbol, Value>,
    name: &str,
    op: &str,
) -> Result<&'a Value, String> {
    request
        .get(&Symbol::from(name))
        .ok_or_else(|| format!("{op} request is missing required field `{name}`"))
}

fn optional_bytes_field(
    request: &BTreeMap<Symbol, Value>,
    name: &str,
) -> Result<Option<Vec<u8>>, String> {
    request
        .get(&Symbol::from(name))
        .map(|value| {
            parse_bytes(
                value,
                &format!("process.spawn field `{name}` must be bytes"),
            )
        })
        .transpose()
}

fn parse_bytes(value: &Value, message: &str) -> Result<Vec<u8>, String> {
    match value {
        Value::Bytes(bytes) => Ok(bytes.clone()),
        _ => Err(message.to_string()),
    }
}

fn parse_integer(value: &Value, message: &str) -> Result<i64, String> {
    match value {
        Value::Integer(value) => Ok(*value),
        _ => Err(message.to_string()),
    }
}

fn parse_bytes_list(value: &Value, message: &str) -> Result<Vec<Vec<u8>>, String> {
    match value {
        Value::List(items) => items
            .iter()
            .map(|item| parse_bytes(item, message))
            .collect(),
        _ => Err(message.to_string()),
    }
}

fn parse_env_record(value: &Value) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, String> {
    match value {
        Value::Record(entries) => entries
            .iter()
            .map(|(key, value)| {
                Ok((
                    key.as_str().as_bytes().to_vec(),
                    parse_bytes(value, "process.spawn field `env` values must be bytes")?,
                ))
            })
            .collect(),
        _ => Err("process.spawn field `env` must be a record of bytes values".to_string()),
    }
}

fn parse_env_mode(value: &Value) -> Result<EnvMode, String> {
    let mode = parse_bytes(value, "process.spawn field `env_mode` must be bytes")?;
    match mode.as_slice() {
        bytes if bytes == ENV_MODE_CLEAR.as_bytes() => Ok(EnvMode::Clear),
        bytes if bytes == ENV_MODE_INHERIT.as_bytes() => Ok(EnvMode::Inherit),
        _ => Err(
            "process.spawn field `env_mode` must be either \"clear\" or \"inherit\"".to_string(),
        ),
    }
}

fn unix_time_nanos() -> Result<i64, String> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("clock.now failed to read unix time: {error}"))?;
    i64::try_from(duration.as_nanos())
        .map_err(|_| "clock.now unix time exceeded i64 nanosecond range".to_string())
}

fn tagged_record_value(
    tag: &'static str,
    entries: impl IntoIterator<Item = (&'static str, Value)>,
) -> Value {
    Value::Tagged {
        tag: Symbol::from(tag),
        fields: vec![Value::Record(
            entries
                .into_iter()
                .map(|(key, value)| (Symbol::from(key), value))
                .collect(),
        )],
    }
}

fn ok_record_value(entries: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    tagged_record_value("ok", entries)
}

fn boolean_value(value: bool) -> Value {
    Value::Symbol(Symbol::from(if value { "true" } else { "false" }))
}

fn error_value(message: impl Into<String>) -> RuntimeValue {
    RuntimeValue::Data(Value::Tagged {
        tag: Symbol::from("error"),
        fields: vec![Value::Bytes(message.into().into_bytes())],
    })
}

fn io_error_value(error: io::Error) -> Value {
    tagged_record_value(
        io_error_tag(error.kind()),
        [("message", Value::Bytes(error.to_string().into_bytes()))],
    )
}

fn io_error_tag(kind: ErrorKind) -> &'static str {
    match kind {
        ErrorKind::NotFound => "not_found",
        ErrorKind::PermissionDenied => "permission_denied",
        ErrorKind::AlreadyExists => "already_exists",
        ErrorKind::InvalidInput => "invalid_input",
        ErrorKind::Interrupted => "interrupted",
        _ => "other_error",
    }
}

#[cfg(unix)]
fn path_from_bytes(bytes: Vec<u8>) -> PathBuf {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    PathBuf::from(OsString::from_vec(bytes))
}

#[cfg(not(unix))]
fn path_from_bytes(bytes: Vec<u8>) -> PathBuf {
    PathBuf::from(String::from_utf8_lossy(&bytes).into_owned())
}

#[cfg(unix)]
fn os_string_from_bytes(bytes: Vec<u8>) -> std::ffi::OsString {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    OsString::from_vec(bytes)
}

#[cfg(not(unix))]
fn os_string_from_bytes(bytes: Vec<u8>) -> std::ffi::OsString {
    std::ffi::OsString::from(String::from_utf8_lossy(&bytes).into_owned())
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::data::Term;
    use crate::effects::{clock, fs};
    use crate::runtime::Runtime;
    use crate::thunk;

    #[test]
    fn fs_read_runs_end_to_end_with_explicit_result_shape() {
        let path = unique_temp_path("r2-fs-read");
        let expected = b"hello from host".to_vec();
        std::fs::write(&path, &expected).expect("temp file should write");

        let mut runtime = Runtime::new();
        let mut host = Host::new();
        host.install_fs_read();

        let program = fs::read(path_to_bytes(&path));

        let result = runtime.run(program, &mut host).expect("program should run");

        match result {
            RuntimeValue::Data(value) => {
                let decoded = fs::decode_read_result(&value).expect("result should decode");
                assert_eq!(decoded, fs::ReadResult::Ok { contents: expected });
            }
            other => panic!("unexpected result: {other:?}"),
        }

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn fs_read_accepts_legacy_bytes_argument_shape() {
        let path = unique_temp_path("r2-fs-read-legacy");
        let expected = b"legacy".to_vec();
        std::fs::write(&path, &expected).expect("temp file should write");

        let mut runtime = Runtime::new();
        let mut host = Host::new();
        host.install_fs_read();

        let program = Term::Perform {
            op: Symbol::from(FS_READ_OP),
            args: vec![Term::Value(Value::Bytes(path_to_bytes(&path)))],
        };

        let result = runtime.run(program, &mut host).expect("program should run");

        match result {
            RuntimeValue::Data(value) => {
                let decoded = fs::decode_read_result(&value).expect("result should decode");
                assert_eq!(decoded, fs::ReadResult::Ok { contents: expected });
            }
            other => panic!("unexpected result: {other:?}"),
        }

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn fs_write_runs_end_to_end_with_explicit_result_shape() {
        let path = unique_temp_path("r2-fs-write");
        let expected = b"hello from write".to_vec();

        let mut runtime = Runtime::new();
        let mut host = Host::new();
        host.install_fs_write();

        let program = fs::write(path_to_bytes(&path), expected.clone());

        let result = runtime.run(program, &mut host).expect("program should run");

        match result {
            RuntimeValue::Data(value) => {
                let decoded = fs::decode_write_result(&value).expect("result should decode");
                assert_eq!(
                    decoded,
                    fs::WriteResult::Ok {
                        written: i64::try_from(expected.len()).unwrap()
                    }
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
        assert_eq!(std::fs::read(&path).unwrap(), expected);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn fs_read_not_found_returns_typed_error_result() {
        let path = unique_temp_path("r2-fs-read-missing");
        let expected_message = std::fs::read(&path).unwrap_err().to_string();

        let mut runtime = Runtime::new();
        let mut host = Host::new();
        host.install_fs_read();

        let result = runtime
            .run(fs::read(path_to_bytes(&path)), &mut host)
            .expect("program should run");

        match result {
            RuntimeValue::Data(value) => {
                let decoded = fs::decode_read_result(&value).expect("result should decode");
                assert_eq!(
                    decoded,
                    fs::ReadResult::Error(fs::ErrorResult {
                        kind: fs::ErrorKind::NotFound,
                        message: expected_message,
                    })
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn process_spawn_runs_a_child_with_explicit_request_shape() {
        let executable = std::env::current_exe().expect("test binary path should resolve");
        let mut runtime = Runtime::new();
        let mut host = Host::new();
        host.install_process_spawn();

        let program = Term::Perform {
            op: Symbol::from(PROCESS_SPAWN_OP),
            args: vec![Term::Value(Value::Record(BTreeMap::from([
                (
                    Symbol::from("argv"),
                    Value::List(vec![
                        Value::Bytes(path_to_bytes(&executable)),
                        Value::Bytes(b"--help".to_vec()),
                    ]),
                ),
                (
                    Symbol::from("env_mode"),
                    Value::Bytes(ENV_MODE_CLEAR.as_bytes().to_vec()),
                ),
                (Symbol::from("env"), Value::Record(BTreeMap::new())),
                (Symbol::from("stdin"), Value::Bytes(Vec::new())),
                (Symbol::from("declared_inputs"), Value::List(Vec::new())),
                (Symbol::from("declared_outputs"), Value::List(Vec::new())),
            ])))],
        };

        let result = runtime.run(program, &mut host).expect("program should run");

        match result {
            RuntimeValue::Data(Value::Tagged { tag, fields }) => {
                assert_eq!(tag, Symbol::from("ok"));
                assert_eq!(fields.len(), 1);
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn process_spawn_rejects_missing_hermetic_fields() {
        let mut runtime = Runtime::new();
        let mut host = Host::new();
        host.install_process_spawn();

        let program = Term::Perform {
            op: Symbol::from(PROCESS_SPAWN_OP),
            args: vec![Term::Value(Value::Record(BTreeMap::from([(
                Symbol::from("argv"),
                Value::List(vec![Value::Bytes(b"/bin/echo".to_vec())]),
            )])))],
        };

        let result = runtime.run(program, &mut host).expect("program should run");

        match result {
            RuntimeValue::Data(Value::Tagged { tag, fields }) => {
                assert_eq!(tag, Symbol::from("error"));
                assert_eq!(fields.len(), 1);
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn clock_now_runs_end_to_end_with_explicit_result_shape() {
        let mut runtime = Runtime::new();
        let mut host = Host::new();
        host.install_clock_now();

        let result = runtime
            .run(clock::now(), &mut host)
            .expect("program should run");

        match result {
            RuntimeValue::Data(value) => {
                let decoded = clock::decode_now_result(&value).expect("result should decode");
                assert!(decoded.unix_nanos > 0);
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn clock_sleep_runs_end_to_end_with_explicit_result_shape() {
        let mut runtime = Runtime::new();
        let mut host = Host::new();
        host.install_clock_sleep();

        let result = runtime
            .run(clock::sleep(0), &mut host)
            .expect("program should run");

        match result {
            RuntimeValue::Data(value) => {
                let decoded = clock::decode_sleep_result(&value).expect("result should decode");
                assert_eq!(decoded.duration_nanos, 0);
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn clock_effects_are_registered_as_volatile() {
        let mut host = Host::new();
        host.install_clock();

        assert_eq!(
            host.effect_policy(&Symbol::from(clock::NOW_OP)),
            HostEffectPolicy::volatile()
        );
        assert_eq!(
            host.effect_policy(&Symbol::from(clock::SLEEP_OP)),
            HostEffectPolicy::volatile()
        );
    }

    #[test]
    fn process_spawn_is_registered_as_declared() {
        let mut host = Host::new();
        host.install_process_spawn();

        assert_eq!(
            host.effect_policy(&Symbol::from(PROCESS_SPAWN_OP)),
            HostEffectPolicy::declared()
        );
    }

    #[test]
    fn hermetic_process_spawn_is_registered_as_hermetic() {
        let mut host = Host::new();
        host.install_hermetic_process_spawn();

        assert_eq!(
            host.effect_policy(&Symbol::from(PROCESS_SPAWN_OP)),
            HostEffectPolicy::hermetic()
        );
    }

    #[test]
    fn volatile_clock_effects_do_not_enter_thunk_cache() {
        let mut runtime = Runtime::new();
        let mut host = Host::new();
        host.install_clock_sleep();
        let thunk = thunk::delay(clock::sleep(0));
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
            RuntimeValue::Data(value) => {
                let decoded = clock::decode_sleep_result(&value).expect("result should decode");
                assert_eq!(decoded.duration_nanos, 0);
            }
            other => panic!("unexpected result: {other:?}"),
        }
        assert_eq!(runtime.thunk_cache_len(), 0);
    }

    fn unique_temp_path(prefix: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after unix epoch")
            .as_nanos();
        path.push(format!("{prefix}-{}-{nanos}", std::process::id()));
        path
    }

    #[cfg(unix)]
    fn path_to_bytes(path: &Path) -> Vec<u8> {
        use std::os::unix::ffi::OsStrExt;

        path.as_os_str().as_bytes().to_vec()
    }

    #[cfg(not(unix))]
    fn path_to_bytes(path: &Path) -> Vec<u8> {
        path.to_string_lossy().into_owned().into_bytes()
    }
}
