use std::collections::BTreeMap;
use std::fmt;
use std::io::Write;
use std::io::{self, ErrorKind};
use std::path::PathBuf;
use std::process::{Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{Continuation, EvalError, EvalResult, RuntimeValue, Symbol, Value};

type Handler = dyn FnMut(Vec<RuntimeValue>, Continuation) -> Result<EvalResult, HostError>;

const FS_READ_OP: &str = "fs.read";
const FS_WRITE_OP: &str = "fs.write";
const PROCESS_SPAWN_OP: &str = "process.spawn";
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
}

impl HostEffectPolicy {
    pub const fn new(caching: HostEffectCaching, provenance: HostEffectProvenance) -> Self {
        Self {
            caching,
            provenance,
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
        Self::new(HostEffectCaching::Allow, HostEffectProvenance::Declared)
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
}

struct RegisteredHandler {
    handler: Box<Handler>,
    policy: HostEffectPolicy,
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

pub struct Host {
    handlers: BTreeMap<Symbol, RegisteredHandler>,
}

impl Host {
    pub fn new() -> Self {
        Self {
            handlers: BTreeMap::new(),
        }
    }

    pub fn register<F>(&mut self, op: impl Into<Symbol>, handler: F)
    where
        F: FnMut(Vec<RuntimeValue>, Continuation) -> Result<EvalResult, HostError> + 'static,
    {
        self.register_with_policy(op, HostEffectPolicy::volatile(), handler);
    }

    pub fn register_stable<F>(&mut self, op: impl Into<Symbol>, handler: F)
    where
        F: FnMut(Vec<RuntimeValue>, Continuation) -> Result<EvalResult, HostError> + 'static,
    {
        self.register_with_policy(op, HostEffectPolicy::stable(), handler);
    }

    pub fn register_declared<F>(&mut self, op: impl Into<Symbol>, handler: F)
    where
        F: FnMut(Vec<RuntimeValue>, Continuation) -> Result<EvalResult, HostError> + 'static,
    {
        self.register_with_policy(op, HostEffectPolicy::declared(), handler);
    }

    pub fn register_hermetic<F>(&mut self, op: impl Into<Symbol>, handler: F)
    where
        F: FnMut(Vec<RuntimeValue>, Continuation) -> Result<EvalResult, HostError> + 'static,
    {
        self.register_with_policy(op, HostEffectPolicy::hermetic(), handler);
    }

    pub fn register_with_policy<F>(
        &mut self,
        op: impl Into<Symbol>,
        policy: HostEffectPolicy,
        handler: F,
    ) where
        F: FnMut(Vec<RuntimeValue>, Continuation) -> Result<EvalResult, HostError> + 'static,
    {
        self.handlers.insert(
            op.into(),
            RegisteredHandler {
                handler: Box::new(handler),
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

    pub fn install_clock(&mut self) {
        self.install_clock_now();
        self.install_clock_sleep();
    }

    pub fn install_clock_now(&mut self) {
        self.register(CLOCK_NOW_OP, clock_now_handler);
    }

    pub fn install_clock_sleep(&mut self) {
        self.register(CLOCK_SLEEP_OP, clock_sleep_handler);
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
) -> Result<EvalResult, HostError> {
    let value = match args.as_slice() {
        [RuntimeValue::Data(Value::Record(request))] => match parse_sleep_request(request) {
            Ok(duration_nanos) => {
                thread::sleep(Duration::from_nanos(
                    u64::try_from(duration_nanos).expect("validated duration should fit u64"),
                ));
                RuntimeValue::Data(ok_record_value([(
                    "duration_nanos",
                    Value::Integer(duration_nanos),
                )]))
            }
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
    let output = execute_process(&request)?;
    Ok(RuntimeValue::Data(process_result_value(request, output)))
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

fn execute_process(request: &ProcessSpawnRequest) -> Result<std::process::Output, String> {
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

    child
        .wait_with_output()
        .map_err(|error| format!("process.spawn failed to wait for child: {error}"))
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

fn exit_status_value(status: ExitStatus) -> Value {
    match status.code() {
        Some(code) => Value::Tagged {
            tag: Symbol::from("exit_code"),
            fields: vec![Value::Integer(i64::from(code))],
        },
        None => {
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;

                if let Some(signal) = status.signal() {
                    return Value::Tagged {
                        tag: Symbol::from("signal"),
                        fields: vec![Value::Integer(i64::from(signal))],
                    };
                }
            }

            Value::Tagged {
                tag: Symbol::from("unknown_status"),
                fields: Vec::new(),
            }
        }
    }
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
    let mut record = BTreeMap::new();
    record.insert(Symbol::from("path"), Value::Bytes(path.clone()));
    record.insert(Symbol::from("contents"), read_declared_output_value(path));
    Value::Record(record)
}

fn read_declared_output_value(path: Vec<u8>) -> Value {
    match std::fs::read(path_from_bytes(path)) {
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
