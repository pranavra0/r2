use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;

use crate::data::{Symbol, Value};
use crate::eval::{Continuation, EvalError, EvalResult, RuntimeValue};

type Handler = dyn FnMut(Vec<RuntimeValue>, Continuation) -> Result<EvalResult, HostError>;

pub trait HostHandler {
    fn handle(
        &mut self,
        op: &Symbol,
        args: Vec<RuntimeValue>,
        continuation: Continuation,
    ) -> Result<Option<EvalResult>, HostError>;
}

pub struct Host {
    handlers: BTreeMap<Symbol, Box<Handler>>,
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
        self.handlers.insert(op.into(), Box::new(handler));
    }

    pub fn install_fs_read(&mut self) {
        self.register("fs.read", fs_read_handler);
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
            return handler(args, continuation).map(Some);
        }

        Ok(None)
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
    let value = match args.as_slice() {
        [RuntimeValue::Data(Value::Bytes(path))] => {
            let path = path_from_bytes(path.clone());
            match std::fs::read(path) {
                Ok(bytes) => RuntimeValue::Data(Value::Bytes(bytes)),
                Err(error) => error_value(error.to_string()),
            }
        }
        _ => error_value("fs.read expected one bytes path argument"),
    };

    continuation.resume(value).map_err(Into::into)
}

fn error_value(message: impl Into<String>) -> RuntimeValue {
    RuntimeValue::Data(Value::Tagged {
        tag: Symbol::from("error"),
        fields: vec![Value::Bytes(message.into().into_bytes())],
    })
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::data::Term;
    use crate::runtime::Runtime;

    #[test]
    fn fs_read_runs_end_to_end() {
        let path = unique_temp_path("r2-fs-read");
        let expected = b"hello from host".to_vec();
        std::fs::write(&path, &expected).expect("temp file should write");

        let mut runtime = Runtime::new();
        let mut host = Host::new();
        host.install_fs_read();

        let program = Term::Perform {
            op: Symbol::from("fs.read"),
            args: vec![Term::Value(Value::Bytes(path_to_bytes(&path)))],
        };

        let result = runtime.run(program, &mut host).expect("program should run");

        match result {
            RuntimeValue::Data(Value::Bytes(bytes)) => assert_eq!(bytes, expected),
            other => panic!("unexpected result: {other:?}"),
        }

        let _ = std::fs::remove_file(path);
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
    fn path_to_bytes(path: &PathBuf) -> Vec<u8> {
        use std::os::unix::ffi::OsStrExt;

        path.as_os_str().as_bytes().to_vec()
    }

    #[cfg(not(unix))]
    fn path_to_bytes(path: &PathBuf) -> Vec<u8> {
        path.to_string_lossy().into_owned().into_bytes()
    }
}
