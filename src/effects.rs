use std::collections::BTreeMap;
use std::fmt;

use crate::{Symbol, Term, Value};

pub mod fs {
    use super::*;

    pub const READ_OP: &str = "fs.read";
    pub const WRITE_OP: &str = "fs.write";

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct ReadRequest {
        pub path: Vec<u8>,
    }

    impl ReadRequest {
        pub fn new(path: impl Into<Vec<u8>>) -> Self {
            Self { path: path.into() }
        }

        pub fn to_value(&self) -> Value {
            Value::Record(BTreeMap::from([(
                Symbol::from("path"),
                Value::Bytes(self.path.clone()),
            )]))
        }

        pub fn into_term(self) -> Term {
            Term::Perform {
                op: Symbol::from(READ_OP),
                args: vec![Term::Value(self.to_value())],
            }
        }
    }

    pub fn read(path: impl Into<Vec<u8>>) -> Term {
        ReadRequest::new(path).into_term()
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct WriteRequest {
        pub path: Vec<u8>,
        pub contents: Vec<u8>,
    }

    impl WriteRequest {
        pub fn new(path: impl Into<Vec<u8>>, contents: impl Into<Vec<u8>>) -> Self {
            Self {
                path: path.into(),
                contents: contents.into(),
            }
        }

        pub fn to_value(&self) -> Value {
            Value::Record(BTreeMap::from([
                (Symbol::from("path"), Value::Bytes(self.path.clone())),
                (
                    Symbol::from("contents"),
                    Value::Bytes(self.contents.clone()),
                ),
            ]))
        }

        pub fn into_term(self) -> Term {
            Term::Perform {
                op: Symbol::from(WRITE_OP),
                args: vec![Term::Value(self.to_value())],
            }
        }
    }

    pub fn write(path: impl Into<Vec<u8>>, contents: impl Into<Vec<u8>>) -> Term {
        WriteRequest::new(path, contents).into_term()
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum ErrorKind {
        NotFound,
        PermissionDenied,
        AlreadyExists,
        InvalidInput,
        Interrupted,
        Other,
    }

    impl ErrorKind {
        fn from_tag(tag: &str) -> Option<Self> {
            match tag {
                "not_found" => Some(Self::NotFound),
                "permission_denied" => Some(Self::PermissionDenied),
                "already_exists" => Some(Self::AlreadyExists),
                "invalid_input" => Some(Self::InvalidInput),
                "interrupted" => Some(Self::Interrupted),
                "other_error" => Some(Self::Other),
                _ => None,
            }
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct ErrorResult {
        pub kind: ErrorKind,
        pub message: String,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub enum ReadResult {
        Ok { contents: Vec<u8> },
        Error(ErrorResult),
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub enum WriteResult {
        Ok { written: i64 },
        Error(ErrorResult),
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct DecodeError(pub String);

    impl fmt::Display for DecodeError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(&self.0)
        }
    }

    impl std::error::Error for DecodeError {}

    pub fn decode_read_result(value: &Value) -> Result<ReadResult, DecodeError> {
        let (tag, record) = decode_tagged_record(value, "fs.read")?;
        match tag {
            "ok" => Ok(ReadResult::Ok {
                contents: decode_bytes(required_field(record, "contents", "fs.read")?, "contents")?,
            }),
            _ => Ok(ReadResult::Error(decode_error_result(tag, record, "fs.read")?)),
        }
    }

    pub fn decode_write_result(value: &Value) -> Result<WriteResult, DecodeError> {
        let (tag, record) = decode_tagged_record(value, "fs.write")?;
        match tag {
            "ok" => Ok(WriteResult::Ok {
                written: decode_integer(required_field(record, "written", "fs.write")?, "written")?,
            }),
            _ => Ok(WriteResult::Error(decode_error_result(tag, record, "fs.write")?)),
        }
    }

    fn decode_tagged_record<'a>(
        value: &'a Value,
        effect_name: &str,
    ) -> Result<(&'a str, &'a BTreeMap<Symbol, Value>), DecodeError> {
        let Value::Tagged { tag, fields } = value else {
            return Err(DecodeError(format!(
                "{effect_name} result must be a tagged value"
            )));
        };
        if fields.len() != 1 {
            return Err(DecodeError(format!(
                "{effect_name} result must contain exactly one record payload"
            )));
        }
        let Value::Record(record) = &fields[0] else {
            return Err(DecodeError(format!(
                "{effect_name} result payload must be a record"
            )));
        };
        Ok((tag.as_str(), record))
    }

    fn decode_error_result(
        tag: &str,
        record: &BTreeMap<Symbol, Value>,
        effect_name: &str,
    ) -> Result<ErrorResult, DecodeError> {
        let Some(kind) = ErrorKind::from_tag(tag) else {
            return Err(DecodeError(format!(
                "{effect_name} result had unrecognized tag `{tag}`"
            )));
        };
        Ok(ErrorResult {
            kind,
            message: decode_string(
                required_field(record, "message", effect_name)?,
                "message",
            )?,
        })
    }

    fn required_field<'a>(
        record: &'a BTreeMap<Symbol, Value>,
        name: &str,
        effect_name: &str,
    ) -> Result<&'a Value, DecodeError> {
        record.get(&Symbol::from(name)).ok_or_else(|| {
            DecodeError(format!(
                "missing {effect_name} result field `{name}`"
            ))
        })
    }

    fn decode_bytes(value: &Value, field: &str) -> Result<Vec<u8>, DecodeError> {
        match value {
            Value::Bytes(bytes) => Ok(bytes.clone()),
            _ => Err(DecodeError(format!("field `{field}` must be bytes"))),
        }
    }

    fn decode_string(value: &Value, field: &str) -> Result<String, DecodeError> {
        Ok(String::from_utf8_lossy(&decode_bytes(value, field)?).into_owned())
    }

    fn decode_integer(value: &Value, field: &str) -> Result<i64, DecodeError> {
        match value {
            Value::Integer(value) => Ok(*value),
            _ => Err(DecodeError(format!("field `{field}` must be an integer"))),
        }
    }
}

pub mod clock {
    use super::*;

    pub const NOW_OP: &str = "clock.now";
    pub const SLEEP_OP: &str = "clock.sleep";

    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    pub struct NowRequest;

    impl NowRequest {
        pub fn to_value(&self) -> Value {
            Value::Record(BTreeMap::new())
        }

        pub fn into_term(self) -> Term {
            Term::Perform {
                op: Symbol::from(NOW_OP),
                args: vec![Term::Value(self.to_value())],
            }
        }
    }

    pub fn now() -> Term {
        NowRequest.into_term()
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct NowResult {
        pub unix_nanos: i64,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct SleepRequest {
        pub duration_nanos: i64,
    }

    impl SleepRequest {
        pub fn to_value(&self) -> Value {
            Value::Record(BTreeMap::from([(
                Symbol::from("duration_nanos"),
                Value::Integer(self.duration_nanos),
            )]))
        }

        pub fn into_term(self) -> Term {
            Term::Perform {
                op: Symbol::from(SLEEP_OP),
                args: vec![Term::Value(self.to_value())],
            }
        }
    }

    pub fn sleep(duration_nanos: i64) -> Term {
        SleepRequest { duration_nanos }.into_term()
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct SleepResult {
        pub duration_nanos: i64,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct DecodeError(pub String);

    impl fmt::Display for DecodeError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(&self.0)
        }
    }

    impl std::error::Error for DecodeError {}

    pub fn decode_now_result(value: &Value) -> Result<NowResult, DecodeError> {
        let record = decode_ok_record(value, "clock.now")?;
        Ok(NowResult {
            unix_nanos: decode_integer(required_field(record, "unix_nanos")?, "unix_nanos")?,
        })
    }

    pub fn decode_sleep_result(value: &Value) -> Result<SleepResult, DecodeError> {
        let record = decode_ok_record(value, "clock.sleep")?;
        Ok(SleepResult {
            duration_nanos: decode_integer(
                required_field(record, "duration_nanos")?,
                "duration_nanos",
            )?,
        })
    }

    fn decode_ok_record<'a>(
        value: &'a Value,
        effect_name: &str,
    ) -> Result<&'a BTreeMap<Symbol, Value>, DecodeError> {
        let Value::Tagged { tag, fields } = value else {
            return Err(DecodeError(format!(
                "{effect_name} result must be a tagged value"
            )));
        };
        if tag.as_str() != "ok" || fields.len() != 1 {
            return Err(DecodeError(format!(
                "{effect_name} result must be ok(record)"
            )));
        }

        let Value::Record(record) = &fields[0] else {
            return Err(DecodeError(format!(
                "{effect_name} result payload must be a record"
            )));
        };

        Ok(record)
    }

    fn required_field<'a>(
        record: &'a BTreeMap<Symbol, Value>,
        name: &str,
    ) -> Result<&'a Value, DecodeError> {
        record
            .get(&Symbol::from(name))
            .ok_or_else(|| DecodeError(format!("missing clock result field `{name}`")))
    }

    fn decode_integer(value: &Value, field: &str) -> Result<i64, DecodeError> {
        match value {
            Value::Integer(value) => Ok(*value),
            _ => Err(DecodeError(format!("field `{field}` must be an integer"))),
        }
    }
}

pub mod process {
    use super::*;

    pub const SPAWN_OP: &str = "process.spawn";

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum EnvMode {
        Clear,
        Inherit,
    }

    impl EnvMode {
        fn as_bytes(self) -> &'static [u8] {
            match self {
                Self::Clear => b"clear",
                Self::Inherit => b"inherit",
            }
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct SpawnRequest {
        pub argv: Vec<Vec<u8>>,
        pub env_mode: EnvMode,
        pub env: BTreeMap<Symbol, Vec<u8>>,
        pub cwd: Option<Vec<u8>>,
        pub stdin: Vec<u8>,
        pub declared_inputs: Vec<Vec<u8>>,
        pub declared_outputs: Vec<Vec<u8>>,
    }

    impl SpawnRequest {
        pub fn new(argv: impl IntoIterator<Item = Vec<u8>>) -> Self {
            Self {
                argv: argv.into_iter().collect(),
                env_mode: EnvMode::Clear,
                env: BTreeMap::new(),
                cwd: None,
                stdin: Vec::new(),
                declared_inputs: Vec::new(),
                declared_outputs: Vec::new(),
            }
        }

        pub fn inherit_env(mut self) -> Self {
            self.env_mode = EnvMode::Inherit;
            self
        }

        pub fn cwd(mut self, cwd: impl Into<Vec<u8>>) -> Self {
            self.cwd = Some(cwd.into());
            self
        }

        pub fn stdin(mut self, stdin: impl Into<Vec<u8>>) -> Self {
            self.stdin = stdin.into();
            self
        }

        pub fn env(mut self, name: impl Into<Symbol>, value: impl Into<Vec<u8>>) -> Self {
            self.env.insert(name.into(), value.into());
            self
        }

        pub fn declared_input(mut self, path: impl Into<Vec<u8>>) -> Self {
            self.declared_inputs.push(path.into());
            self
        }

        pub fn declared_output(mut self, path: impl Into<Vec<u8>>) -> Self {
            self.declared_outputs.push(path.into());
            self
        }

        pub fn to_value(&self) -> Value {
            let mut record = BTreeMap::new();
            record.insert(
                Symbol::from("argv"),
                Value::List(self.argv.iter().cloned().map(Value::Bytes).collect()),
            );
            record.insert(
                Symbol::from("env_mode"),
                Value::Bytes(self.env_mode.as_bytes().to_vec()),
            );
            record.insert(
                Symbol::from("env"),
                Value::Record(
                    self.env
                        .iter()
                        .map(|(key, value)| (key.clone(), Value::Bytes(value.clone())))
                        .collect(),
                ),
            );
            if let Some(cwd) = &self.cwd {
                record.insert(Symbol::from("cwd"), Value::Bytes(cwd.clone()));
            }
            record.insert(Symbol::from("stdin"), Value::Bytes(self.stdin.clone()));
            record.insert(
                Symbol::from("declared_inputs"),
                Value::List(
                    self.declared_inputs
                        .iter()
                        .cloned()
                        .map(Value::Bytes)
                        .collect(),
                ),
            );
            record.insert(
                Symbol::from("declared_outputs"),
                Value::List(
                    self.declared_outputs
                        .iter()
                        .cloned()
                        .map(Value::Bytes)
                        .collect(),
                ),
            );
            Value::Record(record)
        }

        pub fn into_term(self) -> Term {
            Term::Perform {
                op: Symbol::from(SPAWN_OP),
                args: vec![Term::Value(self.to_value())],
            }
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub enum ProcessStatus {
        ExitCode(i64),
        Signal(i64),
        Unknown,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct DeclaredOutputFile {
        pub path: Vec<u8>,
        pub contents: Result<Vec<u8>, String>,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct SpawnResult {
        pub status: ProcessStatus,
        pub stdout: Vec<u8>,
        pub stderr: Vec<u8>,
        pub declared_inputs: Vec<Vec<u8>>,
        pub declared_outputs: Vec<Vec<u8>>,
        pub output_files: Vec<DeclaredOutputFile>,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct DecodeError(pub String);

    impl fmt::Display for DecodeError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(&self.0)
        }
    }

    impl std::error::Error for DecodeError {}

    pub fn decode_result(value: &Value) -> Result<SpawnResult, DecodeError> {
        let Value::Tagged { tag, fields } = value else {
            return Err(DecodeError(
                "process result must be a tagged value".to_string(),
            ));
        };
        if tag.as_str() != "ok" || fields.len() != 1 {
            return Err(DecodeError("process result must be ok(record)".to_string()));
        }

        let Value::Record(record) = &fields[0] else {
            return Err(DecodeError(
                "process result payload must be a record".to_string(),
            ));
        };

        Ok(SpawnResult {
            status: decode_status(required_field(record, "status")?)?,
            stdout: decode_bytes(required_field(record, "stdout")?, "stdout")?,
            stderr: decode_bytes(required_field(record, "stderr")?, "stderr")?,
            declared_inputs: decode_bytes_list(
                required_field(record, "declared_inputs")?,
                "declared_inputs",
            )?,
            declared_outputs: decode_bytes_list(
                required_field(record, "declared_outputs")?,
                "declared_outputs",
            )?,
            output_files: decode_output_files(required_field(record, "output_files")?)?,
        })
    }

    fn required_field<'a>(
        record: &'a BTreeMap<Symbol, Value>,
        name: &str,
    ) -> Result<&'a Value, DecodeError> {
        record
            .get(&Symbol::from(name))
            .ok_or_else(|| DecodeError(format!("missing process result field `{name}`")))
    }

    fn decode_status(value: &Value) -> Result<ProcessStatus, DecodeError> {
        let Value::Tagged { tag, fields } = value else {
            return Err(DecodeError("status must be tagged".to_string()));
        };
        match (tag.as_str(), fields.as_slice()) {
            ("exit_code", [Value::Integer(code)]) => Ok(ProcessStatus::ExitCode(*code)),
            ("signal", [Value::Integer(signal)]) => Ok(ProcessStatus::Signal(*signal)),
            ("unknown_status", []) => Ok(ProcessStatus::Unknown),
            _ => Err(DecodeError("unrecognized process status".to_string())),
        }
    }

    fn decode_bytes(value: &Value, field: &str) -> Result<Vec<u8>, DecodeError> {
        match value {
            Value::Bytes(bytes) => Ok(bytes.clone()),
            _ => Err(DecodeError(format!("field `{field}` must be bytes"))),
        }
    }

    fn decode_bytes_list(value: &Value, field: &str) -> Result<Vec<Vec<u8>>, DecodeError> {
        match value {
            Value::List(items) => items
                .iter()
                .map(|item| match item {
                    Value::Bytes(bytes) => Ok(bytes.clone()),
                    _ => Err(DecodeError(format!(
                        "field `{field}` must be a list of bytes"
                    ))),
                })
                .collect(),
            _ => Err(DecodeError(format!(
                "field `{field}` must be a list of bytes"
            ))),
        }
    }

    fn decode_output_files(value: &Value) -> Result<Vec<DeclaredOutputFile>, DecodeError> {
        let Value::List(items) = value else {
            return Err(DecodeError(
                "field `output_files` must be a list".to_string(),
            ));
        };

        items.iter().map(decode_output_file).collect()
    }

    fn decode_output_file(value: &Value) -> Result<DeclaredOutputFile, DecodeError> {
        let Value::Record(record) = value else {
            return Err(DecodeError(
                "output file entries must be records".to_string(),
            ));
        };
        let path = decode_bytes(required_field(record, "path")?, "output_files.path")?;
        let contents = decode_output_contents(required_field(record, "contents")?)?;
        Ok(DeclaredOutputFile { path, contents })
    }

    fn decode_output_contents(value: &Value) -> Result<Result<Vec<u8>, String>, DecodeError> {
        let Value::Tagged { tag, fields } = value else {
            return Err(DecodeError(
                "output file contents must be tagged".to_string(),
            ));
        };
        match (tag.as_str(), fields.as_slice()) {
            ("ok", [Value::Bytes(bytes)]) => Ok(Ok(bytes.clone())),
            ("error", [Value::Bytes(bytes)]) => {
                Ok(Err(String::from_utf8_lossy(bytes).into_owned()))
            }
            _ => Err(DecodeError(
                "output file contents must be ok(bytes) or error(bytes)".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fs::{
        ErrorKind as FsErrorKind, ReadRequest, ReadResult, WriteRequest, WriteResult,
        decode_read_result, decode_write_result,
    };
    use super::clock::{NowRequest, SleepRequest, decode_now_result, decode_sleep_result};
    use super::process::{DeclaredOutputFile, ProcessStatus, SpawnRequest, decode_result};
    use crate::{Symbol, Value};
    use std::collections::BTreeMap;

    #[test]
    fn fs_requests_build_explicit_record_shapes() {
        let Value::Record(read_record) = ReadRequest::new(b"/tmp/input".to_vec()).to_value() else {
            panic!("read request should encode as a record");
        };
        assert_eq!(
            read_record.get(&Symbol::from("path")),
            Some(&Value::Bytes(b"/tmp/input".to_vec()))
        );

        let Value::Record(write_record) =
            WriteRequest::new(b"/tmp/output".to_vec(), b"hello".to_vec()).to_value()
        else {
            panic!("write request should encode as a record");
        };
        assert_eq!(
            write_record.get(&Symbol::from("path")),
            Some(&Value::Bytes(b"/tmp/output".to_vec()))
        );
        assert_eq!(
            write_record.get(&Symbol::from("contents")),
            Some(&Value::Bytes(b"hello".to_vec()))
        );
    }

    #[test]
    fn fs_result_decoders_read_typed_payloads() {
        let read_ok = Value::Tagged {
            tag: Symbol::from("ok"),
            fields: vec![Value::Record(BTreeMap::from([(
                Symbol::from("contents"),
                Value::Bytes(b"hello".to_vec()),
            )]))],
        };
        let write_error = Value::Tagged {
            tag: Symbol::from("not_found"),
            fields: vec![Value::Record(BTreeMap::from([(
                Symbol::from("message"),
                Value::Bytes(b"missing".to_vec()),
            )]))],
        };

        assert_eq!(
            decode_read_result(&read_ok).unwrap(),
            ReadResult::Ok {
                contents: b"hello".to_vec()
            }
        );
        assert_eq!(
            decode_write_result(&write_error).unwrap(),
            WriteResult::Error(super::fs::ErrorResult {
                kind: FsErrorKind::NotFound,
                message: "missing".to_string(),
            })
        );
    }

    #[test]
    fn clock_requests_build_explicit_record_shapes() {
        let Value::Record(now_record) = NowRequest.to_value() else {
            panic!("now request should encode as a record");
        };
        assert!(now_record.is_empty());

        let Value::Record(sleep_record) = SleepRequest {
            duration_nanos: 123,
        }
        .to_value() else {
            panic!("sleep request should encode as a record");
        };

        assert_eq!(
            sleep_record.get(&Symbol::from("duration_nanos")),
            Some(&Value::Integer(123))
        );
    }

    #[test]
    fn clock_result_decoders_read_typed_payloads() {
        let now_value = Value::Tagged {
            tag: Symbol::from("ok"),
            fields: vec![Value::Record(BTreeMap::from([(
                Symbol::from("unix_nanos"),
                Value::Integer(42),
            )]))],
        };
        let sleep_value = Value::Tagged {
            tag: Symbol::from("ok"),
            fields: vec![Value::Record(BTreeMap::from([(
                Symbol::from("duration_nanos"),
                Value::Integer(7),
            )]))],
        };

        assert_eq!(decode_now_result(&now_value).unwrap().unix_nanos, 42);
        assert_eq!(decode_sleep_result(&sleep_value).unwrap().duration_nanos, 7);
    }

    #[test]
    fn process_request_builds_explicit_record_shape() {
        let request = SpawnRequest::new(vec![b"/bin/tool".to_vec(), b"--flag".to_vec()])
            .env(Symbol::from("HOME"), b"/tmp".to_vec())
            .cwd(b"/work".to_vec())
            .stdin(b"hello".to_vec())
            .declared_input(b"/input".to_vec())
            .declared_output(b"/output".to_vec());

        let Value::Record(record) = request.to_value() else {
            panic!("request should encode as a record");
        };

        assert!(record.contains_key(&Symbol::from("argv")));
        assert!(record.contains_key(&Symbol::from("env_mode")));
        assert!(record.contains_key(&Symbol::from("env")));
        assert!(record.contains_key(&Symbol::from("stdin")));
        assert!(record.contains_key(&Symbol::from("declared_inputs")));
        assert!(record.contains_key(&Symbol::from("declared_outputs")));
    }

    #[test]
    fn process_result_decoder_reads_output_files() {
        let mut inner = BTreeMap::new();
        inner.insert(
            Symbol::from("status"),
            Value::Tagged {
                tag: Symbol::from("exit_code"),
                fields: vec![Value::Integer(0)],
            },
        );
        inner.insert(Symbol::from("stdout"), Value::Bytes(b"stdout".to_vec()));
        inner.insert(Symbol::from("stderr"), Value::Bytes(Vec::new()));
        inner.insert(
            Symbol::from("declared_inputs"),
            Value::List(vec![Value::Bytes(b"/in".to_vec())]),
        );
        inner.insert(
            Symbol::from("declared_outputs"),
            Value::List(vec![Value::Bytes(b"/out".to_vec())]),
        );
        inner.insert(
            Symbol::from("output_files"),
            Value::List(vec![Value::Record(BTreeMap::from([
                (Symbol::from("path"), Value::Bytes(b"/out".to_vec())),
                (
                    Symbol::from("contents"),
                    Value::Tagged {
                        tag: Symbol::from("ok"),
                        fields: vec![Value::Bytes(b"artifact".to_vec())],
                    },
                ),
            ]))]),
        );

        let value = Value::Tagged {
            tag: Symbol::from("ok"),
            fields: vec![Value::Record(inner)],
        };

        let decoded = decode_result(&value).expect("result should decode");

        assert_eq!(decoded.status, ProcessStatus::ExitCode(0));
        assert_eq!(decoded.stdout, b"stdout".to_vec());
        assert_eq!(
            decoded.output_files,
            vec![DeclaredOutputFile {
                path: b"/out".to_vec(),
                contents: Ok(b"artifact".to_vec()),
            }]
        );
    }
}
