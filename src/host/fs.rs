use super::*;

pub(super) fn read_handler(
    args: Vec<RuntimeValue>,
    continuation: Continuation,
) -> Result<EvalResult, HostError> {
    let value = match parse_read_args(args.as_slice()) {
        Ok(path) => RuntimeValue::Data(read_result_value(path)),
        Err(message) => error_value(message),
    };

    continuation.resume(value).map_err(Into::into)
}

pub(super) fn write_handler(
    args: Vec<RuntimeValue>,
    continuation: Continuation,
) -> Result<EvalResult, HostError> {
    let value = match parse_write_args(args.as_slice()) {
        Ok((path, contents)) => RuntimeValue::Data(write_result_value(path, contents)),
        Err(message) => error_value(message),
    };

    continuation.resume(value).map_err(Into::into)
}

fn parse_read_args(args: &[RuntimeValue]) -> Result<Vec<u8>, String> {
    match args {
        [RuntimeValue::Data(Value::Record(request))] => parse_read_request(request),
        [RuntimeValue::Data(Value::Bytes(path))] => Ok(path.clone()),
        _ => Err(
            "fs.read expected either one record request argument or one bytes path argument"
                .to_string(),
        ),
    }
}

fn parse_write_args(args: &[RuntimeValue]) -> Result<(Vec<u8>, Vec<u8>), String> {
    match args {
        [RuntimeValue::Data(Value::Record(request))] => parse_write_request(request),
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

fn parse_read_request(request: &BTreeMap<Symbol, Value>) -> Result<Vec<u8>, String> {
    parse_bytes(
        required_record_field(request, "path", FS_READ_OP)?,
        "fs.read field `path` must be bytes",
    )
}

fn parse_write_request(request: &BTreeMap<Symbol, Value>) -> Result<(Vec<u8>, Vec<u8>), String> {
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

fn read_result_value(path: Vec<u8>) -> Value {
    match std::fs::read(path_from_bytes(path)) {
        Ok(contents) => ok_record_value([("contents", Value::Bytes(contents))]),
        Err(error) => io_error_value(error),
    }
}

fn write_result_value(path: Vec<u8>, contents: Vec<u8>) -> Value {
    match std::fs::write(path_from_bytes(path), &contents) {
        Ok(()) => ok_record_value([(
            "written",
            Value::Integer(i64::try_from(contents.len()).expect("byte length should fit into i64")),
        )]),
        Err(error) => io_error_value(error),
    }
}
