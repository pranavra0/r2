use super::*;
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) fn now_handler(
    args: Vec<RuntimeValue>,
    continuation: Continuation,
) -> Result<EvalResult, HostError> {
    let value = match args.as_slice() {
        [RuntimeValue::Data(Value::Record(request))] => {
            match parse_empty_request(request, CLOCK_NOW_OP) {
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

pub(super) fn sleep_handler(
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

fn parse_empty_request(request: &BTreeMap<Symbol, Value>, op: &str) -> Result<(), String> {
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

fn unix_time_nanos() -> Result<i64, String> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("clock.now failed to read unix time: {error}"))?;
    i64::try_from(duration.as_nanos())
        .map_err(|_| "clock.now unix time exceeded i64 nanosecond range".to_string())
}
