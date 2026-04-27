use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RestartMode {
    Never,
    Always,
    OnFailure,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RestartPolicy {
    mode: RestartMode,
    max_restarts: Option<u32>,
    delay_nanos: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RestartDecision {
    Stop,
    Restart { delay_nanos: i64 },
}

pub(super) fn handler(
    args: Vec<RuntimeValue>,
    continuation: Continuation,
    trace_events: HostTraceEvents,
    cancellation: &CancellationToken,
) -> Result<EvalResult, HostError> {
    let value = match args.as_slice() {
        [RuntimeValue::Data(Value::Record(request))] => {
            match run(request, trace_events, cancellation) {
                Ok(value) => RuntimeValue::Data(value),
                Err(message) => error_value(message),
            }
        }
        _ => error_value("service.supervise expected one record service spec argument"),
    };

    continuation.resume(value).map_err(Into::into)
}

fn run(
    request: &BTreeMap<Symbol, Value>,
    trace_events: HostTraceEvents,
    cancellation: &CancellationToken,
) -> Result<Value, String> {
    let (spawn_request, restart_policy) = parse_spec(request)?;
    let mut spawn_count = 0_u32;
    let mut restart_count = 0_u32;

    loop {
        check_cancelled(cancellation)?;
        spawn_count = spawn_count
            .checked_add(1)
            .ok_or_else(|| "service.supervise spawn count overflowed u32".to_string())?;
        push_trace(
            &trace_events,
            "spawn",
            [("iteration", Value::Integer(i64::from(spawn_count)))],
        );

        validate_declared_inputs(&spawn_request)?;
        enforce_process_sandbox(&spawn_request)?;
        ensure_declared_output_parents(&spawn_request)?;
        let output = execute_process(&spawn_request, cancellation)?;
        let status = process_status_from_exit_status(output.status);
        let status_value = process_status_value(&status);
        push_trace(
            &trace_events,
            "exit",
            [
                ("iteration", Value::Integer(i64::from(spawn_count))),
                ("status", status_value.clone()),
            ],
        );

        match restart_decision(restart_policy, &status, spawn_count)? {
            RestartDecision::Stop => {
                push_trace(
                    &trace_events,
                    "stop",
                    [
                        ("restart_count", Value::Integer(i64::from(restart_count))),
                        ("final_status", status_value.clone()),
                    ],
                );
                return Ok(result_value(status_value, restart_count));
            }
            RestartDecision::Restart { delay_nanos } => {
                restart_count = restart_count
                    .checked_add(1)
                    .ok_or_else(|| "service.supervise restart count overflowed u32".to_string())?;
                if delay_nanos > 0 {
                    sleep_with_cancellation(delay_nanos, cancellation)?;
                }
                push_trace(
                    &trace_events,
                    "restart",
                    [("next_iteration", Value::Integer(i64::from(spawn_count + 1)))],
                );
            }
        }
    }
}

fn parse_spec(
    request: &BTreeMap<Symbol, Value>,
) -> Result<(ProcessSpawnRequest, RestartPolicy), String> {
    let spawn_request = parse_process_request(request)?;
    let restart_policy = match request.get(&Symbol::from("restart_policy")) {
        Some(Value::Record(record)) => parse_restart_policy(record)?,
        Some(_) => {
            return Err("service.supervise field `restart_policy` must be a record".to_string());
        }
        None => RestartPolicy {
            mode: RestartMode::Never,
            max_restarts: None,
            delay_nanos: 0,
        },
    };

    Ok((spawn_request, restart_policy))
}

fn parse_restart_policy(record: &BTreeMap<Symbol, Value>) -> Result<RestartPolicy, String> {
    let mode = match required_record_field(record, "mode", SERVICE_SUPERVISE_OP)? {
        Value::Bytes(bytes) if bytes == b"never" => RestartMode::Never,
        Value::Bytes(bytes) if bytes == b"always" => RestartMode::Always,
        Value::Bytes(bytes) if bytes == b"on_failure" => RestartMode::OnFailure,
        Value::Symbol(symbol) if symbol.as_str() == "never" => RestartMode::Never,
        Value::Symbol(symbol) if symbol.as_str() == "always" => RestartMode::Always,
        Value::Symbol(symbol) if symbol.as_str() == "on_failure" => RestartMode::OnFailure,
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

    Ok(RestartPolicy {
        mode,
        max_restarts,
        delay_nanos,
    })
}

fn restart_decision(
    policy: RestartPolicy,
    status: &process::ProcessStatus,
    spawn_count: u32,
) -> Result<RestartDecision, String> {
    let should_restart = match policy.mode {
        RestartMode::Never => false,
        RestartMode::Always => true,
        RestartMode::OnFailure => !matches!(status, process::ProcessStatus::ExitCode(0)),
    };

    if !should_restart {
        return Ok(RestartDecision::Stop);
    }

    if let Some(max_restarts) = policy.max_restarts
        && spawn_count >= max_restarts
    {
        return Ok(RestartDecision::Stop);
    }

    Ok(RestartDecision::Restart {
        delay_nanos: policy.delay_nanos,
    })
}

fn result_value(final_status: Value, restart_count: u32) -> Value {
    ok_record_value([
        ("final_status", final_status),
        ("restart_count", Value::Integer(i64::from(restart_count))),
    ])
}

fn push_trace(
    trace_events: &HostTraceEvents,
    phase: &'static str,
    fields: impl IntoIterator<Item = (&'static str, Value)>,
) {
    trace_events
        .lock()
        .expect("host trace event mutex should not be poisoned")
        .push(HostTraceEvent::Lifecycle {
            op: Symbol::from(SERVICE_SUPERVISE_OP),
            phase: Symbol::from(phase),
            fields: fields
                .into_iter()
                .map(|(name, value)| (Symbol::from(name), value))
                .collect(),
        });
}
