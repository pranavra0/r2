#[cfg(unix)]
use std::collections::BTreeMap;
#[cfg(unix)]
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::process::{Command, Stdio};
#[cfg(unix)]
use std::time::{Duration, Instant};
#[cfg(unix)]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use r2::{Host, Runtime, RuntimeTraceEvent, RuntimeValue, Symbol, Term, Value};

#[cfg(unix)]
#[test]
fn service_supervise_stops_after_one_successful_spawn() {
    let marker_path = unique_temp_path("r2-service-success-marker", "txt");
    let mut runtime = Runtime::new();
    let mut host = Host::new();
    host.install_service_supervise();

    let traced = runtime
        .run_with_trace(
            service_supervise_term(
                r#"printf 'run\n' >> "$1"; exit 0"#,
                &marker_path,
                "on_failure",
                Some(3),
            ),
            &mut host,
        )
        .expect("service should run");

    assert_supervise_result(&traced.value, 0, 0);
    assert_eq!(std::fs::read_to_string(&marker_path).unwrap(), "run\n");
    assert_eq!(traced.trace.summary().service_spawns, 1);
    assert_eq!(traced.trace.summary().service_exits, 1);
    assert_eq!(traced.trace.summary().service_restarts, 0);
    assert_eq!(traced.trace.summary().service_stops, 1);

    let _ = std::fs::remove_file(marker_path);
}

#[cfg(unix)]
#[test]
fn service_supervise_restarts_failures_until_limit() {
    let marker_path = unique_temp_path("r2-service-failure-marker", "txt");
    let mut runtime = Runtime::new();
    let mut host = Host::new();
    host.install_service_supervise();

    let traced = runtime
        .run_with_trace(
            service_supervise_term(
                r#"printf 'run\n' >> "$1"; exit 1"#,
                &marker_path,
                "on_failure",
                Some(3),
            ),
            &mut host,
        )
        .expect("service should run");

    assert_supervise_result(&traced.value, 1, 2);
    assert_eq!(
        std::fs::read_to_string(&marker_path).unwrap(),
        "run\nrun\nrun\n"
    );
    assert_eq!(traced.trace.summary().service_spawns, 3);
    assert_eq!(traced.trace.summary().service_exits, 3);
    assert_eq!(traced.trace.summary().service_restarts, 2);
    assert_eq!(traced.trace.summary().service_stops, 1);

    let events = traced.trace.events();
    assert!(matches!(
        events
            .iter()
            .find(|event| matches!(event, RuntimeTraceEvent::ServiceSpawn { iteration: 1 })),
        Some(RuntimeTraceEvent::ServiceSpawn { iteration: 1 })
    ));
    assert_event_order(events);

    let _ = std::fs::remove_file(marker_path);
}

#[cfg(unix)]
#[test]
fn cli_can_run_service_supervise_program() {
    let program_path = unique_temp_path("r2-cli-service-program", "r2");
    let marker_path = unique_temp_path("r2-cli-service-marker", "txt");
    let script = r#"printf 'run\n' >> "$1"; exit 0"#;
    let program = format!(
        "perform service.supervise({{ argv: [{}, {}, {}, {}, {}], env_mode: {}, env: {{}}, stdin: {}, declared_inputs: [], declared_outputs: [{}], restart_policy: {{ mode: {}, max_restarts: 3, delay_nanos: 0 }} }})",
        string_literal("/bin/sh"),
        string_literal("-c"),
        string_literal(script),
        string_literal("sh"),
        string_literal(marker_path.to_string_lossy().as_ref()),
        string_literal("clear"),
        string_literal(""),
        string_literal(marker_path.to_string_lossy().as_ref()),
        string_literal("on_failure"),
    );
    std::fs::write(&program_path, program).expect("program should write");

    let output = Command::new(env!("CARGO_BIN_EXE_r2"))
        .arg("run")
        .arg("--memory-store")
        .arg(&program_path)
        .output()
        .expect("cli should run");

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("ok({"), "{stdout}");
    assert!(stdout.contains("final_status: exit_code(0)"), "{stdout}");
    assert!(stdout.contains("restart_count: 0"), "{stdout}");
    assert_eq!(std::fs::read_to_string(&marker_path).unwrap(), "run\n");

    let _ = std::fs::remove_file(program_path);
    let _ = std::fs::remove_file(marker_path);
}

#[cfg(unix)]
#[test]
fn cli_sigint_cancels_supervised_service_without_orphaning_child() {
    let program_path = unique_temp_path("r2-cli-service-cancel-program", "r2");
    let pid_path = unique_temp_path("r2-cli-service-cancel-pid", "txt");
    let script = r#"printf '%s' "$$" > "$1"; sleep 30"#;
    let program = format!(
        "perform service.supervise({{ argv: [{}, {}, {}, {}, {}], env_mode: {}, env: {{}}, stdin: {}, declared_inputs: [], declared_outputs: [{}], restart_policy: {{ mode: {}, delay_nanos: 0 }} }})",
        string_literal("/bin/sh"),
        string_literal("-c"),
        string_literal(script),
        string_literal("sh"),
        string_literal(pid_path.to_string_lossy().as_ref()),
        string_literal("clear"),
        string_literal(""),
        string_literal(pid_path.to_string_lossy().as_ref()),
        string_literal("never"),
    );
    std::fs::write(&program_path, program).expect("program should write");

    let child = Command::new(env!("CARGO_BIN_EXE_r2"))
        .arg("run")
        .arg("--memory-store")
        .arg(&program_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("cli should start");

    wait_for_path(&pid_path);
    let supervised_pid = std::fs::read_to_string(&pid_path)
        .expect("pid should read")
        .parse::<libc::pid_t>()
        .expect("pid should parse");

    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGINT);
    }

    let output = child.wait_with_output().expect("cli should exit");

    assert!(!output.status.success());
    assert!(stderr(&output).contains("cancelled"));
    wait_until_not_running(supervised_pid);
    assert!(
        !process_is_running(supervised_pid),
        "supervised child should not survive cancellation"
    );

    let _ = std::fs::remove_file(program_path);
    let _ = std::fs::remove_file(pid_path);
}

#[cfg(unix)]
fn service_supervise_term(
    script: &str,
    marker_path: &Path,
    restart_mode: &str,
    max_restarts: Option<i64>,
) -> Term {
    let mut restart_policy = BTreeMap::from([
        (
            Symbol::from("mode"),
            Value::Bytes(restart_mode.as_bytes().to_vec()),
        ),
        (Symbol::from("delay_nanos"), Value::Integer(0)),
    ]);
    if let Some(max_restarts) = max_restarts {
        restart_policy.insert(Symbol::from("max_restarts"), Value::Integer(max_restarts));
    }

    Term::Perform {
        op: Symbol::from("service.supervise"),
        args: vec![Term::Value(Value::Record(BTreeMap::from([
            (
                Symbol::from("argv"),
                Value::List(vec![
                    Value::Bytes(b"/bin/sh".to_vec()),
                    Value::Bytes(b"-c".to_vec()),
                    Value::Bytes(script.as_bytes().to_vec()),
                    Value::Bytes(b"sh".to_vec()),
                    Value::Bytes(path_to_bytes(marker_path)),
                ]),
            ),
            (Symbol::from("env_mode"), Value::Bytes(b"clear".to_vec())),
            (Symbol::from("env"), Value::Record(BTreeMap::new())),
            (Symbol::from("stdin"), Value::Bytes(Vec::new())),
            (Symbol::from("declared_inputs"), Value::List(Vec::new())),
            (
                Symbol::from("declared_outputs"),
                Value::List(vec![Value::Bytes(path_to_bytes(marker_path))]),
            ),
            (
                Symbol::from("restart_policy"),
                Value::Record(restart_policy),
            ),
        ])))],
    }
}

#[cfg(unix)]
fn assert_supervise_result(value: &RuntimeValue, expected_status: i64, expected_restarts: i64) {
    let RuntimeValue::Data(Value::Tagged { tag, fields }) = value else {
        panic!("unexpected result: {value:?}");
    };
    assert_eq!(tag, &Symbol::from("ok"));
    let [Value::Record(record)] = fields.as_slice() else {
        panic!("unexpected result fields: {fields:?}");
    };
    assert_eq!(
        record.get(&Symbol::from("final_status")),
        Some(&Value::Tagged {
            tag: Symbol::from("exit_code"),
            fields: vec![Value::Integer(expected_status)],
        })
    );
    assert_eq!(
        record.get(&Symbol::from("restart_count")),
        Some(&Value::Integer(expected_restarts))
    );
}

#[cfg(unix)]
fn assert_event_order(events: &[RuntimeTraceEvent]) {
    let service_events = events
        .iter()
        .filter(|event| {
            matches!(
                event,
                RuntimeTraceEvent::ServiceSpawn { .. }
                    | RuntimeTraceEvent::ServiceExit { .. }
                    | RuntimeTraceEvent::ServiceRestart { .. }
                    | RuntimeTraceEvent::ServiceStop { .. }
            )
        })
        .collect::<Vec<_>>();

    assert!(matches!(
        service_events.as_slice(),
        [
            RuntimeTraceEvent::ServiceSpawn { iteration: 1 },
            RuntimeTraceEvent::ServiceExit { iteration: 1, .. },
            RuntimeTraceEvent::ServiceRestart { next_iteration: 2 },
            RuntimeTraceEvent::ServiceSpawn { iteration: 2 },
            RuntimeTraceEvent::ServiceExit { iteration: 2, .. },
            RuntimeTraceEvent::ServiceRestart { next_iteration: 3 },
            RuntimeTraceEvent::ServiceSpawn { iteration: 3 },
            RuntimeTraceEvent::ServiceExit { iteration: 3, .. },
            RuntimeTraceEvent::ServiceStop { .. },
        ]
    ));
}

#[cfg(unix)]
fn path_to_bytes(path: &Path) -> Vec<u8> {
    path.to_string_lossy().as_bytes().to_vec()
}

#[cfg(unix)]
fn unique_temp_path(prefix: &str, extension: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "{prefix}-{}-{nanos}.{extension}",
        std::process::id()
    ))
}

#[cfg(unix)]
fn wait_for_path(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if path.exists() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("timed out waiting for {}", path.display());
}

#[cfg(unix)]
fn wait_until_not_running(pid: libc::pid_t) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if !process_is_running(pid) {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(unix)]
fn process_is_running(pid: libc::pid_t) -> bool {
    unsafe { libc::kill(pid, 0) == 0 }
}

#[cfg(unix)]
fn string_literal(value: &str) -> String {
    let mut rendered = String::from("\"");
    for ch in value.chars() {
        match ch {
            '"' => rendered.push_str("\\\""),
            '\\' => rendered.push_str("\\\\"),
            '\n' => rendered.push_str("\\n"),
            '\r' => rendered.push_str("\\r"),
            '\t' => rendered.push_str("\\t"),
            other => rendered.push(other),
        }
    }
    rendered.push('"');
    rendered
}

#[cfg(unix)]
fn stderr(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}
