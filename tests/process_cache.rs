#[cfg(unix)]
use std::collections::BTreeMap;
#[cfg(unix)]
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use r2::{Host, Runtime, RuntimeValue, Symbol, Term, Value, thunk};

#[cfg(unix)]
#[test]
fn hermetic_process_spawn_caches_and_materializes_declared_outputs() {
    let input_path = unique_temp_path("r2-process-cache-input", "txt");
    let output_path = unique_temp_path("r2-process-cache-output", "txt");
    let marker_path = unique_temp_path("r2-process-cache-marker", "txt");
    std::fs::write(&input_path, "v1").expect("input should write");

    let mut runtime = Runtime::new();
    let mut host = Host::new();
    host.install_hermetic_process_spawn();
    let request = process_request(&input_path, &output_path, &marker_path);

    let first = runtime
        .run(request.clone(), &mut host)
        .expect("first process should run");
    assert_ok_result(first);
    assert_eq!(std::fs::read_to_string(&output_path).unwrap(), "v1");
    assert_eq!(std::fs::read_to_string(&marker_path).unwrap(), "run\n");

    std::fs::remove_file(&output_path).expect("output should remove");

    let second = runtime
        .run(request.clone(), &mut host)
        .expect("second process should run");
    assert_ok_result(second);
    assert_eq!(std::fs::read_to_string(&output_path).unwrap(), "v1");
    assert_eq!(
        std::fs::read_to_string(&marker_path).unwrap(),
        "run\n",
        "cache hit should not run the process again"
    );

    std::fs::write(&input_path, "v2").expect("input should update");
    std::fs::remove_file(&output_path).expect("output should remove");

    let third = runtime
        .run(request, &mut host)
        .expect("third process should run");
    assert_ok_result(third);
    assert_eq!(std::fs::read_to_string(&output_path).unwrap(), "v2");
    assert_eq!(
        std::fs::read_to_string(&marker_path).unwrap(),
        "run\nrun\n",
        "changed declared input should miss the cache"
    );

    let _ = std::fs::remove_file(input_path);
    let _ = std::fs::remove_file(output_path);
    let _ = std::fs::remove_file(marker_path);
}

#[cfg(unix)]
#[test]
fn hermetic_process_spawn_does_not_cache_missing_declared_outputs() {
    let input_path = unique_temp_path("r2-process-cache-missing-input", "txt");
    let output_path = unique_temp_path("r2-process-cache-missing-output", "txt");
    let marker_path = unique_temp_path("r2-process-cache-missing-marker", "txt");
    std::fs::write(&input_path, "v1").expect("input should write");

    let mut runtime = Runtime::new();
    let mut host = Host::new();
    host.install_hermetic_process_spawn();
    let request = process_request_with_script(
        &input_path,
        &output_path,
        &marker_path,
        r#"printf 'run\n' >> "$3""#,
    );

    let first = runtime
        .run(request.clone(), &mut host)
        .expect("first process should run");
    assert_ok_result(first);
    assert!(!output_path.exists());
    assert_eq!(std::fs::read_to_string(&marker_path).unwrap(), "run\n");

    let second = runtime
        .run(request, &mut host)
        .expect("second process should run");
    assert_ok_result(second);
    assert!(!output_path.exists());
    assert_eq!(
        std::fs::read_to_string(&marker_path).unwrap(),
        "run\nrun\n",
        "missing declared output should prevent cache population"
    );

    let _ = std::fs::remove_file(input_path);
    let _ = std::fs::remove_file(output_path);
    let _ = std::fs::remove_file(marker_path);
}

#[cfg(all(unix, feature = "sandbox"))]
#[test]
fn sandbox_denies_undeclared_absolute_paths() {
    let input_path = unique_temp_path("r2-process-sandbox-input", "txt");
    let output_path = unique_temp_path("r2-process-sandbox-output", "txt");
    let marker_path = unique_temp_path("r2-process-sandbox-marker", "txt");
    std::fs::write(&input_path, "v1").expect("input should write");

    let mut runtime = Runtime::new();
    let mut host = Host::new();
    host.install_hermetic_process_spawn();
    let request = process_request_with_script(
        &input_path,
        &output_path,
        &marker_path,
        r#"cat /etc/passwd > "$2"; printf 'run\n' >> "$3""#,
    );

    let result = runtime.run(request, &mut host).expect("program should run");

    match result {
        RuntimeValue::Data(Value::Tagged { tag, fields }) => {
            assert_eq!(tag, Symbol::from("error"));
            assert!(
                String::from_utf8_lossy(&extract_error_bytes(&fields))
                    .contains("permission_denied")
            );
        }
        other => panic!("unexpected result: {other:?}"),
    }
    assert!(!output_path.exists());
    assert!(!marker_path.exists());

    let _ = std::fs::remove_file(input_path);
    let _ = std::fs::remove_file(output_path);
    let _ = std::fs::remove_file(marker_path);
}

#[cfg(all(unix, feature = "sandbox"))]
#[test]
fn sandbox_allows_declared_input_and_output_paths() {
    let input_path = unique_temp_path("r2-process-sandbox-allowed-input", "txt");
    let output_path = unique_temp_path("r2-process-sandbox-allowed-output", "txt");
    let marker_path = unique_temp_path("r2-process-sandbox-allowed-marker", "txt");
    std::fs::write(&input_path, "allowed").expect("input should write");

    let mut runtime = Runtime::new();
    let mut host = Host::new();
    host.install_hermetic_process_spawn();
    let request = process_request(&input_path, &output_path, &marker_path);

    let result = runtime.run(request, &mut host).expect("program should run");

    assert_ok_result(result);
    assert_eq!(std::fs::read_to_string(&output_path).unwrap(), "allowed");

    let _ = std::fs::remove_file(input_path);
    let _ = std::fs::remove_file(output_path);
    let _ = std::fs::remove_file(marker_path);
}

#[cfg(unix)]
fn process_request(input: &Path, output: &Path, marker: &Path) -> Term {
    process_request_with_script(
        input,
        output,
        marker,
        r#"cat "$1" > "$2"; printf 'run\n' >> "$3""#,
    )
}

#[cfg(unix)]
fn process_request_with_script(input: &Path, output: &Path, marker: &Path, script: &str) -> Term {
    Term::Perform {
        op: Symbol::from("process.spawn"),
        args: vec![Term::Value(Value::Record(BTreeMap::from([
            (
                Symbol::from("argv"),
                Value::List(vec![
                    Value::Bytes(b"/bin/sh".to_vec()),
                    Value::Bytes(b"-c".to_vec()),
                    Value::Bytes(script.as_bytes().to_vec()),
                    Value::Bytes(b"sh".to_vec()),
                    Value::Bytes(path_to_bytes(input)),
                    Value::Bytes(path_to_bytes(output)),
                    Value::Bytes(path_to_bytes(marker)),
                ]),
            ),
            (Symbol::from("env_mode"), Value::Bytes(b"clear".to_vec())),
            (Symbol::from("env"), Value::Record(BTreeMap::new())),
            (Symbol::from("stdin"), Value::Bytes(Vec::new())),
            (
                Symbol::from("declared_inputs"),
                Value::List(vec![Value::Bytes(path_to_bytes(input))]),
            ),
            (
                Symbol::from("declared_outputs"),
                Value::List(vec![
                    Value::Bytes(path_to_bytes(output)),
                    Value::Bytes(path_to_bytes(marker)),
                ]),
            ),
        ])))],
    }
}

#[cfg(unix)]
#[test]
fn hermetic_process_spawn_thunk_cache_invalidates_when_input_changes() {
    let input_path = unique_temp_path("r2-thunk-invalidation-input", "txt");
    let output_path = unique_temp_path("r2-thunk-invalidation-output", "txt");
    let marker_path = unique_temp_path("r2-thunk-invalidation-marker", "txt");
    std::fs::write(&input_path, "v1").expect("input should write");

    let mut runtime = Runtime::new();
    let mut host = Host::new();
    host.install_hermetic_process_spawn();

    let spawn_term = process_request_with_script(
        &input_path,
        &output_path,
        &marker_path,
        r#"cat "$1" > "$2"; printf 'run\n' >> "$3""#,
    );
    let program = thunk::force(thunk::delay(spawn_term));

    let first = runtime
        .run(program.clone(), &mut host)
        .expect("first run should execute");
    assert_ok_result(first);
    assert_eq!(std::fs::read_to_string(&output_path).unwrap(), "v1");
    assert_eq!(std::fs::read_to_string(&marker_path).unwrap(), "run\n");

    let second = runtime
        .run(program.clone(), &mut host)
        .expect("second run should hit thunk cache");
    assert_ok_result(second);
    assert_eq!(
        std::fs::read_to_string(&marker_path).unwrap(),
        "run\n",
        "warm thunk cache should not re-execute process"
    );

    std::fs::write(&input_path, "v2").expect("input should update");

    let third = runtime
        .run(program, &mut host)
        .expect("third run should invalidate thunk cache");
    assert_ok_result(third);
    assert_eq!(std::fs::read_to_string(&output_path).unwrap(), "v2");
    assert_eq!(
        std::fs::read_to_string(&marker_path).unwrap(),
        "run\nrun\n",
        "changed input should invalidate thunk cache and re-execute process"
    );

    let _ = std::fs::remove_file(input_path);
    let _ = std::fs::remove_file(output_path);
    let _ = std::fs::remove_file(marker_path);
}

#[cfg(unix)]
fn assert_ok_result(value: RuntimeValue) {
    match value {
        RuntimeValue::Data(Value::Tagged { tag, .. }) => assert_eq!(tag, Symbol::from("ok")),
        other => panic!("unexpected result: {other:?}"),
    }
}

#[cfg(all(unix, feature = "sandbox"))]
fn extract_error_bytes(fields: &[Value]) -> Vec<u8> {
    match fields {
        [Value::Bytes(bytes)] => bytes.clone(),
        other => panic!("unexpected error fields: {other:?}"),
    }
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
