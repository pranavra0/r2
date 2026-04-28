use r2::{BuildAction, BuildArtifact, RuntimeRunner, RuntimeValue, Term, Value, thunk};

#[test]
fn memory_runner_runs_pure_term() {
    let mut runner = RuntimeRunner::memory();

    let value = runner
        .run(Term::Value(Value::Integer(7)))
        .expect("runner should run pure term");

    match value {
        RuntimeValue::Data(Value::Integer(7)) => {}
        other => panic!("unexpected runner value: {other:?}"),
    }
}

#[cfg(unix)]
#[test]
fn file_store_runner_runs_process_build_host() {
    let root = unique_temp_dir("r2-runner-process");
    let store_path = root.join("store");
    let input_path = root.join("input.txt");
    let output_path = root.join("out").join("output.txt");
    std::fs::create_dir_all(&root).expect("temp root should create");
    std::fs::write(&input_path, "hello runner\n").expect("input should write");

    let action = BuildAction::new(vec![
        b"/bin/sh".to_vec(),
        b"-c".to_vec(),
        b"cat \"$1\" > \"$2\"".to_vec(),
        b"sh".to_vec(),
        path_bytes(&input_path),
        path_bytes(&output_path),
    ])
    .input(BuildArtifact::new(path_bytes(&input_path)))
    .output(BuildArtifact::new(path_bytes(&output_path)));

    let term = thunk::force(thunk::delay(action.into_term()));
    let mut runner = RuntimeRunner::file_store(&store_path)
        .expect("file store should open")
        .process_build_host();

    let traced = runner
        .run_traced(term)
        .expect("runner should run build term");

    assert!(matches!(
        traced.value,
        RuntimeValue::Data(Value::Tagged { .. })
    ));
    assert_eq!(
        std::fs::read_to_string(&output_path).expect("output should materialize"),
        "hello runner\n"
    );
    assert_eq!(traced.trace.summary().host_handles, 1);

    let _ = std::fs::remove_dir_all(root);
}

#[cfg(unix)]
fn path_bytes(path: impl AsRef<std::path::Path>) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    path.as_ref().as_os_str().as_bytes().to_vec()
}

#[cfg(unix)]
fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()))
}
