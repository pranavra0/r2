use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn runs_a_surface_language_program() {
    let program_path = unique_temp_path("r2-cli-program", "r2");
    std::fs::write(&program_path, "let id = fn(x) => x; id(7)").expect("program should write");

    let output = Command::new(env!("CARGO_BIN_EXE_r2"))
        .arg("run")
        .arg("--memory-store")
        .arg(&program_path)
        .output()
        .expect("cli should run");

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert_eq!(String::from_utf8(output.stdout).unwrap(), "7\n");

    let _ = std::fs::remove_file(program_path);
}

#[test]
fn writes_files_through_the_default_host() {
    let program_path = unique_temp_path("r2-cli-program-write", "r2");
    let target_path = unique_temp_path("r2-cli-output", "txt");
    let program = format!(
        "perform fs.write({}, {})",
        string_literal(target_path.to_string_lossy().as_ref()),
        string_literal("hello from cli")
    );
    std::fs::write(&program_path, program).expect("program should write");

    let output = Command::new(env!("CARGO_BIN_EXE_r2"))
        .arg("run")
        .arg("--memory-store")
        .arg(&program_path)
        .output()
        .expect("cli should run");

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "ok({written: 14})\n"
    );
    assert_eq!(
        std::fs::read_to_string(&target_path).unwrap(),
        "hello from cli"
    );

    let _ = std::fs::remove_file(program_path);
    let _ = std::fs::remove_file(target_path);
}

#[test]
fn runs_clock_effects_through_the_default_host() {
    let program_path = unique_temp_path("r2-cli-program-clock", "r2");
    std::fs::write(&program_path, "perform clock.sleep({ duration_nanos: 0 })")
        .expect("program should write");

    let output = Command::new(env!("CARGO_BIN_EXE_r2"))
        .arg("run")
        .arg("--memory-store")
        .arg(&program_path)
        .output()
        .expect("cli should run");

    assert!(output.status.success(), "stderr: {}", stderr(&output));

    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("ok({duration_nanos: 0})"), "{stdout}");

    let _ = std::fs::remove_file(program_path);
}

#[test]
fn runs_processes_through_the_default_host() {
    let program_path = unique_temp_path("r2-cli-program-process", "r2");
    let program = format!(
        "perform process.spawn({{ argv: [{}, {}], env_mode: {}, env: {{}}, stdin: {}, declared_inputs: [], declared_outputs: [] }})",
        string_literal(env!("CARGO_BIN_EXE_r2")),
        string_literal("--help"),
        string_literal("clear"),
        string_literal("")
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
    assert!(stdout.contains("status: exit_code(0)"), "{stdout}");
    assert!(stdout.contains("Usage: r2"), "{stdout}");

    let _ = std::fs::remove_file(program_path);
}

#[test]
fn process_spawn_materializes_declared_outputs() {
    let parent_program_path = unique_temp_path("r2-cli-program-process-outputs", "r2");
    let child_program_path = unique_temp_path("r2-cli-child-program", "r2");
    let output_path = unique_temp_path("r2-cli-child-output", "txt");

    let child_program = format!(
        "perform fs.write({}, {})",
        string_literal(output_path.to_string_lossy().as_ref()),
        string_literal("hello from child")
    );
    std::fs::write(&child_program_path, child_program).expect("child program should write");

    let parent_program = format!(
        "perform process.spawn({{ argv: [{}, {}, {}], env_mode: {}, env: {{}}, stdin: {}, declared_inputs: [{}], declared_outputs: [{}] }})",
        string_literal(env!("CARGO_BIN_EXE_r2")),
        string_literal("run"),
        string_literal(child_program_path.to_string_lossy().as_ref()),
        string_literal("clear"),
        string_literal(""),
        string_literal(child_program_path.to_string_lossy().as_ref()),
        string_literal(output_path.to_string_lossy().as_ref())
    );
    std::fs::write(&parent_program_path, parent_program).expect("parent program should write");

    let output = Command::new(env!("CARGO_BIN_EXE_r2"))
        .arg("run")
        .arg("--memory-store")
        .arg(&parent_program_path)
        .output()
        .expect("cli should run");

    assert!(output.status.success(), "stderr: {}", stderr(&output));

    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("output_files"), "{stdout}");
    assert!(stdout.contains("hello from child"), "{stdout}");
    assert!(
        stdout.contains(output_path.to_string_lossy().as_ref()),
        "{stdout}"
    );

    let _ = std::fs::remove_file(parent_program_path);
    let _ = std::fs::remove_file(child_program_path);
    let _ = std::fs::remove_file(output_path);
}

#[test]
fn trace_command_reports_pure_thunk_cache_hits() {
    let program_path = unique_temp_path("r2-cli-program-trace-pure", "r2");
    std::fs::write(
        &program_path,
        "let thunk = lazy { 5 }; let _ = force thunk; force thunk",
    )
    .expect("program should write");

    let output = Command::new(env!("CARGO_BIN_EXE_r2"))
        .arg("trace")
        .arg("--memory-store")
        .arg(&program_path)
        .output()
        .expect("cli should run");

    assert!(output.status.success(), "stderr: {}", stderr(&output));

    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("result: 5\n"), "{stdout}");
    assert!(stdout.contains("trace:\n"), "{stdout}");
    assert!(stdout.contains("yield: thunk.force"), "{stdout}");
    assert!(stdout.contains("builtin handle: thunk.force"), "{stdout}");
    assert!(stdout.contains("thunk cache hit:"), "{stdout}");

    let _ = std::fs::remove_file(program_path);
}

#[test]
fn trace_command_reports_force_all_batches() {
    let program_path = unique_temp_path("r2-cli-program-trace-force-all", "r2");
    std::fs::write(
        &program_path,
        "perform thunk.force_all(lazy { 1 }, lazy { 2 })",
    )
    .expect("program should write");

    let output = Command::new(env!("CARGO_BIN_EXE_r2"))
        .arg("trace")
        .arg("--summary")
        .arg("--memory-store")
        .arg(&program_path)
        .output()
        .expect("cli should run");

    assert!(output.status.success(), "stderr: {}", stderr(&output));

    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("result: [1, 2]\n"), "{stdout}");
    assert!(stdout.contains("- thunk forces: single "), "{stdout}");
    assert!(stdout.contains("batches 1"), "{stdout}");
    assert!(stdout.contains("yield: thunk.force_all"), "{stdout}");
    assert!(stdout.contains("thunk force_all: 2"), "{stdout}");
    assert!(
        stdout.contains("builtin handle: thunk.force_all"),
        "{stdout}"
    );

    let _ = std::fs::remove_file(program_path);
}

#[test]
fn trace_command_reports_runtime_and_volatile_thunk_activity() {
    let program_path = unique_temp_path("r2-cli-program-trace", "r2");
    let target_path = unique_temp_path("r2-cli-trace-output", "txt");
    let program = format!(
        "let thunk = lazy {{ perform fs.write({}, {}) }}; let _ = force thunk; force thunk",
        string_literal(target_path.to_string_lossy().as_ref()),
        string_literal("hello")
    );
    std::fs::write(&program_path, program).expect("program should write");

    let output = Command::new(env!("CARGO_BIN_EXE_r2"))
        .arg("trace")
        .arg("--memory-store")
        .arg(&program_path)
        .output()
        .expect("cli should run");

    assert!(output.status.success(), "stderr: {}", stderr(&output));

    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("result: ok({written: 5})\n"), "{stdout}");
    assert!(stdout.contains("trace:\n"), "{stdout}");
    assert!(stdout.contains("yield: thunk.force"), "{stdout}");
    assert!(stdout.contains("builtin handle: thunk.force"), "{stdout}");
    assert!(stdout.contains("yield: fs.write"), "{stdout}");
    assert!(stdout.contains("host handle: fs.write"), "{stdout}");
    assert!(stdout.contains("[volatile]"), "{stdout}");
    assert!(stdout.contains("thunk cache bypass:"), "{stdout}");
    assert_eq!(std::fs::read_to_string(&target_path).unwrap(), "hello");

    let _ = std::fs::remove_file(program_path);
    let _ = std::fs::remove_file(target_path);
}

#[test]
fn trace_command_reports_process_spawn_activity() {
    let program_path = unique_temp_path("r2-cli-program-trace-process", "r2");
    let program = format!(
        "perform process.spawn({{ argv: [{}, {}], env_mode: {}, env: {{}}, stdin: {}, declared_inputs: [], declared_outputs: [] }})",
        string_literal(env!("CARGO_BIN_EXE_r2")),
        string_literal("--help"),
        string_literal("clear"),
        string_literal("")
    );
    std::fs::write(&program_path, program).expect("program should write");

    let output = Command::new(env!("CARGO_BIN_EXE_r2"))
        .arg("trace")
        .arg("--memory-store")
        .arg(&program_path)
        .output()
        .expect("cli should run");

    assert!(output.status.success(), "stderr: {}", stderr(&output));

    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("yield: process.spawn"), "{stdout}");
    assert!(
        stdout.contains("host handle: process.spawn [hermetic]"),
        "{stdout}"
    );
    assert!(stdout.contains("result: ok({"), "{stdout}");

    let _ = std::fs::remove_file(program_path);
}

#[cfg(unix)]
#[test]
fn trace_command_caches_hermetic_process_spawn_thunks() {
    let program_path = unique_temp_path("r2-cli-program-trace-process-cache", "r2");
    let input_path = unique_temp_path("r2-cli-process-cache-input", "txt");
    let output_path = unique_temp_path("r2-cli-process-cache-output", "txt");
    let marker_path = unique_temp_path("r2-cli-process-cache-marker", "txt");
    std::fs::write(&input_path, "cached via cli").expect("input should write");

    let script = "cat \"$1\" > \"$2\"; printf 'run\\n' >> \"$3\"";
    let request = format!(
        "{{ argv: [{}, {}, {}, {}, {}, {}, {}], env_mode: {}, env: {{}}, stdin: {}, declared_inputs: [{}], declared_outputs: [{}, {}] }}",
        string_literal("/bin/sh"),
        string_literal("-c"),
        string_literal(script),
        string_literal("sh"),
        string_literal(input_path.to_string_lossy().as_ref()),
        string_literal(output_path.to_string_lossy().as_ref()),
        string_literal(marker_path.to_string_lossy().as_ref()),
        string_literal("clear"),
        string_literal(""),
        string_literal(input_path.to_string_lossy().as_ref()),
        string_literal(output_path.to_string_lossy().as_ref()),
        string_literal(marker_path.to_string_lossy().as_ref()),
    );
    std::fs::write(
        &program_path,
        format!(
            "let thunk = lazy {{ perform process.spawn({request}) }}; let _ = force thunk; force thunk"
        ),
    )
    .expect("program should write");

    let output = Command::new(env!("CARGO_BIN_EXE_r2"))
        .arg("trace")
        .arg("--memory-store")
        .arg(&program_path)
        .output()
        .expect("cli should run");

    assert!(output.status.success(), "stderr: {}", stderr(&output));

    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("result: ok({"), "{stdout}");
    assert!(
        stdout.contains("host handle: process.spawn [hermetic]"),
        "{stdout}"
    );
    assert!(stdout.contains("thunk cache store:"), "{stdout}");
    assert!(stdout.contains("thunk cache hit:"), "{stdout}");
    assert_eq!(
        std::fs::read_to_string(&output_path).unwrap(),
        "cached via cli"
    );
    assert_eq!(std::fs::read_to_string(&marker_path).unwrap(), "run\n");

    let _ = std::fs::remove_file(program_path);
    let _ = std::fs::remove_file(input_path);
    let _ = std::fs::remove_file(output_path);
    let _ = std::fs::remove_file(marker_path);
}

#[test]
fn trace_command_can_print_a_summary() {
    let program_path = unique_temp_path("r2-cli-program-trace-summary", "r2");
    std::fs::write(
        &program_path,
        "let thunk = lazy { 5 }; let _ = force thunk; force thunk",
    )
    .expect("program should write");

    let output = Command::new(env!("CARGO_BIN_EXE_r2"))
        .arg("trace")
        .arg("--memory-store")
        .arg("--summary")
        .arg(&program_path)
        .output()
        .expect("cli should run");

    assert!(output.status.success(), "stderr: {}", stderr(&output));

    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("result: 5\n"), "{stdout}");
    assert!(stdout.contains("summary:\n"), "{stdout}");
    assert!(stdout.contains("- boundary steps:"), "{stdout}");
    assert!(stdout.contains("- yields: 2"), "{stdout}");
    assert!(stdout.contains("- builtin handles: 2"), "{stdout}");
    assert!(
        stdout.contains("- host handles: 0 (stable: 0, volatile: 0, declared: 0, hermetic: 0)"),
        "{stdout}"
    );
    assert!(
        stdout.contains("- thunk cache: hits 1, stores 1, bypasses 0"),
        "{stdout}"
    );
    assert!(stdout.contains("trace:\n"), "{stdout}");

    let _ = std::fs::remove_file(program_path);
}

#[test]
fn trace_command_reports_stable_math_effects() {
    let program_path = unique_temp_path("r2-cli-program-trace-math", "r2");
    std::fs::write(&program_path, "2 + 3").expect("program should write");

    let output = Command::new(env!("CARGO_BIN_EXE_r2"))
        .arg("trace")
        .arg("--memory-store")
        .arg(&program_path)
        .output()
        .expect("cli should run");

    assert!(output.status.success(), "stderr: {}", stderr(&output));

    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("result: 5\n"), "{stdout}");
    assert!(stdout.contains("yield: math.add"), "{stdout}");
    assert!(
        stdout.contains("host handle: math.add [stable]"),
        "{stdout}"
    );

    let _ = std::fs::remove_file(program_path);
}

#[test]
fn file_store_persists_thunk_cache_across_cli_runs() {
    let program_path = unique_temp_path("r2-cli-program-persistent-cache", "r2");
    let store_path = unique_temp_dir("r2-cli-store");
    std::fs::write(&program_path, "force lazy { 5 }").expect("program should write");

    let first = Command::new(env!("CARGO_BIN_EXE_r2"))
        .arg("run")
        .arg("--store")
        .arg(&store_path)
        .arg(&program_path)
        .output()
        .expect("first cli run should execute");

    assert!(first.status.success(), "stderr: {}", stderr(&first));
    assert_eq!(String::from_utf8(first.stdout).unwrap(), "5\n");
    assert!(
        store_path.join("objects").exists(),
        "store should contain object directory"
    );
    assert!(
        has_files_under(&store_path.join("objects")),
        "first run should persist at least one object"
    );

    let second = Command::new(env!("CARGO_BIN_EXE_r2"))
        .arg("run")
        .arg("--trace")
        .arg("--store")
        .arg(&store_path)
        .arg(&program_path)
        .output()
        .expect("second cli run should execute");

    assert!(second.status.success(), "stderr: {}", stderr(&second));

    let stdout = String::from_utf8(second.stdout).unwrap();
    assert!(stdout.contains("result: 5\n"), "{stdout}");
    assert!(stdout.contains("thunk cache hit:"), "{stdout}");
    assert!(!stdout.contains("thunk cache store:"), "{stdout}");

    let _ = std::fs::remove_file(program_path);
    let _ = std::fs::remove_dir_all(store_path);
}

#[test]
fn store_gc_command_deletes_unrooted_objects() {
    let program_path = unique_temp_path("r2-cli-program-gc", "r2");
    let store_path = unique_temp_dir("r2-cli-gc-store");
    std::fs::write(&program_path, "force lazy { 5 }").expect("program should write");

    let run = Command::new(env!("CARGO_BIN_EXE_r2"))
        .arg("run")
        .arg("--store")
        .arg(&store_path)
        .arg(&program_path)
        .output()
        .expect("cli run should execute");

    assert!(run.status.success(), "stderr: {}", stderr(&run));
    assert!(has_files_under(&store_path.join("objects")));

    let gc = Command::new(env!("CARGO_BIN_EXE_r2"))
        .arg("store")
        .arg("gc")
        .arg("--store")
        .arg(&store_path)
        .output()
        .expect("store gc should execute");

    assert!(gc.status.success(), "stderr: {}", stderr(&gc));
    let stdout = String::from_utf8(gc.stdout).unwrap();
    assert!(stdout.contains("deleted objects:"), "{stdout}");
    assert!(!has_files_under(&store_path.join("objects")));

    let _ = std::fs::remove_file(program_path);
    let _ = std::fs::remove_dir_all(store_path);
}

#[test]
fn help_mentions_store_flags() {
    let output = Command::new(env!("CARGO_BIN_EXE_r2"))
        .arg("--help")
        .output()
        .expect("cli help should run");

    assert!(output.status.success(), "stderr: {}", stderr(&output));

    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("--store <path>"), "{stdout}");
    assert!(stdout.contains("--memory-store"), "{stdout}");
}

fn unique_temp_path(prefix: &str, extension: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();
    path.push(format!(
        "{prefix}-{}-{nanos}.{extension}",
        std::process::id()
    ));
    path
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let path = unique_temp_path(prefix, "dir");
    let _ = std::fs::remove_dir_all(&path);
    path
}

fn has_files_under(path: &std::path::Path) -> bool {
    let Ok(entries) = std::fs::read_dir(path) else {
        return false;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_file() || has_files_under(&path) {
            return true;
        }
    }

    false
}

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

fn stderr(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}
