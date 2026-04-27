mod support;

use std::process::Command;

use support::{stderr, string_literal, unique_temp_path};

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
    assert!(stdout.contains("thunk force_all["), "{stdout}");
    assert!(stdout.contains("]: 2"), "{stdout}");
    assert!(stdout.contains("task "), "{stdout}");
    assert!(stdout.contains("start: frontier"), "{stdout}");
    assert!(stdout.contains("end: frontier"), "{stdout}");
    assert!(
        stdout.contains("builtin handle: thunk.force_all"),
        "{stdout}"
    );

    let _ = std::fs::remove_file(program_path);
}

#[test]
fn trace_command_keeps_force_all_volatile_branches_uncached() {
    let program_path = unique_temp_path("r2-cli-program-trace-force-all-volatile", "r2");
    let target_path = unique_temp_path("r2-cli-force-all-volatile-output", "txt");
    let program = format!(
        "perform thunk.force_all(lazy {{ perform fs.write({}, {}) }}, lazy {{ 99 }})",
        string_literal(target_path.to_string_lossy().as_ref()),
        string_literal("hello")
    );
    std::fs::write(&program_path, program).expect("program should write");

    let output = Command::new(env!("CARGO_BIN_EXE_r2"))
        .arg("trace")
        .arg("--summary")
        .arg("--memory-store")
        .arg(&program_path)
        .output()
        .expect("cli should run");

    assert!(output.status.success(), "stderr: {}", stderr(&output));

    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout.contains("result: [ok({written: 5}), 99]\n"),
        "{stdout}"
    );
    assert!(
        stdout.contains("- thunk cache: hits 0, stores 2, bypasses 2, invalidations 0"),
        "{stdout}"
    );
    assert!(
        stdout.contains("host handle: fs.write [volatile]"),
        "{stdout}"
    );
    assert!(
        stdout.contains("due to volatile effect fs.write"),
        "{stdout}"
    );
    assert_eq!(std::fs::read_to_string(&target_path).unwrap(), "hello");

    let _ = std::fs::remove_file(program_path);
    let _ = std::fs::remove_file(target_path);
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
        stdout.contains("- thunk cache: hits 1, stores 1, bypasses 0, invalidations 0"),
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
