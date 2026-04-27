mod support;

use std::process::Command;

use support::{stderr, string_literal, unique_temp_path};

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
        stdout.contains(&string_literal(output_path.to_string_lossy().as_ref())),
        "{stdout}"
    );

    let _ = std::fs::remove_file(parent_program_path);
    let _ = std::fs::remove_file(child_program_path);
    let _ = std::fs::remove_file(output_path);
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
