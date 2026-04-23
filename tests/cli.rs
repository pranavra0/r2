use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn runs_a_surface_language_program() {
    let program_path = unique_temp_path("r2-cli-program", "r2");
    std::fs::write(&program_path, "let id = fn(x) => x; id(7)").expect("program should write");

    let output = Command::new(env!("CARGO_BIN_EXE_r2"))
        .arg("run")
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
        .arg(&program_path)
        .output()
        .expect("cli should run");

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert_eq!(String::from_utf8(output.stdout).unwrap(), "14\n");
    assert_eq!(
        std::fs::read_to_string(&target_path).unwrap(),
        "hello from cli"
    );

    let _ = std::fs::remove_file(program_path);
    let _ = std::fs::remove_file(target_path);
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
