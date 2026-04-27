mod support;

use std::process::Command;

use support::{has_files_under, stderr, unique_temp_dir, unique_temp_path};

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
