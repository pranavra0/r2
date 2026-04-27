#![cfg(unix)]

mod support;

use std::path::PathBuf;
use std::process::Command;
use std::sync::{Mutex, MutexGuard, OnceLock};

use support::{command_path, stderr, unique_temp_dir};

#[test]
fn build_demo_cli_cold_and_warm_runs_materialize_outputs() {
    if command_path("cc").is_none() {
        eprintln!("skipping build demo CLI acceptance because no cc was found");
        return;
    }

    let _demo = BuildDemoGuard::new();
    let store_path = unique_temp_dir("r2-cli-build-demo-store");

    let first = Command::new(env!("CARGO_BIN_EXE_r2"))
        .arg("trace")
        .arg("--summary")
        .arg("--store")
        .arg(&store_path)
        .arg("examples/build-demo/build.r2")
        .output()
        .expect("cold build demo should run");

    assert!(first.status.success(), "stderr: {}", stderr(&first));
    let first_stdout = String::from_utf8(first.stdout).unwrap();
    assert!(first_stdout.contains("result: ok({"), "{first_stdout}");
    assert!(
        first_stdout.contains("- tasks: starts 5, ends 5"),
        "{first_stdout}"
    );
    assert!(
        first_stdout.contains("- thunk cache: hits 0, stores "),
        "{first_stdout}"
    );
    assert!(
        first_stdout.contains("host handle: process.spawn [hermetic]"),
        "{first_stdout}"
    );
    assert_build_demo_binary_runs("10\n");

    clean_build_demo_outputs();
    assert!(
        !build_demo_binary_path().exists(),
        "warm run should start without the output binary on disk"
    );

    let second = Command::new(env!("CARGO_BIN_EXE_r2"))
        .arg("trace")
        .arg("--summary")
        .arg("--store")
        .arg(&store_path)
        .arg("examples/build-demo/build.r2")
        .output()
        .expect("warm build demo should run");

    assert!(second.status.success(), "stderr: {}", stderr(&second));
    let second_stdout = String::from_utf8(second.stdout).unwrap();
    assert!(second_stdout.contains("result: ok({"), "{second_stdout}");
    assert!(
        second_stdout.contains("- thunk cache: hits 6, stores 0, bypasses 0, invalidations 0"),
        "{second_stdout}"
    );
    assert!(
        second_stdout.contains("thunk cache hit:"),
        "{second_stdout}"
    );
    assert!(
        !second_stdout.contains("host handle: process.spawn [hermetic]"),
        "{second_stdout}"
    );
    assert_build_demo_binary_runs("10\n");

    let _ = std::fs::remove_dir_all(store_path);
    clean_build_demo_outputs();
}

#[test]
fn build_demo_cli_incremental_rebuilds_only_changed_source() {
    if command_path("cc").is_none() {
        eprintln!("skipping build demo incremental because no cc was found");
        return;
    }

    let _demo = BuildDemoGuard::new();
    let store_path = unique_temp_dir("r2-cli-build-demo-incremental-store");

    let first = Command::new(env!("CARGO_BIN_EXE_r2"))
        .arg("trace")
        .arg("--summary")
        .arg("--store")
        .arg(&store_path)
        .arg("examples/build-demo/build.r2")
        .output()
        .expect("cold build demo should run");

    assert!(first.status.success(), "stderr: {}", stderr(&first));
    assert_build_demo_binary_runs("10\n");

    let one_c_path = PathBuf::from("examples/build-demo/src/one.c");
    std::fs::write(&one_c_path, "int one(void) { return 100; }\n").unwrap();

    clean_build_demo_outputs();

    let second = Command::new(env!("CARGO_BIN_EXE_r2"))
        .arg("trace")
        .arg("--summary")
        .arg("--store")
        .arg(&store_path)
        .arg("examples/build-demo/build.r2")
        .output()
        .expect("incremental build demo should run");

    assert!(second.status.success(), "stderr: {}", stderr(&second));
    let second_stdout = String::from_utf8(second.stdout).unwrap();
    assert!(second_stdout.contains("result: ok({"), "{second_stdout}");
    assert!(
        second_stdout.contains("- thunk cache: hits 4, stores 3, bypasses 0"),
        "{second_stdout}"
    );
    assert!(
        second_stdout.contains("thunk cache invalidated:"),
        "{second_stdout}"
    );
    assert!(
        second_stdout.contains("host handle: process.spawn [hermetic]"),
        "{second_stdout}"
    );
    assert_eq!(
        second_stdout
            .matches("host handle: process.spawn [hermetic]")
            .count(),
        2,
        "{second_stdout}"
    );
    assert_build_demo_binary_runs("109\n");

    let _ = std::fs::remove_dir_all(store_path);
}

fn build_demo_binary_path() -> PathBuf {
    PathBuf::from("examples/build-demo/out/hello-demo")
}

fn clean_build_demo_outputs() {
    for name in [
        "main.o",
        "one.o",
        "two.o",
        "three.o",
        "four.o",
        "hello-demo",
    ] {
        let _ = std::fs::remove_file(PathBuf::from("examples/build-demo/out").join(name));
    }
}

struct BuildDemoGuard {
    _lock: MutexGuard<'static, ()>,
    original_one_c: String,
}

impl BuildDemoGuard {
    fn new() -> Self {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let lock = LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("build demo lock should not be poisoned");
        let original_one_c = std::fs::read_to_string("examples/build-demo/src/one.c")
            .expect("build demo source should be readable");
        clean_build_demo_outputs();
        Self {
            _lock: lock,
            original_one_c,
        }
    }
}

impl Drop for BuildDemoGuard {
    fn drop(&mut self) {
        let _ = std::fs::write("examples/build-demo/src/one.c", &self.original_one_c);
        clean_build_demo_outputs();
    }
}

fn assert_build_demo_binary_runs(expected_stdout: &str) {
    let output = Command::new(build_demo_binary_path())
        .output()
        .expect("build demo binary should run");

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert_eq!(String::from_utf8(output.stdout).unwrap(), expected_stdout);
}
