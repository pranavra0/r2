#![cfg(unix)]

mod support;

use std::path::PathBuf;
use std::process::Command;
use std::sync::{Mutex, MutexGuard, OnceLock};

use support::{command_path, has_files_under, stderr, unique_temp_dir};

#[test]
fn file_store_persists_thunk_cache_across_cli_runs() {
    if command_path("cc").is_none() {
        eprintln!("skipping store CLI cache test because no cc was found");
        return;
    }

    let _outputs = BuildDemoOutputsGuard::new();
    let store_path = unique_temp_dir("r2-cli-store");

    let first = Command::new(env!("CARGO_BIN_EXE_r2"))
        .arg("build-demo")
        .arg("--store")
        .arg(&store_path)
        .output()
        .expect("first cli run should execute");

    assert!(first.status.success(), "stderr: {}", stderr(&first));
    assert!(
        store_path.join("objects").exists(),
        "store should contain object directory"
    );
    assert!(
        has_files_under(&store_path.join("objects")),
        "first run should persist at least one object"
    );

    let second = Command::new(env!("CARGO_BIN_EXE_r2"))
        .arg("build-demo")
        .arg("--summary")
        .arg("--store")
        .arg(&store_path)
        .output()
        .expect("second cli run should execute");

    assert!(second.status.success(), "stderr: {}", stderr(&second));

    let stdout = String::from_utf8(second.stdout).unwrap();
    assert!(stdout.contains("binary: ok"), "{stdout}");
    assert!(stdout.contains("thunk cache: hits"), "{stdout}");

    let _ = std::fs::remove_dir_all(store_path);
}

#[test]
fn store_gc_command_deletes_unrooted_objects() {
    if command_path("cc").is_none() {
        eprintln!("skipping store CLI GC test because no cc was found");
        return;
    }

    let _outputs = BuildDemoOutputsGuard::new();
    let store_path = unique_temp_dir("r2-cli-gc-store");

    let run = Command::new(env!("CARGO_BIN_EXE_r2"))
        .arg("build-demo")
        .arg("--store")
        .arg(&store_path)
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

    let _ = std::fs::remove_dir_all(store_path);
}

struct BuildDemoOutputsGuard {
    _lock: MutexGuard<'static, ()>,
}

impl BuildDemoOutputsGuard {
    fn new() -> Self {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let lock = LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("build demo output lock should not be poisoned");
        clean_build_demo_outputs();
        Self { _lock: lock }
    }
}

impl Drop for BuildDemoOutputsGuard {
    fn drop(&mut self) {
        clean_build_demo_outputs();
    }
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
