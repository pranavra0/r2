#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use r2::{
    BuildAction, BuildArtifact, BuildGraph, FileStore, Host, Runtime, RuntimeValue, Symbol, Value,
};

#[test]
fn build_demo_runtime_cold_and_warm_runs_materialize_outputs() {
    let Some(cc) = find_cc() else {
        eprintln!("skipping build demo runtime because no cc was found");
        return;
    };

    let root = unique_temp_dir("r2-build-demo-runtime");
    let store_path = root.join("store");
    let src_dir = root.join("src");
    let out_dir = root.join("out");
    std::fs::create_dir_all(&src_dir).unwrap();
    std::fs::create_dir_all(&out_dir).unwrap();
    write_build_demo_sources(&src_dir);

    // Cold build
    let store = FileStore::open(&store_path).expect("store should open");
    let mut runtime = Runtime::with_store(store);
    let mut host = Host::new();
    host.install_hermetic_process_spawn();

    let term = build_demo_graph(&src_dir, &out_dir, &cc)
        .to_expression()
        .unwrap();
    let traced = runtime
        .run_with_trace(term, &mut host)
        .expect("cold build should run");

    let summary = traced.trace.summary();
    assert_eq!(summary.thunk_cache_hits, 0, "cold run should have no hits");
    assert_eq!(
        summary.thunk_cache_stores, 11,
        "cold run should store 11 thunks (5 compiles * 2 keys each + 1 link)"
    );
    assert_eq!(summary.thunk_force_all, 1, "cold run should force_all once");
    assert_eq!(
        summary.task_starts, 5,
        "cold run should start 5 tasks for parallel compiles"
    );
    assert_eq!(summary.host_handles, 6, "cold run should spawn 6 processes");
    assert_target_succeeded(&traced.value);
    assert_build_demo_binary_runs(&out_dir, "10\n");

    // Clean outputs to simulate a fresh checkout
    clean_build_demo_outputs(&out_dir);
    assert!(
        !binary_path(&out_dir).exists(),
        "warm run should start without the output binary on disk"
    );

    // Warm build with a fresh runtime (matches CLI behavior of separate invocations)
    let store = FileStore::open(&store_path).expect("store should reopen");
    let mut runtime = Runtime::with_store(store);
    let mut host = Host::new();
    host.install_hermetic_process_spawn();

    let term = build_demo_graph(&src_dir, &out_dir, &cc)
        .to_expression()
        .unwrap();
    let traced = runtime
        .run_with_trace(term, &mut host)
        .expect("warm build should run");

    let summary = traced.trace.summary();
    assert_eq!(
        summary.thunk_cache_hits, 5,
        "warm run should hit 5 compile thunks (link invalidated because .o files were deleted)"
    );
    assert_eq!(
        summary.thunk_cache_stores, 1,
        "warm run should store the rebuilt link thunk"
    );
    assert_eq!(
        summary.thunk_force_all, 1,
        "warm run should force_all inside link thunk"
    );
    assert_eq!(
        summary.task_starts, 5,
        "warm run should start 5 tasks for parallel compiles inside link thunk"
    );
    assert_eq!(
        summary.host_handles, 1,
        "warm run should spawn 1 process (link, after .o files are rematerialized from cache)"
    );
    assert_target_succeeded(&traced.value);
    assert_build_demo_binary_runs(&out_dir, "10\n");

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn build_demo_runtime_incremental_rebuilds_only_changed_source() {
    let Some(cc) = find_cc() else {
        eprintln!("skipping build demo runtime incremental because no cc was found");
        return;
    };

    let root = unique_temp_dir("r2-build-demo-runtime-incremental");
    let store_path = root.join("store");
    let src_dir = root.join("src");
    let out_dir = root.join("out");
    std::fs::create_dir_all(&src_dir).unwrap();
    std::fs::create_dir_all(&out_dir).unwrap();
    write_build_demo_sources(&src_dir);

    // Cold build
    let store = FileStore::open(&store_path).expect("store should open");
    let mut runtime = Runtime::with_store(store);
    let mut host = Host::new();
    host.install_hermetic_process_spawn();

    let term = build_demo_graph(&src_dir, &out_dir, &cc)
        .to_expression()
        .unwrap();
    let traced = runtime
        .run_with_trace(term, &mut host)
        .expect("cold build should run");
    assert_target_succeeded(&traced.value);
    assert_build_demo_binary_runs(&out_dir, "10\n");

    // Change one.c and clean outputs
    std::fs::write(src_dir.join("one.c"), "int one(void) { return 100; }\n").unwrap();
    clean_build_demo_outputs(&out_dir);

    // Incremental build with a fresh runtime (matches CLI behavior)
    let store = FileStore::open(&store_path).expect("store should reopen");
    let mut runtime = Runtime::with_store(store);
    let mut host = Host::new();
    host.install_hermetic_process_spawn();

    let term = build_demo_graph(&src_dir, &out_dir, &cc)
        .to_expression()
        .unwrap();
    let traced = runtime
        .run_with_trace(term, &mut host)
        .expect("incremental build should run");

    let summary = traced.trace.summary();
    assert_eq!(
        summary.thunk_cache_hits, 4,
        "incremental run should hit 4 unaffected compile outer thunks"
    );
    assert_eq!(
        summary.thunk_cache_stores, 3,
        "incremental run should store changed compile (outer+inner keys) and link"
    );
    assert_eq!(
        summary.thunk_cache_invalidations, 3,
        "incremental run should invalidate link, changed compile outer, and changed compile inner"
    );
    assert_eq!(
        summary.thunk_force_all, 1,
        "incremental run should force_all inside link thunk"
    );
    assert_eq!(
        summary.host_handles, 2,
        "incremental run should spawn 2 processes (one compile + link)"
    );
    assert_target_succeeded(&traced.value);
    assert_build_demo_binary_runs(&out_dir, "109\n");

    let _ = std::fs::remove_dir_all(root);
}

fn build_demo_graph(src_dir: &Path, out_dir: &Path, cc: &[u8]) -> BuildGraph {
    let mut graph = BuildGraph::new();

    let sources = ["main.c", "one.c", "two.c", "three.c", "four.c"];

    for source_name in &sources {
        let source = src_dir.join(source_name);
        let object = out_dir.join(source_name.replace(".c", ".o"));
        graph.input(path_bytes(&source));
        graph.action(
            BuildAction::new(vec![
                cc.to_vec(),
                b"-c".to_vec(),
                path_bytes(&source),
                b"-o".to_vec(),
                path_bytes(&object),
            ])
            .inherit_env()
            .input(BuildArtifact::new(path_bytes(&source)))
            .output(BuildArtifact::new(path_bytes(&object))),
        );
    }

    let binary = out_dir.join("hello-demo");
    let mut link_argv = vec![cc.to_vec(), b"-o".to_vec(), path_bytes(&binary)];
    for object_name in ["main.o", "one.o", "two.o", "three.o", "four.o"] {
        link_argv.push(path_bytes(out_dir.join(object_name)));
    }
    let mut link = BuildAction::new(link_argv)
        .inherit_env()
        .output(BuildArtifact::new(path_bytes(&binary)));
    for object_name in ["main.o", "one.o", "two.o", "three.o", "four.o"] {
        link = link.input(BuildArtifact::new(path_bytes(out_dir.join(object_name))));
    }
    let binary_handle = graph.action(link);
    graph
        .target("binary", binary_handle)
        .expect("target should register");

    graph
}

fn write_build_demo_sources(src_dir: &Path) {
    std::fs::write(
        src_dir.join("main.c"),
        r#"
#include <stdio.h>
int one(void);
int two(void);
int three(void);
int four(void);
int main(void) {
  printf("%d\n", one() + two() + three() + four());
  return 0;
}
"#,
    )
    .unwrap();
    std::fs::write(src_dir.join("one.c"), "int one(void) { return 1; }\n").unwrap();
    std::fs::write(src_dir.join("two.c"), "int two(void) { return 2; }\n").unwrap();
    std::fs::write(src_dir.join("three.c"), "int three(void) { return 3; }\n").unwrap();
    std::fs::write(src_dir.join("four.c"), "int four(void) { return 4; }\n").unwrap();
}

fn assert_target_succeeded(value: &RuntimeValue) {
    let RuntimeValue::Data(Value::Record(targets)) = value else {
        panic!("unexpected graph result: {value:?}");
    };
    let Some(Value::Tagged { tag, fields }) = targets.get(&Symbol::from("binary")) else {
        panic!("missing binary target: {targets:?}");
    };
    assert_eq!(tag, &Symbol::from("ok"));
    let [Value::Record(record)] = fields.as_slice() else {
        panic!("unexpected target fields: {fields:?}");
    };
    assert_eq!(
        record.get(&Symbol::from("status")),
        Some(&Value::Tagged {
            tag: Symbol::from("exit_code"),
            fields: vec![Value::Integer(0)],
        }),
        "target record: {record:?}"
    );
}

fn assert_build_demo_binary_runs(out_dir: &Path, expected: &str) {
    let output = Command::new(out_dir.join("hello-demo"))
        .output()
        .expect("binary should run");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8(output.stdout).unwrap(), expected);
}

fn clean_build_demo_outputs(out_dir: &Path) {
    for name in [
        "main.o",
        "one.o",
        "two.o",
        "three.o",
        "four.o",
        "hello-demo",
    ] {
        let _ = std::fs::remove_file(out_dir.join(name));
    }
}

fn binary_path(out_dir: &Path) -> PathBuf {
    out_dir.join("hello-demo")
}

fn find_cc() -> Option<Vec<u8>> {
    std::env::var_os("CC")
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
        .or_else(|| command_path("cc"))
        .or_else(|| command_path("gcc"))
        .map(path_bytes)
}

fn command_path(command: &str) -> Option<PathBuf> {
    let output = Command::new("sh")
        .arg("-c")
        .arg(format!("command -v {command}"))
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let path = String::from_utf8(output.stdout).ok()?;
    let path = path.trim();
    (!path.is_empty()).then(|| PathBuf::from(path))
}

fn path_bytes(path: impl AsRef<Path>) -> Vec<u8> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        path.as_ref().as_os_str().as_bytes().to_vec()
    }
    #[cfg(not(unix))]
    {
        path.as_ref().to_string_lossy().as_bytes().to_vec()
    }
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()))
}
