use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use r2::{BuildAction, BuildArtifact, BuildGraph, Host, Runtime, RuntimeValue, Symbol, Value};

#[test]
fn build_graph_compiles_and_links_a_small_c_project() {
    let Some(cc) = find_cc() else {
        eprintln!("skipping build graph smoke because no cc was found");
        return;
    };

    let root = unique_temp_dir("r2-build-graph");
    std::fs::create_dir_all(&root).expect("temp dir should create");
    write_sources(&root);

    let mut graph = BuildGraph::new();
    let mut source_handles = Vec::new();
    let mut object_handles = Vec::new();
    let mut objects = Vec::new();

    for source_name in ["main.c", "one.c", "two.c", "three.c", "four.c"] {
        let source = root.join(source_name);
        let object = root.join(source_name.replace(".c", ".o"));
        let source_handle = graph.input(path_bytes(&source));
        let object_handle = graph.action(
            BuildAction::new(vec![
                cc.clone(),
                b"-c".to_vec(),
                path_bytes(&source),
                b"-o".to_vec(),
                path_bytes(&object),
            ])
            .inherit_env()
            .input(BuildArtifact::new(path_bytes(&source)))
            .output(BuildArtifact::new(path_bytes(&object))),
        );
        source_handles.push(source_handle);
        object_handles.push(object_handle);
        objects.push(object);
    }

    let binary = root.join("app");
    let mut link_argv = vec![cc, b"-o".to_vec(), path_bytes(&binary)];
    link_argv.extend(objects.iter().map(path_bytes));
    let mut link = BuildAction::new(link_argv)
        .inherit_env()
        .output(BuildArtifact::new(path_bytes(&binary)));
    for object in &objects {
        link = link.input(BuildArtifact::new(path_bytes(object)));
    }
    let binary_handle = graph.action(link);
    graph
        .target("binary", binary_handle)
        .expect("target should register");

    assert_eq!(graph.len(), 11);
    let dependencies = graph
        .dependencies_of(binary_handle)
        .expect("dependencies should resolve");
    for handle in source_handles.iter().chain(object_handles.iter()) {
        assert!(
            dependencies.contains(handle),
            "missing dependency {handle:?}"
        );
    }
    assert_eq!(
        graph
            .reverse_dependencies_of(source_handles[0])
            .expect("reverse deps should resolve"),
        vec![object_handles[0], binary_handle]
    );
    assert_eq!(
        graph
            .topological_order()
            .expect("topo should resolve")
            .last(),
        Some(&binary_handle)
    );
    let layers = graph
        .topological_layers()
        .expect("topological layers should resolve");
    assert_eq!(layers.len(), 3);
    assert_eq!(layers[0], source_handles);
    assert_eq!(layers[1], object_handles);
    assert_eq!(layers[2], vec![binary_handle]);
    let dot = graph.render_dot();
    assert!(dot.starts_with("digraph build {"));
    assert!(dot.contains("target:binary"));

    let mut runtime = Runtime::new();
    let mut host = Host::new();
    host.install_hermetic_process_spawn();
    let traced = runtime
        .run_with_trace(
            graph.to_expression().expect("graph should project"),
            &mut host,
        )
        .expect("graph expression should run");
    assert_eq!(traced.trace.summary().thunk_force_all, 1);

    assert_target_succeeded(traced.value);
    let output = Command::new(&binary)
        .output()
        .expect("linked binary should run");
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert_eq!(String::from_utf8(output.stdout).unwrap(), "10\n");

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn independent_slow_graph_actions_capture_current_sequential_baseline() {
    let Some(shell) = command_path("sh") else {
        eprintln!("skipping slow graph baseline because no sh was found");
        return;
    };

    let root = unique_temp_dir("r2-build-graph-slow");
    std::fs::create_dir_all(&root).expect("temp dir should create");
    let mut graph = BuildGraph::new();
    let mut handles = Vec::new();

    for index in 0..4 {
        let output = root.join(format!("out-{index}.txt"));
        handles.push(
            graph.action(
                BuildAction::new(vec![
                    path_bytes(&shell),
                    b"-c".to_vec(),
                    b"sleep 0.1; printf ok > \"$1\"".to_vec(),
                    b"sh".to_vec(),
                    path_bytes(&output),
                ])
                .inherit_env()
                .output(BuildArtifact::new(path_bytes(&output))),
            ),
        );
    }
    for (index, handle) in handles.iter().enumerate() {
        graph
            .target(format!("out_{index}"), *handle)
            .expect("target should register");
    }

    assert_eq!(
        graph.topological_layers().expect("layers should resolve"),
        vec![handles]
    );

    let mut runtime = Runtime::new();
    let mut host = Host::new();
    host.install_hermetic_process_spawn();
    let started = Instant::now();
    let result = runtime
        .run(
            graph.to_expression().expect("graph should project"),
            &mut host,
        )
        .expect("graph expression should run");
    let elapsed = started.elapsed();

    let RuntimeValue::Data(Value::Record(targets)) = result else {
        panic!("unexpected graph result");
    };
    assert_eq!(targets.len(), 4);
    assert!(
        elapsed >= Duration::from_millis(350),
        "current runtime should still be sequential before §2.0 parallelism; elapsed {elapsed:?}"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn dependent_slow_graph_frontier_runs_in_parallel() {
    let Some(shell) = command_path("sh") else {
        eprintln!("skipping slow graph frontier because no sh was found");
        return;
    };

    let root = unique_temp_dir("r2-build-graph-frontier");
    std::fs::create_dir_all(&root).expect("temp dir should create");
    let mut graph = BuildGraph::new();
    let mut outputs = Vec::new();

    for index in 0..4 {
        let output = root.join(format!("out-{index}.txt"));
        outputs.push(output.clone());
        graph.action(
            BuildAction::new(vec![
                path_bytes(&shell),
                b"-c".to_vec(),
                b"sleep 0.2; printf ok > \"$1\"".to_vec(),
                b"sh".to_vec(),
                path_bytes(&output),
            ])
            .inherit_env()
            .output(BuildArtifact::new(path_bytes(&output))),
        );
    }

    let combined = root.join("combined.txt");
    let mut combine = BuildAction::new(vec![
        path_bytes(&shell),
        b"-c".to_vec(),
        b"cat \"$1\" \"$2\" \"$3\" \"$4\" > \"$5\"".to_vec(),
        b"sh".to_vec(),
        path_bytes(&outputs[0]),
        path_bytes(&outputs[1]),
        path_bytes(&outputs[2]),
        path_bytes(&outputs[3]),
        path_bytes(&combined),
    ])
    .inherit_env()
    .output(BuildArtifact::new(path_bytes(&combined)));
    for output in &outputs {
        combine = combine.input(BuildArtifact::new(path_bytes(output)));
    }
    let combined_handle = graph.action(combine);
    graph
        .target("combined", combined_handle)
        .expect("target should register");

    let mut runtime = Runtime::new();
    let mut host = Host::new();
    host.install_hermetic_process_spawn();
    let started = Instant::now();
    let traced = runtime
        .run_with_trace(
            graph.to_expression().expect("graph should project"),
            &mut host,
        )
        .expect("graph expression should run");
    let elapsed = started.elapsed();

    let RuntimeValue::Data(Value::Record(targets)) = traced.value else {
        panic!("unexpected graph result");
    };
    assert_eq!(targets.len(), 1);
    assert!(
        elapsed < Duration::from_millis(750),
        "dependency frontier should run below serial wall time; elapsed {elapsed:?}"
    );
    let summary = traced.trace.summary();
    assert_eq!(summary.thunk_force_all, 1);
    assert_eq!(summary.task_starts, 4);
    assert_eq!(summary.task_ends, 4);
    assert_eq!(
        std::fs::read_to_string(&combined).expect("combined output should exist"),
        "okokokok"
    );

    let _ = std::fs::remove_dir_all(root);
}

fn write_sources(root: &Path) {
    std::fs::write(
        root.join("main.c"),
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
    .expect("main should write");
    std::fs::write(root.join("one.c"), "int one(void) { return 1; }\n").expect("one should write");
    std::fs::write(root.join("two.c"), "int two(void) { return 2; }\n").expect("two should write");
    std::fs::write(root.join("three.c"), "int three(void) { return 3; }\n")
        .expect("three should write");
    std::fs::write(root.join("four.c"), "int four(void) { return 4; }\n")
        .expect("four should write");
}

fn assert_target_succeeded(value: RuntimeValue) {
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
    path.as_ref().to_string_lossy().as_bytes().to_vec()
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()))
}

fn stderr(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}
