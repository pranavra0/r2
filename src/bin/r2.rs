use std::env;
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::OnceLock;

use r2::{
    BuildAction, BuildArtifact, BuildGraph, CancellationToken, Digest, FileStore, Ref, Reified,
    RuntimeRunner, RuntimeTrace, RuntimeTraceSummary, RuntimeValue, TracedRun, Value,
};

static CURRENT_CANCELLATION: OnceLock<CancellationToken> = OnceLock::new();

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let mut args = env::args().skip(1);
    match args.next().as_deref() {
        Some("build-demo") => run_build_demo(args),
        Some("store") => run_store(args),
        Some("--help") | Some("-h") => {
            println!("{}", usage());
            Ok(())
        }
        Some(command) => Err(format!("unknown command `{command}`\n\n{}", usage())),
        None => Err(usage().to_string()),
    }
}

fn usage() -> &'static str {
    "Usage: r2 build-demo [--summary] [--store <path>|--memory-store]\n       r2 store gc [--store <path>] [--root <hash>]...\n       r2 --help\n\nStore defaults to $XDG_STATE_HOME/r2/store on Unix, %LOCALAPPDATA%\\r2\\store on Windows, or .r2-store."
}

fn run_build_demo(args: impl Iterator<Item = String>) -> Result<(), String> {
    let mut summary_requested = false;
    let mut store_path = None;
    let mut memory_store = false;

    let mut args = args.peekable();
    while let Some(arg) = args.next() {
        if arg == "--summary" {
            summary_requested = true;
            continue;
        }

        if arg == "--memory-store" {
            memory_store = true;
            continue;
        }

        if arg == "--store" {
            let Some(value) = args.next() else {
                return Err(format!("expected path after --store\n\n{}", usage()));
            };
            store_path = Some(PathBuf::from(value));
            continue;
        }

        if let Some(value) = arg.strip_prefix("--store=") {
            if value.is_empty() {
                return Err(format!("expected path after --store=\n\n{}", usage()));
            }
            store_path = Some(PathBuf::from(value));
            continue;
        }

        if arg.starts_with("--") {
            return Err(format!("unknown flag `{arg}`\n\n{}", usage()));
        }

        return Err(format!("unexpected argument `{arg}`\n\n{}", usage()));
    }

    if memory_store && store_path.is_some() {
        return Err(format!(
            "--store and --memory-store cannot be used together\n\n{}",
            usage()
        ));
    }

    let cc = find_cc().ok_or("no C compiler found (tried $CC, cc, gcc)")?;
    let src_dir = PathBuf::from("examples/build-demo/src");
    let out_dir = PathBuf::from("examples/build-demo/out");

    std::fs::create_dir_all(&out_dir)
        .map_err(|e| format!("failed to create output directory: {e}"))?;

    let mut graph = BuildGraph::new();
    let sources = ["main.c", "one.c", "two.c", "three.c", "four.c"];

    for source_name in &sources {
        let source = src_dir.join(source_name);
        let object = out_dir.join(source_name.replace(".c", ".o"));
        graph.input(path_bytes(&source));
        graph.action(
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
    }

    let binary = out_dir.join("hello-demo");
    let mut link_argv = vec![cc.clone(), b"-o".to_vec(), path_bytes(&binary)];
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
        .map_err(|e| format!("target error: {e}"))?;

    let term = graph
        .to_expression()
        .map_err(|e| format!("graph error: {e}"))?;

    let traced = if memory_store {
        let mut runner = RuntimeRunner::memory().process_build_host();
        install_sigint_handler(runner.cancellation().clone());
        runner
            .run_traced(term)
            .map_err(|error| format!("runtime error: {error}"))?
    } else {
        let store_path = store_path.unwrap_or_else(default_store_path);
        let mut runner = RuntimeRunner::file_store(&store_path)
            .map_err(|error| format!("failed to open store {}: {error}", store_path.display()))?;
        runner = runner.process_build_host();
        install_sigint_handler(runner.cancellation().clone());
        runner
            .run_traced(term)
            .map_err(|error| format!("runtime error: {error}"))?
    };

    print_traced_run(&traced, summary_requested);
    Ok(())
}

fn run_store(args: impl Iterator<Item = String>) -> Result<(), String> {
    let mut args = args;
    match args.next().as_deref() {
        Some("gc") => run_store_gc(args),
        Some(command) => Err(format!("unknown store command `{command}`\n\n{}", usage())),
        None => Err(format!("expected store command\n\n{}", usage())),
    }
}

fn run_store_gc(args: impl Iterator<Item = String>) -> Result<(), String> {
    let mut store_path = None;
    let mut roots = Vec::new();
    let mut args = args.peekable();

    while let Some(arg) = args.next() {
        if arg == "--store" {
            let Some(value) = args.next() else {
                return Err(format!("expected path after --store\n\n{}", usage()));
            };
            store_path = Some(PathBuf::from(value));
            continue;
        }
        if let Some(value) = arg.strip_prefix("--store=") {
            if value.is_empty() {
                return Err(format!("expected path after --store=\n\n{}", usage()));
            }
            store_path = Some(PathBuf::from(value));
            continue;
        }
        if arg == "--root" {
            let Some(value) = args.next() else {
                return Err(format!("expected hash after --root\n\n{}", usage()));
            };
            roots.push(parse_root(&value)?);
            continue;
        }
        if let Some(value) = arg.strip_prefix("--root=") {
            roots.push(parse_root(value)?);
            continue;
        }
        return Err(format!("unknown flag `{arg}`\n\n{}", usage()));
    }

    let store_path = store_path.unwrap_or_else(default_store_path);
    let store = FileStore::open(&store_path)
        .map_err(|error| format!("failed to open store {}: {error}", store_path.display()))?;
    let report = store
        .gc(&roots)
        .map_err(|error| format!("failed to gc store {}: {error}", store_path.display()))?;

    println!("reachable: {}", report.reachable);
    println!("kept objects: {}", report.kept_objects);
    println!("deleted objects: {}", report.deleted_objects);
    println!("deleted cache entries: {}", report.deleted_cache_entries);
    Ok(())
}

fn parse_root(value: &str) -> Result<Ref, String> {
    Digest::from_str(value)
        .map(Ref::new)
        .map_err(|error| format!("invalid root hash `{value}`: {error}"))
}

fn find_cc() -> Option<Vec<u8>> {
    env::var_os("CC")
        .map(PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty())
        .or_else(|| command_path("cc"))
        .or_else(|| command_path("gcc"))
        .map(path_bytes)
}

fn command_path(command: &str) -> Option<PathBuf> {
    let output = std::process::Command::new("sh")
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

#[cfg(unix)]
fn path_bytes(path: impl AsRef<std::path::Path>) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    path.as_ref().as_os_str().as_bytes().to_vec()
}

#[cfg(not(unix))]
fn path_bytes(path: impl AsRef<std::path::Path>) -> Vec<u8> {
    path.as_ref().to_string_lossy().as_bytes().to_vec()
}

fn install_sigint_handler(cancellation: CancellationToken) {
    let _ = CURRENT_CANCELLATION.set(cancellation);
    install_platform_sigint_handler();
}

#[cfg(unix)]
fn install_platform_sigint_handler() {
    unsafe extern "C" fn handle_sigint(_: libc::c_int) {
        if let Some(token) = CURRENT_CANCELLATION.get() {
            token.cancel();
        }
    }

    unsafe {
        libc::signal(
            libc::SIGINT,
            handle_sigint as *const () as libc::sighandler_t,
        );
    }
}

#[cfg(not(unix))]
fn install_platform_sigint_handler() {}

fn default_store_path() -> PathBuf {
    default_store_base()
        .map(|base| base.join("r2").join("store"))
        .unwrap_or_else(|| PathBuf::from(".r2-store"))
}

#[cfg(windows)]
fn default_store_base() -> Option<PathBuf> {
    env::var_os("LOCALAPPDATA")
        .filter(|value| !value.as_os_str().is_empty())
        .map(PathBuf::from)
}

#[cfg(not(windows))]
fn default_store_base() -> Option<PathBuf> {
    env::var_os("XDG_STATE_HOME")
        .filter(|value| !value.as_os_str().is_empty())
        .map(PathBuf::from)
}

fn print_traced_run(traced: &TracedRun, include_summary: bool) {
    println!("result: {}", format_runtime_value(&traced.value));
    if include_summary {
        println!("summary:");
        print_trace_summary(&traced.trace.summary());
    }
    println!("trace:");
    print_trace(&traced.trace);
}

fn print_trace(trace: &RuntimeTrace) {
    for event in trace.events() {
        println!("- {event}");
    }
}

fn print_trace_summary(summary: &RuntimeTraceSummary) {
    println!("- boundary steps: {}", summary.total_events);
    println!("- eval starts: {}", summary.eval_starts);
    println!("- yields: {}", summary.yields);
    println!("- builtin handles: {}", summary.builtin_handles);
    println!(
        "- host handles: {} (stable: {}, volatile: {}, declared: {}, hermetic: {})",
        summary.host_handles,
        summary.stable_host_handles,
        summary.volatile_host_handles,
        summary.declared_host_handles,
        summary.hermetic_host_handles
    );
    println!(
        "- service: spawns {}, exits {}, restarts {}, stops {}",
        summary.service_spawns,
        summary.service_exits,
        summary.service_restarts,
        summary.service_stops
    );
    println!(
        "- thunk forces: single {}, batches {}",
        summary.thunk_forces, summary.thunk_force_all
    );
    println!(
        "- tasks: starts {}, ends {}",
        summary.task_starts, summary.task_ends
    );
    println!(
        "- thunk cache: hits {}, stores {}, bypasses {}, invalidations {}",
        summary.thunk_cache_hits,
        summary.thunk_cache_stores,
        summary.thunk_cache_bypasses,
        summary.thunk_cache_invalidations
    );
    println!(
        "- memo table: hits {}, stores {}",
        summary.memo_hits, summary.memo_stores
    );
    println!(
        "- persisted refs: {} (terms: {}, values: {})",
        summary.persisted, summary.persisted_terms, summary.persisted_values
    );
}

fn format_runtime_value(value: &RuntimeValue) -> String {
    match value {
        RuntimeValue::Data(data) => format_value(data),
        RuntimeValue::Closure(_) | RuntimeValue::RecursiveClosure(_) => match value.reify() {
            Some(Reified::Lambda(lambda)) => format!("<lambda/{}>", lambda.parameters),
            _ => "<closure>".to_string(),
        },
        RuntimeValue::Continuation(_) => "<continuation>".to_string(),
        RuntimeValue::Ref(reference) => format!("ref({})", reference.hash),
    }
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Integer(value) => value.to_string(),
        Value::Symbol(symbol) => format!(":{}", symbol.as_str()),
        Value::Bytes(bytes) => format_bytes(bytes),
        Value::List(items) => {
            let rendered = items
                .iter()
                .map(format_value)
                .collect::<Vec<_>>()
                .join(", ");
            format!("[{rendered}]")
        }
        Value::Record(entries) => {
            let rendered = entries
                .iter()
                .map(|(key, value)| format!("{}: {}", key.as_str(), format_value(value)))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{{{rendered}}}")
        }
        Value::Tagged { tag, fields } => {
            let rendered = fields
                .iter()
                .map(format_value)
                .collect::<Vec<_>>()
                .join(", ");
            format!("{}({rendered})", tag.as_str())
        }
    }
}

fn format_bytes(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(text) => format!("{text:?}"),
        Err(_) => {
            let rendered = bytes
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            format!("0x{rendered}")
        }
    }
}
