use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::OnceLock;

use r2::{
    CancellationToken, Digest, FileStore, Host, ObjectStore, Ref, Reified, Runtime, RuntimeTrace,
    RuntimeTraceSummary, RuntimeValue, TracedRun, Value, parse_program,
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
        Some("run") => run_file(args, false),
        Some("trace") => run_file(args, true),
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
    "Usage: r2 run [--trace] [--summary] [--store <path>|--memory-store] <file>\n       r2 trace [--summary] [--store <path>|--memory-store] <file>\n       r2 store gc [--store <path>] [--root <hash>]...\n       r2 --help\n\nStore defaults to $XDG_STATE_HOME/r2/store on Unix, %LOCALAPPDATA%\\r2\\store on Windows, or .r2-store."
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

fn run_file(args: impl Iterator<Item = String>, force_trace: bool) -> Result<(), String> {
    let parsed = parse_run_args(args, force_trace)?;
    let source = fs::read_to_string(&parsed.path)
        .map_err(|error| format!("failed to read {}: {error}", parsed.path))?;
    let term = parse_program(&source)
        .map_err(|error| format!("failed to parse {}: {error}", parsed.path))?;

    if parsed.memory_store {
        let mut runtime = Runtime::new();
        run_term(term, &mut runtime, &parsed)
    } else {
        let store_path = parsed.store_path.clone().unwrap_or_else(default_store_path);
        let store = FileStore::open(&store_path)
            .map_err(|error| format!("failed to open store {}: {error}", store_path.display()))?;
        let mut runtime = Runtime::with_store(store);
        run_term(term, &mut runtime, &parsed)
    }
}

fn run_term<S: ObjectStore>(
    term: r2::Term,
    runtime: &mut Runtime<S>,
    parsed: &ParsedRunArgs,
) -> Result<(), String> {
    let cancellation = CancellationToken::new();
    install_sigint_handler(cancellation.clone());
    let mut host = Host::with_cancellation(cancellation.clone());
    host.install_fs_read();
    host.install_fs_write();
    host.install_clock();
    host.install_math();
    host.install_hermetic_process_spawn();
    host.install_service_supervise();

    if parsed.trace_requested {
        let traced = runtime
            .run_with_trace_and_cancellation(term, &mut host, &cancellation)
            .map_err(|error| format!("runtime error: {error}"))?;
        print_traced_run(&traced, parsed.summary_requested);
    } else {
        let value = runtime
            .run_with_cancellation(term, &mut host, &cancellation)
            .map_err(|error| format!("runtime error: {error}"))?;
        println!("{}", format_runtime_value(&value));
    }

    Ok(())
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

struct ParsedRunArgs {
    path: String,
    trace_requested: bool,
    summary_requested: bool,
    store_path: Option<PathBuf>,
    memory_store: bool,
}

fn parse_run_args(
    args: impl Iterator<Item = String>,
    force_trace: bool,
) -> Result<ParsedRunArgs, String> {
    let mut trace_requested = force_trace;
    let mut summary_requested = false;
    let mut store_path = None;
    let mut memory_store = false;
    let mut path = None;

    let mut args = args.peekable();
    while let Some(arg) = args.next() {
        if !force_trace && arg == "--trace" {
            trace_requested = true;
            continue;
        }

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

        if path.is_none() {
            path = Some(arg);
        } else {
            return Err(format!("expected exactly one file path\n\n{}", usage()));
        }
    }

    if memory_store && store_path.is_some() {
        return Err(format!(
            "--store and --memory-store cannot be used together\n\n{}",
            usage()
        ));
    }

    let path = path.ok_or_else(|| usage().to_string())?;
    Ok(ParsedRunArgs {
        path,
        trace_requested,
        summary_requested,
        store_path,
        memory_store,
    })
}

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
