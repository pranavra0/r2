use std::env;
use std::fs;
use std::process::ExitCode;

use r2::{
    Host, Reified, Runtime, RuntimeTrace, RuntimeTraceSummary, RuntimeValue, TracedRun, Value,
    parse_program,
};

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
        Some("--help") | Some("-h") => {
            println!("{}", usage());
            Ok(())
        }
        Some(command) => Err(format!("unknown command `{command}`\n\n{}", usage())),
        None => Err(usage().to_string()),
    }
}

fn usage() -> &'static str {
    "Usage: r2 run [--trace] [--summary] <file>\n       r2 trace [--summary] <file>\n       r2 --help"
}

fn run_file(args: impl Iterator<Item = String>, force_trace: bool) -> Result<(), String> {
    let parsed = parse_run_args(args, force_trace)?;
    let source = fs::read_to_string(&parsed.path)
        .map_err(|error| format!("failed to read {}: {error}", parsed.path))?;
    let term = parse_program(&source)
        .map_err(|error| format!("failed to parse {}: {error}", parsed.path))?;

    let mut runtime = Runtime::new();
    let mut host = Host::new();
    host.install_fs_read();
    host.install_fs_write();
    host.install_clock();
    host.install_process_spawn();

    if parsed.trace_requested {
        let traced = runtime
            .run_with_trace(term, &mut host)
            .map_err(|error| format!("runtime error: {error}"))?;
        print_traced_run(&traced, parsed.summary_requested);
    } else {
        let value = runtime
            .run(term, &mut host)
            .map_err(|error| format!("runtime error: {error}"))?;
        println!("{}", format_runtime_value(&value));
    }

    Ok(())
}

struct ParsedRunArgs {
    path: String,
    trace_requested: bool,
    summary_requested: bool,
}

fn parse_run_args(
    args: impl Iterator<Item = String>,
    force_trace: bool,
) -> Result<ParsedRunArgs, String> {
    let mut trace_requested = force_trace;
    let mut summary_requested = false;
    let mut path = None;

    for arg in args {
        if !force_trace && arg == "--trace" {
            trace_requested = true;
            continue;
        }

        if arg == "--summary" {
            summary_requested = true;
            continue;
        }

        if path.is_none() {
            path = Some(arg);
        } else {
            return Err(format!("expected exactly one file path\n\n{}", usage()));
        }
    }

    let path = path.ok_or_else(|| usage().to_string())?;
    Ok(ParsedRunArgs {
        path,
        trace_requested,
        summary_requested,
    })
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
        "- thunk cache: hits {}, stores {}, bypasses {}",
        summary.thunk_cache_hits, summary.thunk_cache_stores, summary.thunk_cache_bypasses
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
        RuntimeValue::Closure(_) => match value.reify() {
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
