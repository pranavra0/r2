use std::env;
use std::fs;
use std::process::ExitCode;

use r2::{Host, Reified, Runtime, RuntimeValue, Value, parse_program};

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
        Some("run") => {
            let path = args.next().ok_or_else(|| usage().to_string())?;
            if args.next().is_some() {
                return Err(format!("expected exactly one file path\n\n{}", usage()));
            }

            let source = fs::read_to_string(&path)
                .map_err(|error| format!("failed to read {path}: {error}"))?;
            let term = parse_program(&source)
                .map_err(|error| format!("failed to parse {path}: {error}"))?;

            let mut runtime = Runtime::new();
            let mut host = Host::new();
            host.install_fs_read();
            host.install_fs_write();

            let value = runtime
                .run(term, &mut host)
                .map_err(|error| format!("runtime error: {error}"))?;
            println!("{}", format_runtime_value(&value));
            Ok(())
        }
        Some("--help") | Some("-h") => {
            println!("{}", usage());
            Ok(())
        }
        Some(command) => Err(format!("unknown command `{command}`\n\n{}", usage())),
        None => Err(usage().to_string()),
    }
}

fn usage() -> &'static str {
    "Usage: r2 run <file>\n       r2 --help"
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
