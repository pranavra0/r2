use r2::{Hash, Node, Outcome, Runtime, TreeEntry, Value};
use std::str::FromStr;

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();
    let store = if args.first().map(String::as_str) == Some("--store") {
        if args.len() < 2 {
            anyhow::bail!("--store requires a path");
        }
        let path = args.remove(1);
        args.remove(0);
        path
    } else {
        ".r2".to_owned()
    };

    let Some(command) = args.first().map(String::as_str) else {
        print_usage();
        return Ok(());
    };

    let rt = Runtime::new(store)?;
    match command {
        "stats" => {
            let stats = rt.store_stats()?;
            println!("objects: {}", stats.object_count);
            println!("outcomes: {}", stats.outcome_count);
            println!("pins: {}", stats.root_count);
            println!("aliases: {}", stats.alias_count);
            println!("bytes: {}", stats.total_bytes);
        }
        "gc-plan" => {
            let plan = rt.gc_plan()?;
            println!("roots: {}", plan.roots.len());
            println!("reachable objects: {}", plan.reachable_objects.len());
            println!("reachable outcomes: {}", plan.reachable_outcomes.len());
            println!("unreachable objects: {}", plan.unreachable_objects.len());
            println!("unreachable outcomes: {}", plan.unreachable_outcomes.len());
        }
        "gc" => {
            let report = rt.gc()?;
            println!("deleted objects: {}", report.deleted_objects);
            println!("deleted outcomes: {}", report.deleted_outcomes);
            println!("deleted bytes: {}", report.deleted_bytes);
        }
        "pin" => {
            let (name, hash) = name_hash_args(&args)?;
            rt.pin(name, hash)?;
        }
        "unpin" => {
            let name = one_arg(&args, "unpin")?;
            match rt.unpin(name)? {
                Some(hash) => println!("{hash}"),
                None => println!("not pinned"),
            }
        }
        "alias" => {
            let (name, hash) = name_hash_args(&args)?;
            rt.alias(name, hash)?;
        }
        "unalias" => {
            let name = one_arg(&args, "unalias")?;
            match rt.unalias(name)? {
                Some(hash) => println!("{hash}"),
                None => println!("not aliased"),
            }
        }
        "resolve" => {
            let name = one_arg(&args, "resolve")?;
            if let Some(hash) = rt.resolve_pin(name)? {
                println!("pin {name} {hash}");
            } else if let Some(hash) = rt.resolve_alias(name)? {
                println!("alias {name} {hash}");
            } else {
                println!("not found");
            }
        }
        "export" => {
            if args.len() != 3 {
                anyhow::bail!("export expects HASH DESTINATION");
            }
            let hash = Hash::from_str(&args[1])?;
            rt.export(hash, &args[2])?;
        }
        "inspect" => {
            let hash = Hash::from_str(one_arg(&args, "inspect")?)?;
            inspect(&rt, &hash)?;
        }
        _ => print_usage(),
    }

    Ok(())
}

fn print_usage() {
    println!(
        "usage: r2 [--store PATH] <stats|gc-plan|gc|pin NAME HASH|unpin NAME|alias NAME HASH|unalias NAME|resolve NAME|export HASH DESTINATION|inspect HASH>"
    );
}

fn inspect(rt: &Runtime, hash: &Hash) -> anyhow::Result<()> {
    if let Some(node) = rt.get_node(hash)? {
        println!("node {hash}");
        match node {
            Node::Value(value) => {
                println!("  value node");
                print_value(&value, "  ");
            }
            Node::Thunk { target } => println!("  thunk -> {target}"),
            Node::Apply { function, args } => {
                println!("  apply {function}");
                for arg in args {
                    println!("    arg {arg}");
                }
            }
            Node::HostCall {
                capability,
                args,
                effect,
            } => {
                println!("  host-call {capability} {effect:?}");
                for arg in args {
                    println!("    arg {arg}");
                }
            }
            Node::Action(spec) => {
                println!("  action {}", spec.program);
                println!("    tool {}", spec.tool);
                println!("    platform {}", spec.platform);
                for input in spec.inputs {
                    println!("    input {} {}", input.path, input.hash);
                }
                for output in spec.outputs {
                    println!("    output {output}");
                }
            }
        }
    }

    if let Some(outcome) = rt.get_outcome(hash)? {
        println!("outcome {hash}");
        match outcome {
            Outcome::Success(value_hash) => println!("  success {value_hash}"),
            Outcome::Failure(failure) => {
                println!("  failure {}", failure.kind);
                print!("  trace:\n{}", failure.trace);
            }
        }
    }

    if let Ok(value) = rt.get_value(hash) {
        println!("value {hash}");
        print_value(&value, "  ");
    }

    Ok(())
}

fn print_value(value: &Value, indent: &str) {
    match value {
        Value::Int(value) => println!("{indent}int {value}"),
        Value::Text(value) => println!("{indent}text {value:?}"),
        Value::Bytes(bytes) => println!("{indent}bytes {}", bytes.len()),
        Value::Blob(bytes) => println!("{indent}blob {}", bytes.len()),
        Value::Tree(tree) => {
            println!("{indent}tree entries={}", tree.entries.len());
            for (name, entry) in &tree.entries {
                match entry {
                    TreeEntry::Blob(hash) => println!("{indent}  blob {name} {hash}"),
                    TreeEntry::Tree(hash) => println!("{indent}  tree {name} {hash}"),
                }
            }
        }
        Value::Tuple(items) => {
            println!("{indent}tuple entries={}", items.len());
            for item in items {
                println!("{indent}  {item}");
            }
        }
        Value::Artifact(hash) => println!("{indent}artifact {hash}"),
        Value::ActionResult {
            outputs,
            stdout,
            stderr,
        } => {
            println!("{indent}action-result");
            println!("{indent}  outputs {outputs}");
            println!("{indent}  stdout {stdout}");
            println!("{indent}  stderr {stderr}");
        }
    }
}

fn one_arg<'a>(args: &'a [String], command: &str) -> anyhow::Result<&'a str> {
    if args.len() != 2 {
        anyhow::bail!("{command} expects one argument");
    }
    Ok(&args[1])
}

fn name_hash_args(args: &[String]) -> anyhow::Result<(&str, Hash)> {
    if args.len() != 3 {
        anyhow::bail!("expected NAME HASH");
    }
    Ok((&args[1], Hash::from_str(&args[2])?))
}
