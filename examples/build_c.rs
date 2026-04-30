use r2::{ActionInput, ActionSpec, FailureKind, Outcome, Runtime, TreeEntry, Value};
use std::collections::BTreeMap;
use std::path::Path;

fn main() -> anyhow::Result<()> {
    let rt = Runtime::new(".r2")?;
    let demo_src = Path::new("examples/hello-c");
    std::fs::create_dir_all(demo_src)?;
    std::fs::write(
        demo_src.join("main.c"),
        br#"#include <stdio.h>

int main(void) {
    puts("hello from r2");
    return 0;
}
"#,
    )?;

    let src = rt.import_tree(demo_src)?;
    let cc = rt.import_tool("/bin/cc")?;
    let action = rt.action(ActionSpec {
        program: "/bin/cc".to_owned(),
        tool: cc,
        args: vec![
            "-c".to_owned(),
            "src/main.c".to_owned(),
            "-O2".to_owned(),
            "-o".to_owned(),
            "main.o".to_owned(),
        ],
        env: BTreeMap::new(),
        platform: std::env::consts::OS.to_owned(),
        inputs: vec![ActionInput {
            path: "src".to_owned(),
            hash: src,
        }],
        outputs: vec!["main.o".to_owned()],
    })?;

    let first = rt.force(action.clone())?;
    println!("first force: cache_hit={}", first.cache_hit);
    print_compile_result(&rt, first.outcome)?;

    rt.pin("demo.hello-c", action.clone())?;
    println!("pinned: demo.hello-c {action}");

    let second = rt.force(action)?;
    println!("second force: cache_hit={}", second.cache_hit);
    print_compile_result(&rt, second.outcome)?;

    Ok(())
}

fn print_compile_result(rt: &Runtime, outcome: Outcome) -> anyhow::Result<()> {
    match outcome {
        Outcome::Success(hash) => {
            let Value::ActionResult {
                outputs,
                stdout,
                stderr,
            } = rt.get_value(&hash)?
            else {
                anyhow::bail!("compile did not produce an action result");
            };
            let Value::Tree(tree) = rt.force_value(outputs)?.0 else {
                anyhow::bail!("compile result did not reference an output tree");
            };
            match tree.entries.get("main.o") {
                Some(TreeEntry::Blob(binary)) => {
                    let Value::Blob(bytes) = rt.force_value(binary.clone())?.0 else {
                        anyhow::bail!("main.o output was not a blob");
                    };
                    println!("main.o bytes: {}", bytes.len());
                    std::fs::create_dir_all("examples/out")?;
                    rt.export(binary.clone(), "examples/out/main.o")?;
                    println!("exported: examples/out/main.o");
                }
                _ => anyhow::bail!("compile output tree did not contain main.o"),
            }
            let Value::Blob(stdout) = rt.force_value(stdout)?.0 else {
                anyhow::bail!("stdout was not a blob");
            };
            let Value::Blob(stderr) = rt.force_value(stderr)?.0 else {
                anyhow::bail!("stderr was not a blob");
            };
            println!("stdout bytes: {}", stdout.len());
            println!("stderr bytes: {}", stderr.len());
        }
        Outcome::Failure(failure) => {
            println!("failure: {}", failure.kind);
            if let FailureKind::ActionFailed { stderr, .. } = &failure.kind {
                println!("stderr:\n{stderr}");
            }
            print!("trace:\n{}", failure.trace);
        }
    }

    Ok(())
}
