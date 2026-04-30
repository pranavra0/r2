use r2::{ActionInput, ActionSpec, Outcome, Runtime, TreeEntry, Value};
use std::collections::BTreeMap;
use std::path::Path;

fn main() -> anyhow::Result<()> {
    let rt = Runtime::new(".r2")?;
    let demo_src = Path::new("examples/demo-src");
    std::fs::create_dir_all(demo_src)?;
    std::fs::write(demo_src.join("main.txt"), b"hello from declared input\n")?;

    let src = rt.import_tree(demo_src)?;
    let cp = rt.import_tool("/bin/cp")?;
    let action = rt.action(ActionSpec {
        program: "/bin/cp".to_owned(),
        tool: cp,
        args: vec!["src/main.txt".to_owned(), "out.txt".to_owned()],
        env: BTreeMap::new(),
        platform: std::env::consts::OS.to_owned(),
        inputs: vec![ActionInput {
            path: "src".to_owned(),
            hash: src,
        }],
        outputs: vec!["out.txt".to_owned()],
    })?;

    let first = rt.force(action.clone())?;
    println!("first force: cache_hit={}", first.cache_hit);
    print_output(&rt, first.outcome)?;

    rt.pin("demo.build-copy", action.clone())?;
    println!("pinned: demo.build-copy {action}");

    let second = rt.force(action)?;
    println!("second force: cache_hit={}", second.cache_hit);
    print_output(&rt, second.outcome)?;

    Ok(())
}

fn print_output(rt: &Runtime, outcome: Outcome) -> anyhow::Result<()> {
    match outcome {
        Outcome::Success(hash) => {
            let value = rt.get_value(&hash)?;
            let Value::ActionResult {
                outputs,
                stdout,
                stderr,
            } = value
            else {
                anyhow::bail!("action output was not an action result");
            };
            let Value::Tree(tree) = rt.force_value(outputs)?.0 else {
                anyhow::bail!("action result did not reference an output tree");
            };
            let Some(TreeEntry::Blob(blob)) = tree.entries.get("out.txt") else {
                anyhow::bail!("action output tree did not contain out.txt");
            };
            match rt.force_value(blob.clone())?.0 {
                Value::Blob(bytes) => {
                    println!("out.txt = {}", String::from_utf8_lossy(&bytes));
                }
                other => println!("out.txt = {other:?}"),
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
            print!("trace:\n{}", failure.trace);
        }
    }
    Ok(())
}
