use r2::{ActionInput, ActionSpec, FailureKind, Outcome, Runtime};
use std::collections::BTreeMap;
use std::path::Path;

fn main() -> anyhow::Result<()> {
    let rt = Runtime::new(".r2")?;
    let demo_src = Path::new("examples/bad-c");
    std::fs::create_dir_all(demo_src)?;
    std::fs::write(
        demo_src.join("main.c"),
        br#"int main(void) {
    return nope
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

    rt.pin("demo.bad-c", action.clone())?;
    println!("pinned: demo.bad-c {action}");

    let forced = rt.force(action)?;
    println!("cache_hit={}", forced.cache_hit);
    match forced.outcome {
        Outcome::Success(hash) => println!("unexpected success: {hash}"),
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
