use r2::{Outcome, Runtime};

fn main() -> anyhow::Result<()> {
    let rt = Runtime::new(".r2")?;

    let a = rt.int(20)?;
    let b = rt.int(22)?;
    let sum_expr = rt.call("+", vec![a, b])?;
    let sum = rt.thunk(sum_expr)?;

    match rt.force(sum)?.outcome {
        Outcome::Success(hash) => {
            println!("unexpected success: {hash}");
        }
        Outcome::Failure(failure) => {
            println!("failure: {}", failure.kind);
            print!("trace:\n{}", failure.trace);
        }
    }

    Ok(())
}
