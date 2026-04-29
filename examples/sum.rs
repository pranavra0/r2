use r2::{FailureKind, HostFn, Outcome, Runtime, Value};

fn main() -> anyhow::Result<()> {
    let mut rt = Runtime::new(".r2")?;
    rt.register(
        "+",
        HostFn::pure(|args| match args {
            [Value::Int(a), Value::Int(b)] => Ok(Value::Int(a + b)),
            _ => Err(FailureKind::TypeError("+ expects two ints".to_owned())),
        }),
    );

    let a = rt.int(20)?;
    let b = rt.int(22)?;
    let sum = rt.call("+", vec![a, b])?;

    let first = rt.force(sum.clone())?;
    println!("first force: cache_hit={}", first.cache_hit);
    print_outcome(&rt, first.outcome)?;

    let second = rt.force(sum)?;
    println!("second force: cache_hit={}", second.cache_hit);
    print_outcome(&rt, second.outcome)?;

    Ok(())
}

fn print_outcome(rt: &Runtime, outcome: Outcome) -> anyhow::Result<()> {
    match outcome {
        Outcome::Success(hash) => {
            let value = rt.get_value(&hash)?;
            println!("success: {value:?}");
        }
        Outcome::Failure(failure) => {
            println!("failure: {failure:?}");
        }
    }

    Ok(())
}
