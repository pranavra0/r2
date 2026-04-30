use r2::{EffectKind, HostFn, Outcome, Runtime, Value};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

fn main() -> anyhow::Result<()> {
    let counter = Arc::new(AtomicI64::new(0));
    let counter_for_cap = Arc::clone(&counter);

    let mut rt = Runtime::new(".r2")?;
    rt.register(
        "next",
        HostFn::live(move |_| {
            let value = counter_for_cap.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(Value::Int(value))
        }),
    );

    let next = rt.host_call("next", vec![], EffectKind::Live)?;

    let first = rt.force(next.clone())?;
    println!("first force: cache_hit={}", first.cache_hit);
    print_outcome(&rt, first.outcome)?;

    let second = rt.force(next)?;
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
            println!("failure: {}", failure.kind);
            print!("trace:\n{}", failure.trace);
        }
    }

    Ok(())
}
