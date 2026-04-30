use r2::{FailureKind, HostFn, Outcome, Runtime, Value};

fn main() -> anyhow::Result<()> {
    let store = ".r2";
    let _ = std::fs::remove_dir_all(store);

    let mut rt = Runtime::new(store)?;

    // Register spreadsheet operations
    rt.register(
        "*",
        HostFn::pure(|args| match args {
            [Value::Int(a), Value::Int(b)] => Ok(Value::Int(a * b)),
            _ => Err(FailureKind::TypeError("* expects two ints".to_owned())),
        }),
    );

    rt.register(
        "+",
        HostFn::pure(|args| match args {
            [Value::Int(a), Value::Int(b)] => Ok(Value::Int(a + b)),
            _ => Err(FailureKind::TypeError("+ expects two ints".to_owned())),
        }),
    );

    // Create config cells
    let price_initial = rt.int(100)?;
    let price_cell = rt.cell_new(price_initial)?;
    println!("Created price cell: {}", price_cell.0);

    let quantity_initial = rt.int(3)?;
    let quantity_cell = rt.cell_new(quantity_initial)?;
    println!("Created quantity cell: {}", quantity_cell.0);

    // Build derived expressions
    let price_read = rt.read_cell(price_cell.clone())?;
    let quantity_read = rt.read_cell(quantity_cell.clone())?;
    let subtotal_expr = rt.call("*", vec![price_read, quantity_read])?;

    let shipping = rt.int(10)?;
    let total_expr = rt.call("+", vec![subtotal_expr.clone(), shipping])?;
    let total_thunk = rt.thunk(total_expr)?;

    println!("\n=== First force (cold) ===");
    let first = rt.force(total_thunk.clone())?;
    println!("cache_hit={}", first.cache_hit);
    print_outcome(&rt, first.outcome)?;

    println!("\n=== Second force (warm) ===");
    let second = rt.force(total_thunk.clone())?;
    println!("cache_hit={}", second.cache_hit);
    print_outcome(&rt, second.outcome)?;

    println!("\n=== Update price cell (100 -> 150) ===");
    let new_price = rt.int(150)?;
    let version = rt.cell_set(&price_cell, new_price)?;
    println!(
        "New version: index={}, hash={}",
        version.index, version.value
    );

    println!("\n=== Third force after cell update ===");
    let third = rt.force(total_thunk.clone())?;
    println!("cache_hit={}", third.cache_hit);
    print_outcome(&rt, third.outcome)?;

    println!("\n=== Fourth force (warm again) ===");
    let fourth = rt.force(total_thunk.clone())?;
    println!("cache_hit={}", fourth.cache_hit);
    print_outcome(&rt, fourth.outcome)?;

    println!("\n=== Observed cell versions in cached outcome ===");
    if let Some(cached) = rt.get_outcome(&total_thunk)? {
        println!("Cached outcome for total_thunk: {:?}", cached);
    }

    println!("\n=== Full graph trace / explain ===");
    println!("{}", rt.explain(&total_thunk)?);

    // Also show the subtotal thunk to demonstrate intermediate caching
    let subtotal_thunk = rt.thunk(subtotal_expr)?;
    println!("\n=== Subtotal thunk explain ===");
    println!("{}", rt.explain(&subtotal_thunk)?);

    Ok(())
}

fn print_outcome(rt: &Runtime, outcome: Outcome) -> anyhow::Result<()> {
    match outcome {
        Outcome::Success(hash) => {
            let value = rt.get_value(&hash)?;
            println!("value: {:?}", value);
        }
        Outcome::Failure(failure) => {
            println!("failure: {}", failure.kind);
            print!("trace:\n{}", failure.trace);
        }
    }
    Ok(())
}
