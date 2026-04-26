use r2::{
    Canonical, CaseBranch, EvalError, EvalResult, Host, Pattern, Runtime, RuntimeValue, Symbol,
    Term, Value, eval, parse_program,
};

#[test]
fn if_matches_boolean_symbols() {
    let term = parse_program("if true then 1 else 2").expect("program should parse");
    let result = eval(term).expect("program should evaluate");

    assert_integer(result, 1);
}

#[test]
fn match_supports_symbols_and_wildcards() {
    let term = parse_program("match :left { :right => 1; _ => 2 }").expect("program should parse");
    let result = eval(term).expect("program should evaluate");

    assert_integer(result, 2);
}

#[test]
fn let_rec_can_call_itself_through_match() {
    let term = parse_program(
        "let rec loop = fn(x) => match x { :done => 1; _ => loop(:done) }; loop(:go)",
    )
    .expect("program should parse");
    let result = eval(term).expect("program should evaluate");

    assert_integer(result, 1);
}

#[test]
fn let_rec_supports_mutual_recursion() {
    let term = parse_program(
        "let rec even = fn(x) => match x { :stop => :even; _ => odd(:stop) }, odd = fn(x) => match x { :stop => :odd; _ => even(:stop) }; even(:go)",
    )
    .expect("program should parse");
    let result = eval(term).expect("program should evaluate");

    match result {
        EvalResult::Done(RuntimeValue::Data(Value::Symbol(symbol))) => {
            assert_eq!(symbol, Symbol::from("odd"))
        }
        other => panic!("unexpected result: {other:?}"),
    }
}

#[test]
fn let_rec_digest_is_stable_across_binding_names() {
    let left = parse_program("let rec f = fn(x) => f(x); f").expect("left should parse");
    let right = parse_program("let rec g = fn(y) => g(y); g").expect("right should parse");

    assert_eq!(left.digest(), right.digest());
}

#[test]
fn case_matches_tagged_values_and_binds_fields() {
    let term = Term::Case {
        scrutinee: Box::new(Term::Value(Value::Tagged {
            tag: Symbol::from("pair"),
            fields: vec![Value::Integer(1), Value::Integer(2)],
        })),
        branches: vec![CaseBranch::new(
            Pattern::Tagged {
                tag: Symbol::from("pair"),
                fields: vec![Pattern::Bind, Pattern::Bind],
            },
            Term::var(1),
        )],
    };

    let result = eval(term).expect("case should evaluate");

    assert_integer(result, 1);
}

#[test]
fn unmatched_case_reports_the_scrutinee_shape() {
    let term = Term::Case {
        scrutinee: Box::new(Term::Value(Value::Symbol(Symbol::from("missing")))),
        branches: vec![CaseBranch::new(
            Pattern::Symbol(Symbol::from("present")),
            Term::Value(Value::Integer(1)),
        )],
    };

    let error = eval(term).expect_err("case should not match");

    assert!(matches!(error, EvalError::UnmatchedCase { .. }));
    assert!(error.to_string().contains("symbol :missing"));
}

#[test]
fn arithmetic_uses_standard_precedence() {
    let result = run("2 + 3 * 4");

    assert_runtime_integer(result, 14);
}

#[test]
fn comparisons_return_boolean_symbols() {
    let result = run("5 == 5");

    assert_runtime_symbol(result, "true");
}

#[test]
fn thunk_caches_stable_arithmetic_effects() {
    let term = parse_program("let x = lazy { 2 + 3 }; let _ = force x; force x")
        .expect("program should parse");
    let mut runtime = Runtime::new();
    let mut host = Host::new();
    host.install_math();

    let traced = runtime
        .run_with_trace(term, &mut host)
        .expect("program should run");
    let summary = traced.trace.summary();

    assert_runtime_integer(traced.value, 5);
    assert_eq!(summary.stable_host_handles, 1);
    assert_eq!(summary.thunk_cache_stores, 1);
    assert_eq!(summary.thunk_cache_hits, 1);
    assert_eq!(summary.thunk_cache_bypasses, 0);
}

#[test]
fn integer_factorial_runs_with_recursion_and_arithmetic() {
    let result = run(
        "let rec factorial = fn(n) => if n == 0 then 1 else n * factorial(n - 1); factorial(5)",
    );

    assert_runtime_integer(result, 120);
}

fn assert_integer(result: EvalResult, expected: i64) {
    match result {
        EvalResult::Done(RuntimeValue::Data(Value::Integer(value))) => assert_eq!(value, expected),
        other => panic!("unexpected result: {other:?}"),
    }
}

fn run(source: &str) -> RuntimeValue {
    let term = parse_program(source).expect("program should parse");
    let mut runtime = Runtime::new();
    let mut host = Host::new();
    host.install_math();
    runtime.run(term, &mut host).expect("program should run")
}

fn assert_runtime_integer(value: RuntimeValue, expected: i64) {
    match value {
        RuntimeValue::Data(Value::Integer(value)) => assert_eq!(value, expected),
        other => panic!("unexpected value: {other:?}"),
    }
}

fn assert_runtime_symbol(value: RuntimeValue, expected: &str) {
    match value {
        RuntimeValue::Data(Value::Symbol(symbol)) => assert_eq!(symbol, Symbol::from(expected)),
        other => panic!("unexpected value: {other:?}"),
    }
}
