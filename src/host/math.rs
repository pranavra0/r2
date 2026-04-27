use super::*;

pub(super) fn install(host: &mut Host) {
    host.register_stable(MATH_ADD_OP, |args, continuation| {
        integer_handler(args, continuation, MATH_ADD_OP, |left, right| {
            left.checked_add(right)
                .ok_or_else(|| "math.add overflowed i64".to_string())
        })
    });
    host.register_stable(MATH_SUB_OP, |args, continuation| {
        integer_handler(args, continuation, MATH_SUB_OP, |left, right| {
            left.checked_sub(right)
                .ok_or_else(|| "math.sub overflowed i64".to_string())
        })
    });
    host.register_stable(MATH_MUL_OP, |args, continuation| {
        integer_handler(args, continuation, MATH_MUL_OP, |left, right| {
            left.checked_mul(right)
                .ok_or_else(|| "math.mul overflowed i64".to_string())
        })
    });
    host.register_stable(MATH_DIV_OP, |args, continuation| {
        integer_handler(args, continuation, MATH_DIV_OP, |left, right| {
            if right == 0 {
                Err("math.div cannot divide by zero".to_string())
            } else {
                left.checked_div(right)
                    .ok_or_else(|| "math.div overflowed i64".to_string())
            }
        })
    });
    host.register_stable(MATH_REM_OP, |args, continuation| {
        integer_handler(args, continuation, MATH_REM_OP, |left, right| {
            if right == 0 {
                Err("math.rem cannot divide by zero".to_string())
            } else {
                left.checked_rem(right)
                    .ok_or_else(|| "math.rem overflowed i64".to_string())
            }
        })
    });

    for (op, compare) in [
        (MATH_EQ_OP, i64::eq as fn(&i64, &i64) -> bool),
        (MATH_NE_OP, i64::ne),
        (MATH_LT_OP, i64::lt),
        (MATH_LE_OP, i64::le),
        (MATH_GT_OP, i64::gt),
        (MATH_GE_OP, i64::ge),
    ] {
        host.register_stable(op, move |args, continuation| {
            compare_handler(args, continuation, op, |left, right| compare(&left, &right))
        });
    }
}

fn integer_handler(
    args: Vec<RuntimeValue>,
    continuation: Continuation,
    op: &str,
    operation: impl FnOnce(i64, i64) -> Result<i64, String>,
) -> Result<EvalResult, HostError> {
    let value = match parse_args(args.as_slice(), op).and_then(|(left, right)| {
        operation(left, right).map(|value| RuntimeValue::Data(Value::Integer(value)))
    }) {
        Ok(value) => value,
        Err(message) => error_value(message),
    };

    continuation.resume(value).map_err(Into::into)
}

fn compare_handler(
    args: Vec<RuntimeValue>,
    continuation: Continuation,
    op: &str,
    operation: impl FnOnce(i64, i64) -> bool,
) -> Result<EvalResult, HostError> {
    let value = match parse_args(args.as_slice(), op) {
        Ok((left, right)) => RuntimeValue::Data(boolean_value(operation(left, right))),
        Err(message) => error_value(message),
    };

    continuation.resume(value).map_err(Into::into)
}

fn parse_args(args: &[RuntimeValue], op: &str) -> Result<(i64, i64), String> {
    match args {
        [RuntimeValue::Data(left), RuntimeValue::Data(right)] => Ok((
            parse_integer(left, &format!("{op} left argument must be an integer"))?,
            parse_integer(right, &format!("{op} right argument must be an integer"))?,
        )),
        _ => Err(format!("{op} expected two integer arguments")),
    }
}
