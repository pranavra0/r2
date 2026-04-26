# Calculus and IR

r2 programs parse into a small, explicit IR in `src/data.rs`. The surface
language is meant to be pleasant to write, but the runtime works over these
terms.

## Values

`Value` is closed data:

- `Integer`
- `Symbol`
- `Bytes`
- `List`
- `Record`
- `Tagged`

Records use sorted symbol keys, so their canonical digest is stable regardless
of source insertion order. Tagged values represent result-like and variant-like
data such as `ok(...)`, `error(...)`, `exit_code(0)`, and user constructors.

## Terms

`Term` is executable structure:

- `Var`: de Bruijn-indexed local variable.
- `Value`: literal closed data.
- `Lambda`: a closure body with an arity.
- `Apply`: function application.
- `Perform`: an effect request such as `fs.read`, `process.spawn`, or `service.supervise`.
- `Handle`: local effect handlers.
- `Ref`: content-addressed reference into the object store.
- `Rec`: recursive function bindings.
- `Case`: pattern matching.
- `Record`: dynamic record construction.
- `List`: dynamic list construction.

The important split is that `Value::Record` and `Value::List` are already data,
while `Term::Record` and `Term::List` evaluate their fields/items first. That
lets source programs write dynamic data structures without making the value
language effectful.

## Closedness and Digests

Closed terms and values have canonical byte encodings. The store hashes those
bytes with BLAKE3 and uses the digest as the object identity. Open terms are not
accepted by the store.

The runtime uses digests for:

- memoizing closed pure evaluations in-process
- keying thunk cache entries
- storing and loading `Ref` objects
- comparing values structurally across equivalent source spellings

## Surface Lowering

The surface syntax is a readable projection into the calculus:

- `let` lowers to lambda application.
- `let rec` lowers to `Term::Rec`.
- `if` lowers to a `Case` over `true` and `false`.
- `match` lowers to `Term::Case`.
- Arithmetic and comparisons lower to stable math effects.
- `lazy { ... }` and `force ...` lower to the thunk effect.

The surface is intentionally not Lisp syntax, but it keeps the Lisp-like idea
that code is structured data with a small semantic core.
