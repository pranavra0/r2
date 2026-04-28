# Calculus

r2 runs explicit Rust-built IR.

Glossary:

- `Value`: closed data.
- `Term`: executable structure.
- `Ref`: content-addressed store reference.
- `Digest`: canonical BLAKE3 identity.
- `Symbol`: interned-ish name value.
- `Lambda`: closure body with arity.
- `Perform`: effect request.
- `Handle`: local effect handler term.
- `Record` / `List`: data containers; term forms evaluate children first.
- `Case` / `Pattern`: pattern dispatch.
- `Rec`: recursive function bindings.

Store rule:

- closed terms and values may be stored
- open terms may not be stored
- canonical bytes determine identity

Runtime uses digests for:

- pure memoization
- thunk cache keys
- stored refs
- structural equality across equivalent data
