# Architecture

Flow:

```text
Rust API / CLI
  -> Term / Value
  -> eval
  -> Runtime
  -> Host handlers
  -> Store
```

Glossary:

- `src/data.rs`: `Term`, `Value`, `Ref`, `Digest`, canonical encoding.
- `src/eval.rs`: pure evaluator; no host IO.
- `src/runtime.rs`: effect loop, built-ins, tracing, thunk cache, cancellation.
- `src/host.rs`: handler registry, policies, process cache/materialization.
- `src/host/*`: leaf handlers for filesystem, clock, math, supervision.
- `src/effects.rs`: typed request/result helpers.
- `src/store.rs`: memory and file content-addressed stores.
- `src/thunk.rs`: delay/force helpers.
- `src/build.rs`: runtime client for process build actions and graph views.
- `src/service.rs`: service specs and restart policy helpers.
- `src/bin/r2.rs`: small CLI for runtime demos and store inspection.

Invariants:

- Evaluation is pure until it yields an effect.
- Host handlers own outside-world authority.
- Cache admission follows effect policy.
- Hermetic effects must include meaningful inputs in provenance.
- Volatile effects must not enter the thunk cache.
- Traces should explain cache, policy, and lifecycle decisions.
- Build graphs are views over runtime work, not core semantics.
