# Architecture Map

This guide is the codebase tour for new contributors. r2 is small enough that
the modules are meant to stay understandable, but several pieces interact:
syntax lowers to IR, evaluation yields effects, runtime drives those effects,
host handlers touch the world, and the store gives cacheable work a durable
identity.

## Big Picture

```text
.r2 source
  -> syntax.rs parses and lowers
  -> data.rs Term/Value IR
  -> eval.rs pure evaluator
  -> runtime.rs driver, tracing, memo/thunk cache, cancellation
  -> host.rs effect handlers
  -> store.rs content-addressed persistence
```

The binary in `src/bin/r2.rs` wires those pieces together for `run`, `trace`,
and `store gc`.

## Module Guide

`src/data.rs`
: Defines the canonical IR: `Value`, `Term`, `Pattern`, `Ref`, `Digest`, and
  `Canonical`. If you add a new term or value variant, update closedness,
  canonical encoding, serde expectations, docs, and tests.

`src/syntax.rs`
: Parses the surface language and lowers it into `Term`. Keep surface sugar
  here. The runtime should not need to know whether a term came from a nice
  source spelling or was built directly by Rust APIs.

`src/eval.rs`
: The pure evaluator. It handles closures, applications, records/lists,
  pattern matching, and effect yields. It should not perform host IO.

`src/runtime.rs`
: The orchestration layer. It repeatedly evaluates, handles built-ins, delegates
  host effects, records traces, manages memo/thunk caches, persists cacheable
  thunk results, and honors cancellation tokens.

`src/host.rs`
: Default host effects: filesystem, clock, math, process spawn, and service
  supervision. This is where outside-world behavior lives. Any cache-related
  decision here must line up with a `HostEffectPolicy`.

`src/store.rs`
: Content-addressed object storage. `MemoryStore` is for tests and ephemeral
  runs; `FileStore` persists objects and thunk cache entries, supports GC, and
  enforces a soft size cap.

`src/thunk.rs`
: Tiny helper module for lazy thunks. The actual force semantics live in
  `runtime.rs` because forcing needs access to caches and host policy.

`src/effects.rs`
: Typed builders and decoders for effect request/result shapes. Use these when
  Rust code wants to construct or decode r2 effect values without hand-building
  records everywhere.

`src/build.rs`
: Build-oriented typed API over `process.spawn`. This currently models one
  action; graph authoring is planned next.

`src/service.rs`
: Service-oriented typed API and restart-policy logic. The actual supervisor
  effect handler is in `host.rs`.

`src/bin/r2.rs`
: CLI plumbing. Keep policy and runtime semantics out of here where possible;
  the CLI should mostly parse flags, open stores, install host handlers, and
  print values/traces.

## Runtime Flow

1. `Runtime::run*` calls `eval`.
2. `eval` returns either `Done(value)` or `Yielded(effect)`.
3. The runtime records `yield`.
4. Built-ins get first chance. Today the important built-in is `thunk.force`.
5. Otherwise, the host handles the effect and returns the next `EvalResult`.
6. The runtime records host policy and continues until `Done`.

Thunk caching happens inside the built-in `thunk.force` path. A thunk is cached
only if the whole forced computation avoids non-cacheable host effects.

## Store Model

Stored objects are either closed `Term`s or `Value`s. The store hashes canonical
bytes, so identical objects share a `Ref`.

FileStore layout:

```text
store/
  objects/          content-addressed Stored values
  cache/thunks/     thunk digest -> cached result ref
  access/objects/   LRU sidecar markers for size eviction
```

GC starts from explicit `Ref` roots, walks `Term::Ref` edges, deletes
unreachable object files, and drops thunk cache entries pointing at unreachable
objects.

## Adding a Feature

For a surface-language feature:

1. Add syntax parsing/lowering in `src/syntax.rs`.
2. Add or reuse IR in `src/data.rs`.
3. Add evaluator behavior in `src/eval.rs` only if the IR needs new semantics.
4. Add integration tests in `tests/language.rs`.
5. Run a real CLI smoke if the feature is user-facing.

For a host effect:

1. Add typed request/result helpers in `src/effects.rs` if useful.
2. Register a handler in `src/host.rs`.
3. Pick the correct `HostEffectPolicy`.
4. Add trace expectations if policy/caching behavior matters.
5. Add CLI acceptance coverage if users can invoke it from `.r2`.

For runtime/store behavior:

1. Prefer focused unit tests near `runtime.rs` or `store.rs`.
2. Add integration/CLI tests when behavior is visible outside the module.
3. Check persistent-store behavior, not just `MemoryStore`.

## Design Invariants

- The store accepts only closed terms.
- Canonical encoding changes are compatibility-affecting.
- Volatile effects must not silently enter the thunk cache.
- Hermetic effects must include all meaningful inputs in their cache key.
- Service supervision is volatile, even though it may spawn processes.
- Cancellation should be checked at yield boundaries and inside long host loops.
- Public acceptance tests live in `tests/`; private invariants can stay inline.
