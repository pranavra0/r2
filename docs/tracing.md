# Tracing

Traces explain what the runtime did.

Top-level output:

- `result`: final runtime value.
- `summary`: counters.
- `trace`: ordered events.

Common events:

- `eval start`: evaluation began.
- `yield`: a term performed an effect.
- `builtin handle`: runtime handled a built-in effect.
- `host handle`: host handled an effect and records policy.
- `host event`: structured lifecycle event from a host handler.
- `run complete`: execution finished.

Cache events:

- `memo hit`
- `memo store`
- `thunk force`
- `thunk force_all`
- `task start`
- `task end`
- `thunk cache hit`
- `thunk cache store`
- `thunk cache invalidated`
- `thunk cache bypass`

Reading policy:

- `stable` and `hermetic` effects may participate in thunk caching.
- `volatile` effects force cache bypass.
- invalidations mean declared provenance no longer matched.
