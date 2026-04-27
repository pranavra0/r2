# Tracing

Tracing is how r2 explains what happened at runtime. Use:

```sh
target/debug/r2 trace --summary --memory-store program.r2
```

The trace output has three parts:

- `result`: the final runtime value.
- `summary`: counters for evaluation, effects, caching, services, and store activity.
- `trace`: ordered events.

## Common Events

- `eval start`: evaluation began for a term. Closed terms may include a digest.
- `yield`: evaluation performed an effect.
- `host handle`: the host handled an effect and shows its policy.
- `builtin handle`: the runtime handled a built-in effect.
- `run complete`: a run finished with data, closure, continuation, or ref.

## Cache Events

- `memo hit` / `memo store`: in-process memoization of closed pure terms.
- `thunk force`: a thunk was forced.
- `thunk force_all`: an internal batch of independent thunks was forced.
- `task start` / `task end`: a `force_all` branch began or finished; the
  event includes the task id and frontier id.
- `thunk cache store`: a cacheable thunk result was stored.
- `thunk cache hit`: a cached thunk result was reused.
- `thunk cache invalidated`: a cached thunk result was rejected because its
  declared inputs no longer match the cached provenance.
- `thunk cache bypass`: a thunk touched a non-cacheable effect.

The policy shown on `host handle` explains most cache decisions. For example,
`math.add [stable]` can participate in thunk caching, while `fs.write
[volatile]` forces a bypass.

## Service Events

Service supervision emits structured lifecycle events:

- `service spawn`
- `service exit`
- `service restart`
- `service stop`

These make service behavior visible in the same trace stream as build-like
cache behavior. That is the point of r2: builds and services are not separate
worlds in the runtime.

## Reading a Tiny Trace

For:

```r2
let thunk = lazy { 2 + 3 };
let _ = force thunk;
force thunk
```

You should see one `math.add [stable]`, one `thunk cache store`, and one
`thunk cache hit`. The first force computes and stores the result; the second
force reuses it.

For:

```r2
let thunk = lazy { perform fs.write("/tmp/r2-trace.txt", "hello") };
let _ = force thunk;
force thunk
```

You should see `fs.write [volatile]` and `thunk cache bypass`, because writes
change the outside world and are not cacheable by default.
