# Effects and Policies

Effects are the boundary between pure r2 evaluation and the outside world. A
term performs an effect with `Term::Perform { op, args }`; the runtime yields
that request to either a built-in handler or the host.

## Built-in vs Host Effects

Built-in effects are part of the runtime. Today the main built-in is
`thunk.force`, which implements lazy thunks and policy-aware caching.

Host effects are registered on `Host`:

- `fs.read`
- `fs.write`
- `clock.now`
- `clock.sleep`
- `math.*`
- `process.spawn`
- `service.supervise`

Each host handler receives evaluated runtime values, performs whatever outside
work it owns, and resumes the captured continuation with a runtime value.

## Policy Quadrants

Every host effect has a `HostEffectPolicy`:

- `volatile`: ambient and not cacheable. Use this for time, sleeps, filesystem writes, services, and other live-world actions.
- `stable`: ambient but cacheable. Use this only for deterministic host helpers such as integer math.
- `declared`: has declared provenance but is not cacheable. This is useful while an effect knows its inputs but is not trusted as reproducible.
- `hermetic`: declared and cacheable. Use this when all relevant inputs are declared and included in the cache key.

Policy controls thunk caching. If a thunk hits a non-cacheable effect, the
runtime records a thunk-cache bypass and does not store the result.

## Writing a New Effect

1. Pick an op name, usually `module.verb`.
2. Define a record-shaped request value. Prefer explicit fields over positional magic.
3. Define a tagged result shape, usually `ok(record)` or a typed error tag.
4. Register a host handler with the right policy.
5. Add trace/acceptance tests that exercise the effect through the public runtime or CLI.

Example shape:

```rust
host.register_stable("math.add", |args, continuation| {
    // parse args, compute result
    continuation.resume(RuntimeValue::Data(Value::Integer(5))).map_err(Into::into)
});
```

## Process Effects

`process.spawn` takes:

- `argv`
- `env_mode`
- `env`
- `cwd` optionally
- `stdin`
- `declared_inputs`
- `declared_outputs`

The hermetic process handler hashes declared input contents into its cache key,
stores result values, and re-materializes declared outputs on cache hits.

With the `sandbox` Cargo feature enabled, r2 also applies a conservative
declared-path guard before spawning. Stronger OS-level isolation is still future
work.

## Service Effects

`service.supervise` mirrors `process.spawn` and adds:

```r2
restart_policy: { mode: "on_failure", max_restarts: 3, delay_nanos: 0 }
```

It is volatile. It records service lifecycle trace events and returns
`ok({ final_status, restart_count })` when the policy stops.
