# r2

A dynamic, cacheable, effectful language.

## Build

```sh
cargo build
cargo test
```

Run the CLI from the repo:

```sh
cargo run -- --help
```

Or use the built binary:

```sh
target/debug/r2 --help
```

## Tutorial 

Create `hello.r2`:

```r2
let input = "/tmp/r2-hello-input.txt";
let output = "/tmp/r2-hello-output.txt";

let build = lazy {
  perform process.spawn({
    argv: ["/bin/sh", "-c", "cat \"$1\" > \"$2\"", "sh", input, output],
    env_mode: "clear",
    env: {},
    stdin: "",
    declared_inputs: [input],
    declared_outputs: [output]
  })
};

let _ = force build;
force build
```

Prepare the input and run with tracing:

```sh
printf 'hello from r2\n' > /tmp/r2-hello-input.txt
target/debug/r2 trace --memory-store --summary hello.r2
```

The result is a `process.spawn` record containing the exit status, stdout,
stderr, declared inputs, declared outputs, and captured output file contents.
The trace should show a first `thunk cache store` and a second `thunk cache hit`:
the same lazy build step was forced twice, but the hermetic process effect made
the thunk cacheable.

Now try a live service-style effect:

```r2
perform service.supervise({
  argv: ["/bin/sh", "-c", "exit 1"],
  env_mode: "clear",
  env: {},
  stdin: "",
  declared_inputs: [],
  declared_outputs: [],
  restart_policy: { mode: "on_failure", max_restarts: 3, delay_nanos: 0 }
})
```

Run it with:

```sh
target/debug/r2 trace --memory-store --summary service.r2
```

This is deliberately volatile: the service supervisor is about running and
observing live processes, not caching them. The trace reports service spawn,
exit, restart, and stop events.

## Useful Commands

```sh
target/debug/r2 run --memory-store program.r2
target/debug/r2 trace --summary --memory-store program.r2
target/debug/r2 store gc --store .r2-store
```

The default persistent store is `$XDG_STATE_HOME/r2/store` on Unix,
`%LOCALAPPDATA%\r2\store` on Windows, or `.r2-store` if no platform state
directory is available.
