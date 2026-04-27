# r2 Build Demo

This demo builds a tiny five-file C program with r2. The compile actions are
independent thunks forced through `thunk.force_all`, then a final link action
depends on the object frontier.

`build.r2` defines a small ordinary r2 `action(argv, inputs, outputs)` helper
and then authors the build as expressions over that helper. The only raw
`process.spawn` request is inside that helper; the demo should not grow Rust or
CLI-specific build helpers. Future ergonomics should move into importable r2
stdlib modules, not into `build::Graph` as the privileged authoring path.

Run from the repository root:

```sh
cargo run -- run --store /tmp/r2-build-demo-store examples/build-demo/build.r2
cargo run -- trace --summary --store /tmp/r2-build-demo-store examples/build-demo/build.r2
examples/build-demo/out/hello-demo
```
