# r2 Build Demo

This demo builds a tiny five-file C program with r2. The compile actions are
independent thunks forced through `thunk.force_all`, then a final link action
depends on the object frontier.

Run from the repository root:

```sh
cargo run -- run --store /tmp/r2-build-demo-store examples/build-demo/build.r2
cargo run -- trace --summary --store /tmp/r2-build-demo-store examples/build-demo/build.r2
examples/build-demo/out/hello-demo
```

On a cold store, the trace shows hermetic `process.spawn` actions and task
events for the compile frontier. On a warm store, the compile and link thunks
hit the thunk cache and the declared output binary is re-materialized from the
store.

Current limitation: the warm no-change path is real, but source-edit
incremental invalidation still needs dependency-keyed thunk caching. Until that
lands, treat this demo as a cold build, warm build, and output
re-materialization demo rather than proof that only one changed source rebuilds.
