# Build Demo

Five C files compile through runtime build actions, then link into
`out/hello-demo`.

Run:

```sh
cargo run -- build-demo --summary
examples/build-demo/out/hello-demo
```

Runtime pieces used:

- `BuildGraph`
- `BuildAction`
- `Runtime`
- `Host`
- `FileStore`
