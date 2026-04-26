let main_o = lazy {
  perform process.spawn({
    argv: ["/bin/sh", "-c", "cc -c examples/build-demo/src/main.c -o examples/build-demo/out/main.o", "sh"],
    env_mode: "inherit",
    env: {},
    stdin: "",
    declared_inputs: ["examples/build-demo/src/main.c"],
    declared_outputs: ["examples/build-demo/out/main.o"]
  })
};
let one_o = lazy {
  perform process.spawn({
    argv: ["/bin/sh", "-c", "cc -c examples/build-demo/src/one.c -o examples/build-demo/out/one.o", "sh"],
    env_mode: "inherit",
    env: {},
    stdin: "",
    declared_inputs: ["examples/build-demo/src/one.c"],
    declared_outputs: ["examples/build-demo/out/one.o"]
  })
};
let two_o = lazy {
  perform process.spawn({
    argv: ["/bin/sh", "-c", "cc -c examples/build-demo/src/two.c -o examples/build-demo/out/two.o", "sh"],
    env_mode: "inherit",
    env: {},
    stdin: "",
    declared_inputs: ["examples/build-demo/src/two.c"],
    declared_outputs: ["examples/build-demo/out/two.o"]
  })
};
let three_o = lazy {
  perform process.spawn({
    argv: ["/bin/sh", "-c", "cc -c examples/build-demo/src/three.c -o examples/build-demo/out/three.o", "sh"],
    env_mode: "inherit",
    env: {},
    stdin: "",
    declared_inputs: ["examples/build-demo/src/three.c"],
    declared_outputs: ["examples/build-demo/out/three.o"]
  })
};
let four_o = lazy {
  perform process.spawn({
    argv: ["/bin/sh", "-c", "cc -c examples/build-demo/src/four.c -o examples/build-demo/out/four.o", "sh"],
    env_mode: "inherit",
    env: {},
    stdin: "",
    declared_inputs: ["examples/build-demo/src/four.c"],
    declared_outputs: ["examples/build-demo/out/four.o"]
  })
};
let link = lazy {
  perform process.spawn({
    argv: ["/bin/sh", "-c", "cc examples/build-demo/out/main.o examples/build-demo/out/one.o examples/build-demo/out/two.o examples/build-demo/out/three.o examples/build-demo/out/four.o -o examples/build-demo/out/hello-demo", "sh"],
    env_mode: "inherit",
    env: {},
    stdin: "",
    declared_inputs: [
      "examples/build-demo/out/main.o",
      "examples/build-demo/out/one.o",
      "examples/build-demo/out/two.o",
      "examples/build-demo/out/three.o",
      "examples/build-demo/out/four.o"
    ],
    declared_outputs: ["examples/build-demo/out/hello-demo"]
  })
};
let _ = perform thunk.force_all(main_o, one_o, two_o, three_o, four_o);
force link
