let action = fn(argv, inputs, outputs) => lazy {
  perform process.spawn({
    argv: argv,
    env_mode: "inherit",
    env: {},
    stdin: "",
    declared_inputs: inputs,
    declared_outputs: outputs
  })
};

let main_o = action(
  ["/bin/sh", "-c", "cc -c examples/build-demo/src/main.c -o examples/build-demo/out/main.o", "sh"],
  ["examples/build-demo/src/main.c"],
  ["examples/build-demo/out/main.o"]
);
let one_o = action(
  ["/bin/sh", "-c", "cc -c examples/build-demo/src/one.c -o examples/build-demo/out/one.o", "sh"],
  ["examples/build-demo/src/one.c"],
  ["examples/build-demo/out/one.o"]
);
let two_o = action(
  ["/bin/sh", "-c", "cc -c examples/build-demo/src/two.c -o examples/build-demo/out/two.o", "sh"],
  ["examples/build-demo/src/two.c"],
  ["examples/build-demo/out/two.o"]
);
let three_o = action(
  ["/bin/sh", "-c", "cc -c examples/build-demo/src/three.c -o examples/build-demo/out/three.o", "sh"],
  ["examples/build-demo/src/three.c"],
  ["examples/build-demo/out/three.o"]
);
let four_o = action(
  ["/bin/sh", "-c", "cc -c examples/build-demo/src/four.c -o examples/build-demo/out/four.o", "sh"],
  ["examples/build-demo/src/four.c"],
  ["examples/build-demo/out/four.o"]
);

let objects = [
  "examples/build-demo/out/main.o",
  "examples/build-demo/out/one.o",
  "examples/build-demo/out/two.o",
  "examples/build-demo/out/three.o",
  "examples/build-demo/out/four.o"
];

let link = action(
  [
    "/bin/sh",
    "-c",
    "cc examples/build-demo/out/main.o examples/build-demo/out/one.o examples/build-demo/out/two.o examples/build-demo/out/three.o examples/build-demo/out/four.o -o examples/build-demo/out/hello-demo",
    "sh"
  ],
  objects,
  ["examples/build-demo/out/hello-demo"]
);

let _ = perform thunk.force_all(main_o, one_o, two_o, three_o, four_o);
force link
