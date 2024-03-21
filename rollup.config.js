import typescript from "@rollup/plugin-typescript";
import dts from "rollup-plugin-dts";

const input = {
  lib: "executor.ts",
  helpers: "helpers.ts",
  "backends/faunadb": "backends/faunadb.ts",
};

const external = [
  "faunadb",
  "graphql",
  "graphql/jsutils/Path",
  "graphql/execution/execute",
];

export default [
  {
    input,
    output: [
      {
        entryFileNames: "[name].mjs",
        chunkFileNames: "[name]-[hash].mjs",
        paths: {
          "graphql/jsutils/Path": "graphql/jsutils/Path.mjs",
          "graphql/execution/execute": "graphql/execution/execute.mjs",
        },
        format: "esm",
        dir: "./",
      },
      {
        entryFileNames: "[name].cjs",
        chunkFileNames: "[name]-[hash].cjs",
        format: "cjs",
        dir: "./",
      },
    ],
    external,
    plugins: [
      typescript({
        compilerOptions: {
          target: "esnext",
          lib: ["esnext", "dom"],
          paths: {
            "*.ts": ["*"],
          },
          moduleResolution: "node",
        },
        exclude: [/_test\.ts$/],
      }),
    ],
  },
  {
    input,
    output: [
      {
        entryFileNames: "[name].d.ts",
        dir: "./",
      },
    ],
    external,
    plugins: [dts()],
  },
];
