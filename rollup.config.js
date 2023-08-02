import typescript from "@rollup/plugin-typescript";
import dts from "rollup-plugin-dts";

export default [
  {
    input: {
      lib: "executor.ts",
      'backends/faunadb': "backends/faunadb.ts",
    },
    output: [
      {
        entryFileNames: "[name].mjs",
        format: "esm",
        dir: './',
      },
      {
        entryFileNames: "[name].cjs",
        format: "cjs",
        dir: './',
      },
    ],
    external: [
      "faunadb",
      "graphql",
      "graphql/jsutils/Path",
      "graphql/execution/execute",
    ],
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
        exclude: [
          /_test\.ts$/,
        ]
      }),
    ],
  },
  {
    input: {
      lib: "executor.ts",
      'backends/faunadb': "backends/faunadb.ts",
    },
    output: [
      {
        entryFileNames: "[name].d.ts",
        dir: './',
      },
    ],
    external: [
      "faunadb",
      "graphql",
      "graphql/jsutils/Path",
    ],
    plugins: [
      dts(),
    ],
  }
]
