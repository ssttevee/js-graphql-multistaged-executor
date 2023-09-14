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
        chunkFileNames: "[name]-[hash].mjs",
        format: "esm",
        dir: './',
      },
      {
        entryFileNames: "[name].cjs",
        chunkFileNames: "[name]-[hash].cjs",
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
