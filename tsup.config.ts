import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/index.ts"],
  format: ["esm", "cjs"],
  dts: true,
  sourcemap: false,
  clean: true,
  target: "es2022",
  treeshake: true,
  splitting: false,
  minify: false,
  // @feathq/datafile-schema and @feathq/feat-eval are real npm dependencies
  // resolved at consumer install time; tsup leaves them external (the
  // default for declared dependencies).
  external: ["@openfeature/server-sdk", "@openfeature/core"],
});
