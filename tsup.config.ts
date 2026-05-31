import { defineConfig } from "tsup";

// Same posture as @feathq/web-sdk: bundle the internal eval engine and
// schema packages (they're not on npm) and keep OpenFeature external as
// a peer dependency.
export default defineConfig({
  entry: ["src/index.ts"],
  format: ["esm", "cjs"],
  dts: true,
  // Sourcemaps deliberately disabled for published builds; we don't want
  // original comments or internal package paths visible to consumers.
  sourcemap: false,
  clean: true,
  target: "es2022",
  treeshake: true,
  splitting: false,
  minify: false,
  // Force-bundle the internal packages (tsup externalizes `dependencies`
  // by default; we want these in the published artifact since they're
  // not on npm).
  noExternal: ["@feathq/datafile-schema", "@feathq/feat-eval"],
  external: ["@openfeature/server-sdk", "@openfeature/core"],
});
