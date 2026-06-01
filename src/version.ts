// Single source of truth for the SDK's reported version. Bumped together
// with package.json on every release; the build step does not import the
// manifest because that requires JSON import assertions and forces
// every bundler config to opt in.
export const SDK_VERSION = "0.1.1";
