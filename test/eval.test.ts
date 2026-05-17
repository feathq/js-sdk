import { describe, expect, it } from "vitest";
import type { Datafile, FlagSpec } from "@feathq/datafile-schema";
import { evaluate, type EvalContext } from "@feathq/feat-eval";

// Tiny datafile builder. Each test composes the bits it cares about and
// inherits sensible defaults for the rest.
function makeDatafile(overrides: Partial<Datafile> = {}): Datafile {
  return {
    schemaVersion: 1,
    envId: "env-1",
    envKey: "staging",
    projectId: "proj-1",
    version: 1,
    etag: "etag",
    generatedAt: "2026-05-17T00:00:00Z",
    flags: {},
    segments: {},
    contextKinds: {
      user: { key: "user", availableForRules: true, availableForExperiments: true },
    },
    ...overrides,
  };
}

const TRUE_VAR = { id: "var-true", name: "true", value: true };
const FALSE_VAR = { id: "var-false", name: "false", value: false };

function boolFlag(overrides: Partial<FlagSpec> = {}): FlagSpec {
  return {
    id: "flag-1",
    key: "checkout",
    valueType: "boolean",
    salt: "abcdef0123456789",
    archived: false,
    isEnabled: true,
    offVariationId: FALSE_VAR.id,
    defaultVariationId: FALSE_VAR.id,
    defaultRollout: null,
    defaultBucketingContextKindKey: null,
    variations: [TRUE_VAR, FALSE_VAR],
    targets: [],
    rules: [],
    ...overrides,
  };
}

describe("evaluate", () => {
  it("returns off when archived", async () => {
    const df = makeDatafile({
      flags: { checkout: boolFlag({ archived: true }) },
    });
    const r = await evaluate("checkout", false, { user: { key: "u1" } }, df);
    expect(r.value).toBe(false);
    expect(r.variationId).toBe(FALSE_VAR.id);
    expect(r.reason).toBe("DISABLED");
  });

  it("returns off when isEnabled is false", async () => {
    const df = makeDatafile({
      flags: { checkout: boolFlag({ isEnabled: false }) },
    });
    const r = await evaluate("checkout", true, { user: { key: "u1" } }, df);
    expect(r.value).toBe(false);
    expect(r.reason).toBe("DISABLED");
  });

  it("returns default when targeting doesn't match", async () => {
    const df = makeDatafile({ flags: { checkout: boolFlag() } });
    const r = await evaluate("checkout", true, { user: { key: "u1" } }, df);
    expect(r.value).toBe(false);
    expect(r.variationId).toBe(FALSE_VAR.id);
    expect(r.reason).toBe("FALLTHROUGH");
  });

  it("individual target wins over rules", async () => {
    const df = makeDatafile({
      flags: {
        checkout: boolFlag({
          targets: [
            { contextKindKey: "user", contextKey: "u-vip", variationId: TRUE_VAR.id },
          ],
          rules: [
            {
              id: "r1",
              bucketingContextKindKey: null,
              variationId: FALSE_VAR.id,
              rollout: null,
              groups: [
                {
                  conditions: [
                    {
                      attributePath: "user.key",
                      operator: "is_one_of",
                      values: ["u-vip"],
                    },
                  ],
                },
              ],
            },
          ],
        }),
      },
    });
    const r = await evaluate("checkout", false, { user: { key: "u-vip" } }, df);
    expect(r.value).toBe(true);
    expect(r.reason).toBe("TARGETING_MATCH");
    expect(r.variationId).toBe(TRUE_VAR.id);
  });

  it("matches a rule with ends_with on user.email", async () => {
    const df = makeDatafile({
      flags: {
        checkout: boolFlag({
          rules: [
            {
              id: "r1",
              bucketingContextKindKey: null,
              variationId: TRUE_VAR.id,
              rollout: null,
              groups: [
                {
                  conditions: [
                    {
                      attributePath: "user.email",
                      operator: "ends_with",
                      values: ["@example.com"],
                    },
                  ],
                },
              ],
            },
          ],
        }),
      },
    });
    const ctx: EvalContext = { user: { key: "u1", email: "alice@example.com" } };
    const r = await evaluate("checkout", false, ctx, df);
    expect(r.value).toBe(true);
    expect(r.reason).toBe("TARGETING_MATCH");
  });

  it("rule with multiple groups uses OR semantics", async () => {
    const df = makeDatafile({
      flags: {
        checkout: boolFlag({
          rules: [
            {
              id: "r1",
              bucketingContextKindKey: null,
              variationId: TRUE_VAR.id,
              rollout: null,
              groups: [
                {
                  conditions: [
                    {
                      attributePath: "user.email",
                      operator: "ends_with",
                      values: ["@nope.com"],
                    },
                  ],
                },
                {
                  conditions: [
                    {
                      attributePath: "user.plan",
                      operator: "is_one_of",
                      values: ["pro", "enterprise"],
                    },
                  ],
                },
              ],
            },
          ],
        }),
      },
    });
    const r = await evaluate(
      "checkout",
      false,
      { user: { key: "u1", email: "x@elsewhere.com", plan: "pro" } },
      df,
    );
    expect(r.value).toBe(true);
  });

  it("rollout buckets deterministically by (salt, flagKey, contextKey)", async () => {
    const df = makeDatafile({
      flags: {
        checkout: boolFlag({
          defaultVariationId: null,
          defaultRollout: {
            bucketingContextKindKey: "user",
            variations: [
              { variationId: TRUE_VAR.id, weight: 50_000 },
              { variationId: FALSE_VAR.id, weight: 50_000 },
            ],
          },
        }),
      },
    });
    const r1 = await evaluate("checkout", false, { user: { key: "stable-key" } }, df);
    const r2 = await evaluate("checkout", false, { user: { key: "stable-key" } }, df);
    expect(r1.value).toBe(r2.value);
    expect(r1.reason).toBe("SPLIT");
  });

  it("100% rollout always picks that variation", async () => {
    const df = makeDatafile({
      flags: {
        checkout: boolFlag({
          defaultVariationId: null,
          defaultRollout: {
            bucketingContextKindKey: "user",
            variations: [{ variationId: TRUE_VAR.id, weight: 100_000 }],
          },
        }),
      },
    });
    for (const key of ["u1", "u2", "u3", "u4", "u5"]) {
      const r = await evaluate("checkout", false, { user: { key } }, df);
      expect(r.value).toBe(true);
    }
  });

  it("segment_match recurses into the datafile's segments map", async () => {
    const df = makeDatafile({
      segments: {
        "internal-users": {
          key: "internal-users",
          rules: [
            {
              conditions: [
                {
                  attributePath: "user.email",
                  operator: "ends_with",
                  values: ["@feathq.com"],
                },
              ],
            },
          ],
        },
      },
      flags: {
        checkout: boolFlag({
          rules: [
            {
              id: "r1",
              bucketingContextKindKey: null,
              variationId: TRUE_VAR.id,
              rollout: null,
              groups: [
                {
                  conditions: [
                    {
                      attributePath: "",
                      operator: "segment_match",
                      values: ["internal-users"],
                    },
                  ],
                },
              ],
            },
          ],
        }),
      },
    });
    const hit = await evaluate(
      "checkout",
      false,
      { user: { key: "u1", email: "bob@feathq.com" } },
      df,
    );
    expect(hit.value).toBe(true);
    expect(hit.reason).toBe("TARGETING_MATCH");

    const miss = await evaluate(
      "checkout",
      false,
      { user: { key: "u2", email: "bob@other.com" } },
      df,
    );
    expect(miss.value).toBe(false);
  });

  it("semver_gt compares versions correctly", async () => {
    const df = makeDatafile({
      flags: {
        checkout: boolFlag({
          rules: [
            {
              id: "r1",
              bucketingContextKindKey: null,
              variationId: TRUE_VAR.id,
              rollout: null,
              groups: [
                {
                  conditions: [
                    {
                      attributePath: "user.app_version",
                      operator: "semver_gte",
                      values: ["1.2.0"],
                    },
                  ],
                },
              ],
            },
          ],
        }),
      },
    });
    const newer = await evaluate(
      "checkout",
      false,
      { user: { key: "u1", app_version: "1.5.0" } },
      df,
    );
    expect(newer.value).toBe(true);
    const older = await evaluate(
      "checkout",
      false,
      { user: { key: "u2", app_version: "1.1.5" } },
      df,
    );
    expect(older.value).toBe(false);
  });

  it("returns defaultValue with reason ERROR when flag missing", async () => {
    const df = makeDatafile();
    const r = await evaluate("missing", "fallback", { user: { key: "u" } }, df);
    expect(r.value).toBe("fallback");
    expect(r.reason).toBe("ERROR");
  });
});
