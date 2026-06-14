import { describe, expect, it, vi } from "vitest";
import { FeatClient } from "../src/client";

const DATAFILE = {
  schemaVersion: 1,
  envId: "env-1",
  envKey: "staging",
  projectId: "proj-1",
  version: 1,
  etag: "e1",
  generatedAt: "2026-05-17T00:00:00Z",
  flags: {},
  segments: {},
  contextKinds: {
    user: { key: "user", availableForRules: true, availableForExperiments: true },
  },
};

function makeFetch() {
  const calls: { url: string; init: RequestInit | undefined }[] = [];
  const fetchImpl = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    calls.push({ url, init });
    if (url.endsWith("/sdk/v1/datafile")) {
      return new Response(JSON.stringify(DATAFILE), { status: 200, headers: { etag: "e1" } });
    }
    if (url.endsWith("/sdk/v1/events")) return new Response(null, { status: 202 });
    return new Response(null, { status: 404 });
  }) as unknown as typeof fetch;
  return { fetchImpl, calls };
}

describe("FeatClient usage events", () => {
  it("records evaluated contexts and flushes them to /sdk/v1/events on close", async () => {
    const { fetchImpl, calls } = makeFetch();
    const client = new FeatClient({ apiKey: "k", url: "https://dp.test", fetch: fetchImpl });
    await client.ready();
    await client.evaluate("checkout", false, { user: { key: "u1" }, organization: { key: "acme" } });
    client.close();

    await vi.waitFor(() => {
      const ev = calls.find((c) => c.url.endsWith("/sdk/v1/events"));
      expect(ev).toBeDefined();
      expect(JSON.parse(ev!.init!.body as string)).toEqual({
        contexts: [
          { kind: "user", key: "u1" },
          { kind: "organization", key: "acme" },
        ],
      });
    });
  });

  it("sends no events when events: false", async () => {
    const { fetchImpl, calls } = makeFetch();
    const client = new FeatClient({
      apiKey: "k",
      url: "https://dp.test",
      fetch: fetchImpl,
      events: false,
    });
    await client.ready();
    await client.evaluate("checkout", false, { user: { key: "u1" } });
    client.close();

    await new Promise((r) => setTimeout(r, 20));
    expect(calls.some((c) => c.url.endsWith("/sdk/v1/events"))).toBe(false);
  });
});
