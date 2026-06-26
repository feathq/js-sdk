import { afterEach, describe, expect, it, vi } from "vitest";
import type { Datafile, FlagSpec } from "@feathq/datafile-schema";
import { FeatClient } from "../src/client";
import { fetchSseTransport, type SseFrame, type SseTransport, type SseTransportOptions } from "../src/streaming";

// --- datafile fixtures ---------------------------------------------------

const TRUE_VAR = { id: "var-true", name: "true", value: true };
const FALSE_VAR = { id: "var-false", name: "false", value: false };

function boolFlag(defaultVariationId: string): FlagSpec {
  return {
    id: "flag-1",
    key: "checkout",
    valueType: "boolean",
    salt: "abcdef0123456789",
    archived: false,
    isEnabled: true,
    offVariationId: FALSE_VAR.id,
    defaultVariationId,
    defaultRollout: null,
    defaultBucketingContextKindKey: null,
    variations: [TRUE_VAR, FALSE_VAR],
    targets: [],
    rules: [],
  };
}

// Each version flips the default variation so an adopted datafile is
// observable through evaluate(): v with TRUE_VAR resolves true, FALSE_VAR
// resolves false.
function makeDatafile(version: number, on: boolean): Datafile {
  return {
    schemaVersion: 1,
    envId: "env-1",
    envKey: "staging",
    projectId: "proj-1",
    version,
    etag: `etag-${version}`,
    generatedAt: "2026-05-17T00:00:00Z",
    flags: { checkout: boolFlag(on ? TRUE_VAR.id : FALSE_VAR.id) },
    segments: {},
    contextKinds: {
      user: { key: "user", availableForRules: true, availableForExperiments: true },
    },
  };
}

const CTX = { user: { key: "u1" } };

// --- mock SSE transport --------------------------------------------------

interface MockConnection {
  options: SseTransportOptions;
  open: () => void;
  put: (df: Datafile) => void;
  frame: (frame: SseFrame) => void;
  endCleanly: () => void;
  fail: (err: unknown) => void;
}

function makeMockTransport() {
  const connections: MockConnection[] = [];
  const transport: SseTransport = (options) =>
    new Promise<void>((resolve, reject) => {
      const conn: MockConnection = {
        options,
        open: () => options.onOpen?.(),
        put: (df) =>
          options.onFrame({ event: "put", id: String(df.version), data: JSON.stringify(df) }),
        frame: (frame) => options.onFrame(frame),
        endCleanly: () => resolve(),
        fail: (err) => reject(err),
      };
      connections.push(conn);
      options.signal.addEventListener(
        "abort",
        () => reject(Object.assign(new Error("aborted"), { name: "AbortError" })),
        { once: true },
      );
    });
  return {
    transport,
    connections,
    latest: () => connections[connections.length - 1]!,
  };
}

// Always-rejecting transport, for the no-stream / fallback paths.
function makeFailingTransport() {
  const attempts: SseTransportOptions[] = [];
  const transport: SseTransport = (options) => {
    attempts.push(options);
    return Promise.reject(new Error("connect refused"));
  };
  return { transport, attempts };
}

// --- mock datafile fetch -------------------------------------------------

// Returns a fetch that serves `versions[i]` on the i-th call and the last
// entry thereafter. Records calls so headers can be asserted.
function makeFetch(versions: Datafile[]) {
  const calls: Array<{ url: string; headers: Record<string, string> }> = [];
  const fetchImpl = (async (url: string | URL, init?: RequestInit) => {
    const headers = Object.fromEntries(
      Object.entries((init?.headers ?? {}) as Record<string, string>),
    );
    calls.push({ url: String(url), headers });
    const idx = Math.min(calls.length - 1, versions.length - 1);
    const df = versions[idx]!;
    return new Response(JSON.stringify(df), {
      status: 200,
      headers: { "content-type": "application/json", etag: df.etag },
    });
  }) as unknown as typeof fetch;
  return { fetch: fetchImpl, calls };
}

function flush(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

let clients: FeatClient[] = [];
function track(client: FeatClient): FeatClient {
  clients.push(client);
  return client;
}
afterEach(() => {
  for (const c of clients) c.close();
  clients = [];
  vi.useRealTimers();
});

// --- tests ---------------------------------------------------------------

describe("datafile streaming", () => {
  it("opens a stream after bootstrap when streaming is on (the default)", async () => {
    const mock = makeMockTransport();
    const { fetch } = makeFetch([makeDatafile(1, false)]);
    const client = track(
      new FeatClient({
        apiKey: "feat_sdk_key",
        url: "http://localhost:8787",
        fetch,
        streamTransport: mock.transport,
      }),
    );

    await client.ready();

    expect(mock.connections.length).toBe(1);
    expect(mock.latest().options.url).toBe("http://localhost:8787/sdk/v1/datafile/stream");
  });

  it("does not open a stream when streaming is false", async () => {
    const mock = makeMockTransport();
    const { fetch } = makeFetch([makeDatafile(1, false)]);
    const client = track(
      new FeatClient({
        apiKey: "feat_sdk_key",
        url: "http://localhost:8787",
        fetch,
        streaming: false,
        streamTransport: mock.transport,
      }),
    );

    await client.ready();
    await flush();

    expect(mock.connections.length).toBe(0);
  });

  it("sends the Authorization bearer header on the stream request", async () => {
    const mock = makeMockTransport();
    const { fetch } = makeFetch([makeDatafile(1, false)]);
    const client = track(
      new FeatClient({
        apiKey: "feat_sdk_secret",
        url: "http://localhost:8787",
        fetch,
        streamTransport: mock.transport,
      }),
    );

    await client.ready();

    expect(mock.latest().options.headers.Authorization).toBe("Bearer feat_sdk_secret");
    expect(mock.latest().options.headers["User-Agent"]).toMatch(/^feat-sdk-js\//);
  });

  it("adopts a newer-version put and reflects it in evaluate()", async () => {
    const mock = makeMockTransport();
    const { fetch } = makeFetch([makeDatafile(1, false)]);
    const client = track(
      new FeatClient({
        apiKey: "feat_sdk_key",
        url: "http://localhost:8787",
        fetch,
        streamTransport: mock.transport,
      }),
    );
    await client.ready();

    const before = await client.evaluate("checkout", false, CTX);
    expect(before.value).toBe(false);

    mock.latest().open();
    mock.latest().put(makeDatafile(2, true));

    const after = await client.evaluate("checkout", false, CTX);
    expect(after.value).toBe(true);
  });

  it("ignores a put whose version is equal to or older than the current datafile", async () => {
    const mock = makeMockTransport();
    const { fetch } = makeFetch([makeDatafile(5, false)]);
    const client = track(
      new FeatClient({
        apiKey: "feat_sdk_key",
        url: "http://localhost:8787",
        fetch,
        streamTransport: mock.transport,
      }),
    );
    await client.ready();
    mock.latest().open();

    // Older version: ignored.
    mock.latest().put(makeDatafile(4, true));
    expect((await client.evaluate("checkout", false, CTX)).value).toBe(false);

    // Equal version: ignored.
    mock.latest().put(makeDatafile(5, true));
    expect((await client.evaluate("checkout", false, CTX)).value).toBe(false);

    // Strictly newer: adopted.
    mock.latest().put(makeDatafile(6, true));
    expect((await client.evaluate("checkout", false, CTX)).value).toBe(true);
  });

  it("reconnects with backoff after a stream error", async () => {
    vi.useFakeTimers();
    const mock = makeMockTransport();
    const { fetch } = makeFetch([makeDatafile(1, false)]);
    const client = track(
      new FeatClient({
        apiKey: "feat_sdk_key",
        url: "http://localhost:8787",
        fetch,
        streamTransport: mock.transport,
      }),
    );
    await client.ready();
    expect(mock.connections.length).toBe(1);

    mock.latest().open();
    mock.latest().fail(new Error("stream dropped"));

    // Backoff before the retry; advance past it.
    await vi.advanceTimersByTimeAsync(5_000);

    expect(mock.connections.length).toBe(2);
  });

  it("falls back to polling when the stream cannot establish", async () => {
    vi.useFakeTimers();
    const failing = makeFailingTransport();
    // v1 on bootstrap, v2 on every subsequent poll.
    const { fetch, calls } = makeFetch([makeDatafile(1, false), makeDatafile(2, true)]);
    const client = track(
      new FeatClient({
        apiKey: "feat_sdk_key",
        url: "http://localhost:8787",
        fetch,
        pollIntervalMs: 5_000,
        streamTransport: failing.transport,
      }),
    );
    await client.ready();

    // Stream never connected, so the bootstrap value stands.
    expect((await client.evaluate("checkout", false, CTX)).value).toBe(false);
    expect(failing.attempts.length).toBeGreaterThanOrEqual(1);

    // The safety-net poll runs at the normal interval while the stream is
    // down and picks up the newer datafile.
    await vi.advanceTimersByTimeAsync(5_000);

    expect(calls.length).toBeGreaterThanOrEqual(2);
    expect((await client.evaluate("checkout", false, CTX)).value).toBe(true);
  });

  it("close() aborts the stream and stops reconnecting", async () => {
    vi.useFakeTimers();
    const mock = makeMockTransport();
    const { fetch } = makeFetch([makeDatafile(1, false)]);
    const client = track(
      new FeatClient({
        apiKey: "feat_sdk_key",
        url: "http://localhost:8787",
        fetch,
        streamTransport: mock.transport,
      }),
    );
    await client.ready();
    mock.latest().open();
    const conn = mock.latest();

    client.close();
    await vi.advanceTimersByTimeAsync(60_000);

    expect(conn.options.signal.aborted).toBe(true);
    // No new connection attempts after close.
    expect(mock.connections.length).toBe(1);
  });
});

describe("fetchSseTransport (default reader)", () => {
  function streamResponse(chunks: string[]): Response {
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        const enc = new TextEncoder();
        for (const c of chunks) controller.enqueue(enc.encode(c));
        controller.close();
      },
    });
    return new Response(body, {
      status: 200,
      headers: { "content-type": "text/event-stream" },
    });
  }

  it("parses put frames and ignores heartbeat comments", async () => {
    const frames: SseFrame[] = [];
    const fetchImpl = (async () =>
      streamResponse([
        ": heartbeat\n\n",
        "event: put\nid: 7\ndata: {\"version\":7}\n\n",
        ": ping\n\n",
        "event: put\nid: 8\ndata: {\"version\":8}\n\n",
      ])) as unknown as typeof fetch;

    await fetchSseTransport({
      url: "http://localhost/sdk/v1/datafile/stream",
      headers: { Authorization: "Bearer k" },
      fetch: fetchImpl,
      signal: new AbortController().signal,
      onFrame: (f) => frames.push(f),
    });

    expect(frames).toHaveLength(2);
    expect(frames[0]).toEqual({ event: "put", id: "7", data: '{"version":7}' });
    expect(frames[1]).toEqual({ event: "put", id: "8", data: '{"version":8}' });
  });

  it("handles a data frame split across chunk boundaries", async () => {
    const frames: SseFrame[] = [];
    const fetchImpl = (async () =>
      streamResponse(["event: put\nid: 9\nda", 'ta: {"ver', 'sion":9}\n', "\n"])) as unknown as typeof fetch;

    await fetchSseTransport({
      url: "http://localhost/sdk/v1/datafile/stream",
      headers: {},
      fetch: fetchImpl,
      signal: new AbortController().signal,
      onFrame: (f) => frames.push(f),
    });

    expect(frames).toEqual([{ event: "put", id: "9", data: '{"version":9}' }]);
  });

  it("rejects on a non-ok response", async () => {
    const fetchImpl = (async () => new Response("nope", { status: 401 })) as unknown as typeof fetch;
    await expect(
      fetchSseTransport({
        url: "http://localhost/sdk/v1/datafile/stream",
        headers: {},
        fetch: fetchImpl,
        signal: new AbortController().signal,
        onFrame: () => {},
      }),
    ).rejects.toThrow(/401/);
  });
});
