import { afterEach, describe, expect, it, vi } from "vitest";
import type { Datafile, FlagSpec, SegmentSpec } from "@feathq/datafile-schema";
import { FeatClient } from "../src/client";
import {
  fetchSseTransport,
  SseHttpError,
  SseParser,
  type SseFrame,
  type SseTransport,
  type SseTransportOptions,
} from "../src/streaming";

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

  it("surfaces the HTTP status as a typed SseHttpError", async () => {
    const fetchImpl = (async () => new Response("forbidden", { status: 403 })) as unknown as typeof fetch;
    await expect(
      fetchSseTransport({
        url: "http://localhost/sdk/v1/datafile/stream",
        headers: {},
        fetch: fetchImpl,
        signal: new AbortController().signal,
        onFrame: () => {},
      }),
    ).rejects.toMatchObject({ name: "SseHttpError", status: 403 });
  });

  it("parses a frame split exactly between CR and LF across two chunks as one frame", async () => {
    const frames: SseFrame[] = [];
    // The first data line is terminated by CRLF, but the chunk boundary falls
    // between the CR and the LF. A naive parser would dispatch the line early
    // and split this single two-line frame into two.
    const fetchImpl = (async () =>
      streamResponse([
        'event: put\ndata: {"version":9,\r',
        '\ndata: "x":1}\r\n\r\n',
      ])) as unknown as typeof fetch;

    await fetchSseTransport({
      url: "http://localhost/sdk/v1/datafile/stream",
      headers: {},
      fetch: fetchImpl,
      signal: new AbortController().signal,
      onFrame: (f) => frames.push(f),
    });

    expect(frames).toEqual([{ event: "put", id: null, data: '{"version":9,\n"x":1}' }]);
  });

  it("drops a final frame at EOF that has no terminating blank line", async () => {
    const frames: SseFrame[] = [];
    // Data line is terminated, but the stream ends before the blank line that
    // would dispatch the frame. Per spec, the incomplete frame is discarded.
    const fetchImpl = (async () =>
      streamResponse(['event: put\ndata: {"version":9}\n'])) as unknown as typeof fetch;

    await fetchSseTransport({
      url: "http://localhost/sdk/v1/datafile/stream",
      headers: {},
      fetch: fetchImpl,
      signal: new AbortController().signal,
      onFrame: (f) => frames.push(f),
    });

    expect(frames).toEqual([]);
  });

  it("aborts the stream when a never-terminated line exceeds the byte cap", async () => {
    const fetchImpl = (async () =>
      streamResponse(["x".repeat(10_000)])) as unknown as typeof fetch;

    await expect(
      fetchSseTransport({
        url: "http://localhost/sdk/v1/datafile/stream",
        headers: {},
        fetch: fetchImpl,
        signal: new AbortController().signal,
        maxBytes: 1_000,
        onFrame: () => {},
      }),
    ).rejects.toThrow(/exceeds/);
  });
});

describe("SseParser byte cap", () => {
  it("throws when the line buffer grows past the cap without a newline", () => {
    const parser = new SseParser(() => {}, 64);
    expect(() => parser.push("x".repeat(128))).toThrow(/exceeds/);
  });

  it("throws when accumulated data lines exceed the cap", () => {
    const parser = new SseParser(() => {}, 200);
    expect(() => {
      // Each chunk is well under the line-buffer cap and is fully drained, but
      // the data bytes accumulate across frames lines past the cap.
      for (let i = 0; i < 50; i++) parser.push(`data: ${"x".repeat(20)}\n`);
    }).toThrow(/exceeds/);
  });
});

describe("stream reconnect policy", () => {
  function makeClient(transport: SseTransport): FeatClient {
    const { fetch } = makeFetch([makeDatafile(1, false)]);
    return track(
      new FeatClient({
        apiKey: "feat_sdk_key",
        url: "http://localhost:8787",
        fetch,
        streamTransport: transport,
      }),
    );
  }

  for (const status of [401, 403]) {
    it(`does not reconnect after a terminal ${status} (falls back to polling)`, async () => {
      vi.useFakeTimers();
      const mock = makeMockTransport();
      const client = makeClient(mock.transport);
      await client.ready();
      expect(mock.connections.length).toBe(1);

      mock.latest().open();
      mock.latest().fail(new SseHttpError(status));

      await vi.advanceTimersByTimeAsync(60_000);

      // No reconnect: the loop stopped and the poll path carries on.
      expect(mock.connections.length).toBe(1);
    });
  }

  for (const status of [429, 500]) {
    it(`keeps retrying after a retryable ${status}`, async () => {
      vi.useFakeTimers();
      const mock = makeMockTransport();
      const client = makeClient(mock.transport);
      await client.ready();
      expect(mock.connections.length).toBe(1);

      mock.latest().open();
      mock.latest().fail(new SseHttpError(status));

      await vi.advanceTimersByTimeAsync(60_000);

      expect(mock.connections.length).toBe(2);
    });
  }

  it("grows and caps backoff with jitter across repeated failures", async () => {
    vi.useFakeTimers();
    // Pin jitter to its lower bound (factor 0.5) so the delays are exact.
    vi.spyOn(Math, "random").mockReturnValue(0);
    const mock = makeMockTransport();
    const client = makeClient(mock.transport);
    await client.ready();

    // backoff doubles 1s->2s->4s->8s->16s->30s(cap); jitter halves each.
    const expectedDelays = [1_000, 2_000, 4_000, 8_000, 15_000, 15_000];
    let conns = 1;
    for (const delay of expectedDelays) {
      mock.latest().open();
      mock.latest().fail(new Error("stream dropped"));

      await vi.advanceTimersByTimeAsync(delay - 1);
      expect(mock.connections.length).toBe(conns);

      await vi.advanceTimersByTimeAsync(1);
      conns += 1;
      expect(mock.connections.length).toBe(conns);
    }
  });

  it("backs off (not a fixed 1s loop) on accept-then-immediate-close", async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(0); // jitter factor 0.5
    const mock = makeMockTransport();
    const client = makeClient(mock.transport);
    await client.ready();

    // Cycle 1: open then close immediately. The connection was never healthy
    // long enough to reset, so backoff grows to 2s and the delay is 1s.
    mock.latest().open();
    mock.latest().endCleanly();
    await vi.advanceTimersByTimeAsync(999);
    expect(mock.connections.length).toBe(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(mock.connections.length).toBe(2);

    // Cycle 2: same again, backoff grows to 4s, delay is 2s (proof it is not
    // pinned at ~1s by onOpen resetting the backoff).
    mock.latest().open();
    mock.latest().endCleanly();
    await vi.advanceTimersByTimeAsync(1_999);
    expect(mock.connections.length).toBe(2);
    await vi.advanceTimersByTimeAsync(1);
    expect(mock.connections.length).toBe(3);
  });

  it("keeps the jittered delay at or below the full backoff (upper bound)", async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(1); // jitter factor 1.0 (max)
    const mock = makeMockTransport();
    const client = makeClient(mock.transport);
    await client.ready();

    mock.latest().open();
    mock.latest().fail(new Error("stream dropped"));

    // backoff is 2s; at the upper jitter bound the delay is the full 2s.
    await vi.advanceTimersByTimeAsync(1_999);
    expect(mock.connections.length).toBe(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(mock.connections.length).toBe(2);
  });

  it("resets backoff after a healthy connection, then jitters again", async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(0); // jitter factor 0.5
    const mock = makeMockTransport();
    const client = makeClient(mock.transport);
    await client.ready();

    // Stay connected long enough to count as healthy, then close cleanly.
    mock.latest().open();
    await vi.advanceTimersByTimeAsync(10_000);
    mock.latest().endCleanly();

    // Healthy reset means backoff returns to 1s; delay is 0.5s.
    await vi.advanceTimersByTimeAsync(499);
    expect(mock.connections.length).toBe(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(mock.connections.length).toBe(2);
  });

  it("ignores non-put events and put frames with invalid JSON", async () => {
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

    // A non-put event carrying a newer datafile must not be adopted.
    mock.latest().frame({
      event: "message",
      id: "99",
      data: JSON.stringify(makeDatafile(99, true)),
    });
    expect((await client.evaluate("checkout", false, CTX)).value).toBe(false);

    // A put with malformed JSON must not throw and must leave the datafile as is.
    mock.latest().frame({ event: "put", id: "99", data: "{not valid json" });
    expect((await client.evaluate("checkout", false, CTX)).value).toBe(false);

    // Sanity: a well-formed newer put still adopts.
    mock.latest().put(makeDatafile(2, true));
    expect((await client.evaluate("checkout", false, CTX)).value).toBe(true);
  });
});

// --- patch fixtures ------------------------------------------------------

function namedBoolFlag(id: string, key: string, on: boolean): FlagSpec {
  return {
    id,
    key,
    valueType: "boolean",
    salt: "abcdef0123456789",
    archived: false,
    isEnabled: true,
    offVariationId: FALSE_VAR.id,
    defaultVariationId: on ? TRUE_VAR.id : FALSE_VAR.id,
    defaultRollout: null,
    defaultBucketingContextKindKey: null,
    variations: [TRUE_VAR, FALSE_VAR],
    targets: [],
    rules: [],
  };
}

// A flag that serves TRUE only when the context matches segment "vips", and
// FALSE otherwise. Lets a segment add/remove in a patch be observed through
// evaluate().
function segmentGatedFlag(): FlagSpec {
  return {
    id: "flag-gate",
    key: "gate",
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
    rules: [
      {
        id: "rule-1",
        bucketingContextKindKey: null,
        variationId: TRUE_VAR.id,
        rollout: null,
        groups: [{ conditions: [{ attributePath: "", operator: "segment_match", values: ["vips"] }] }],
      },
    ],
  };
}

// A boolean flag that serves TRUE only when the context matches the named
// segment. Lets a specific segment add/remove be observed through evaluate().
function segmentGatedFlagFor(id: string, key: string, segmentKey: string): FlagSpec {
  return {
    id,
    key,
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
    rules: [
      {
        id: `rule-${key}`,
        bucketingContextKindKey: null,
        variationId: TRUE_VAR.id,
        rollout: null,
        groups: [
          { conditions: [{ attributePath: "", operator: "segment_match", values: [segmentKey] }] },
        ],
      },
    ],
  };
}

// A segment matching user "u1", under the given key.
function segmentFor(key: string): SegmentSpec {
  return {
    key,
    rules: [{ conditions: [{ attributePath: "user.key", operator: "is_one_of", values: ["u1"] }] }],
  };
}

// Matches user "u1".
const VIPS_SEGMENT: SegmentSpec = segmentFor("vips");

// Reach into the client's in-memory datafile to assert immutability and
// metadata the public surface does not otherwise expose.
function peekDatafile(client: FeatClient): Datafile | null {
  return (client as unknown as { datafile: Datafile | null }).datafile;
}

interface PatchPayload {
  from: number;
  to: number;
  etag?: string;
  generatedAt?: string;
  flags?: Record<string, FlagSpec>;
  removedFlags?: string[];
  segments?: Record<string, SegmentSpec>;
  removedSegments?: string[];
}

// Build a `patch` SSE frame with sensible defaults so a test only spells out
// the fields it cares about.
function patchFrame(p: PatchPayload): SseFrame {
  const data = JSON.stringify({
    etag: `etag-${p.to}`,
    generatedAt: "2026-05-17T00:00:01Z",
    flags: {},
    removedFlags: [],
    segments: {},
    removedSegments: [],
    ...p,
  });
  return { event: "patch", id: String(p.to), data };
}

async function readyClient(
  versions: Datafile[],
): Promise<{ client: FeatClient; mock: ReturnType<typeof makeMockTransport>; calls: Array<{ url: string; headers: Record<string, string> }> }> {
  const mock = makeMockTransport();
  const { fetch, calls } = makeFetch(versions);
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
  return { client, mock, calls };
}

describe("datafile patch frames", () => {
  it("applies a version-matched patch: a changed flag flips a subsequent evaluate()", async () => {
    const { client, mock } = await readyClient([makeDatafile(1, false)]);
    expect((await client.evaluate("checkout", false, CTX)).value).toBe(false);

    mock.latest().frame(
      patchFrame({ from: 1, to: 2, flags: { checkout: namedBoolFlag("flag-1", "checkout", true) } }),
    );

    expect((await client.evaluate("checkout", false, CTX)).value).toBe(true);
  });

  it("removes a flag named in removedFlags so it no longer evaluates", async () => {
    const base = makeDatafile(1, false);
    base.flags.beta = namedBoolFlag("flag-beta", "beta", true);
    const { client, mock } = await readyClient([base]);
    expect((await client.evaluate("beta", false, CTX)).value).toBe(true);

    mock.latest().frame(patchFrame({ from: 1, to: 2, removedFlags: ["beta"] }));

    const after = await client.evaluate("beta", false, CTX);
    expect(after.value).toBe(false);
    expect(after.reason).toBe("ERROR");
    // The untouched flag is still present.
    expect((await client.evaluate("checkout", true, CTX)).value).toBe(false);
  });

  it("applies a segment add then remove, observed through a segment-gated flag", async () => {
    const base = makeDatafile(1, false);
    base.flags.gate = segmentGatedFlag();
    const { client, mock } = await readyClient([base]);
    // No "vips" segment yet, so the gating rule does not match.
    expect((await client.evaluate("gate", false, CTX)).value).toBe(false);

    // Add the segment: now the rule matches and the flag serves TRUE.
    mock.latest().frame(patchFrame({ from: 1, to: 2, segments: { vips: VIPS_SEGMENT } }));
    expect((await client.evaluate("gate", false, CTX)).value).toBe(true);

    // Remove the segment again: back to the default.
    mock.latest().frame(patchFrame({ from: 2, to: 3, removedSegments: ["vips"] }));
    expect((await client.evaluate("gate", false, CTX)).value).toBe(false);
  });

  it("advances the version so chained patches apply in sequence", async () => {
    const { client, mock } = await readyClient([makeDatafile(1, false)]);

    // v1 -> v2: checkout on.
    mock.latest().frame(
      patchFrame({ from: 1, to: 2, flags: { checkout: namedBoolFlag("flag-1", "checkout", true) } }),
    );
    expect((await client.evaluate("checkout", false, CTX)).value).toBe(true);

    // v2 -> v3: checkout off. Only applies because the version advanced to 2.
    mock.latest().frame(
      patchFrame({ from: 2, to: 3, flags: { checkout: namedBoolFlag("flag-1", "checkout", false) } }),
    );
    expect((await client.evaluate("checkout", true, CTX)).value).toBe(false);

    // A stale patch keyed off the old base version is now ignored.
    mock.latest().frame(
      patchFrame({ from: 2, to: 99, flags: { checkout: namedBoolFlag("flag-1", "checkout", true) } }),
    );
    expect((await client.evaluate("checkout", true, CTX)).value).toBe(false);
  });

  it("refreshes the conditional-poll ETag so the next poll sends the patched etag", async () => {
    const { client, mock, calls } = await readyClient([makeDatafile(1, false)]);
    // Bootstrap fetch carried the v1 etag.
    expect(calls[0]!.headers["If-None-Match"]).toBeUndefined();

    mock.latest().frame(
      patchFrame({ from: 1, to: 2, etag: "etag-patched", flags: { checkout: namedBoolFlag("flag-1", "checkout", true) } }),
    );

    // A subsequent conditional fetch must carry the etag the patch advanced to.
    await client.refresh();
    expect(calls[calls.length - 1]!.headers["If-None-Match"]).toBe("etag-patched");
  });

  it("ignores a patch whose from does not match the current version", async () => {
    const { client, mock } = await readyClient([makeDatafile(1, false)]);

    // Gap: base version 5 does not line up with the current version 1.
    mock.latest().frame(
      patchFrame({ from: 5, to: 6, flags: { checkout: namedBoolFlag("flag-1", "checkout", true) } }),
    );
    expect((await client.evaluate("checkout", false, CTX)).value).toBe(false);

    // A correctly-based patch still applies afterwards.
    mock.latest().frame(
      patchFrame({ from: 1, to: 2, flags: { checkout: namedBoolFlag("flag-1", "checkout", true) } }),
    );
    expect((await client.evaluate("checkout", false, CTX)).value).toBe(true);
  });

  it("ignores malformed patch frames and leaves the datafile intact", async () => {
    const { client, mock } = await readyClient([makeDatafile(1, false)]);

    // Invalid JSON.
    mock.latest().frame({ event: "patch", id: "2", data: "{not valid json" });
    // Missing the from/to gating versions.
    mock.latest().frame({
      event: "patch",
      id: "2",
      data: JSON.stringify({ etag: "etag-2", generatedAt: "now", flags: {} }),
    });
    // from is not a number.
    mock.latest().frame({
      event: "patch",
      id: "2",
      data: JSON.stringify({ from: "1", to: 2, etag: "etag-2", generatedAt: "now" }),
    });
    // removedFlags is not an array.
    mock.latest().frame({
      event: "patch",
      id: "2",
      data: JSON.stringify({ from: 1, to: 2, etag: "etag-2", generatedAt: "now", removedFlags: "checkout" }),
    });
    // Missing etag metadata.
    mock.latest().frame({
      event: "patch",
      id: "2",
      data: JSON.stringify({ from: 1, to: 2, generatedAt: "now", flags: {} }),
    });

    expect((await client.evaluate("checkout", false, CTX)).value).toBe(false);

    // Sanity: a well-formed patch on the same base still applies.
    mock.latest().frame(
      patchFrame({ from: 1, to: 2, flags: { checkout: namedBoolFlag("flag-1", "checkout", true) } }),
    );
    expect((await client.evaluate("checkout", false, CTX)).value).toBe(true);
  });

  it("rejects a patch with to <= from or non-integer versions and does not roll the version backward", async () => {
    const { client, mock } = await readyClient([makeDatafile(5, false)]);
    expect(peekDatafile(client)!.version).toBe(5);

    const win = () => ({ checkout: namedBoolFlag("flag-1", "checkout", true) });
    // to === from.
    mock.latest().frame(patchFrame({ from: 5, to: 5, flags: win() }));
    // to < from.
    mock.latest().frame(patchFrame({ from: 5, to: 4, flags: win() }));
    // Non-integer to.
    mock.latest().frame(patchFrame({ from: 5, to: 5.5, flags: win() }));
    // Non-integer from.
    mock.latest().frame(patchFrame({ from: 5.5, to: 6, flags: win() }));

    // None applied: the flag and the version are untouched.
    expect((await client.evaluate("checkout", false, CTX)).value).toBe(false);
    expect(peekDatafile(client)!.version).toBe(5);

    // Sanity: a strictly-increasing integer patch on the same base applies.
    mock.latest().frame(patchFrame({ from: 5, to: 6, flags: win() }));
    expect((await client.evaluate("checkout", false, CTX)).value).toBe(true);
    expect(peekDatafile(client)!.version).toBe(6);
  });

  it("ignores a patch that arrives before any datafile is bootstrapped", async () => {
    const mock = makeMockTransport();
    // The datafile endpoint 404s, so bootstrap leaves the datafile null.
    const fetch404 = (async () => new Response("", { status: 404 })) as unknown as typeof fetch;
    const client = track(
      new FeatClient({
        apiKey: "feat_sdk_key",
        url: "http://localhost:8787",
        fetch: fetch404,
        streamTransport: mock.transport,
      }),
    );
    await client.ready();
    mock.latest().open();
    expect(peekDatafile(client)).toBeNull();

    // A patch with no datafile to apply against must be dropped, not throw.
    expect(() =>
      mock.latest().frame(
        patchFrame({ from: 1, to: 2, flags: { checkout: namedBoolFlag("flag-1", "checkout", true) } }),
      ),
    ).not.toThrow();
    expect(peekDatafile(client)).toBeNull();
  });

  it("is idempotent: re-applying a consumed patch leaves version and flags unchanged", async () => {
    const { client, mock } = await readyClient([makeDatafile(1, false)]);

    mock.latest().frame(
      patchFrame({ from: 1, to: 2, flags: { checkout: namedBoolFlag("flag-1", "checkout", true) } }),
    );
    expect((await client.evaluate("checkout", false, CTX)).value).toBe(true);
    expect(peekDatafile(client)!.version).toBe(2);

    // Re-send the same from:1 patch (now stale: current version is 2). Even
    // though it carries a different flag value, it must be a no-op.
    mock.latest().frame(
      patchFrame({ from: 1, to: 2, flags: { checkout: namedBoolFlag("flag-1", "checkout", false) } }),
    );
    expect((await client.evaluate("checkout", false, CTX)).value).toBe(true);
    expect(peekDatafile(client)!.version).toBe(2);
  });

  it("updates generatedAt on the cached datafile after a patch", async () => {
    const { client, mock } = await readyClient([makeDatafile(1, false)]);
    expect(peekDatafile(client)!.generatedAt).toBe("2026-05-17T00:00:00Z");

    mock.latest().frame(
      patchFrame({
        from: 1,
        to: 2,
        generatedAt: "2026-06-01T12:00:00Z",
        flags: { checkout: namedBoolFlag("flag-1", "checkout", true) },
      }),
    );

    expect(peekDatafile(client)!.generatedAt).toBe("2026-06-01T12:00:00Z");
  });

  it("applies flags-add, removedFlags, segments-add and removedSegments in a single patch", async () => {
    const base = makeDatafile(1, false);
    // A flag to be removed.
    base.flags.legacy = namedBoolFlag("flag-legacy", "legacy", true);
    // Gated on a segment the patch adds: currently off.
    base.flags.gate = segmentGatedFlagFor("flag-gate", "gate", "vips");
    // Gated on a segment that already exists and the patch removes: currently on.
    base.flags.gate2 = segmentGatedFlagFor("flag-gate2", "gate2", "olds");
    base.segments.olds = segmentFor("olds");

    const { client, mock } = await readyClient([base]);
    expect((await client.evaluate("legacy", false, CTX)).value).toBe(true);
    expect((await client.evaluate("gate", false, CTX)).value).toBe(false);
    expect((await client.evaluate("gate2", false, CTX)).value).toBe(true);

    mock.latest().frame(
      patchFrame({
        from: 1,
        to: 2,
        flags: { beta: namedBoolFlag("flag-beta", "beta", true) },
        removedFlags: ["legacy"],
        segments: { vips: VIPS_SEGMENT },
        removedSegments: ["olds"],
      }),
    );

    // flags-add: beta now resolves on.
    expect((await client.evaluate("beta", false, CTX)).value).toBe(true);
    // removedFlags: legacy is gone.
    expect((await client.evaluate("legacy", false, CTX)).reason).toBe("ERROR");
    // segments-add: the vips gate now matches.
    expect((await client.evaluate("gate", false, CTX)).value).toBe(true);
    // removedSegments: the olds gate no longer matches.
    expect((await client.evaluate("gate2", false, CTX)).value).toBe(false);
    expect(peekDatafile(client)!.version).toBe(2);
  });

  it("drops a gap patch, then a safety-net poll resync restores correctness", async () => {
    vi.useFakeTimers();
    const mock = makeMockTransport();
    // Bootstrap serves v1; the resync poll serves v7.
    const { fetch, calls } = makeFetch([makeDatafile(1, false), makeDatafile(7, true)]);
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

    // Gap: from:5 is ahead of the current version 1, so the patch is dropped.
    mock.latest().frame(
      patchFrame({ from: 5, to: 6, flags: { checkout: namedBoolFlag("flag-1", "checkout", true) } }),
    );
    expect((await client.evaluate("checkout", false, CTX)).value).toBe(false);

    // Recovery is via the safety-net poll (no proactive reconnect): while the
    // stream is healthy it runs at the slow safety cadence and resyncs to v7.
    await vi.advanceTimersByTimeAsync(15 * 60 * 1_000);

    expect(calls.length).toBeGreaterThanOrEqual(2);
    expect((await client.evaluate("checkout", false, CTX)).value).toBe(true);
    expect(peekDatafile(client)!.version).toBe(7);
  });

  it("swaps in a new datafile instead of mutating the one captured before the patch", async () => {
    const { client, mock } = await readyClient([makeDatafile(1, false)]);
    const before = peekDatafile(client)!;

    mock.latest().frame(
      patchFrame({
        from: 1,
        to: 2,
        flags: { checkout: namedBoolFlag("flag-1", "checkout", true) },
      }),
    );

    const after = peekDatafile(client)!;
    // A fresh object was swapped in.
    expect(after).not.toBe(before);
    // The captured reference is untouched: same version, same flag value.
    expect(before.version).toBe(1);
    expect(before.flags.checkout!.defaultVariationId).toBe(FALSE_VAR.id);
    // The live datafile reflects the patch.
    expect(after.version).toBe(2);
    expect(after.flags.checkout!.defaultVariationId).toBe(TRUE_VAR.id);
  });
});
