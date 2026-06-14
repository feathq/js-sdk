import { describe, expect, it, vi } from "vitest";
import { EventSummarizer, extractContextPairs } from "../src/events";

describe("extractContextPairs", () => {
  it("uses the user object key when present", () => {
    expect(extractContextPairs({ user: { key: "u1", email: "x" } })).toEqual([
      { kind: "user", key: "u1" },
    ]);
  });

  it("falls back to targetingKey for the user kind", () => {
    expect(extractContextPairs({ targetingKey: "t1" })).toEqual([{ kind: "user", key: "t1" }]);
  });

  it("prefers the user object over targetingKey", () => {
    expect(extractContextPairs({ targetingKey: "t1", user: { key: "u1" } })).toEqual([
      { kind: "user", key: "u1" },
    ]);
  });

  it("collects other kinds and dedups repeats", () => {
    expect(
      extractContextPairs({ user: { key: "u1" }, organization: { key: "acme" }, device: { key: "d1" } }),
    ).toEqual([
      { kind: "user", key: "u1" },
      { kind: "organization", key: "acme" },
      { kind: "device", key: "d1" },
    ]);
  });

  it("ignores kinds without a string key and non-object values", () => {
    expect(
      extractContextPairs({ user: { key: "u1" }, broken: { notkey: 1 }, alsoBad: "string" } as never),
    ).toEqual([{ kind: "user", key: "u1" }]);
  });

  it("returns empty with no usable identity", () => {
    expect(extractContextPairs({})).toEqual([]);
    expect(extractContextPairs({ user: {} } as never)).toEqual([]);
  });
});

function summarizer(fetchImpl: typeof fetch) {
  return new EventSummarizer({
    url: "https://dp.test",
    apiKey: "key-1",
    fetchImpl,
    userAgent: "feat-sdk-js/test",
    flushIntervalMs: 60_000,
  });
}

const ok = () => new Response(null, { status: 202 });

describe("EventSummarizer", () => {
  it("flush POSTs the deduped batch to /sdk/v1/events with auth, then clears", async () => {
    const fetchMock = vi.fn(async () => ok());
    const s = summarizer(fetchMock as unknown as typeof fetch);

    s.record({ user: { key: "u1" }, organization: { key: "acme" } });
    s.record({ user: { key: "u1" } }); // dup within the window
    await s.flush();

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe("https://dp.test/sdk/v1/events");
    expect(init.method).toBe("POST");
    expect((init.headers as Record<string, string>).Authorization).toBe("Bearer key-1");
    expect(JSON.parse(init.body as string)).toEqual({
      contexts: [
        { kind: "user", key: "u1" },
        { kind: "organization", key: "acme" },
      ],
    });

    // Buffer cleared on success: a second flush sends nothing.
    await s.flush();
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("does not fetch when the buffer is empty", async () => {
    const fetchMock = vi.fn(async () => ok());
    await summarizer(fetchMock as unknown as typeof fetch).flush();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("requeues on a 5xx and retries on the next flush", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(null, { status: 503 }))
      .mockResolvedValueOnce(ok());
    const s = summarizer(fetchMock as unknown as typeof fetch);
    s.record({ user: { key: "u1" } });

    await s.flush(); // 503 -> requeued
    await s.flush(); // retried -> delivered
    expect(fetchMock).toHaveBeenCalledTimes(2);

    await s.flush(); // nothing left
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("requeues on a network error", async () => {
    const fetchMock = vi.fn().mockRejectedValueOnce(new Error("offline")).mockResolvedValueOnce(ok());
    const s = summarizer(fetchMock as unknown as typeof fetch);
    s.record({ user: { key: "u1" } });

    await s.flush(); // throws -> requeued (swallowed)
    await s.flush(); // delivered
    expect(fetchMock).toHaveBeenCalledTimes(2);
    await s.flush();
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("drops the batch on a permanent 4xx (no retry loop)", async () => {
    const fetchMock = vi.fn(async () => new Response(null, { status: 401 }));
    const s = summarizer(fetchMock as unknown as typeof fetch);
    s.record({ user: { key: "u1" } });

    await s.flush(); // 401 -> dropped
    await s.flush(); // nothing requeued
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("sends at most 2000 per flush and drains the rest next window", async () => {
    const fetchMock = vi.fn(async () => ok());
    const s = summarizer(fetchMock as unknown as typeof fetch);
    for (let i = 0; i < 2500; i++) s.record({ user: { key: `u${i}` } });

    await s.flush();
    const first = JSON.parse((fetchMock.mock.calls[0] as unknown as [string, RequestInit])[1].body as string);
    expect(first.contexts).toHaveLength(2000);

    await s.flush();
    const second = JSON.parse((fetchMock.mock.calls[1] as unknown as [string, RequestInit])[1].body as string);
    expect(second.contexts).toHaveLength(500);
  });

  it("close stops the timer and makes a final flush", async () => {
    const fetchMock = vi.fn(async () => ok());
    const s = summarizer(fetchMock as unknown as typeof fetch);
    s.start();
    s.record({ user: { key: "u1" } });
    s.close();
    // close() fires the flush without awaiting; let the microtask settle.
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
  });
});
