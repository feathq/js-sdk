import type { Datafile } from "@feathq/datafile-schema";
import { evaluate } from "@feathq/feat-eval";
import { fetchSseTransport, type SseFrame, type SseTransport } from "./streaming";
import type { EvalContext, EvaluationResult } from "./types";
import { SDK_VERSION } from "./version";

const MIN_POLL_INTERVAL_MS = 5_000;
const DEFAULT_POLL_INTERVAL_MS = 30_000;
// While streaming is healthy the poll loop keeps running as a safety net,
// but at a deliberately slow cadence: the stream is the live path, the poll
// only exists to recover from a silently-wedged connection. When the stream
// is down the poll reverts to the normal interval and becomes the primary
// refresh path.
const DEFAULT_SAFETY_NET_POLL_INTERVAL_MS = 15 * 60 * 1_000;
const STREAM_BACKOFF_INITIAL_MS = 1_000;
const STREAM_BACKOFF_MAX_MS = 30_000;
const MAX_DATAFILE_BYTES = 10 * 1024 * 1024;
const DEFAULT_URL = "https://data-01.feat.so";
const USER_AGENT = `feat-sdk-js/${SDK_VERSION}`;

export interface FeatClientConfig {
  apiKey: string;
  // Optional. Defaults to the production endpoint. Override if you have
  // been pointed at a different region or a staging endpoint.
  url?: string;
  // Background-poll interval in ms. Defaults to 30s. Floored at 5s to
  // protect both the SDK consumer and the feat endpoint from accidental
  // hot loops. When streaming is on this is the cadence the poll falls
  // back to if the stream drops.
  pollIntervalMs?: number;
  // Live datafile streaming over Server-Sent Events. On by default: the
  // SDK opens a stream after bootstrap and applies each pushed datafile in
  // version order, with the poll loop kept running as a safety net. Set to
  // false to rely on polling alone.
  streaming?: boolean;
  // SSE transport override. Defaults to a fetch-based reader. Injectable
  // for tests and custom runtimes.
  streamTransport?: SseTransport;
  // Network fetch override. Defaults to globalThis.fetch.
  fetch?: typeof fetch;
}

// Network client that holds the in-memory datafile and keeps it fresh via a
// live stream (when enabled) plus a background poll. Network failures keep
// the last-known-good datafile in place; the SDK only goes "cold" on the
// very first fetch failure.
export class FeatClient {
  private datafile: Datafile | null = null;
  private etag: string | null = null;
  private pollTimer: ReturnType<typeof setTimeout> | null = null;
  private readyPromise: Promise<void> | null = null;
  private closed = false;
  private streamAbort: AbortController | null = null;
  private streamConnected = false;
  private readonly fetchImpl: typeof fetch;
  private readonly pollIntervalMs: number;
  private readonly safetyNetPollIntervalMs: number;
  private readonly url: string;
  private readonly streamingEnabled: boolean;
  private readonly streamTransport: SseTransport;

  constructor(private readonly config: FeatClientConfig) {
    this.url = config.url ?? DEFAULT_URL;
    assertHttpsUrl(this.url);
    this.fetchImpl = config.fetch ?? globalThis.fetch.bind(globalThis);
    this.pollIntervalMs = Math.max(
      config.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS,
      MIN_POLL_INTERVAL_MS,
    );
    // The safety net is never faster than the configured poll interval.
    this.safetyNetPollIntervalMs = Math.max(
      DEFAULT_SAFETY_NET_POLL_INTERVAL_MS,
      this.pollIntervalMs,
    );
    this.streamingEnabled = config.streaming ?? true;
    this.streamTransport = config.streamTransport ?? fetchSseTransport;
  }

  async ready(): Promise<void> {
    if (!this.readyPromise) {
      this.readyPromise = this.bootstrap();
    }
    return this.readyPromise;
  }

  // Fetch once now. Returns true if the datafile changed.
  async refresh(): Promise<boolean> {
    return this.fetchDatafile();
  }

  async evaluate<T = unknown>(
    flagKey: string,
    defaultValue: T,
    context: EvalContext,
  ): Promise<EvaluationResult<T>> {
    if (!this.datafile) {
      return {
        value: defaultValue,
        variationId: null,
        reason: "ERROR",
        errorMessage: "client not ready: call client.ready() before evaluate",
      };
    }
    const result = await evaluate(flagKey, defaultValue, context, this.datafile);
    return result as EvaluationResult<T>;
  }

  close(): void {
    this.closed = true;
    if (this.pollTimer) {
      clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }
    if (this.streamAbort) {
      this.streamAbort.abort();
      this.streamAbort = null;
    }
  }

  private async bootstrap(): Promise<void> {
    await this.fetchDatafile();
    if (this.closed) return;
    if (this.streamingEnabled) {
      // Fire and forget: the loop reconnects on its own and the poll keeps
      // data fresh meanwhile.
      void this.runStreamLoop();
    }
    this.scheduleNextPoll();
  }

  // Self-scheduling poll. The interval depends on stream health: slow while
  // streaming is healthy, normal otherwise (the fallback path).
  private scheduleNextPoll(): void {
    if (this.closed) return;
    if (this.pollTimer) clearTimeout(this.pollTimer);
    const interval =
      this.streamingEnabled && this.streamConnected
        ? this.safetyNetPollIntervalMs
        : this.pollIntervalMs;
    this.pollTimer = setTimeout(() => {
      void this.fetchDatafile()
        .catch((err: unknown) => {
          warn("background poll failed:", err);
        })
        .finally(() => this.scheduleNextPoll());
    }, interval);
    // setTimeout keeps Node processes alive; unref so consumers don't need
    // to call close() just to exit. Other runtimes no-op on missing unref.
    unref(this.pollTimer);
  }

  private async runStreamLoop(): Promise<void> {
    const streamUrl = `${this.url.replace(/\/$/, "")}/sdk/v1/datafile/stream`;
    const headers: Record<string, string> = {
      Authorization: `Bearer ${this.config.apiKey}`,
      "User-Agent": USER_AGENT,
    };
    let backoff = STREAM_BACKOFF_INITIAL_MS;
    while (!this.closed) {
      const abort = new AbortController();
      this.streamAbort = abort;
      try {
        await this.streamTransport({
          url: streamUrl,
          headers,
          fetch: this.fetchImpl,
          signal: abort.signal,
          onOpen: () => {
            backoff = STREAM_BACKOFF_INITIAL_MS;
            this.setStreamConnected(true);
          },
          onFrame: (frame) => this.handleFrame(frame),
        });
        // Clean end: the server closed the stream. Reconnect promptly.
        this.setStreamConnected(false);
      } catch (err) {
        this.setStreamConnected(false);
        if (this.closed || isAbortError(err)) break;
        warn("datafile stream error:", err);
        backoff = Math.min(backoff * 2, STREAM_BACKOFF_MAX_MS);
      }
      if (this.closed) break;
      await abortableDelay(backoff, abort.signal);
    }
    this.setStreamConnected(false);
  }

  private setStreamConnected(connected: boolean): void {
    if (this.streamConnected === connected) return;
    this.streamConnected = connected;
    // Stream health changed: re-evaluate the poll cadence right away so a
    // dropped stream immediately falls back to the normal interval.
    this.scheduleNextPoll();
  }

  private handleFrame(frame: SseFrame): void {
    if (frame.event !== "put") return;
    let next: Datafile;
    try {
      next = JSON.parse(frame.data) as Datafile;
    } catch {
      warn("ignoring stream frame with invalid datafile JSON");
      return;
    }
    this.adoptDatafile(next);
  }

  // Adopt a datafile only if its version is strictly newer than what we
  // hold. Equal or older versions are ignored so out-of-order pushes or a
  // stale poll can never roll the datafile backwards. Returns true if
  // adopted.
  private adoptDatafile(next: Datafile): boolean {
    if (typeof next?.version !== "number") return false;
    const current = this.datafile?.version ?? Number.NEGATIVE_INFINITY;
    if (next.version <= current) return false;
    this.datafile = next;
    if (typeof next.etag === "string") this.etag = next.etag;
    return true;
  }

  private async fetchDatafile(): Promise<boolean> {
    const url = `${this.url.replace(/\/$/, "")}/sdk/v1/datafile`;
    const headers: Record<string, string> = {
      Authorization: `Bearer ${this.config.apiKey}`,
      "User-Agent": USER_AGENT,
    };
    if (this.etag) headers["If-None-Match"] = this.etag;
    const res = await this.fetchImpl(url, { method: "GET", headers });
    if (res.status === 304) return false;
    if (res.status === 404) return false;
    if (!res.ok) {
      throw new Error(`fetchDatafile failed: ${res.status}`);
    }
    const lengthHeader = res.headers.get("content-length");
    if (lengthHeader && Number(lengthHeader) > MAX_DATAFILE_BYTES) {
      throw new Error("datafile exceeds maximum allowed size");
    }
    const next = (await res.json()) as Datafile;
    const adopted = this.adoptDatafile(next);
    if (adopted) {
      const headerEtag = res.headers.get("etag");
      if (headerEtag) this.etag = headerEtag;
    }
    return adopted;
  }
}

function warn(message: string, err?: unknown): void {
  if (err === undefined) {
    console.warn(`feat: ${message}`);
    return;
  }
  console.warn(`feat: ${message}`, err instanceof Error ? err.message : String(err));
}

function isAbortError(err: unknown): boolean {
  return err instanceof Error && err.name === "AbortError";
}

function unref(timer: ReturnType<typeof setTimeout>): void {
  (timer as unknown as { unref?: () => void }).unref?.();
}

// Resolves after `ms`, or early if the signal aborts. Used to space out
// reconnect attempts without blocking teardown.
function abortableDelay(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    unref(timer);
    function onAbort(): void {
      clearTimeout(timer);
      resolve();
    }
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

// Allow https:// and loopback over http for local dev / tests. Anything
// else gets rejected so a misconfigured consumer can't accidentally send
// the bearer token over plaintext.
function assertHttpsUrl(url: string): void {
  try {
    const u = new URL(url);
    if (u.protocol === "https:") return;
    if (u.protocol === "http:" && (u.hostname === "localhost" || u.hostname === "127.0.0.1")) {
      return;
    }
  } catch {
    // fall through
  }
  throw new Error("url must use https:// (http://localhost allowed for tests)");
}
