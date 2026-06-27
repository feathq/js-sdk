import type { Datafile, FlagSpec, SegmentSpec } from "@feathq/datafile-schema";
import { evaluate } from "@feathq/feat-eval";
import { fetchSseTransport, SseHttpError, type SseFrame, type SseTransport } from "./streaming";
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
// A connection must stay open at least this long before we treat it as
// "healthy" and reset the backoff. Without this, a server that accepts then
// immediately closes the stream would reset the backoff on every cycle and
// produce a fixed ~1s reconnect hot loop.
const STREAM_HEALTHY_RESET_MS = 5_000;
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
      let connectedAt: number | null = null;
      try {
        await this.streamTransport({
          url: streamUrl,
          headers,
          fetch: this.fetchImpl,
          signal: abort.signal,
          onOpen: () => {
            connectedAt = Date.now();
            this.setStreamConnected(true);
          },
          onFrame: (frame) => this.handleFrame(frame),
        });
        // Clean end: the server closed the stream. Only reset the backoff if
        // the connection was healthy for a while; an accept-then-immediately-
        // close server must still back off rather than reconnect every ~1s.
        this.setStreamConnected(false);
        if (connectedAt !== null && Date.now() - connectedAt >= STREAM_HEALTHY_RESET_MS) {
          backoff = STREAM_BACKOFF_INITIAL_MS;
        } else {
          backoff = Math.min(backoff * 2, STREAM_BACKOFF_MAX_MS);
        }
      } catch (err) {
        this.setStreamConnected(false);
        if (this.closed || isAbortError(err)) break;
        // A revoked or invalid key is terminal: retrying the stream can never
        // succeed, so stop the loop and let the poll path carry on (it surfaces
        // the same failure and keeps the last-known-good datafile in place).
        if (isTerminalStreamStatus(err)) {
          warn("datafile stream rejected (auth); falling back to polling:", err);
          break;
        }
        warn("datafile stream error:", err);
        backoff = Math.min(backoff * 2, STREAM_BACKOFF_MAX_MS);
      }
      if (this.closed) break;
      // Jitter the delay so a fleet of SDKs that dropped together does not
      // reconnect in lockstep.
      await abortableDelay(jitter(backoff), abort.signal);
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
    // We deliberately do not implement Last-Event-ID resumption. `frame.id`
    // carries the datafile version, but the server reseeds the current
    // datafile in full on every connect and ignores any Last-Event-ID we
    // would send, so relying on that reseed (not resumption) is intentional.
    if (frame.event === "put") {
      this.handlePutFrame(frame);
      return;
    }
    if (frame.event === "patch") {
      this.handlePatchFrame(frame);
      return;
    }
  }

  private handlePutFrame(frame: SseFrame): void {
    let next: Datafile;
    try {
      next = JSON.parse(frame.data) as Datafile;
    } catch {
      warn("ignoring stream frame with invalid datafile JSON");
      return;
    }
    this.adoptDatafile(next);
  }

  // Apply an incremental `patch` frame in place. The patch is gated on the
  // base `from` version: it only applies when it lines up exactly with the
  // datafile we currently hold. On any gap (or before we have bootstrapped a
  // datafile at all) the patch is dropped; the reconnect reseed (a full `put`)
  // and the safety-net poll keep the client correct.
  private handlePatchFrame(frame: SseFrame): void {
    let patch: DatafilePatch | null;
    try {
      patch = parseDatafilePatch(JSON.parse(frame.data));
    } catch {
      warn("ignoring stream frame with invalid patch JSON");
      return;
    }
    if (!patch) {
      warn("ignoring malformed datafile patch");
      return;
    }
    this.applyPatch(patch);
  }

  // Merge a version-matched delta atomically: build the next datafile in full,
  // then swap it in so a partially-applied patch is never observable. Returns
  // true if applied. Updates the ETag the conditional poll sends so the
  // safety-net poll 304s instead of re-downloading the whole datafile.
  private applyPatch(patch: DatafilePatch): boolean {
    const current = this.datafile;
    if (!current || current.version !== patch.from) return false;

    const flags: Record<string, FlagSpec> = { ...current.flags };
    for (const [key, flag] of Object.entries(patch.flags)) flags[key] = flag;
    for (const key of patch.removedFlags) delete flags[key];

    const segments: Record<string, SegmentSpec> = { ...current.segments };
    for (const [key, segment] of Object.entries(patch.segments)) segments[key] = segment;
    for (const key of patch.removedSegments) delete segments[key];

    this.datafile = {
      ...current,
      flags,
      segments,
      version: patch.to,
      etag: patch.etag,
      generatedAt: patch.generatedAt,
    };
    this.etag = patch.etag;
    return true;
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

// A validated incremental datafile delta, decoded from a `patch` SSE frame.
// `from`/`to` gate application against the version we currently hold; the
// flag/segment maps and removal lists describe the change.
interface DatafilePatch {
  from: number;
  to: number;
  etag: string;
  generatedAt: string;
  flags: Record<string, FlagSpec>;
  removedFlags: string[];
  segments: Record<string, SegmentSpec>;
  removedSegments: string[];
}

// Validate a parsed `patch` payload into a DatafilePatch, or return null if it
// is malformed. We require the gating versions (`from`/`to`) and the new
// metadata (`etag`/`generatedAt`); the four collection fields default to empty
// when absent so a patch that only adds, or only removes, is still valid. A
// malformed patch is dropped rather than applied so a bad frame can never
// corrupt the in-memory datafile.
function parseDatafilePatch(raw: unknown): DatafilePatch | null {
  if (typeof raw !== "object" || raw === null) return null;
  const p = raw as Record<string, unknown>;
  if (typeof p.from !== "number" || typeof p.to !== "number") return null;
  if (typeof p.etag !== "string" || typeof p.generatedAt !== "string") return null;

  const flags = asRecord(p.flags);
  const segments = asRecord(p.segments);
  const removedFlags = asStringArray(p.removedFlags);
  const removedSegments = asStringArray(p.removedSegments);
  if (flags === null || segments === null) return null;
  if (removedFlags === null || removedSegments === null) return null;

  return {
    from: p.from,
    to: p.to,
    etag: p.etag,
    generatedAt: p.generatedAt,
    flags: flags as Record<string, FlagSpec>,
    removedFlags,
    segments: segments as Record<string, SegmentSpec>,
    removedSegments,
  };
}

// An absent value defaults to {}; a non-object (or array) value is malformed.
function asRecord(value: unknown): Record<string, unknown> | null {
  if (value === undefined) return {};
  if (typeof value !== "object" || value === null || Array.isArray(value)) return null;
  return value as Record<string, unknown>;
}

// An absent value defaults to []; a non-array, or an array with a non-string
// element, is malformed.
function asStringArray(value: unknown): string[] | null {
  if (value === undefined) return [];
  if (!Array.isArray(value)) return null;
  if (value.some((item) => typeof item !== "string")) return null;
  return value as string[];
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

// 401/403 are terminal: the key is missing, invalid, or revoked and no amount
// of reconnecting will help. Everything else (429, 5xx, network) is retryable.
function isTerminalStreamStatus(err: unknown): boolean {
  return err instanceof SseHttpError && (err.status === 401 || err.status === 403);
}

// Randomised backoff: keep the full delay between 50% and 100% of `ms` so
// reconnect attempts spread out instead of synchronising across clients.
function jitter(ms: number): number {
  return ms * (0.5 + Math.random() * 0.5);
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
