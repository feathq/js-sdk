import type { Datafile } from "@feathq/datafile-schema";
import { evaluate } from "@feathq/feat-eval";
import type { EvalContext, EvaluationResult } from "./types";
import { SDK_VERSION } from "./version";

const MIN_POLL_INTERVAL_MS = 5_000;
const DEFAULT_POLL_INTERVAL_MS = 30_000;
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
  // hot loops.
  pollIntervalMs?: number;
  // Network fetch override. Defaults to globalThis.fetch.
  fetch?: typeof fetch;
}

// Network client that holds the in-memory datafile and refreshes it on a
// background interval. Network failures keep the last-known-good datafile
// in place; the SDK only goes "cold" on the very first fetch failure.
export class FeatClient {
  private datafile: Datafile | null = null;
  private etag: string | null = null;
  private timer: ReturnType<typeof setInterval> | null = null;
  private readyPromise: Promise<void> | null = null;
  private readonly fetchImpl: typeof fetch;
  private readonly pollIntervalMs: number;
  private readonly url: string;

  constructor(private readonly config: FeatClientConfig) {
    this.url = config.url ?? DEFAULT_URL;
    assertHttpsUrl(this.url);
    this.fetchImpl = config.fetch ?? globalThis.fetch.bind(globalThis);
    this.pollIntervalMs = Math.max(
      config.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS,
      MIN_POLL_INTERVAL_MS,
    );
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
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  private async bootstrap(): Promise<void> {
    await this.fetchDatafile();
    this.timer = setInterval(() => {
      void this.fetchDatafile().catch((err: unknown) => {
        console.warn(
          "feat: background poll failed:",
          err instanceof Error ? err.message : String(err),
        );
      });
    }, this.pollIntervalMs);
    // setInterval keeps Node processes alive; unref so consumers don't
    // need to call close() just to exit. Other runtimes no-op on missing
    // unref.
    const t = this.timer as unknown as { unref?: () => void };
    t.unref?.();
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
    this.datafile = next;
    this.etag = res.headers.get("etag");
    return true;
  }
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
