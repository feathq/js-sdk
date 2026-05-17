import type { Datafile } from "@feathq/datafile-schema";
import { evaluate } from "@feathq/feat-eval";
import type { EvalContext, EvaluationResult } from "./types";

export interface FeatClientConfig {
  apiKey: string;
  dataPlaneUrl: string;
  // Background-poll interval in ms. Defaults to 30s, matching Cloudflare
  // KV's typical global-replication ceiling.
  pollIntervalMs?: number;
  // Network fetch override (Workers, Node, custom proxy). Defaults to
  // globalThis.fetch.
  fetch?: typeof fetch;
}

// Network client that holds the in-memory datafile and refreshes it on a
// background interval. The eval engine is synchronous w.r.t. the datafile
// (the only async work is bucketing's SHA-1 hash). Network failures keep
// the last-known-good datafile in place — the SDK only goes "cold" on the
// very first fetch.
export class FeatClient {
  private datafile: Datafile | null = null;
  private etag: string | null = null;
  private timer: ReturnType<typeof setInterval> | null = null;
  private readyPromise: Promise<void> | null = null;
  private readonly fetchImpl: typeof fetch;
  private readonly pollIntervalMs: number;

  constructor(private readonly config: FeatClientConfig) {
    this.fetchImpl = config.fetch ?? globalThis.fetch.bind(globalThis);
    this.pollIntervalMs = config.pollIntervalMs ?? 30_000;
  }

  // Resolves once the first datafile is in memory (or rejects if the
  // first fetch fails). Subsequent polls run in the background.
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
      void this.fetchDatafile().catch((err) => {
        console.warn("feat-js-sdk: background poll failed", err);
      });
    }, this.pollIntervalMs);
    // setInterval keeps Node processes alive; unref so consumers don't
    // need to call close() just to exit. Workers/Browsers no-op on
    // missing unref.
    const t = this.timer as unknown as { unref?: () => void };
    t.unref?.();
  }

  private async fetchDatafile(): Promise<boolean> {
    const url = `${this.config.dataPlaneUrl.replace(/\/$/, "")}/sdk/v1/datafile`;
    const headers: Record<string, string> = {
      Authorization: `Bearer ${this.config.apiKey}`,
    };
    if (this.etag) headers["If-None-Match"] = this.etag;
    const res = await this.fetchImpl(url, { method: "GET", headers });
    if (res.status === 304) return false;
    if (res.status === 404) {
      // No datafile yet; treat as transient and let the next poll catch it.
      return false;
    }
    if (!res.ok) {
      throw new Error(`fetchDatafile failed: ${res.status} ${res.statusText}`);
    }
    const next = (await res.json()) as Datafile;
    this.datafile = next;
    this.etag = res.headers.get("etag");
    return true;
  }
}
