<p align="center">
  <a href="https://feat.so">
    <img src="https://feat.so/logo/wordmark.png" alt="feat.so" width="320" />
  </a>
</p>

---

# feat Node.js SDK

Server-side JavaScript / TypeScript SDK for [feat](https://feat.so) feature flags. Local flag evaluation against a polled datafile, OpenFeature provider included.

For browser code, use [`@feathq/web-sdk`](../web-sdk). For edge runtimes via service binding, use [`@feathq/worker-sdk`](../worker-sdk).

## Install

```bash
npm install @feathq/js-sdk
# or
yarn add @feathq/js-sdk
```

Node.js 18+ (built-in `fetch`). Bun and Deno are supported.

## Usage

```ts
import { FeatClient } from "@feathq/js-sdk";

const client = new FeatClient({
  apiKey: process.env.FEAT_SERVER_KEY!,    // feat_sdk_…
  url: "https://data-01.feat.so",          // optional; this is the default
});

await client.ready();

const result = await client.evaluate("checkout-v2", false, {
  targetingKey: "user-123",
  user: { plan: "pro", email: "alice@example.com" },
});

if (result.value) {
  // …
}
```

Use a **server** API key (`feat_sdk_…`). Mobile and client-side keys are for the mobile and browser SDKs respectively.

## OpenFeature

```ts
import { OpenFeature } from "@openfeature/server-sdk";
import { FeatClient, FeatProvider } from "@feathq/js-sdk";

const featClient = new FeatClient({ apiKey });
await OpenFeature.setProviderAndWait(new FeatProvider(featClient));

const client = OpenFeature.getClient();
const enabled = await client.getBooleanValue("checkout-v2", false, {
  targetingKey: "user-123",
});
```

## How it works

- The SDK fetches a per-environment **datafile** and keeps it in memory.
- **Live streaming is on by default.** After the initial fetch the SDK opens a Server-Sent Events stream and applies each pushed datafile the moment it changes. Updates are applied in version order: a push is adopted only when its `version` is strictly newer than the one in memory.
- A background poll keeps running as a safety net (slow while the stream is healthy). If the stream cannot establish or drops, the SDK falls back to polling at the normal interval and keeps retrying the stream with backoff.
- Set `streaming: false` to rely on polling alone. Poll cadence is `pollIntervalMs` (default 30 s, floored at 5 s). ETag-aware: unchanged polls are 304s.
- Evaluation is local; no per-flag network call.
- Call `client.close()` to tear down the stream and poll loop.
- `url` must use `https://` if you override it (the constructor rejects plaintext URLs except `http://localhost` for tests).

## License

MIT
