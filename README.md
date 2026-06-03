<p align="center">
  <a href="https://feat.so">
    <img src="https://feat.so/logo/wordmark.png" alt="feat.so" width="200" />
  </a>
</p>

---

# @feathq/js-sdk

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
  dataPlaneUrl: "https://data.feat.so",
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

const featClient = new FeatClient({ apiKey, dataPlaneUrl });
await OpenFeature.setProviderAndWait(new FeatProvider(featClient));

const client = OpenFeature.getClient();
const enabled = await client.getBooleanValue("checkout-v2", false, {
  targetingKey: "user-123",
});
```

## How it works

- The SDK fetches a per-environment **datafile** and keeps it in memory.
- Polls every 30 s by default (configurable down to 5 s). ETag-aware: unchanged polls are 304s.
- Evaluation is local; no per-flag network call.
- `dataPlaneUrl` must use `https://` (the constructor rejects plaintext URLs except `http://localhost` for tests).

## License

MIT
