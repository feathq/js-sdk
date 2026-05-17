import { ErrorCode } from "@openfeature/core";
import type {
  EvaluationContext,
  JsonValue,
  Logger,
  ProviderMetadata,
  ResolutionDetails,
} from "@openfeature/core";
import type { Hook, Provider, ProviderStatus } from "@openfeature/server-sdk";
import type { FeatClient } from "./client";
import type { EvalContext } from "./types";

// Bridges feat's local-eval client to the OpenFeature server-side Provider
// surface. Users do `client.getBooleanValue("flag-key", false, ctx)` via
// OpenFeature's typed helpers; this provider's `resolve*` methods coerce
// the datafile-shape result into OpenFeature's ResolutionDetails.
//
// All resolve methods coerce by valueType: a flag declared as boolean
// returns false for non-boolean values (with reason ERROR), preventing
// type drift if the admin UI ever lets a typed flag be reconfigured.
export class FeatProvider implements Provider {
  readonly metadata: ProviderMetadata = { name: "feat" };
  readonly runsOn = "server" as const;
  readonly hooks: Hook[] = [];
  status: ProviderStatus = "NOT_READY" as ProviderStatus;

  constructor(private readonly client: FeatClient) {}

  async initialize(): Promise<void> {
    await this.client.ready();
    this.status = "READY" as ProviderStatus;
  }

  async onClose(): Promise<void> {
    this.client.close();
    this.status = "NOT_READY" as ProviderStatus;
  }

  async resolveBooleanEvaluation(
    flagKey: string,
    defaultValue: boolean,
    context: EvaluationContext,
    _logger: Logger,
  ): Promise<ResolutionDetails<boolean>> {
    const result = await this.client.evaluate(
      flagKey,
      defaultValue,
      toEvalContext(context),
    );
    if (typeof result.value !== "boolean") {
      return wrongType(defaultValue, typeof result.value, "boolean");
    }
    return toResolution(result.value, result);
  }

  async resolveStringEvaluation(
    flagKey: string,
    defaultValue: string,
    context: EvaluationContext,
    _logger: Logger,
  ): Promise<ResolutionDetails<string>> {
    const result = await this.client.evaluate(
      flagKey,
      defaultValue,
      toEvalContext(context),
    );
    if (typeof result.value !== "string") {
      return wrongType(defaultValue, typeof result.value, "string");
    }
    return toResolution(result.value, result);
  }

  async resolveNumberEvaluation(
    flagKey: string,
    defaultValue: number,
    context: EvaluationContext,
    _logger: Logger,
  ): Promise<ResolutionDetails<number>> {
    const result = await this.client.evaluate(
      flagKey,
      defaultValue,
      toEvalContext(context),
    );
    if (typeof result.value !== "number") {
      return wrongType(defaultValue, typeof result.value, "number");
    }
    return toResolution(result.value, result);
  }

  async resolveObjectEvaluation<T extends JsonValue>(
    flagKey: string,
    defaultValue: T,
    context: EvaluationContext,
    _logger: Logger,
  ): Promise<ResolutionDetails<T>> {
    const result = await this.client.evaluate(
      flagKey,
      defaultValue,
      toEvalContext(context),
    );
    return toResolution(result.value as T, result);
  }
}

function toResolution<T>(
  value: T,
  result: { variationId: string | null; reason: string; errorMessage?: string },
): ResolutionDetails<T> {
  return {
    value,
    reason: result.reason,
    ...(result.variationId ? { variant: result.variationId } : {}),
    ...(result.errorMessage
      ? { errorCode: ErrorCode.GENERAL, errorMessage: result.errorMessage }
      : {}),
  };
}

function wrongType<T>(
  defaultValue: T,
  got: string,
  want: string,
): ResolutionDetails<T> {
  return {
    value: defaultValue,
    reason: "ERROR",
    errorCode: ErrorCode.TYPE_MISMATCH,
    errorMessage: `flag value type ${got} does not match requested ${want}`,
  };
}

// OpenFeature's EvaluationContext is a flat key/value bag with a
// targetingKey shorthand. We hand it through to our EvalContext as-is —
// nested per-kind objects use the same { key, ...attrs } shape.
function toEvalContext(ctx: EvaluationContext): EvalContext {
  const out: EvalContext = {};
  for (const [k, v] of Object.entries(ctx)) {
    if (k === "targetingKey" && typeof v === "string") {
      out.targetingKey = v;
      continue;
    }
    if (v && typeof v === "object" && !Array.isArray(v) && "key" in v) {
      out[k] = v as EvalContext[string];
    }
  }
  return out;
}
