export { FeatClient, type FeatClientConfig } from "./client";
export { FeatProvider } from "./provider";
export {
  fetchSseTransport,
  type SseFrame,
  type SseTransport,
  type SseTransportOptions,
} from "./streaming";
export { evaluate } from "@feathq/feat-eval";
export type {
  ContextKindObject,
  Datafile,
  EvalContext,
  EvaluationResult,
  Operator,
  Reason,
} from "./types";
