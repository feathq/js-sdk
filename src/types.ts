// Re-export the shared eval-engine types so SDK consumers don't have to
// reach into @feathq/feat-eval directly.
export type {
  ContextKindObject,
  EvalContext,
  EvaluationResult,
  Reason,
} from "@feathq/feat-eval";
export type { Datafile, Operator } from "@feathq/datafile-schema";
