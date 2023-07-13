import type {
  ExecutionArgs,
  ExecutionResult,
  FieldNode,
  GraphQLOutputType,
} from "graphql";
import type { Path } from "graphql/jsutils/Path.mjs";

export type WrappedValue<
  T extends Record<string, unknown> = Record<string, unknown>,
> = PromiseLike<T> & {
  [key: string]: T[keyof T] extends Record<string, unknown>
    ? WrappedValue<T[keyof T]>
    : Record<never, unknown>;
};

interface PendingField {
  fieldNode: FieldNode;
  path: Path;
  sourceValue: any;
  parentIsList?: boolean;
  resolvedSourceValue?: Promise<unknown>;
  setData: (data: any) => void;
}

export interface ExecutorBackend<TDeferred> {
  unwrapResolvedValue: (value: WrappedValue) => unknown;
  isWrappedValue: (value: unknown) => value is WrappedValue;
  wrapSourceValue(
    sourceValue: unknown,
    getValue: () => Promise<unknown>,
  ): WrappedValue;
  isDeferredValue(value: unknown): value is TDeferred;
  resolveDeferredValues(values: TDeferred[]): Promise<unknown[]>;
  expandChildren(
    path: Path,
    returnType: GraphQLOutputType,
    listValue: TDeferred,
    fieldNodes: readonly FieldNode[],
    setDeferred: (data: TDeferred) => void,
  ): Array<PendingField>;
}

export function createExecuteFn<TDeferred>(
  backend: ExecutorBackend<TDeferred>,
): <T = any>(args: ExecutionArgs) => Promise<ExecutionResult<T>>;
