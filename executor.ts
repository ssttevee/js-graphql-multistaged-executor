import {
  defaultFieldResolver,
  defaultTypeResolver,
  ExecutionArgs,
  ExecutionResult,
  FieldNode,
  FragmentDefinitionNode,
  getNamedType,
  GraphQLAbstractType,
  GraphQLError,
  GraphQLField,
  GraphQLFieldResolver,
  GraphQLInterfaceType,
  GraphQLLeafType,
  GraphQLObjectType,
  GraphQLOutputType,
  GraphQLResolveInfo,
  GraphQLScalarType,
  GraphQLSchema,
  GraphQLTypeResolver,
  GraphQLUnionType,
  isAbstractType,
  isLeafType,
  isListType,
  isNonNullType,
  OperationDefinitionNode,
  SelectionNode,
} from "graphql";
import { addPath, Path, pathToArray } from "graphql/jsutils/Path";
import { extractOperationAndFragments } from "./ast";
import { FragmentDefinitionMap, selectionFields } from "./selection";
import { resolveArguments } from "./arguments";
import { getFieldDef } from "graphql/execution/execute";
import { expandFromObject, ShouldExcludeResultPredicate } from "./expand";
import { flattenMiddleware, isNullValue, Middleware, partition, selectFromObject } from "./utils";
import { getRootType } from "./helpers";

export type WrappedValue<T> = PromiseLike<T> & (
  Exclude<T, null | undefined> extends Array<infer E> ? Array<WrappedValue<E>> :
  Exclude<T, null | undefined> extends object ? { [P in keyof T]-?: WrappedValue<T[P]> } :
  unknown
);

export interface ExpandedChild {
  fieldNode: FieldNode;
  path: Path;
  sourceValue: unknown;
  setData: (data: any) => void;
}

export interface ExpandedAbstractType {
  concreteType: GraphQLObjectType;
  sourceValue: unknown;
  setDeferred: (v: any) => void;
  suppressArrayHandling?: boolean;
}

export interface ExecutorBackend<TDeferred> {
  unwrapResolvedValue: (value: WrappedValue<any>) => unknown;
  isWrappedValue: (value: unknown) => value is WrappedValue<any>;
  wrapSourceValue(
    sourceValue: unknown,
    getValue: () => Promise<unknown>,
  ): WrappedValue<any>;
  isDeferredValue(value: unknown): value is TDeferred;
  resolveDeferredValues(values: Array<[TDeferred, Path]>, executionArgs: ExecutionArgs): Promise<unknown[]>;
  expandChildren(
    path: Path,
    returnType: GraphQLOutputType,
    listValue: TDeferred,
    fieldNodes: readonly FieldNode[],
    setDeferred: (data: TDeferred) => void,
    suppressArrayHandling?: boolean,
  ): Array<ExpandedChild>;
  expandAbstractType?: (schema: GraphQLSchema, path: Path, abstractValue: TDeferred, abstractType: GraphQLAbstractType, handleArray: boolean, setDeferred: (data: TDeferred) => void, executionArgs: ExecutionArgs) => Array<ExpandedAbstractType>;
  getErrorMessage?: (value: unknown) => string | null;
}

type SerializeFunction = (value: any, contextValue: any) => unknown;

const identity: SerializeFunction = (v) => v;

interface ResolveContext<TRoot = any> {
  schema: GraphQLSchema;
  variableValues: { readonly [variable: string]: unknown };
  fragments: { readonly [variable: string]: FragmentDefinitionNode };
  rootValue: TRoot;
  operation: OperationDefinitionNode;
}

interface BuildResolveInfoInput {
  fieldNode: FieldNode;
  fieldNodes: readonly FieldNode[];
  path: Path;
  parentType: GraphQLObjectType;
}

function buildResolveInfo(
  ctx: ResolveContext,
  input: BuildResolveInfoInput,
  returnType: GraphQLOutputType,
): GraphQLResolveInfo {
  return {
    schema: ctx.schema,
    rootValue: ctx.rootValue,
    fragments: ctx.fragments,
    operation: ctx.operation,
    variableValues: ctx.variableValues,
    parentType: input.parentType,
    path: input.path,
    fieldName: input.fieldNode.name.value,
    fieldNodes: input.fieldNodes,
    returnType,
  };
}

function fieldNodeKey(node: FieldNode): string {
  return node.alias?.value ?? node.name.value;
}

const neverResolves = new Promise<never>(() => { });

function equivalentPathKey(a: string | number | undefined, b: string | number | undefined): boolean {
  return (
    a === b ||
    (typeof a === 'number' && b === '[]') ||
    (a === '[]' && typeof b === 'number')
  )
}

function buildUnresolvedFields(
  schema: GraphQLSchema,
  fragmentMap: FragmentDefinitionMap,
  unionMap: Record<string, GraphQLUnionType>,
  prevPath: Path | undefined,
  parentType: ParentType,
  sourceValue: any,
  selectionNodes: readonly SelectionNode[]
) {
  return selectionFields(
    schema,
    fragmentMap,
    unionMap,
    selectionNodes,
    parentType.type,
  ).map(
    (
      fieldNode,
      _,
      fieldNodes,
    ) => {
      return {
        prevPath,
        fieldNode,
        fieldNodes,
        parentType,
        sourceValue,
      };
    },
  );
}

interface ParentType {
  type: GraphQLObjectType;
  parent?: ParentType;
}

interface BaseFieldContext {
  fieldNode: FieldNode;
  fieldNodes: readonly FieldNode[];
  parentType: ParentType;
}

interface FieldToResolve<TDeferred> extends BaseFieldContext {
  prevPath: Path | undefined;
  sourceValue: any;
  overrideFieldResolver?: GraphQLFieldResolver<any, any>;
  shouldExcludeResult?: ShouldExcludeResultPredicate;
  deferral?: {
    set: (v: TDeferred) => void;
    path: Array<string | number>;
  };
}

interface FieldToDiscriminate<TDeferred> extends Omit<FieldToResolve<TDeferred>, 'sourceValue' | 'prevPath'> {
  fieldValue: any;
  fieldType: GraphQLOutputType;
  path: Path;
}

interface FieldToValidate extends BaseFieldContext {
  fieldType: GraphQLOutputType;
  fieldValue: any;
  path: Path;
}

interface FieldToRestage extends BaseFieldContext {
  prevPath: Path | undefined;
  deferredPath: Array<string | number>;
  shouldExcludeResult?: ShouldExcludeResultPredicate;
}

interface FieldToRevalidate extends BaseFieldContext {
  fieldType: GraphQLOutputType;
  fieldPath: Path;
  deferredPath: Array<string | number>;
  shouldExcludeResult?: ShouldExcludeResultPredicate;
}

/**
 * Return the new elements in next that were not in prev
 */
function arrayNewElems<T>(prev: T[], next: T[]): T[] {
  if (prev.length === next.length) {
    return [];
  }

  if (prev.length > next.length) {
    throw new Error("arrayNewElems: prev.length > next.length");
  }

  for (let i = 0; i < prev.length; i++) {
    if (prev[i] !== next[i]) {
      throw new Error("arrayNewElems: prev[i] !== next[i]");
    }
  }

  return next.slice(prev.length);
}

const nextStage = Symbol();

const resolveAbstractTypename = (source: any) => source.__typename;

export type FieldResolverGetter<TSource, TContext> = {
  (fieldDefinition: GraphQLField<TSource, TContext, any>, executionArgs: ExecutionArgs, defaultFieldResolver: GraphQLFieldResolver<TSource, TContext>): GraphQLFieldResolver<TSource, TContext>;
};

function defaultFieldResolverGetter(fieldDef: GraphQLField<unknown, unknown, any>, args: ExecutionArgs, defaultFieldResolver: GraphQLFieldResolver<unknown, unknown>) {
  return fieldDef.resolve ?? args.fieldResolver ?? defaultFieldResolver;
}

export type TypeResolverGetter<TSource, TContext> = {
  (fieldDefinition: GraphQLAbstractType, executionArgs: ExecutionArgs, defaultTypeResolver: GraphQLTypeResolver<TSource, TContext>): GraphQLTypeResolver<TSource, TContext>;
};

function defaultTypeResolverGetter(fieldType: GraphQLAbstractType, args: ExecutionArgs, defaultTypeResolver: GraphQLTypeResolver<unknown, unknown>) {
  return fieldType.resolveType ?? args.typeResolver ?? defaultTypeResolver;
}

export type SerializerGetter<TSource, TContext> = {
  (fieldValue: unknown, fieldType: GraphQLLeafType, parentType: GraphQLObjectType<TSource, TContext>, path: Path, executionArgs: ExecutionArgs): SerializeFunction | undefined | null;
};

function defaultSerializerGetter(_: unknown, fieldType: GraphQLLeafType) {
  return fieldType.serialize?.bind(fieldType);
}

export type FieldResolverGetterMiddleware<TSource, TContext> = Middleware<FieldResolverGetter<TSource, TContext>>;

export type FieldResolverMiddleware<TSource, TContext, TArgs = any, TResult = unknown> = Middleware<GraphQLFieldResolver<TSource, TContext, TArgs, TResult>>;

export type TypeResolverGetterMiddleware<TSource, TContext> = Middleware<TypeResolverGetter<TSource, TContext>>;

export type TypeResolverMiddleware<TSource, TContext> = Middleware<GraphQLTypeResolver<TSource, TContext>>;

export type SerializerGetterMiddleware<TSource, TContext> = Middleware<SerializerGetter<TSource, TContext>>;

export type ResolveDeferredValuesMiddleware<TDeferred> = Middleware<ExecutorBackend<TDeferred>['resolveDeferredValues']>

type MaybeArray<T> = T | T[];

interface Middlewares<TSource, TContext, TDeferred> {
  fieldResolverMiddleware?: MaybeArray<FieldResolverMiddleware<TSource, TContext>>;
  fieldResolverGetterMiddleware?: MaybeArray<FieldResolverGetterMiddleware<TSource, TContext>>;
  typeResolverMiddleware?: MaybeArray<TypeResolverMiddleware<TSource, TContext>>;
  typeResolverGetterMiddleware?: MaybeArray<TypeResolverGetterMiddleware<TSource, TContext>>;
  serializerGetterMiddleware?: MaybeArray<SerializerGetterMiddleware<TSource, TContext>>;
  resolveDeferredValuesMiddleware?: MaybeArray<ResolveDeferredValuesMiddleware<TDeferred>>;
}

export interface CreateExecuteFnOptions<TSource, TContext, TDeferred> extends Middlewares<TSource, TContext, TDeferred> {
}

export function createExecuteFn<TDeferred>(
  backend: ExecutorBackend<TDeferred>,
  options: CreateExecuteFnOptions<unknown, unknown, TDeferred> = {},
): <T = any>(args: ExecutionArgs) => Promise<ExecutionResult<T>> {
  const rootFieldResolverMiddleware = flattenMiddleware(options.fieldResolverMiddleware);
  const rootFieldResolverGetterMiddleware = flattenMiddleware(options.fieldResolverGetterMiddleware);
  const rootTypeResolverMiddleware = flattenMiddleware(options.typeResolverMiddleware);
  const rootTypeResolverGetterMiddleware = flattenMiddleware(options.typeResolverGetterMiddleware);
  const rootSerializerGetterMiddleware = flattenMiddleware(options.serializerGetterMiddleware);
  const rootResolveDeferredValuesMiddleware = flattenMiddleware(options.resolveDeferredValuesMiddleware);

  return async function execute<T = any>(args: ExecutionArgs & Middlewares<unknown, unknown, TDeferred>): Promise<ExecutionResult<T>> {
    const {
      schema,
      rootValue,
      contextValue,
    } = args;

    const fieldResolverMiddleware = flattenMiddleware([flattenMiddleware(args.fieldResolverMiddleware), rootFieldResolverMiddleware]);
    const getFieldResolver = flattenMiddleware([flattenMiddleware(args.fieldResolverGetterMiddleware), rootFieldResolverGetterMiddleware])(defaultFieldResolverGetter);
    const typeResolverMiddleware = flattenMiddleware([flattenMiddleware(args.typeResolverMiddleware), rootTypeResolverMiddleware]);
    const getTypeResolver = flattenMiddleware([flattenMiddleware(args.typeResolverGetterMiddleware), rootTypeResolverGetterMiddleware])(defaultTypeResolverGetter);
    const getSerializer = flattenMiddleware([flattenMiddleware(args.serializerGetterMiddleware), rootSerializerGetterMiddleware])(defaultSerializerGetter);
    const resolveDeferredValues = flattenMiddleware([flattenMiddleware(args.resolveDeferredValuesMiddleware), rootResolveDeferredValuesMiddleware])(backend.resolveDeferredValues);

    /**
     * GOALS:
     *
     * 1. Correctly execute the query
     * 2. Execution should not be recursive
     * 3. Execution of deferred fields should be batched
     */

    const [operation, fragmentNodes] = extractOperationAndFragments(
      args.document,
    );

    const rootType = getRootType(schema, operation);
    if (!rootType) {
      throw new Error(`missing ${operation.operation} type`);
    }

    const unions = Object.fromEntries(
      Object.entries(schema.getTypeMap()).filter((pair): pair is [string, GraphQLUnionType]  => pair[1] instanceof GraphQLUnionType),
    );

    const ctx: ResolveContext = {
      schema,
      fragments: Object.fromEntries(
        (fragmentNodes || []).map(
          (fragment) => [fragment.name.value, fragment],
        ),
      ),
      operation,
      rootValue,
      variableValues: args.variableValues || {},
    };

    try {
      const resultErrors: GraphQLError[] = [];

      const completedFields: Array<{ path: Path; value: any; fieldNode?: FieldNode; serialize: SerializeFunction }> = [];

      const step1_resolve: Array<FieldToResolve<TDeferred>> = buildUnresolvedFields(schema, ctx.fragments, unions, undefined, { type: rootType }, rootValue, operation.selectionSet.selections);
      const step2_discriminate: Array<FieldToDiscriminate<TDeferred>> = [];
      const step3_validate: Array<FieldToValidate> = [];

      while (step1_resolve.length || step2_discriminate.length || step3_validate.length) {
        const step4_restage: Array<FieldToRestage> = [];
        const step5_revalidate: Array<FieldToRevalidate> = [];

        const deferredExprs: Array<[TDeferred, Path]> = [];

        while (step1_resolve.length || step2_discriminate.length || step3_validate.length) {
          while (step1_resolve.length) {
            const { fieldNode, fieldNodes, prevPath, parentType, sourceValue: originalSourceValue, deferral, overrideFieldResolver, shouldExcludeResult } = step1_resolve.shift()!;
            const fieldKey = fieldNodeKey(fieldNode);
            const path = addPath(prevPath, fieldKey, parentType.type.name);
            try {
              const fieldDef = getFieldDef(schema, parentType.type, fieldNode)!;
              const resolveField = overrideFieldResolver ?? getFieldResolver(fieldDef, args, defaultFieldResolver);

              let sourceValue = await originalSourceValue;
              if (backend.isDeferredValue(sourceValue)) {
                sourceValue = backend.wrapSourceValue(
                  sourceValue,
                  // () => new Promise<unknown>((r) => pause(r)),
                  () => Promise.reject(nextStage),
                );
              }

              try {
                let fieldValue = fieldResolverMiddleware(resolveField)(
                  sourceValue,
                  resolveArguments(
                    ctx.variableValues,
                    fieldNode.arguments,
                    fieldDef.args,
                  ),
                  contextValue,
                  buildResolveInfo(
                    ctx,
                    {
                      fieldNode,
                      fieldNodes,
                      path,
                      parentType: parentType.type,
                    },
                    fieldDef.type,
                  ),
                );

                if (backend.isWrappedValue(fieldValue)) {
                  fieldValue = backend.unwrapResolvedValue(fieldValue);
                }

                fieldValue = await fieldValue;

                // console.log('step1_resolve: send to step2_discriminate', pathToArray(path), fieldValue);
                step2_discriminate.push({ fieldNode, fieldNodes, fieldValue, fieldType: fieldDef.type, parentType, path, deferral, shouldExcludeResult });
                continue;
              } catch (e) {
                if (e !== nextStage) {
                  throw e;
                }
              }

              // the field resolver awaited a deferred value, so we need to wait for it to resolve
              if (!deferral) {
                throw new Error('expected deferral value');
              }

              // console.log('step1_resolve: send to step4_restage', pathToArray(prevPath));
              deferral.set(sourceValue);
              step4_restage.push({
                fieldNode,
                fieldNodes,
                parentType,
                prevPath,
                deferredPath: [...deferral.path, fieldKey],
                shouldExcludeResult,
              });
            } catch (e) {
              resultErrors.push(new GraphQLError((e as any)?.message ?? String(e), {
                nodes: (e as any).nodes ?? [fieldNode],
                source: (e as any).source ?? fieldNode.loc?.source,
                positions: (e as any).positions ?? (fieldNode.loc?.source && [fieldNode.loc.start]),
                path: (e as any).path ?? pathToArray(path),
                originalError: e as any,
              }));
            }
          }

          while (step2_discriminate.length) {
            const { fieldNode, fieldNodes, fieldValue, fieldType, parentType, path, deferral, shouldExcludeResult } = step2_discriminate.shift()!;
            try {
              if (!backend.isDeferredValue(fieldValue) && !pathToArray(path).includes('[]')) {
                // console.log('step2_discriminate: send to step3_validate', pathToArray(path), fieldValue);
                step3_validate.push({ fieldType, fieldValue, parentType, fieldNode, fieldNodes, path });
                continue;
              }

              let setDeferredChild: (v: TDeferred) => void;
              let deferredPath: Array<string | number>;
              if (deferral) {
                (setDeferredChild = deferral.set)(fieldValue);
                deferredPath = (deferral?.path ?? []).concat(fieldNodeKey(fieldNode));
              } else {
                const index = deferredExprs.length;
                deferredExprs.push([fieldValue, path]);
                setDeferredChild = (expr) => {
                  deferredExprs[index] = [expr, path];
                };
                deferredPath = [index];
              }

              const namedFieldType = getNamedType(fieldType);
              if (isLeafType(namedFieldType)) {
                // console.log('step2_discriminate: send to step5_revalidate', pathToArray(path), deferredPath);
                step5_revalidate.push({
                  fieldType,
                  fieldNode,
                  fieldNodes,
                  fieldPath: path,
                  parentType,
                  deferredPath: deferredPath,
                  shouldExcludeResult,
                });
                continue;
              }

              const concreteTypes: Array<{
                concreteType: GraphQLObjectType;
                selectedFieldNodes: FieldNode[];
                setDeferredChild: (v: TDeferred) => void;
                deferredValue: TDeferred;
                shouldExcludeResult?: ShouldExcludeResultPredicate;
                suppressArrayHandling?: boolean;
              }> = [];
              if (isAbstractType(namedFieldType)) {
                if (!backend.expandAbstractType) {
                  throw new Error("eager determination of union and interface types are not supported yet for this backend");
                }

                const expanded = backend
                  .expandAbstractType(schema, path, fieldValue, namedFieldType, isListType(fieldType) || (isNonNullType(fieldType) && isListType(fieldType.ofType)), setDeferredChild, args)
                  .map(({ concreteType, ...rest }) => ({
                    ...rest,
                    concreteType,
                    selectedFieldNodes: selectionFields(ctx.schema, ctx.fragments, unions, fieldNode.selectionSet?.selections ?? [], concreteType),
                  }));

                const typeFieldsMap = Object.fromEntries(
                  expanded.map(
                    ({ concreteType, selectedFieldNodes }) => [
                      concreteType.name,
                      new Set(selectedFieldNodes.map((node) => fieldNodeKey(node))),
                    ],
                  ),
                );

                const thisDeferredPath = deferredPath;
                const shouldExcludeResultNext = (deferredPath: Array<string | number>, deferredValue: any) => {
                  if (shouldExcludeResult?.(deferredPath, deferredValue)) {
                    return true;
                  }

                  if (thisDeferredPath.length > deferredPath.length || !thisDeferredPath.every((v, i) => v === deferredPath[i])) {
                    return false;
                  }

                  let path = thisDeferredPath;
                  const key = deferredPath[thisDeferredPath.length] as string;
                  if (key === '[]') {
                    return false;
                  } else if (typeof key === 'number') {
                    path = [...path, key];
                  }

                  const value = selectFromObject(deferredValue, path);
                  if (!value || backend.getErrorMessage?.(value)) {
                    return false;
                  }

                  if (!value.__typename) {
                    throw new Error("missing __typename");
                  }

                  if (!typeFieldsMap[value.__typename]) {
                    throw new Error(`unexpected typename: expected one of ${JSON.stringify(Object.keys(typeFieldsMap))} but got ${JSON.stringify(value.__typename)}`);
                  }

                  return !typeFieldsMap[value.__typename]?.has(key);
                }


                concreteTypes.push(
                  ...expanded.map(({ concreteType, sourceValue, setDeferred, selectedFieldNodes, suppressArrayHandling }) => ({
                    concreteType,
                    selectedFieldNodes,
                    deferredValue: sourceValue as TDeferred,
                    setDeferredChild: setDeferred,
                    shouldExcludeResult: shouldExcludeResultNext,
                    suppressArrayHandling,
                  })),
                )
              } else {
                concreteTypes.push({
                  concreteType: namedFieldType,
                  selectedFieldNodes: selectionFields(
                    ctx.schema,
                    ctx.fragments,
                    unions,
                    fieldNode.selectionSet?.selections ?? [],
                    namedFieldType,
                  ),
                  deferredValue: fieldValue,
                  setDeferredChild,
                });
              }


              // console.log('step2_discriminate: send to step1_resolve', pathToArray(path), deferredPath);
              step1_resolve.push(
                ...concreteTypes.flatMap(({ concreteType, selectedFieldNodes, setDeferredChild, deferredValue, shouldExcludeResult, suppressArrayHandling }) => backend.expandChildren(
                  path,
                  fieldType,
                  deferredValue,
                  selectedFieldNodes,
                  setDeferredChild,
                  suppressArrayHandling,
                ).map((child) => ({
                  fieldNode: child.fieldNode,
                  fieldNodes: selectedFieldNodes,
                  prevPath: child.path,
                  parentType: {
                    type: concreteType,
                    parent: parentType,
                  },
                  sourceValue: child.sourceValue,
                  overrideFieldResolver: child.fieldNode.name.value === '__typename' && concreteType !== namedFieldType ? resolveAbstractTypename : undefined,
                  shouldExcludeResult,
                  deferral: {
                    set: child.setData,
                    path: deferredPath.concat(arrayNewElems(pathToArray(path), pathToArray(child.path))),
                  },
                })))
              );
            } catch (e) {
              resultErrors.push(new GraphQLError((e as any)?.message ?? String(e), {
                nodes: (e as any).nodes ?? [fieldNode],
                source: (e as any).source ?? fieldNode.loc?.source,
                positions: (e as any).positions ?? (fieldNode.loc?.source && [fieldNode.loc.start]),
                path: (e as any).path ?? pathToArray(path),
                originalError: e as any,
              }));
            }
          }

          while (step3_validate.length) {
            const { fieldType: originalFieldType, fieldNode, fieldNodes, fieldValue, parentType, path } = step3_validate.shift()!;

            // console.log('step3_validate', pathToArray(path), fieldValue, fieldValue, parentType.toString(), originalFieldType.toString());
            try {
              let fieldType = originalFieldType;
              if (isNonNullType(fieldType)) {
                if (isNullValue(fieldValue)) {
                  resultErrors.push(new GraphQLError(
                    `Cannot return null for non-nullable field`,
                    {
                      nodes: fieldNode,
                      source: fieldNode.loc?.source,
                      positions: null,
                      path: pathToArray(path),
                    }
                  ));
                  continue;
                }

                fieldType = fieldType.ofType;
              }

              if (isNullValue(fieldValue)) {
                // console.log('step3_validate: send to completedFields (1)', pathToArray(path), fieldValue);
                completedFields.push({ path, value: null, fieldNode, serialize: identity });
                continue;
              }

              if (isListType(fieldType)) {
                if (!Array.isArray(fieldValue)) {
                  resultErrors.push(new GraphQLError(
                    `Cannot return non-list value for list field`,
                    {
                      nodes: fieldNode,
                      source: fieldNode.loc?.source,
                      positions: null,
                      path: pathToArray(path),
                    }
                  ));
                } else if (fieldValue.length === 0) {
                  // console.log('step3_validate: send to completedFields (2)', pathToArray(path), fieldValue);
                  completedFields.push({ path, value: fieldValue, fieldNode, serialize: identity });
                } else {
                  const elementFieldType = fieldType.ofType;
                  // console.log('step3_validate: send to step3_validate', pathToArray(path), fieldValue);
                  step3_validate.push(...fieldValue.map((value, index) => ({
                    fieldType: elementFieldType,
                    fieldValue: value,
                    parentType: parentType,
                    fieldNode: fieldNode,
                    fieldNodes: fieldNodes,
                    path: addPath(path, index, undefined),
                  })));
                }

                continue;
              } else {
                if (Array.isArray(fieldValue) && !(fieldType instanceof GraphQLScalarType)) {
                  resultErrors.push(new GraphQLError(
                    `Cannot return list value for non-list field`,
                    {
                      nodes: fieldNode,
                      source: fieldNode.loc?.source,
                      positions: null,
                      path: pathToArray(path),
                    }
                  ));
                  continue;
                }
              }

              if (isLeafType(fieldType)) {
                // console.log('step3_validate: send to completedFields (3)', pathToArray(path), fieldValue);
                completedFields.push({ path, value: fieldValue, fieldNode, serialize: getSerializer(fieldValue, fieldType, parentType.type, path, args) ?? identity });
                continue;
              }

              let concreteType: GraphQLObjectType;
              if (fieldType instanceof GraphQLInterfaceType || fieldType instanceof GraphQLUnionType) {
                const resolveType = getTypeResolver(fieldType, args, defaultTypeResolver);
                const typeName = await typeResolverMiddleware(resolveType)(
                  fieldValue,
                  contextValue,
                  buildResolveInfo(
                    ctx,
                    {
                      fieldNode,
                      fieldNodes,
                      path,
                      parentType: parentType.type,
                    },
                    fieldType,
                  ),
                  fieldType,
                );

                const resolvedType = typeName && schema.getType(typeName);
                if (!resolvedType || !(resolvedType instanceof GraphQLObjectType)) {
                  resultErrors.push(new GraphQLError(
                    `Failed to resolve concrete type`,
                    {
                      nodes: fieldNode,
                      source: fieldNode.loc?.source,
                      positions: null,
                      path: pathToArray(path),
                    }
                  ));
                  continue;
                }

                concreteType = resolvedType;
              } else {
                concreteType = fieldType;
              }

              if (!fieldNode.selectionSet) {
                resultErrors.push(new GraphQLError(
                  `At least one selection must be provided for a composite type`,
                  {
                    nodes: fieldNode,
                    source: fieldNode.loc?.source,
                    positions: null,
                    path: pathToArray(path),
                  },
                ));
                continue;
              }

              // console.log('step3_validate: send to step1_resolve', pathToArray(path), fieldValue);
              step1_resolve.push(
                ...buildUnresolvedFields(
                  schema,
                  ctx.fragments,
                  unions,
                  path,
                  {
                    type: concreteType,
                    parent: parentType,
                  },
                  fieldValue,
                  fieldNode.selectionSet.selections,
                ),
              );
            } catch (e) {
              resultErrors.push(new GraphQLError((e as any)?.message ?? String(e), {
                nodes: (e as any).nodes ?? fieldNode,
                source: (e as any).source ?? fieldNode.loc?.source,
                positions: (e as any).positions ?? (fieldNode.loc?.source && [fieldNode.loc.start]),
                path: (e as any).path ?? pathToArray(path),
                originalError: e as any,
              }));
            }
          }
        }

        if (deferredExprs.length) {
          const deferredValues = await resolveDeferredValues(deferredExprs, args);
          // console.log(`resolved ${deferredExprs.length} deferred values`, deferredValues);

          while (step4_restage.length) {
            const { fieldNode, fieldNodes, parentType, prevPath, deferredPath, shouldExcludeResult } = step4_restage.shift()!;
            try {
              const [finishedValues, nextValues] = partition(
                expandFromObject(deferredValues, deferredPath, addPath(prevPath, fieldNodeKey(fieldNode), undefined), shouldExcludeResult, resultErrors, backend.getErrorMessage),
                ({ path, value }) => !equivalentPathKey(path?.prev?.key, prevPath?.key) || isNullValue(value),
              );
              completedFields.push(
                ...finishedValues.map(({ path, value }) => {
                  // console.log('step4_restage: send to completedFields', pathToArray(path), value);
                  return ({
                    path: path!,
                    value,
                    serialize: identity,
                  });
                }),
              );

              step1_resolve.push(
                ...nextValues.flatMap(({ path, value }) => {
                  // console.log('step4_restage: send to step1_resolve', pathToArray(path), value);
                  return [{
                    // TODO: field nodes are probably invalid when prevPath.length !== path.length
                    fieldNode,
                    fieldNodes,
                    parentType: pathToArray(prevPath).slice(pathToArray(path).length).reduce((parentType) => {
                      if (!parentType.parent) {
                        throw new Error('Expected parent');
                      }

                      return parentType.parent;
                    }, parentType),
                    prevPath: path.prev,
                    sourceValue: value,
                  }];
                }),
              );
            } catch (e) {
              resultErrors.push(new GraphQLError((e as any)?.message ?? String(e), {
                nodes: (e as any).nodes ?? [fieldNode],
                source: (e as any).source ?? fieldNode.loc?.source,
                positions: (e as any).positions ?? (fieldNode.loc?.source && [fieldNode.loc.start]),
                path: (e as any).path ?? pathToArray(prevPath),
                originalError: e as any,
              }));
            }
          }

          while (step5_revalidate.length) {
            const { fieldNode, fieldNodes, fieldType, fieldPath, deferredPath, parentType, shouldExcludeResult } = step5_revalidate.shift()!;
            try {
              const [finishedValues, nextValues] = partition(
                expandFromObject(deferredValues, deferredPath, fieldPath, shouldExcludeResult, resultErrors, backend.getErrorMessage),
                ({ path, value }) => !equivalentPathKey(path?.key, fieldPath.key) || isNullValue(value),
              );
              completedFields.push(
                ...finishedValues.map(({ path, value }) => {
                  // console.log('step5_revalidate: send to completedFields', pathToArray(path), value);
                  return ({
                    path: path!,
                    value,
                    serialize: identity,
                  });
                }),
              );

              step3_validate.push(
                ...nextValues.map(({ path, value }) => {
                  // console.log('step5_revalidate: send to step3_validate', pathToArray(path), value);
                  return ({
                    fieldNode,
                    fieldNodes,
                    fieldType,
                    fieldValue: value,
                    parentType: pathToArray(fieldPath).slice(pathToArray(path).length).reduce((parentType) => {
                      if (!parentType.parent) {
                        throw new Error('Expected parent');
                      }

                      return parentType.parent;
                    }, parentType),
                    // ts is not happy that path might be undefined, but it's guaranteed to be defined as long as the
                    // last path element is not an array index placeholder, which is not possible in graphql
                    path: path!,
                  });
                
                }),
              );
            } catch (e) {
              resultErrors.push(new GraphQLError((e as any)?.message ?? String(e), {
                nodes: (e as any).nodes ?? [fieldNode],
                source: (e as any).source ?? fieldNode.loc?.source,
                positions: (e as any).positions ?? (fieldNode.loc?.source && [fieldNode.loc.start]),
                path: (e as any).path ?? pathToArray(fieldPath),
                originalError: e as any,
              }));
            }
          }
        }

        if (step4_restage.length) {
          throw new Error('Expected no restage');
        }

        if (step5_revalidate.length) {
          throw new Error('Expected no backfill');
        }
      }

      const resultData: Record<string, any> = {};
      for (const { path, value, serialize, fieldNode } of completedFields) {
        let parent: [any, string | number] | null = null;
        let container: Record<string, any> | Array<any> = resultData;
        const pathArray = pathToArray(path);
        for (const key of pathArray) {
          if (typeof key === 'number') {
            if (!container) {
              if (!parent) {
                throw new Error('Expected parent');
              }

              container = ((parent[0][parent[1]] as any) = []);
            } else if (!Array.isArray(container)) {
              throw new Error('Expected array');
            }

            while (container.length <= key) {
              container.push(null);
            }
          } else if (!container) {
            if (!parent) {
              throw new Error('Expected parent');
            }

            container = ((parent[0][parent[1]] as any) = {});
          }

          parent = [container, key];
          container = (container as any)[key];
        }

        if (!parent) {
          throw new Error('Expected parent');
        }

        try {
          parent[0][parent[1]] = await serialize(await value, contextValue);
        } catch (e) {
          resultErrors.push(new GraphQLError((e as any)?.message ?? String(e), {
            nodes: (e as any).nodes ?? fieldNode,
            source: (e as any).source ?? fieldNode?.loc?.source,
            positions: (e as any).positions ?? (fieldNode?.loc?.source && [fieldNode?.loc.start]),
            path: (e as any).path ?? pathToArray(path),
            originalError: (e as any).originalError ?? (e as any),
          }));
        }
      }

      const result: ExecutionResult<T> = { data: Object.keys(resultData).length ? resultData as T : null };
      if (resultErrors.length) {
        // dedupe errors
        const fingerprints = new Set();
        result.errors = [];
        for (const error of resultErrors) {
            const fingerprint = JSON.stringify(error);
            if (fingerprints.has(fingerprint) && fingerprints.add(fingerprint)) {
                continue;
            }

            (result.errors as GraphQLError[]).push(error);
        }
      }

      return result;
    } catch (err) {
      return {
        errors: Array.from(
          Array.isArray(err) ? err : [err],
          (err) => err instanceof GraphQLError ? err : new GraphQLError(err.message, { originalError: err }),
        ),
      };
    }
  };
}
