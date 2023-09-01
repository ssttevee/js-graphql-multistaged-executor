import {
  defaultFieldResolver,
  defaultTypeResolver,
  ExecutionArgs,
  ExecutionResult,
  FieldNode,
  FragmentDefinitionNode,
  getNamedType,
  GraphQLAbstractType,
  GraphQLEnumType,
  GraphQLError,
  GraphQLFieldResolver,
  GraphQLInterfaceType,
  GraphQLObjectType,
  GraphQLOutputType,
  GraphQLResolveInfo,
  GraphQLScalarType,
  GraphQLSchema,
  GraphQLUnionType,
  isAbstractType,
  isListType,
  isNonNullType,
  OperationDefinitionNode,
  OperationTypeNode,
  SelectionNode,
} from "graphql";
import { addPath, Path, pathToArray } from "graphql/jsutils/Path";
import { extractOperationAndFragments } from "./ast";
import { FragmentDefinitionMap, selectionFields } from "./selection";
import { resolveArguments } from "./arguments";
import { getFieldDef } from "graphql/execution/execute";

export type WrappedValue<T> = PromiseLike<T> & (
  T extends Array<infer E> ? Array<WrappedValue<E>> :
  T extends object ? { [P in keyof T]-?: WrappedValue<T[P]> } :
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
}

export interface ExecutorBackend<TDeferred> {
  unwrapResolvedValue: (value: WrappedValue<any>) => unknown;
  isWrappedValue: (value: unknown) => value is WrappedValue<any>;
  wrapSourceValue(
    sourceValue: unknown,
    getValue: () => Promise<unknown>,
  ): WrappedValue<any>;
  isDeferredValue(value: unknown): value is TDeferred;
  resolveDeferredValues(values: Array<[TDeferred, Path]>, observer?: (metrics: any) => void): Promise<unknown[]>;
  expandChildren(
    path: Path,
    returnType: GraphQLOutputType,
    listValue: TDeferred,
    fieldNodes: readonly FieldNode[],
    setDeferred: (data: TDeferred) => void,
  ): Array<ExpandedChild>;
  expandAbstractType?: (schema: GraphQLSchema, path: Path, abstractValue: TDeferred, abstractType: GraphQLAbstractType, setDeferred: (data: TDeferred) => void) => Array<ExpandedAbstractType>;
  getErrorMessage?: (value: unknown) => string | null;
}

type SerializeFunction = (value: any) => unknown;

const identity: SerializeFunction = (v) => v;

function isNullValue(value: unknown): boolean {
  return value === null || value === undefined;
}

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

const neverResolves = new Promise<never>(() => {});

function buildUnresolvedFields(
  schema: GraphQLSchema,
  fragmentMap: FragmentDefinitionMap,
  prevPath: Path | undefined,
  parentType: GraphQLObjectType,
  sourceValue: any,
  selectionNodes: readonly SelectionNode[]
) {
  return selectionFields(
    schema,
    fragmentMap,
    selectionNodes,
    parentType,
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

interface BaseFieldContext {
  fieldNode: FieldNode;
  fieldNodes: readonly FieldNode[];
  parentType: GraphQLObjectType;
}

interface ShouldExcludeResultPredicate {
  (deferredPath: Array<string | number>, deferredValue: any): boolean;
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

function selectFromObject(obj: any, path: Array<string | number>, getErrorMessage?: (value: any) => string | null): any {
  for (const [i, key] of path.entries()) {
    const errorMessage = getErrorMessage?.(obj);
    if (errorMessage) {
      throw new GraphQLError(errorMessage, {
        path: path.slice(0, i),
      });
    }

    obj = obj?.[key];
  }

  return obj;
}

function expandFromObject(obj: any, deferredPath: Array<string | number>, path: Path | undefined, getErrorMessage?: (value: any) => string | null): Array<{ path: Path; value: any }> {
  const pathArray = pathToArray(path);
  const arrayCountFromDeferred = deferredPath.filter((p) => p === '[]').length;
  const arrayCountFromPath = pathArray.filter((p) => p === '[]').length;
  if (arrayCountFromPath !== arrayCountFromDeferred) {
    throw new Error('expandFromObject: arraysFromPath !== arraysFromDeferred');
  }

  if (!arrayCountFromDeferred) {
    if (!path) {
      throw new Error('expandFromObject: path is undefined');
    }

    try {
      return [{ path, value: selectFromObject(obj, deferredPath, getErrorMessage) }];
    } catch (err) {
      if (err instanceof GraphQLError) {
        const errPath = Array.from(err.path!);
        if (typeof errPath[0] === 'number') {
          errPath.shift();
        }

        const fixedDeferredPathLength = (deferredPath.length - (typeof deferredPath[0] === 'number' ? 1 : 0));
        let realPath: Array<string | number>;
        if (!errPath.length) {
          realPath = pathArray.slice(0, -fixedDeferredPathLength);
        } else {
          let pos = -1;
          do {
            pos = pathArray.indexOf(errPath[0], pos + 1);
            if (pos === -1) {
              // give up
              realPath = pathArray;
              break;
            }
          } while (pathArray.slice(pos, pos + errPath.length).some((v, i) => v !== errPath[i]));
          realPath = pathArray.slice(0, pos + errPath.length);
        }

        throw new GraphQLError(err.message, {
          path: realPath,
        });
      }

      throw err;
    }
  }

  let pathPrefix = path;
  let pathSuffix: Path | undefined;
  for (let i = 0; i < arrayCountFromPath; i++) {
    if (pathSuffix) {
      pathSuffix = addPath(pathSuffix, '[]', undefined);
    }

    while (true) {
      if (!pathPrefix) {
        throw new Error('expandFromObject: path is too short');
      }

      if (pathPrefix.key === '[]') {
        break;
      }

      pathSuffix = addPath(pathSuffix, pathPrefix.key, pathPrefix.typename);
      pathPrefix = pathPrefix.prev;
    }

    pathPrefix = pathPrefix.prev;
  }

  const indexPlaceholderPos = deferredPath.indexOf('[]');
  const arrayValue = selectFromObject(obj, deferredPath.slice(0, indexPlaceholderPos));
  if (!Array.isArray(arrayValue)) {
    throw new Error("expandFromObject: !Array.isArray(arrayValue)");
  }

  return arrayValue.flatMap((elem, index) => {
    let elemPath = addPath(pathPrefix, index, undefined);
    let elemPathSuffix = pathSuffix;
    while (elemPathSuffix) {
      elemPath = addPath(elemPath, elemPathSuffix.key, elemPathSuffix.typename);
      elemPathSuffix = elemPathSuffix.prev;
    }

    return expandFromObject(elem, deferredPath.slice(indexPlaceholderPos + 1), elemPath);
  });
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

function getRootType(schema: GraphQLSchema, operation: OperationDefinitionNode): GraphQLObjectType | undefined | null {
  if (operation.operation === OperationTypeNode.SUBSCRIPTION) {
    return schema.getSubscriptionType();
  }

  if (operation.operation === OperationTypeNode.MUTATION) {
    return schema.getMutationType();
  }

  return schema.getQueryType();
}

function didParentError(path: Array<string | number>, errors: GraphQLError[]) {
  return errors.some((error) => {
    const errorPath = error.path;
    if (!errorPath) {
      return false;
    }

    if (path.length < errorPath.length) {
      return false;
    }

    return Array.prototype.every.call(errorPath, (key, i) => key === path[i]);
  });
}

export function createExecuteFn<TDeferred>(
  backend: ExecutorBackend<TDeferred>,
): <T = any>(args: ExecutionArgs) => Promise<ExecutionResult<T>> {
  return async function execute<T = any>(
    {
      schema,
      rootValue,
      contextValue,
      ...args
    }: ExecutionArgs,
  ): Promise<ExecutionResult<T>> {
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

      const completedFields: Array<{ path: Path; value: any; fieldNode: FieldNode; serialize: SerializeFunction }> = [];

      const step1_resolve: Array<FieldToResolve<TDeferred>> = buildUnresolvedFields(schema, ctx.fragments, undefined, rootType, rootValue, operation.selectionSet.selections);
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
            const path = addPath(prevPath, fieldKey, parentType.name);
            try {
              const fieldDef = getFieldDef(schema, parentType, fieldNode)!;
              const resolveField = overrideFieldResolver ?? fieldDef.resolve ?? args.fieldResolver ?? defaultFieldResolver;

              let sourceValue = await originalSourceValue;
              if (backend.isDeferredValue(sourceValue)) {
                sourceValue = backend.wrapSourceValue(
                  sourceValue,
                  // () => new Promise<unknown>((r) => pause(r)),
                  () => Promise.reject(nextStage),
                );
              }

              try {
                let fieldValue = resolveField(
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
                      parentType,
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

              // console.log('step1_resolve: send to step4_restage', pathToArray(prevPath), fieldValue);
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
              if (!backend.isDeferredValue(fieldValue)) {
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
              if (namedFieldType instanceof GraphQLEnumType || namedFieldType instanceof GraphQLScalarType) {
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
              }> = [];
              if (isAbstractType(namedFieldType)) {
                if (!backend.expandAbstractType) {
                  throw new Error("eager determination of union and interface types are not supported yet");
                }

                const expanded = backend
                  .expandAbstractType(schema, path, fieldValue, namedFieldType, setDeferredChild)
                  .map(({ concreteType, ...rest }) => ({
                    ...rest,
                    concreteType,
                    selectedFieldNodes: selectionFields(ctx.schema, ctx.fragments, fieldNode.selectionSet?.selections ?? [], concreteType),
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

                    const value = selectFromObject(deferredValue, thisDeferredPath)?.__typename;
                    const key = deferredPath[thisDeferredPath.length] as string;

                    // console.log();
                    return !typeFieldsMap[value]?.has(key);
                }


                concreteTypes.push(
                  ...expanded.map(({ concreteType, sourceValue, setDeferred, selectedFieldNodes }) => ({
                    concreteType,
                    selectedFieldNodes,
                    deferredValue: sourceValue as TDeferred,
                    setDeferredChild: setDeferred,
                    shouldExcludeResult: shouldExcludeResultNext,
                  })),
                )
              } else {
                concreteTypes.push({
                  concreteType: namedFieldType,
                  selectedFieldNodes: selectionFields(
                    ctx.schema,
                    ctx.fragments,
                    fieldNode.selectionSet?.selections ?? [],
                    namedFieldType,
                  ),
                  deferredValue: fieldValue,
                  setDeferredChild,
                });
              }


              // console.log('step2_discriminate: send to step1_resolve', pathToArray(path), deferredPath);
              step1_resolve.push(
                ...concreteTypes.flatMap(({ concreteType, selectedFieldNodes, setDeferredChild, deferredValue, shouldExcludeResult }) => backend.expandChildren(
                  path,
                  fieldType,
                  deferredValue,
                  selectedFieldNodes,
                  setDeferredChild,
                ).map((child) => ({
                  fieldNode: child.fieldNode,
                  fieldNodes: selectedFieldNodes,
                  prevPath: child.path,
                  parentType: concreteType,
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
    
              if (fieldType instanceof GraphQLScalarType || fieldType instanceof GraphQLEnumType) {
                completedFields.push({ path, value: fieldValue, fieldNode, serialize: fieldType.serialize.bind(fieldType) ?? identity });
                continue;
              }
    
              let concreteType: GraphQLObjectType;
              if (fieldType instanceof GraphQLInterfaceType || fieldType instanceof GraphQLUnionType) {
                const resolveType = fieldType.resolveType ?? args.typeResolver ?? defaultTypeResolver;
                const typeName = await resolveType(
                  fieldValue,
                  contextValue,
                  buildResolveInfo(
                    ctx,
                    {
                      fieldNode,
                      fieldNodes,
                      path,
                      parentType,
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
                  path,
                  concreteType,
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
          const deferredValues = await backend.resolveDeferredValues(deferredExprs, (contextValue as any)?.observer);
          // console.log(`resolved ${deferredExprs.length} deferred values`, deferredValues);

          while (step4_restage.length) {
            const { fieldNode, fieldNodes, parentType, prevPath, deferredPath, shouldExcludeResult } = step4_restage.shift()!;
            try {
              if (shouldExcludeResult?.(deferredPath, deferredValues)) {
                continue;
              }

              if (didParentError(pathToArray(prevPath), resultErrors)) {
                continue;
              }

              // console.log('step4_restage', pathToArray(prevPath), deferredPath, expandFromObject(deferredValues, deferredPath, prevPath));

              step1_resolve.push(
                ...expandFromObject(deferredValues, deferredPath, prevPath).flatMap(({ path, value }) => {
                  // console.log('step4_restage: send to step1_resolve', pathToArray(path), value);
                  return [{
                    fieldNode,
                    fieldNodes,
                    parentType,
                    prevPath: path,
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
              if (shouldExcludeResult?.(deferredPath, deferredValues)) {
                continue;
              }

              if (didParentError(pathToArray(fieldPath), resultErrors)) {
                continue;
              }

              // console.log('step5_revalidate:', pathToArray(fieldPath), deferredPath, deferredValues)

              step3_validate.push(
                ...expandFromObject(deferredValues, deferredPath, fieldPath).map(({ path, value }) => {
                  // console.log('step5_revalidate: send to step3_validate', pathToArray(path), value);
                  return ({
                    fieldNode,
                    fieldNodes,
                    fieldType,
                    fieldValue: value,
                    parentType,
                    path: path,
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
          parent[0][parent[1]] = await serialize(await value);
        } catch (e) {
          resultErrors.push(new GraphQLError((e as any)?.message ?? String(e), {
            nodes: (e as any).nodes ?? fieldNode,
            source: (e as any).source ?? fieldNode.loc?.source,
            positions: (e as any).positions ?? (fieldNode.loc?.source && [fieldNode.loc.start]),
            path: (e as any).path ?? pathToArray(path),
            originalError: (e as any).originalError ?? (e as any),
          }));
        }
      }

      const result: ExecutionResult<T> = { data: resultData as T };
      if (resultErrors.length) {
        result.errors = resultErrors;
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
