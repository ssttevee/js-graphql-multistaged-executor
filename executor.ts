import {
  defaultFieldResolver,
  defaultTypeResolver,
  ExecutionArgs,
  ExecutionResult,
  FieldNode,
  FragmentDefinitionNode,
  getNamedType,
  GraphQLEnumType,
  GraphQLError,
  GraphQLFieldMap,
  GraphQLFieldResolver,
  GraphQLInterfaceType,
  GraphQLObjectType,
  GraphQLOutputType,
  GraphQLResolveInfo,
  GraphQLScalarType,
  GraphQLSchema,
  GraphQLUnionType,
  isListType,
  isNonNullType,
  OperationDefinitionNode,
  SelectionNode,
} from "graphql";
import { addPath, Path, pathToArray } from "graphql/jsutils/Path";
import { extractOperationAndFragments } from "./ast";
import { FragmentDefinitionMap, selectionFields } from "./selection";
import { resolveArguments } from "./arguments";

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

export interface ExecutorBackend<TDeferred> {
  unwrapResolvedValue: (value: WrappedValue<any>) => unknown;
  isWrappedValue: (value: unknown) => value is WrappedValue<any>;
  wrapSourceValue(
    sourceValue: unknown,
    getValue: () => Promise<unknown>,
  ): WrappedValue<any>;
  isDeferredValue(value: unknown): value is TDeferred;
  resolveDeferredValues(values: TDeferred[]): Promise<unknown[]>;
  expandChildren(
    path: Path,
    returnType: GraphQLOutputType,
    listValue: TDeferred,
    fieldNodes: readonly FieldNode[],
    setDeferred: (data: TDeferred) => void,
  ): Array<ExpandedChild>;
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

interface FieldToResolve<TDeferred> extends BaseFieldContext {
  prevPath: Path | undefined;
  sourceValue: any;
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
}

interface FieldToRevalidate extends BaseFieldContext {
  fieldType: GraphQLOutputType;
  fieldPath: Path;
  deferredPath: Array<string | number>;
}

function selectFromObject(obj: any, path: Array<string | number>): any {
  return path.reduce((value, key) => value?.[key], obj);
}

function expandFromObject(obj: any, deferredPath: Array<string | number>, path: Path | undefined): Array<{ path: Path; value: any }> {
  const arrayCountFromDeferred = deferredPath.filter((p) => p === '[]').length;
  const arrayCountFromPath = pathToArray(path).filter((p) => p === '[]').length;
  if (arrayCountFromPath !== arrayCountFromDeferred) {
    throw new Error('expandFromObject: arraysFromPath !== arraysFromDeferred');
  }

  if (!arrayCountFromDeferred) {
    if (!path) {
      throw new Error('expandFromObject: path is undefined');
    }

    return [{ path, value: selectFromObject(obj, deferredPath) }];
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

    const queryType = schema.getQueryType();
    if (!queryType) {
      throw new Error("missing Query type");
    }

    const [operation, fragmentNodes] = extractOperationAndFragments(
      args.document,
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

      const completedFields: Array<{ path: Path; value: any; serialize: SerializeFunction }> = [];

      let step1_resolve: Array<FieldToResolve<TDeferred>> = buildUnresolvedFields(schema, ctx.fragments, undefined, queryType, rootValue, operation.selectionSet.selections);
      let step2_discriminate: Array<FieldToDiscriminate<TDeferred>> = [];
      let step3_validate: Array<FieldToValidate> = [];
      let step4_restage: Array<FieldToRestage> = [];
      let step5_revalidate: Array<FieldToRevalidate> = [];

      let deferredExprs: TDeferred[] = [];

      while (step1_resolve.length || step2_discriminate.length || step3_validate.length || step4_restage.length || step5_revalidate.length) {
        while (step1_resolve.length || step2_discriminate.length || step3_validate.length) {
          while (step1_resolve.length) {
            const { fieldNode, fieldNodes, prevPath, parentType, sourceValue: originalSourceValue, deferral } = step1_resolve.shift()!;
            const fieldKey = fieldNodeKey(fieldNode);
            const path = addPath(prevPath, fieldKey, parentType.name);
            const fieldDef = parentType.getFields()[fieldNode.name.value];
            const resolveField = fieldDef.resolve ?? args.fieldResolver ?? defaultFieldResolver;

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
              step2_discriminate.push({ fieldNode, fieldNodes, fieldValue, fieldType: fieldDef.type, parentType, path, deferral });
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
            });
          }

          while (step2_discriminate.length) {
            const { fieldNode, fieldNodes, fieldValue, fieldType, parentType, path, deferral } = step2_discriminate.shift()!;

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
              deferredExprs.push(fieldValue);
              setDeferredChild = (expr) => {
                deferredExprs[index] = expr;
              };
              deferredPath = [index];
            }

            const concreteType = getNamedType(fieldType);
            if (concreteType instanceof GraphQLUnionType || concreteType instanceof GraphQLInterfaceType) {
              throw new Error("union and interface types are not supported yet");
            }

            if (concreteType instanceof GraphQLEnumType || concreteType instanceof GraphQLScalarType) {
              // console.log('step2_discriminate: send to step5_revalidate', pathToArray(path), deferredPath);
              step5_revalidate.push({
                fieldType,
                fieldNode,
                fieldNodes,
                fieldPath: path,
                parentType,
                deferredPath: deferredPath,
              });
              continue;
            }

            const childFieldNodes = selectionFields(
              ctx.schema,
              ctx.fragments,
              fieldNode.selectionSet?.selections ?? [],
              concreteType,
            );

            // console.log('step2_discriminate: send to step1_resolve', pathToArray(path), deferredPath);
            step1_resolve.push(
              ...backend.expandChildren(
                path,
                fieldType,
                fieldValue,
                childFieldNodes,
                setDeferredChild,
              ).map((child) => ({
                fieldNode: child.fieldNode,
                fieldNodes: childFieldNodes,
                prevPath: child.path,
                parentType: concreteType,
                sourceValue: child.sourceValue,
                deferral: {
                  set: child.setData,
                  path: deferredPath.concat(arrayNewElems(pathToArray(path), pathToArray(child.path))),
                },
              }))
            );
          }

          while (step3_validate.length) {
            const { fieldType: originalFieldType, fieldNode, fieldNodes, fieldValue, parentType, path } = step3_validate.shift()!;
  
            // console.log('step3_validate', pathToArray(path), fieldValue, fieldValue, parentType.toString(), originalFieldType.toString());
  
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
              completedFields.push({ path, value: fieldValue, serialize: fieldType.serialize.bind(fieldType) ?? identity });
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
          }
        }

        if (deferredExprs.length) {
          const deferredValues = await backend.resolveDeferredValues(deferredExprs);
          // console.log(`resolved ${deferredExprs.length} deferred values`, deferredValues);

          while (step4_restage.length) {
            const { fieldNode, fieldNodes, parentType, prevPath, deferredPath } = step4_restage.shift()!;
            // console.log('step4_restage', pathToArray(prevPath), deferredPath, expandFromObject(deferredValues, deferredPath, prevPath));

            step1_resolve.push(
              ...expandFromObject(deferredValues, deferredPath, prevPath).map(({ path, value }) => {
                // console.log('step4_restage: send to step1_resolve', pathToArray(path), value);
                return {
                  fieldNode,
                  fieldNodes,
                  parentType,
                  prevPath: path,
                  sourceValue: value,
                };
              }),
            );
          }

          while (step5_revalidate.length) {
            const { fieldNode, fieldNodes, fieldType, fieldPath, deferredPath, parentType } = step5_revalidate.shift()!;
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
          }
        }

        if (step4_restage.length) {
          throw new Error('Expected no restage');
        }

        if (step5_revalidate.length) {
          throw new Error('Expected no backfill');
        }

        deferredExprs = [];
      }

      const resultData: Record<string, any> = {};
      for (const { path, value, serialize } of completedFields) {
        let parent: [any, string | number] | null = null;
        let container: Record<string, any> | Array<any> = resultData;
        for (const key of pathToArray(path)) {
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

        parent[0][parent[1]] = await serialize(await value);
      }

      const result: ExecutionResult<T> = { data: resultData as T };
      if (resultErrors.length) {
        result.errors = resultErrors;
      }

      return result;
    } catch (err) {
      return {
        errors: [
          new GraphQLError((err as any).message, {
            originalError: (err as any).originalError ?? err,
          }),
        ],
      };
    }
  };
}
