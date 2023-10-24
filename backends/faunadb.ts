import { Client, Expr, Let, Select, Var, Map, Lambda, type ClientConfig, If, Equals, Merge, errors, ContainsField, IsArray, IsObject, And, IsNull, ExprArg, QueryOptions } from "faunadb";
import {
  ExecutionArgs,
  FieldNode,
  GraphQLError,
  GraphQLInterfaceType,
  GraphQLObjectType,
  GraphQLOutputType,
  isListType,
  isNonNullType,
} from "graphql";
import { addPath, Path, pathToArray } from "graphql/jsutils/Path";

import type { ExecutorBackend, WrappedValue } from "../executor";
import { Middleware, findImplementors, flattenMiddleware } from "../utils";

function isExpr(e: any) {
  return e && (
    e instanceof Expr ||
    Object.prototype.hasOwnProperty.call(e, '_isFaunaExpr')
  )
}

const wrapped = Symbol("is wrapped");
const original = Symbol("get original");
export const isWrappedValue = (value: any): value is WrappedValue<any> => Boolean(value?.[wrapped]);

export async function unwrapValue(expr: any) {
  expr = (expr as any)?.[wrapped] ? (expr as any)[original] : expr;

  if (isExpr(expr)) {
    expr.raw = await unwrapValue(expr.raw);
  } else if (Array.isArray(expr)) {
    for (let [i, result] of (await Promise.all(expr.map(unwrapValue))).entries()) {
      expr[i] = result;
    }
  } else if (expr instanceof Object) {
    for (let [k, v] of (await Promise.all(Object.entries(expr).map(async ([k, v]) => [k, await unwrapValue(v)])))) {
      expr[k] = v;
    }
  }

  return expr;
}

function wrapChildObject(varName: string, dataContainer: any) {
  return If(
    IsNull(Var(varName)),
    null,
    If(
      And(
        IsObject(Var(varName)),
        ContainsField("@error", Var(varName)),
      ),
      Var(varName),
      dataContainer,
    ),
  );
}

export type QueryFunction = (client: Client, query: ExprArg, executionArgs: ExecutionArgs, options?: QueryOptions | undefined) => any;

export type QueryMiddleware = Middleware<QueryFunction>;

export interface CreateExecutorBackendOptions {
  queryMiddleware?: QueryMiddleware | QueryMiddleware[];
}

const realQuerySymbol = Symbol("real query");

function defaultQueryFunction(client: Client, query: ExprArg, executionArgs: ExecutionArgs, options: QueryOptions | undefined) {
  (executionArgs.contextValue as any)[realQuerySymbol] = query;
  return client.query(query, options);
}

export default function createExecutorBackend(
  input?: ClientConfig | Client,
  options: CreateExecutorBackendOptions = {},
): ExecutorBackend<Expr> {
  const client = input instanceof Client ? input : new Client(input);

  const runQuery = flattenMiddleware(options.queryMiddleware)(defaultQueryFunction);

  const wrapSourceValue = (
    sourceValue: unknown,
    getValue: () => Promise<unknown>,
  ) => {
    if (!(sourceValue instanceof Expr)) {
      return sourceValue;
    }

    return new Proxy(sourceValue, {
      get: (target: any, prop: PropertyKey): any => {
        if (prop === wrapped) {
          return true;
        }

        if (prop === original) {
          return sourceValue;
        }

        if (typeof prop === "symbol" || prop === "toJSON") {
          let v = Reflect.get(sourceValue, prop);
          if (typeof v === 'function') {
            v = v.bind(sourceValue);
          }

          return v;
        }

        if (prop === "then") {
          return (...args: Parameters<PromiseLike<unknown>["then"]>) =>
            getValue().then(...args);
        }

        return wrapSourceValue(
          Select(prop, sourceValue, null),
          async () => Reflect.get(await getValue() as any, prop),
        );
      },
      set: () => {
        throw new Error("cannot set properties on a execution placeholder");
      },
    });
  };

  return {
    resolveDeferredValues: async (input, executionArgs) => {
      const query = Array.from(input, ([expr]) => expr);
      const paths = Array.from(input, ([, path]) => pathToArray(path));
      try {
        return await runQuery(client, query, executionArgs);
      } catch (e) {
        if (!(e instanceof errors.FaunaHTTPError)) {
          throw e;
        }

        const rawQuery = JSON.parse(JSON.stringify((executionArgs.contextValue as any)[realQuerySymbol]));
        throw Array.from(e.requestResult.responseContent.errors, (responseErr) => {
          const pathPrefix = paths[responseErr.position.find((pos) => typeof pos === "number") as number];

          const objectPath: Array<string | number> = [];

          let faunapath = responseErr.position.slice(1);
          let objectpos;
          while ((objectpos = faunapath.indexOf("object")) !== -1) {
            faunapath = faunapath.slice(objectpos + 1);
            if (faunapath.length > 0) {
              objectPath.push(faunapath[0]);
            }
          }

          let errQuery = rawQuery;
          let cause: any = responseErr;
          if (responseErr.code === 'call error') {
              for (const pos of responseErr.position) {
                  errQuery = errQuery[pos];
              }

              responseErr.description = responseErr.description.replace('the function', JSON.stringify(errQuery.call))
              cause = responseErr.cause;
          } else {
            cause = { position: faunapath.slice(1) };
          }

          return new GraphQLError(responseErr.description + ': ' + JSON.stringify(cause), {
            path: [
              ...pathPrefix,
              ...objectPath,
            ],
          });
        });
      } finally {
        delete (executionArgs.contextValue as any)[realQuerySymbol]
      }
    },
    isDeferredValue: (value: unknown): value is Expr => {
      return value instanceof Expr;
    },
    wrapSourceValue,
    isWrappedValue,
    unwrapResolvedValue: unwrapValue,
    expandChildren: (
      path: Path,
      returnType: GraphQLOutputType,
      value: Expr,
      fieldNodes: readonly FieldNode[],
      setDeferred: (data: Expr) => void,
      suppressArrayHandling?: boolean,
    ) => {
      const varName = pathToArray(path).join("_");
      const dataContainer: Record<string, any> = {};

      if (isNonNullType(returnType)) {
        returnType = returnType.ofType;
      }

      let getDeferred: () => Expr;
      if (!suppressArrayHandling && isListType(returnType)) {
        getDeferred = () =>
          Map(
            value,
            Lambda(
              varName,
              wrapChildObject(varName, dataContainer),
            ),
          );
      } else {
        getDeferred = () =>
          Let(
            { [varName]: value },
            wrapChildObject(varName, dataContainer),
          );
      }

      if (isListType(returnType)) {
        returnType = returnType.ofType;
        path = addPath(path, "[]", undefined);
      }

      if (isNonNullType(returnType)) {
        returnType = returnType.ofType;
      }

      // if (fieldNodes.length === 1 && fieldNodes[0].name.value === "id") {
      //   // TODO: handle other types of IDs
      //   return [
      //     {
      //       fieldNode: fieldNodes[0],
      //       path: addPath(path, "id", "ID"),
      //       sourceValue:
      //     }
      //   ]
      // }

      return fieldNodes.map((fieldNode) => {
        const key = (fieldNode.alias ?? fieldNode.name).value;
        return {
          fieldNode,
          path,
          sourceValue: Var(varName),
          setData: (data) => {
            dataContainer[key] = data;
            setDeferred(getDeferred());
          },
        };
      });
    },
    expandAbstractType: (schema, path, deferredValue, abstractType, handleArray, setDeferred) => {
      let concreteTypes: readonly GraphQLObjectType[];
      if (abstractType instanceof GraphQLInterfaceType) {
        concreteTypes = findImplementors(schema, abstractType);
      } else {
        concreteTypes = abstractType.getTypes();
      }

      const varName = pathToArray(path).join("_");
      const varNameType = varName + ":typename";

      const branches: Record<string, Expr> = {};
      const getDeferred = () => {
        let expr: any = null;
        for (let [k, v] of Object.entries(branches)) {
          expr = If(
            Equals(Var(varNameType), k),
            v,
            expr,
          );
        }

        expr = wrapChildObject(
          varName,
          Let(
            {
              [varNameType]: Select("__typename", Var(varName), null)
            },
            Merge(
              expr,
              {
                __typename: Var(varNameType),
              }
            ),
          )
        );

        if (!handleArray) {
          return Let(
            {
              [varName]: deferredValue,
            },
            expr,
          );
        }

        return Let(
          {
            [varName + "_"]: deferredValue,
          },
          wrapChildObject(
            varName + "_",
            Map(
              Var(varName + "_"),
              Lambda(varName, expr),
            ),
          ),
        );
      };

      const sourceValue = Var(varName);
      return concreteTypes.map((concreteType) => ({
        concreteType,
        sourceValue,
        setDeferred: (expr) => {
          branches[concreteType.name] = expr;
          setDeferred(getDeferred());
        },
        suppressArrayHandling: true,
      }));
    },
    getErrorMessage(value) {
      return (value as any)?.["@error"] ?? null;
    },
  };
}
