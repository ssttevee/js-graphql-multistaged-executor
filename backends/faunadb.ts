import { Client, Expr, Let, Select, Var, Map, Lambda, type ClientConfig, If, Equals, Merge, errors } from "faunadb";
import {
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
import { findImplementors } from "../utils";

function isExpr(e: any) {
  return e && (
    e instanceof Expr ||
    Object.prototype.hasOwnProperty.call(e, '_isFaunaExpr')
  )
}

const wrapped = Symbol("is wrapped");
const original = Symbol("get original");
const isWrappedValue = (value: any): value is WrappedValue<any> => Boolean(value?.[wrapped]);

async function unwrapResolvedValue(expr: any) {
  expr = (expr as any)?.[wrapped] ? (expr as any)[original] : expr;

  if (isExpr(expr)) {
    expr.raw = await unwrapResolvedValue(expr.raw);
  } else if (Array.isArray(expr)) {
    for (let [i, result] of (await Promise.all(expr.map(unwrapResolvedValue))).entries()) {
      expr[i] = result;
    };
  } else if (expr instanceof Object) {
    for (let [k, v] of (await Promise.all(Object.entries(expr).map(async ([k, v]) => [k, await unwrapResolvedValue(v)])))) {
      expr[k] = v;
    }
  }

  return expr;
}

export default function createExecutorBackend(
  opts?: ClientConfig,
): ExecutorBackend<Expr> {
  const client = new Client(opts);

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
    resolveDeferredValues: async (input) => {
      const query = Array.from(input, ([expr]) => expr);
      const paths = Array.from(input, ([, path]) => path);
      try {
        return await client.query(query);
      } catch (e) {
        if (!(e instanceof errors.FaunaHTTPError)) {
          throw e;
        }

        throw Array.from(e.requestResult.responseContent.errors, (responseErr) => {
          const objectPath: Array<string | number> = [];

          let faunapath = responseErr.position.slice(1);
          let objectpos;
          while ((objectpos = faunapath.indexOf("object")) !== -1) {
            faunapath = faunapath.slice(objectpos + 1);
            if (faunapath.length > 0) {
              objectPath.push(faunapath[0]);
            }
          }

          return new GraphQLError(responseErr.description + ': ' + JSON.stringify(responseErr), {
            path: [
              ...pathToArray(paths[responseErr.position[0] as number]),
              ...objectPath,
            ],
          });
        });
      }
    },
    isDeferredValue: (value: unknown): value is Expr => {
      return value instanceof Expr;
    },
    wrapSourceValue,
    isWrappedValue,
    unwrapResolvedValue,
    expandChildren: (
      path: Path,
      returnType: GraphQLOutputType,
      value: Expr,
      fieldNodes: readonly FieldNode[],
      setDeferred: (data: Expr) => void,
    ) => {
      const varName = pathToArray(path).join("_");
      const dataContainer: Record<string, any> = {};

      if (isNonNullType(returnType)) {
        returnType = returnType.ofType;
      }

      let getDeferred: () => Expr;
      if (isListType(returnType)) {
        returnType = returnType.ofType;
        path = addPath(path, "[]", undefined);
        getDeferred = () =>
          Map(
            value,
            Lambda(
              varName,
              dataContainer,
            ),
          );
      } else {
        getDeferred = () =>
          Let(
            { [varName]: value },
            dataContainer,
          );
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
    expandAbstractType: (schema, path, deferredValue, abstractType, setDeferred) => {
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

        return Let(
          {
            [varName]: deferredValue,
          },
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
      }));
    },
  };
}
