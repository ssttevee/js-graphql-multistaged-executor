import { Client, Expr, Let, Select, Var, Map, Lambda, type ClientConfig } from "faunadb";
import {
  FieldNode,
  GraphQLOutputType,
  isListType,
  isNonNullType,
} from "graphql";
import { addPath, Path, pathToArray } from "graphql/jsutils/Path";

import type { ExecutorBackend, WrappedValue } from "../executor";

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
    resolveDeferredValues: (query: Expr[]) => {
      return client.query(query);
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
  };
}
