import { Client, Expr, Let, Select, Var, Map, Lambda, type ClientConfig } from "faunadb";
import {
  FieldNode,
  GraphQLOutputType,
  isListType,
  isNonNullType,
} from "graphql";
import { addPath, Path, pathToArray } from "graphql/jsutils/Path";

import type { ExecutorBackend } from "../executor";
import { WrappedValue } from "../executor";

const wrapped = Symbol("is wrapped");
const original = Symbol("get original");

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
          return target;
        }

        if (typeof prop === "symbol") {
          return Reflect.get(target, prop);
        }

        if (prop === "then") {
          return (...args: Parameters<PromiseLike<unknown>["then"]>) =>
            getValue().then(...args);
        }

        return wrapSourceValue(
          Select(prop, target, null),
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
    isWrappedValue: (value: any): value is WrappedValue<any> => Boolean(value?.[wrapped]),
    unwrapResolvedValue: (value: any) => (value?.[wrapped] && value[original]) || value,
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
