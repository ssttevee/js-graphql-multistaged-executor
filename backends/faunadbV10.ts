import {
  Client,
  ClientConfiguration,
  fql,
  QueryInterpolation,
  QueryOptions,
  QueryValue,
  ServiceError,
  type Query as QueryCtor,
} from "fauna";
import {
  ExecutionArgs,
  FieldNode,
  GraphQLAbstractType,
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

// HACK: change this either when there an `isQuery` helper or when `Query` is exported
type Query = import("fauna").Query;
const Query: typeof QueryCtor = fql`null`.constructor as any;

type QueryInput = Query | QueryValue;

function isExpr(e: any): e is Query {
  return e && e instanceof Query;
}

type V4Expr = import("faunadb").ExprVal;

function isV4Expr(e: any): e is V4Expr {
  if (e?.[wrapped]) {
    return false;
  }

  if (e?._isFaunaExpr) {
    return true;
  }

  let prototype = e?.prototype ?? e?.constructor?.prototype;
  while (prototype) {
    if (prototype._isFaunaExpr) {
      return true;
    }

    prototype = prototype.prototype;
  }

  return false;
}

function isV4Value(e: any): boolean {
  if (e?.[wrapped]) {
    return false;
  }

  if (e?._isFaunaValue) {
    return true;
  }

  let prototype = e?.prototype ?? e?.constructor?.prototype;
  while (prototype) {
    if (prototype._isFaunaValue) {
      return true;
    }

    prototype = prototype.prototype;
  }

  return false;
}

function customFQL(
  fragments: string[],
  ...queryArgs: (QueryValue | Query | import("faunadb").ExprVal)[]
): Query {
  return fql(fragments, ...queryArgs.map(unwrapValue));
}

export { customFQL as fql };

const wrapped = Symbol("is wrapped");
const original = Symbol("get original");
export const isWrappedValue = (value: any): value is WrappedValue<any> => {
  // HACK: overload the concept of a "wrapped value" to include fauna v4 expressions
  return Boolean(value?.[wrapped] || isV4Expr(value));
};

function queryToString(query: string | QueryInterpolation | Query): string {
  if (isExpr(query)) {
    return query.encode().fql.map(queryToString).join(",");
  }

  if (typeof query === "string") {
    return query;
  }

  if (typeof query !== "object") {
    throw new Error("unexpected query type: " + typeof query);
  }

  if ("value" in query) {
    return "${" + JSON.stringify(query.value) + "}";
  }

  if (!("fql" in query)) {
    throw new Error("unexpected object shape: " + JSON.stringify(query));
  }

  if (Array.isArray(query.fql)) {
    return query.fql.map(queryToString).join("");
  }

  return queryToString(query.fql);
}

function normalizeV4Object(obj: Record<string, any>): Record<string, any> {
  return Object.fromEntries(
    Object.entries(obj).map(([k, v]) => {
      return [k, normalizeV4Query(v)];
    }),
  );
}

function normalizeV4Query(query: any): any {
  if (Array.isArray(query)) {
    return query.map(normalizeV4Query)
  }

  if (query?.[wrapped]) {
    return unwrapValue(query);
  }

  if (isV4Value(query)) {
    return JSON.parse(JSON.stringify(query));
  }

  if (!isV4Expr(query)) {
    return query;
  }

  const raw: any = (query as any).raw;
  if (Array.isArray(raw)) {
    return raw.map(normalizeV4Query);
  }

  if (raw.object) {
    return { object: normalizeV4Object(raw.object) };
  }

  if (raw.let) {
    if (Array.isArray(raw.let)) {
      return {
        let: raw.let.map(normalizeV4Object),
        in: normalizeV4Query(raw.in),
      };
    }
  }

  return normalizeV4Object(raw);
}

export function v4ToV10(query: V4Expr): Query {
  const normalized = normalizeV4Query(query);
  return fql`FQL.evalV4(${normalized})`;
}

export function unwrapValue(expr: any) {
  if (isV4Expr(expr)) {
    // HACK: overload the concept of a "wrapped value" to include fauna v4 expressions
    return v4ToV10(expr);
  }

  expr = (expr as any)?.[wrapped] ? (expr as any)[original] : expr;

  if (Array.isArray(expr)) {
    for (let [i, result] of expr.map(unwrapValue).entries()) {
      expr[i] = result;
    }
  } else if (expr instanceof Object && !isExpr(expr)) {
    for (const [k, v] of Object.entries(expr).map(([k, v]) => [
      k,
      unwrapValue(v),
    ])) {
      expr[k] = v;
    }
  }

  return expr;
}

function varIsErrorExpr(varName: string): Query {
  return fql`(${fql([varName])} isa Object&&${fql([varName])}["@error"]!=null)`;
}

function varIsIterableExpr(varName: string): Query {
  return fql`(${fql([varName])} isa Set||${fql([varName])} isa Array)`;
}

function assertIterable(varName: string, expr: Query): Query {
  return fql`if (!${varIsIterableExpr(varName)}) {"@error":"expected Set or Array"}else{${expr}}`;
}

function safeMap(varName: string, fnExpr: Query): Query {
  return assertIterable(varName, fql`${fql([varName])}.map(${fnExpr})`);
}

function chainVarErrorOrNull(varName: string, expr: QueryInput): Query {
  return fql`if(${fql([varName])}==null)null else if(${varIsErrorExpr(varName)})${fql([varName])} else{${expr}}`;
}

function dataContainerAsQuery(dataContainer: Record<string, any>): Query {
  const q = fql(
    [
      "{",
      ...new Array(Object.keys(dataContainer).length)
        .fill([":{", "},"])
        .flat()
        .slice(0, -1),
      "}}",
    ],
    ...Object.entries(dataContainer).flatMap(([k, v]) => [
      fql([JSON.stringify(k)]),
      v,
    ]),
  );

  return q;
}

export type QueryFunction = (
  client: Client,
  query: Query,
  executionArgs: ExecutionArgs,
  options?: QueryOptions | undefined,
) => any;
export type TypeResolver = (
  abstractType: GraphQLAbstractType,
  value: QueryInput,
  executionArgs: ExecutionArgs,
) => QueryInput;

export type QueryMiddleware = Middleware<QueryFunction>;
export type TypeResolverMiddleware = Middleware<TypeResolver>;

export interface CreateExecutorBackendOptions {
  queryMiddleware?: QueryMiddleware | QueryMiddleware[];
  typeResolverMiddleware?: TypeResolverMiddleware | TypeResolverMiddleware[];
}

function defaultQueryFunction(
  client: Client,
  query: Query,
  executionArgs: ExecutionArgs,
  options: QueryOptions | undefined,
) {
  return client.query(query, options);
}

function defaultTypeResolver(
  abstractType: GraphQLAbstractType,
  value: QueryInput,
) {
  return fql`(${value})?.__typename??{"@error":${"failed to resolve type for " + abstractType.name}}`;
}

export default function createExecutorBackend(
  input?: ClientConfiguration | Client,
  options: CreateExecutorBackendOptions = {},
): ExecutorBackend<Query> {
  const client = input instanceof Client ? input : new Client(input);

  const runQuery = flattenMiddleware(options.queryMiddleware)(
    defaultQueryFunction,
  );
  const resolveType = flattenMiddleware(options.typeResolverMiddleware)(
    defaultTypeResolver,
  );

  const wrapSourceValue = (
    sourceValue: unknown,
    getValue: () => Promise<unknown>,
  ) => {
    if (!(sourceValue instanceof Query)) {
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

        if (typeof prop === "symbol" || prop === "toQuery") {
          let v = Reflect.get(sourceValue, prop);
          if (typeof v === "function") {
            v = v.bind(sourceValue);
          }

          return v;
        }

        if (prop === "then") {
          return (...args: Parameters<PromiseLike<unknown>["then"]>) =>
            getValue().then(...args);
        }

        if (prop === "encode") {
          return () => sourceValue.encode();
        }

        return wrapSourceValue(
          fql`(${sourceValue})?.[${fql([JSON.stringify(prop.toString())])}]`,
          async () => Reflect.get((await getValue()) as any, prop),
        );
      },
      set: () => {
        throw new Error("cannot set properties on a execution placeholder");
      },
    });
  };

  return {
    resolveDeferredValues: async (input, executionArgs) => {
      const queries = Array.from(input, ([expr]) => expr);
      // const paths = Array.from(input, ([, path]) => pathToArray(path));
      try {
        const combinedQuery = fql(
          ["[", ...new Array(queries.length - 1).fill(","), "]"],
          ...queries.map((q) => fql`{${q}}`),
        );
        return (await runQuery(client, combinedQuery, executionArgs)).data;
      } catch (e) {
        if (!(e instanceof ServiceError)) {
          throw e;
        }

        console.log(e.queryInfo, e.constraint_failures);

        throw new GraphQLError(e.code, { originalError: e });
      }
    },
    isDeferredValue: (value: unknown): value is Query => {
      return isExpr(value);
    },
    wrapSourceValue,
    isWrappedValue,
    unwrapResolvedValue: unwrapValue,
    expandChildren: (
      path: Path,
      returnType: GraphQLOutputType,
      value: Query,
      fieldNodes: readonly FieldNode[],
      setDeferred: (data: Query) => void,
      suppressArrayHandling?: boolean,
    ) => {
      const varName = pathToArray(path).join("_");
      const dataContainer: Record<string, any> = {};

      if (isNonNullType(returnType)) {
        returnType = returnType.ofType;
      }

      let getDeferred: () => Query;
      if (!suppressArrayHandling && isListType(returnType)) {
        getDeferred = () =>
          fql`let ${fql([varName + "_"])}:Any=${fql(["", ""], value)};${safeMap(varName + "_", fql`(${fql([varName])})=>{${chainVarErrorOrNull(varName, dataContainerAsQuery(dataContainer))}}`)}`;
      } else {
        getDeferred = () =>
          fql`let ${fql([varName])}:Any=${fql(["", ""], value)};${chainVarErrorOrNull(varName, dataContainerAsQuery(dataContainer))}`;
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
          sourceValue: fql([varName]),
          setData: (data) => {
            dataContainer[key] = unwrapValue(data);
            setDeferred(getDeferred());
          },
        };
      });
    },
    expandAbstractType: (
      schema,
      path,
      deferredValue,
      abstractType,
      handleArray,
      setDeferred,
      executionArgs,
    ) => {
      let concreteTypes: readonly GraphQLObjectType[];
      if (abstractType instanceof GraphQLInterfaceType) {
        concreteTypes = findImplementors(schema, abstractType);
      } else {
        concreteTypes = abstractType.getTypes();
      }

      const varName = "__" + pathToArray(path).join("_");
      const varNameType = varName + "__typename";

      const branches: Record<string, Query> = {};
      const getDeferred = () => {
        let expr: QueryInput = null;
        if (Object.entries(branches).length) {
          expr = fql(
            [
              "if(" + varNameType + "==",
              "){",
              ...new Array(Object.keys(branches).length - 1)
                .fill(["}else if(" + varNameType + "==", "){"])
                .flat(),
              "}else null",
            ],
            ...Object.entries(branches).flat(),
          );
        }

        expr = chainVarErrorOrNull(
          varName,
          fql`let ${fql([varNameType])}={${resolveType(abstractType, fql([varName]), executionArgs)}};if(${varIsErrorExpr(varNameType)})${fql([varNameType])} else {let ${fql([varName + "_result"])}:Any={${expr}};${chainVarErrorOrNull(varName + "_result", fql`Object.assign(${fql([varName + "_result"])}, {__typename:${fql([varNameType])}})`)}}`,
        );

        if (!handleArray) {
          return fql`let ${fql([varName])}={${deferredValue}};${expr}`;
        }

        return fql`let ${fql([varName + "_"])}:Any={${deferredValue}};${safeMap(varName + "_", fql`(${fql([varName])})=>{${expr}}`)}`;
      };

      const sourceValue = fql([varName]);
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
