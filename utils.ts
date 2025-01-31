import {
  GraphQLError,
  type GraphQLInterfaceType,
  GraphQLObjectType,
  type GraphQLSchema,
} from "graphql";

const implementorsCache = new WeakMap<
  GraphQLInterfaceType,
  readonly GraphQLObjectType[]
>();

export function findImplementors(
  schema: GraphQLSchema,
  iface: GraphQLInterfaceType,
  cache = implementorsCache,
): readonly GraphQLObjectType[] {
  if (cache?.has(iface)) {
    return cache.get(iface)!;
  }

  const implementors: GraphQLObjectType[] = [];
  for (const type of Object.values(schema.getTypeMap())) {
    if (!(type instanceof GraphQLObjectType)) {
      continue;
    }

    if (type.getInterfaces().includes(iface)) {
      implementors.push(type);
    }
  }

  if (cache) {
    cache.set(iface, implementors);
  }

  return implementors;
}

export function selectFromObject(
  obj: any,
  path: Array<string | number>,
  getErrorMessage?: (value: any) => string | null,
): any {
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

export function partition<T, U>(
  arr: Array<T | U>,
  predicate: (item: T | U) => item is T,
): [T[], U[]];
export function partition<T>(
  arr: Array<T>,
  predicate: (item: T) => boolean,
): [T[], T[]];
export function partition(
  arr: Array<any>,
  predicate: (item: any) => boolean,
): [any[], any[]] {
  const a: any[] = [];
  const b: any[] = [];

  for (const item of arr) {
    if (predicate(item)) {
      a.push(item);
    } else {
      b.push(item);
    }
  }

  return [a, b];
}

export function isNullValue(value: unknown): value is null | undefined {
  return value === null || value === undefined;
}

export type Middleware<F extends (...args: any[]) => any> = {
  (next: F): F;
};

export function flattenMiddleware<
  F extends (this: void, ...args: any[]) => any,
>(middleware?: Middleware<F> | Middleware<F>[]): Middleware<F> {
  if (typeof middleware === "function") {
    return middleware;
  }

  if (middleware?.length === 1) {
    return middleware[0];
  }

  return Array.from(middleware ?? []).reduceRight(
    (stack, middleware) => (next) => middleware(stack(next)),
    (next) => next,
  );
}

export function zip<A, B>(a: A[], b: B[]): [A, B][];
export function zip<Arrs extends unknown[][]>(
  ...arrs: Arrs
): Arrs[number][number][];
export function zip<Arrs extends unknown[][]>(
  ...arrs: Arrs
): Arrs[number][number][] {
  const length = Math.min(...arrs.map((arr) => arr.length));
  return Array.from({ length }, (_, i) => arrs.map((arr) => arr[i])) as any;
}
