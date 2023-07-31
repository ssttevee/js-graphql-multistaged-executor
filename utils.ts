import { GraphQLInterfaceType, GraphQLObjectType, GraphQLSchema } from "graphql";

const implementorsCache = new WeakMap<GraphQLInterfaceType, readonly GraphQLObjectType[]>();

export function findImplementors(schema: GraphQLSchema, iface: GraphQLInterfaceType, cache = implementorsCache): readonly GraphQLObjectType[] {
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