import {
  DocumentNode,
  FragmentDefinitionNode,
  GraphQLError,
  Kind,
  OperationDefinitionNode,
} from "graphql";

function groupBy<T, K extends string>(
  array: readonly T[],
  selector: (el: T) => K,
): Partial<Record<K, T[]>> {
  const result: Partial<Record<K, T[]>> = {};
  for (const el of array) {
    const key = selector(el);
    if (!(key in result)) {
      result[key] = [];
    }

    result[key]!.push(el);
  }

  return result;
}

export function extractOperationAndFragments(
  document: DocumentNode,
  name?: string,
): [OperationDefinitionNode, FragmentDefinitionNode[]] {
  const { OperationDefinition, FragmentDefinition } = groupBy(
    document.definitions,
    (def) => def.kind,
  ) as {
    [Kind.OPERATION_DEFINITION]?: OperationDefinitionNode[];
    [Kind.FRAGMENT_DEFINITION]?: FragmentDefinitionNode[];
  };

  if (name) {
    const operation = OperationDefinition?.find((def) =>
      def.name?.value === name
    );
    if (!operation) {
      throw new GraphQLError("missing operation " + name);
    }

    return [operation, FragmentDefinition || []];
  }

  if (!OperationDefinition?.length) {
    throw new GraphQLError("missing operation");
  }

  if (OperationDefinition?.length !== 1) {
    throw new GraphQLError("only one unnamed operation allowed");
  }

  return [OperationDefinition[0], FragmentDefinition || []];
}
