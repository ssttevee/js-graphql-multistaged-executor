import type {
  FieldNode,
  FragmentDefinitionNode,
  FragmentSpreadNode,
  GraphQLInterfaceType,
  GraphQLObjectType,
  GraphQLSchema,
  GraphQLUnionType,
  InlineFragmentNode,
  NamedTypeNode,
  SelectionNode,
} from "graphql";

export interface FragmentDefinitionMap {
  [name: string]: FragmentDefinitionNode;
}

function isField(node: SelectionNode): node is FieldNode {
  return node.kind === "Field";
}

function isInlineFragment(node: SelectionNode): node is InlineFragmentNode {
  return node.kind === "InlineFragment";
}

function isFragmentSpread(node: SelectionNode): node is FragmentSpreadNode {
  return node.kind === "FragmentSpread";
}

function satisfiesTypeCondition(
  unionMap: Record<string, GraphQLUnionType>,
  type: GraphQLObjectType | GraphQLInterfaceType,
  condition: NamedTypeNode,
): boolean {
  if (condition.name.value === type.name) {
    return true;
  }

  if (
    unionMap[condition.name.value]
      ?.getTypes()
      .some((union) => union.name === type.name)
  ) {
    return true;
  }

  return type
    .getInterfaces()
    .some((iface) => satisfiesTypeCondition(unionMap, iface, condition));
}

/**
 * Deduplicate selection set by merging fields with the same name.
 */
function dedupeSelection(fieldNodes: FieldNode[]): FieldNode[] {
  const fieldsByKey: Record<string, FieldNode> = {};
  for (const node of fieldNodes) {
    const key = node.alias?.value ?? node.name.value;
    if (!fieldsByKey[key]) {
      fieldsByKey[key] = node;
      continue;
    }

    // got duplicate
    const existing = fieldsByKey[key];

    // verify that the fields are identical
    const existingArgs = JSON.stringify(existing.arguments, (key, value) =>
      key === "loc" ? undefined : value,
    );
    const nodeArgs = JSON.stringify(node.arguments, (key, value) =>
      key === "loc" ? undefined : value,
    );
    if (existingArgs !== nodeArgs) {
      throw new Error(
        `duplicate field with different arguments: ${key} (${existingArgs} != ${nodeArgs})`,
      );
    }

    const existingDirectives = JSON.stringify(
      existing.directives,
      (key, value) => (key === "loc" ? undefined : value),
    );
    const nodeDirectives = JSON.stringify(node.directives, (key, value) =>
      key === "loc" ? undefined : value,
    );
    if (existingDirectives !== nodeDirectives) {
      throw new Error(
        `duplicate field with different directives: ${key} (${existingDirectives} != ${nodeDirectives})`,
      );
    }

    // merge selection set
    if (existing.selectionSet && node.selectionSet) {
      fieldsByKey[key] = {
        ...existing,
        selectionSet: {
          ...existing.selectionSet,
          selections: [
            ...existing.selectionSet.selections,
            ...node.selectionSet.selections,
          ],
        },
      };
    }
  }

  return Object.values(fieldsByKey);
}

export function selectionFields(
  schema: GraphQLSchema,
  fragmentMap: FragmentDefinitionMap,
  unionMap: Record<string, GraphQLUnionType>,
  selections: ReadonlyArray<SelectionNode>,
  type: GraphQLObjectType,
): FieldNode[] {
  return dedupeSelection(
    selections.flatMap((node): FieldNode[] => {
      if (isField(node)) {
        return [node];
      }

      if (isInlineFragment(node)) {
        if (
          !node.typeCondition ||
          !satisfiesTypeCondition(unionMap, type, node.typeCondition)
        ) {
          return [];
        }

        return selectionFields(
          schema,
          fragmentMap,
          unionMap,
          node.selectionSet.selections,
          type,
        );
      }

      if (isFragmentSpread(node)) {
        const fragment = fragmentMap[node.name.value];
        if (!fragment) {
          throw new Error(`missing fragment definition: ${node.name.value}`);
        }

        if (
          !fragment.typeCondition ||
          !satisfiesTypeCondition(unionMap, type, fragment.typeCondition)
        ) {
          return [];
        }

        return selectionFields(
          schema,
          fragmentMap,
          unionMap,
          fragment.selectionSet.selections,
          type,
        );
      }

      return [];
    }),
  );
}
