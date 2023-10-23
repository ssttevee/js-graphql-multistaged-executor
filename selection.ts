import {
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

  if (unionMap[condition.name.value]?.getTypes().some((union) => union.name === type.name)) {
    return true;
  }

  return type.getInterfaces().some(
    (iface) => satisfiesTypeCondition(unionMap, iface, condition),
  );
}

export function selectionFields(
  schema: GraphQLSchema,
  fragmentMap: FragmentDefinitionMap,
  unionMap: Record<string, GraphQLUnionType>,
  selections: ReadonlyArray<SelectionNode>,
  type: GraphQLObjectType,
): FieldNode[] {
  return selections.flatMap((node): FieldNode[] => {
    if (isField(node)) {
      return [node];
    }

    if (isInlineFragment(node)) {
      if (
        !node.typeCondition || !satisfiesTypeCondition(unionMap, type, node.typeCondition)
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
        throw new Error("missing fragment definition: " + node.name.value);
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
  });
}
