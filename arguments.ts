import {
  ArgumentNode,
  GraphQLArgument,
  GraphQLError,
  GraphQLInputType,
  isEnumType,
  isListType,
  isNonNullType,
  isScalarType,
  Kind,
  ValueNode,
} from "graphql";

function isNullValue(value: any): boolean {
  return value === null || value === undefined;
}

function parseVariableValue(value: unknown, type: GraphQLInputType): unknown {
  if (isNonNullType(type)) {
    if (isNullValue(value)) {
      throw new GraphQLError("found null value for non-null input type");
    }

    type = type.ofType;
  } else if (isNullValue(value)) {
    return null;
  }

  if (isListType(type)) {
    if (!Array.isArray(value)) {
      throw new GraphQLError("found non-array value for list input type");
    }

    const itemType = type.ofType;
    return value.map((item) => parseVariableValue(item, itemType));
  }

  if (isScalarType(type) || isEnumType(type)) {
    return type.parseValue(value);
  }

  if (typeof value !== "object" || value === null) {
    throw new GraphQLError("found non-object value for object input type");
  }

  return Object.fromEntries(
    Object.values(type.getFields()).map(
      (field) => [field.name, parseVariableValue((value as Record<string, unknown>)[field.name], field.type)],
    ),
  );
}

function resolveArgument(
  valueNode: ValueNode,
  type: GraphQLInputType,
  variables: Record<string, unknown> | undefined,
): unknown {
  try {
    if (isNonNullType(type)) {
      if (!valueNode || valueNode.kind === Kind.NULL) {
        throw new GraphQLError("found null value for non-null input type");
      }

      type = type.ofType;
    } else if (!valueNode || valueNode.kind === Kind.NULL) {
      return null;
    }

    if (valueNode.kind === Kind.VARIABLE) {
      const variableName = valueNode.name.value;
      if (isNonNullType(type) && (!variables || !(variableName in variables))) {
        throw new GraphQLError("found undefined variable: " + variableName);
      }

      return parseVariableValue(variables?.[variableName], type);
    }

    if (isListType(type)) {
      if (valueNode.kind !== Kind.LIST) {
        throw new GraphQLError("found non-list value for list input type");
      }

      const itemType = type.ofType;
      return valueNode.values.map((item) => resolveArgument(item, itemType, variables));
    }

    if (isScalarType(type) || isEnumType(type)) {
      return type.parseLiteral(valueNode, variables);
    }

    if (valueNode.kind !== Kind.OBJECT) {
      throw new GraphQLError("found non-object value for object input type");
    }
  } catch (e) {
    throw new GraphQLError((e as any)?.message ?? String(e), {
      nodes: [valueNode],
      positions: [valueNode.loc?.start ?? -1],
      source: valueNode.loc?.source,
      originalError: e as any,
    });
  }

  const valueFieldNodes = Object.fromEntries(
    valueNode.fields.map((field) => [field.name.value, field.value]),
  );

  return Object.fromEntries(
    Object.values(type.getFields()).map(
      (field) => [field.name, resolveArgument(valueFieldNodes[field.name], field.type, variables)],
    ),
  );
}

export function resolveArguments(
  variables: Record<string, any> = {},
  nodes: readonly ArgumentNode[] | undefined,
  args: readonly GraphQLArgument[],
): Record<string, any> {
  if (!nodes) {
    return {};
  }

  const valueNodeMap = Object.fromEntries(nodes.map((node) => [node.name.value, node.value]));
  return Object.fromEntries(
    args.map((arg) => {
      let type = arg.type;
      let nullable = true;
      if (isNonNullType(type)) {
        type = type.ofType;
        nullable = false;
      }

      const valueNode = valueNodeMap[arg.name];
      if (!valueNode) {
        if (!nullable) {
          throw new Error(`missing required argument: ${arg.name}`);
        }

        return [arg.name, arg.defaultValue];
      }

      const value = resolveArgument(valueNode, type, variables) ?? null;
      if (value === null && !nullable) {
        throw new Error(`missing required argument: ${arg.name}`);
      }

      return [arg.name, value];
    }),
  );
}
