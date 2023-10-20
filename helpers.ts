import { GraphQLObjectType, GraphQLSchema, OperationDefinitionNode, OperationTypeNode } from 'graphql';

export function getRootType(schema: GraphQLSchema, operation: OperationDefinitionNode): GraphQLObjectType | undefined | null {
  if (operation.operation === OperationTypeNode.SUBSCRIPTION) {
    return schema.getSubscriptionType();
  }

  if (operation.operation === OperationTypeNode.MUTATION) {
    return schema.getMutationType();
  }

  return schema.getQueryType();
}

export * from './arguments';
