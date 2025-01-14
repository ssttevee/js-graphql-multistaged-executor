import { expect, test } from "@jest/globals";
import { Concat, Map, Lambda, Var, query as q } from "faunadb";
import {
  GraphQLEnumType,
  GraphQLInterfaceType,
  GraphQLList,
  GraphQLNonNull,
  GraphQLObjectType,
  GraphQLScalarType,
  GraphQLSchema,
  GraphQLString,
  parse,
} from "graphql";

import { createExecuteFn } from "../executor";
import createExecutorBackend from "./faunadbV10";

const JSONStringType = new GraphQLScalarType({
  name: "JSONString",
  serialize: (value) => {
    return JSON.stringify(value);
  },
  parseValue: (value) =>
    typeof value === "string" ? JSON.parse(value) : value,
});

const BooleanEnumType = new GraphQLEnumType({
  name: "BooleanEnum",
  values: {
    TRUE: { value: true },
    FALSE: { value: false },
  },
});

const StringInterfaceType = new GraphQLInterfaceType({
  name: "StringInterface",
  fields: () => ({
    string: {
      type: GraphQLString,
    },
  }),
});

const SimpleStringType = new GraphQLObjectType({
  name: "SimpleString",
  interfaces: [StringInterfaceType],
  fields: () => ({
    string: {
      type: GraphQLString,
      resolve: (src) => src.string,
    },
  }),
});

const WrappedStringType = new GraphQLObjectType({
  name: "WrappedString",
  fields: () => ({
    string: {
      type: GraphQLString,
      resolve: (src) => src,
    },
    deferred: {
      type: GraphQLString,
      resolve: (src) => src,
    },
    awaited: {
      type: GraphQLString,
      resolve: async (src) => await src,
    },
    restaged: {
      type: GraphQLString,
      resolve: async (src) => Concat([await src, ""], ""),
    },
    recurseDeferred: {
      type: WrappedStringType,
      resolve: (src) => src,
    },
    recurseAwaited: {
      type: WrappedStringType,
      resolve: async (src) => await src,
    },
    recurseRestaged: {
      type: WrappedStringType,
      resolve: async (src) => Concat([await src, ""], ""),
    },
    json: {
      type: JSONStringType,
      resolve: (src) => src,
    },
  }),
}) as GraphQLObjectType;

const QueryType = new GraphQLObjectType({
  name: "Query",
  fields: () => ({
    true: {
      type: BooleanEnumType,
      resolve: () => true,
    },
    false: {
      type: BooleanEnumType,
      resolve: () => false,
    },
    hello: {
      type: GraphQLString,
      args: {
        name: {
          type: new GraphQLNonNull(GraphQLString),
          description: "Subject to greet",
        },
      },
      resolve: (_src, args) => `Hello ${args.name}`,
    },
    helloInterface: {
      type: StringInterfaceType,
      args: {
        name: {
          type: new GraphQLNonNull(GraphQLString),
          description: "Subject to greet",
        },
      },
      resolve: (_src, args) => ({
        __typename: "SimpleString",
        string: `Hello ${args.name}`,
      }),
    },
    helloWrapped: {
      type: WrappedStringType,
      args: {
        name: {
          type: new GraphQLNonNull(GraphQLString),
          description: "Subject to greet",
        },
      },
      resolve: (_src, args) => `Hello ${args.name}`,
    },
    helloAll: {
      type: new GraphQLNonNull(
        new GraphQLList(new GraphQLNonNull(GraphQLString)),
      ),
      args: {
        names: {
          type: new GraphQLNonNull(
            new GraphQLList(new GraphQLNonNull(GraphQLString)),
          ),
          description: "Subjects to greet",
        },
      },
      resolve: (_src, args) =>
        args.names.map((name: string) => `Hello ${name}`),
    },
    helloAllInterface: {
      type: new GraphQLNonNull(
        new GraphQLList(new GraphQLNonNull(StringInterfaceType)),
      ),
      args: {
        names: {
          type: new GraphQLNonNull(
            new GraphQLList(new GraphQLNonNull(GraphQLString)),
          ),
          description: "Subjects to greet",
        },
      },
      resolve: (_src, args) =>
        args.names.map((name: string) => ({
          __typename: "SimpleString",
          string: `Hello ${name}`,
        })),
    },
    helloAllWrapped: {
      type: new GraphQLNonNull(
        new GraphQLList(new GraphQLNonNull(WrappedStringType)),
      ),
      args: {
        names: {
          type: new GraphQLNonNull(
            new GraphQLList(new GraphQLNonNull(GraphQLString)),
          ),
          description: "Subjects to greet",
        },
      },
      resolve: (_src, args) =>
        args.names.map((name: string) => `Hello ${name}`),
    },
    helloDeferred: {
      type: GraphQLString,
      args: {
        name: {
          type: new GraphQLNonNull(GraphQLString),
          description: "Subject to greet",
        },
      },
      resolve: (_src, args) => Concat(["Hello", args.name], " "),
    },
    helloDeferredInterface: {
      type: StringInterfaceType,
      args: {
        name: {
          type: new GraphQLNonNull(GraphQLString),
          description: "Subject to greet",
        },
      },
      resolve: (_src, args) =>
        (q as any).wrap({
          __typename: "SimpleString",
          string: Concat(["Hello", args.name], " "),
        }),
    },
    helloAllDeferred: {
      type: new GraphQLNonNull(
        new GraphQLList(new GraphQLNonNull(GraphQLString)),
      ),
      args: {
        names: {
          type: new GraphQLNonNull(
            new GraphQLList(new GraphQLNonNull(GraphQLString)),
          ),
          description: "Subjects to greet",
        },
      },
      resolve: (_src, args) =>
        Map(args.names, Lambda("name", Concat(["Hello", Var("name")], " "))),
    },
    helloAllDeferredInterface: {
      type: new GraphQLNonNull(
        new GraphQLList(new GraphQLNonNull(StringInterfaceType)),
      ),
      args: {
        names: {
          type: new GraphQLNonNull(
            new GraphQLList(new GraphQLNonNull(GraphQLString)),
          ),
          description: "Subjects to greet",
        },
      },
      resolve: (_src, args) =>
        Map(
          args.names,
          Lambda("name", {
            __typename: "SimpleString",
            string: Concat(["Hello", Var("name")], " "),
          }),
        ),
    },
    helloDeferredWrapped: {
      type: WrappedStringType,
      args: {
        name: {
          type: new GraphQLNonNull(GraphQLString),
          description: "Subject to greet",
        },
      },
      resolve: (_src, args) => Concat(["Hello", args.name], " "),
    },
    helloAllDeferredWrapped: {
      type: new GraphQLNonNull(
        new GraphQLList(new GraphQLNonNull(WrappedStringType)),
      ),
      args: {
        names: {
          type: new GraphQLNonNull(
            new GraphQLList(new GraphQLNonNull(GraphQLString)),
          ),
          description: "Subjects to greet",
        },
      },
      resolve: (_src, args) =>
        Map(args.names, Lambda("name", Concat(["Hello", Var("name")], " "))),
    },
    nonNullError: {
      type: new GraphQLNonNull(GraphQLString),
      resolve: () => null,
    },
  }),
}) as GraphQLObjectType;

const schema = new GraphQLSchema({
  query: QueryType,
  types: [QueryType, WrappedStringType, SimpleStringType],
  directives: [],
});

const execute = createExecuteFn(
  createExecutorBackend({
    // endpoint: new URL("https://db.us.fauna.com"),
    // secret: "fnAFp_s3qxAARI6FUbV9uq_5Ghqci2RRe8YRo7Ln",
    // fetch: fetch,

    endpoint: new URL("http://localhost:8443"),
    secret: "secret",
  }),
);

test("true", async () => {
  const result = await execute({
    schema,
    document: parse(`query { true }`),
    rootValue: null,
  });
  if (result.errors?.length) {
    for (const err of result.errors) {
      throw err.originalError ?? err;
    }
  }

  expect(result).toEqual({
    data: {
      true: "TRUE",
    },
  });
});

test("false", async () => {
  const result = await execute({
    schema,
    document: parse(`query { false }`),
    rootValue: null,
  });
  if (result.errors?.length) {
    for (const err of result.errors) {
      throw err.originalError ?? err;
    }
  }

  expect(result).toEqual({
    data: {
      false: "FALSE",
    },
  });
});

test("hello", async () => {
  const result = await execute({
    schema,
    document: parse(`query { hello(name: "world") }`),
    rootValue: null,
  });
  if (result.errors?.length) {
    for (const err of result.errors) {
      throw err.originalError ?? err;
    }
  }

  expect(result).toEqual({
    data: {
      hello: "Hello world",
    },
  });
});

test("helloWrapped.json", async () => {
  const result = await execute({
    schema,
    document: parse(`query { helloWrapped(name: "world") { json } }`),
    rootValue: null,
  });
  if (result.errors?.length) {
    for (const err of result.errors) {
      throw err.originalError ?? err;
    }
  }

  expect(result).toEqual({
    data: {
      helloWrapped: {
        json: '"Hello world"',
      },
    },
  });
});

test("helloAll", async () => {
  const result = await execute({
    schema,
    document: parse(`query { helloAll(names: ["world", "jim"]) }`),
    rootValue: null,
  });
  if (result.errors?.length) {
    for (const err of result.errors) {
      throw err.originalError ?? err;
    }
  }

  expect(result).toEqual({
    data: {
      helloAll: ["Hello world", "Hello jim"],
    },
  });
});

test("helloDeferred", async () => {
  const result = await execute({
    schema,
    document: parse(`query { helloDeferred(name: "world")  }`),
    rootValue: null,
  });
  if (result.errors?.length) {
    for (const err of result.errors) {
      throw err.originalError ?? err;
    }
  }

  expect(result).toEqual({
    data: {
      helloDeferred: "Hello world",
    },
  });
});

test("helloAllDeferred", async () => {
  const result = await execute({
    schema,
    document: parse(`query { helloAllDeferred(names: ["world", "jim"])  }`),
    rootValue: null,
  });
  if (result.errors?.length) {
    for (const err of result.errors) {
      throw err.originalError ?? err;
    }
  }

  expect(result).toEqual({
    data: {
      helloAllDeferred: ["Hello world", "Hello jim"],
    },
  });
});

test("helloAllDeferredInterface", async () => {
  const result = await execute({
    schema,
    document: parse(
      `{ helloAllDeferredInterface(names: ["world", "jim"]) { string } }`,
    ),
    rootValue: null,
  });
  if (result.errors?.length) {
    for (const err of result.errors) {
      throw err.originalError ?? err;
    }
  }

  expect(result).toEqual({
    data: {
      helloAllDeferredInterface: [
        {
          string: "Hello world",
        },
        {
          string: "Hello jim",
        },
      ],
    },
  });
});

test("helloDeferredWrapped.json", async () => {
  const result = await execute({
    schema,
    document: parse(`query { helloDeferredWrapped(name: "world") { json } }`),
    rootValue: null,
  });
  if (result.errors?.length) {
    for (const err of result.errors) {
      throw err.originalError ?? err;
    }
  }

  expect(result).toEqual({
    data: {
      helloDeferredWrapped: {
        json: '"Hello world"',
      },
    },
  });
});

test("helloDeferredWrapped.deferred", async () => {
  const result = await execute({
    schema,
    document: parse(
      `query { helloDeferredWrapped(name: "world") { deferred } }`,
    ),
    rootValue: null,
  });
  if (result.errors?.length) {
    for (const err of result.errors) {
      throw err.originalError ?? err;
    }
  }

  expect(result).toEqual({
    data: {
      helloDeferredWrapped: {
        deferred: "Hello world",
      },
    },
  });
});

test("helloDeferredWrapped.awaited", async () => {
  const result = await execute({
    schema,
    document: parse(
      `query { helloDeferredWrapped(name: "world") { awaited } }`,
    ),
    rootValue: null,
  });
  if (result.errors?.length) {
    for (const err of result.errors) {
      throw err.originalError ?? err;
    }
  }

  expect(result).toEqual({
    data: {
      helloDeferredWrapped: {
        awaited: "Hello world",
      },
    },
  });
});

test("helloDeferredWrapped.restaged", async () => {
  const result = await execute({
    schema,
    document: parse(
      `query { helloDeferredWrapped(name: "world") { restaged } }`,
    ),
    rootValue: null,
  });
  if (result.errors?.length) {
    for (const err of result.errors) {
      throw err.originalError ?? err;
    }
  }

  expect(result).toEqual({
    data: {
      helloDeferredWrapped: {
        restaged: "Hello world",
      },
    },
  });
});

test("helloDeferredWrapped.recurseDeferred.deferred", async () => {
  const result = await execute({
    schema,
    document: parse(
      `query { helloDeferredWrapped(name: "world") { recurseDeferred { deferred } } }`,
    ),
    rootValue: null,
  });
  if (result.errors?.length) {
    for (const err of result.errors) {
      throw err.originalError ?? err;
    }
  }

  expect(result).toEqual({
    data: {
      helloDeferredWrapped: {
        recurseDeferred: {
          deferred: "Hello world",
        },
      },
    },
  });
});

test("helloDeferredWrapped.recurseDeferred.awaited", async () => {
  const result = await execute({
    schema,
    document: parse(
      `query { helloDeferredWrapped(name: "world") { recurseDeferred { awaited } } }`,
    ),
    rootValue: null,
  });
  if (result.errors?.length) {
    for (const err of result.errors) {
      throw err.originalError ?? err;
    }
  }

  expect(result).toEqual({
    data: {
      helloDeferredWrapped: {
        recurseDeferred: {
          awaited: "Hello world",
        },
      },
    },
  });
});

test("helloDeferredWrapped.recurseDeferred.restaged", async () => {
  const result = await execute({
    schema,
    document: parse(
      `query { helloDeferredWrapped(name: "world") { recurseDeferred { restaged } } }`,
    ),
    rootValue: null,
  });
  if (result.errors?.length) {
    for (const err of result.errors) {
      throw err.originalError ?? err;
    }
  }

  expect(result).toEqual({
    data: {
      helloDeferredWrapped: {
        recurseDeferred: {
          restaged: "Hello world",
        },
      },
    },
  });
});

test("helloDeferredWrapped.recurseAwaited.deferred", async () => {
  const result = await execute({
    schema,
    document: parse(
      `query { helloDeferredWrapped(name: "world") { recurseAwaited { deferred } } }`,
    ),
    rootValue: null,
  });
  if (result.errors?.length) {
    for (const err of result.errors) {
      throw err.originalError ?? err;
    }
  }

  expect(result).toEqual({
    data: {
      helloDeferredWrapped: {
        recurseAwaited: {
          deferred: "Hello world",
        },
      },
    },
  });
});

test("helloDeferredWrapped.recurseAwaited.awaited", async () => {
  const result = await execute({
    schema,
    document: parse(
      `query { helloDeferredWrapped(name: "world") { recurseAwaited { awaited } } }`,
    ),
    rootValue: null,
  });
  if (result.errors?.length) {
    for (const err of result.errors) {
      throw err.originalError ?? err;
    }
  }

  expect(result).toEqual({
    data: {
      helloDeferredWrapped: {
        recurseAwaited: {
          awaited: "Hello world",
        },
      },
    },
  });
});

test("helloDeferredWrapped.recurseAwaited.restaged", async () => {
  const result = await execute({
    schema,
    document: parse(
      `query { helloDeferredWrapped(name: "world") { recurseAwaited { restaged } } }`,
    ),
    rootValue: null,
  });
  if (result.errors?.length) {
    for (const err of result.errors) {
      throw err.originalError ?? err;
    }
  }

  expect(result).toEqual({
    data: {
      helloDeferredWrapped: {
        recurseAwaited: {
          restaged: "Hello world",
        },
      },
    },
  });
});

test("helloDeferredWrapped.recurseRestaged.deferred", async () => {
  const result = await execute({
    schema,
    document: parse(
      `query { helloDeferredWrapped(name: "world") { recurseRestaged { deferred } } }`,
    ),
    rootValue: null,
  });
  if (result.errors?.length) {
    for (const err of result.errors) {
      throw err.originalError ?? err;
    }
  }

  expect(result).toEqual({
    data: {
      helloDeferredWrapped: {
        recurseRestaged: {
          deferred: "Hello world",
        },
      },
    },
  });
});

test("helloDeferredWrapped.recurseRestaged.awaited", async () => {
  const result = await execute({
    schema,
    document: parse(
      `query { helloDeferredWrapped(name: "world") { recurseRestaged { awaited } } }`,
    ),
    rootValue: null,
  });
  if (result.errors?.length) {
    for (const err of result.errors) {
      throw err.originalError ?? err;
    }
  }

  expect(result).toEqual({
    data: {
      helloDeferredWrapped: {
        recurseRestaged: {
          awaited: "Hello world",
        },
      },
    },
  });
});

test("helloDeferredWrapped.recurseRestaged.restaged", async () => {
  const result = await execute({
    schema,
    document: parse(
      `query { helloDeferredWrapped(name: "world") { recurseRestaged { restaged } } }`,
    ),
    rootValue: null,
  });
  if (result.errors?.length) {
    for (const err of result.errors) {
      throw err.originalError ?? err;
    }
  }

  expect(result).toEqual({
    data: {
      helloDeferredWrapped: {
        recurseRestaged: {
          restaged: "Hello world",
        },
      },
    },
  });
});

test("helloDeferredWrapped.recurseDeferred.recurseDeferred.awaited", async () => {
  const result = await execute({
    schema,
    document: parse(
      `query { helloDeferredWrapped(name: "world") { recurseDeferred { recurseDeferred { awaited } } } }`,
    ),
    rootValue: null,
  });
  if (result.errors?.length) {
    for (const err of result.errors) {
      throw err.originalError ?? err;
    }
  }

  expect(result).toEqual({
    data: {
      helloDeferredWrapped: {
        recurseDeferred: {
          recurseDeferred: {
            awaited: "Hello world",
          },
        },
      },
    },
  });
});

test("helloDeferredWrapped.recurseDeferred.recurseAwaited.awaited", async () => {
  const result = await execute({
    schema,
    document: parse(
      `query { helloDeferredWrapped(name: "world") { recurseDeferred { recurseAwaited { awaited } } } }`,
    ),
    rootValue: null,
  });
  if (result.errors?.length) {
    for (const err of result.errors) {
      throw err.originalError ?? err;
    }
  }

  expect(result).toEqual({
    data: {
      helloDeferredWrapped: {
        recurseDeferred: {
          recurseAwaited: {
            awaited: "Hello world",
          },
        },
      },
    },
  });
});

test("helloAllDeferredWrapped.deferred", async () => {
  const result = await execute({
    schema,
    document: parse(
      `query { helloAllDeferredWrapped(names: ["world", "jim"]) { deferred } }`,
    ),
    rootValue: null,
  });
  if (result.errors?.length) {
    for (const err of result.errors) {
      throw err.originalError ?? err;
    }
  }

  expect(result).toEqual({
    data: {
      helloAllDeferredWrapped: [
        {
          deferred: "Hello world",
        },
        {
          deferred: "Hello jim",
        },
      ],
    },
  });
});

test("helloAllDeferredWrapped.awaited", async () => {
  const result = await execute({
    schema,
    document: parse(
      `query { helloAllDeferredWrapped(names: ["world", "jim"]) { awaited } }`,
    ),
    rootValue: null,
  });
  if (result.errors?.length) {
    for (const err of result.errors) {
      throw err.originalError ?? err;
    }
  }

  expect(result).toEqual({
    data: {
      helloAllDeferredWrapped: [
        {
          awaited: "Hello world",
        },
        {
          awaited: "Hello jim",
        },
      ],
    },
  });
});

test("helloAllDeferredWrapped.restaged", async () => {
  const result = await execute({
    schema,
    document: parse(
      `query { helloAllDeferredWrapped(names: ["world", "jim"]) { restaged } }`,
    ),
    rootValue: null,
  });
  if (result.errors?.length) {
    for (const err of result.errors) {
      throw err.originalError ?? err;
    }
  }

  expect(result).toEqual({
    data: {
      helloAllDeferredWrapped: [
        {
          restaged: "Hello world",
        },
        {
          restaged: "Hello jim",
        },
      ],
    },
  });
});

test("nonNullError", async () => {
  const result = await execute({
    schema,
    document: parse(`query { nonNullError }`),
    rootValue: null,
  });
  expect(JSON.parse(JSON.stringify(result))).toEqual({
    data: null,
    errors: [
      {
        locations: [{ column: 9, line: 1 }],
        message: "Cannot return null for non-nullable field",
        path: ["nonNullError"],
      },
    ],
  });
});

test("overlapping fragments", async () => {
  const result = await execute({
    schema,
    document: parse(
      `query { helloWrapped(name: "world") { recurseDeferred { deferred } } ...on Query { helloWrapped(name: "world") { recurseDeferred { __typename string } } } }`,
    ),
    rootValue: null,
  });
  expect(JSON.parse(JSON.stringify(result))).toEqual({
    data: {
      helloWrapped: {
        recurseDeferred: {
          __typename: "WrappedString",
          deferred: "Hello world",
          string: "Hello world",
        },
      },
    },
  });
});
