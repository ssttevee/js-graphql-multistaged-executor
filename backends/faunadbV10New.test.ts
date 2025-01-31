import { expect, test, describe } from "@jest/globals";
import createExecutorBackend, {
  type CreateExecutorBackendOptions,
  fql,
} from "./faunadbV10";
import type { Path } from "graphql/jsutils/Path";
import {
  type FieldNode,
  GraphQLObjectType,
  Kind,
  GraphQLNonNull,
  type GraphQLOutputType,
  GraphQLList,
  GraphQLSchema,
  GraphQLString,
  type GraphQLFieldConfig,
  parse,
  type ExecutionResult,
  GraphQLInt,
  GraphQLUnionType,
  getNamedType,
  GraphQLInterfaceType,
} from "graphql";
import { Client, type Query } from "fauna";
import {
  createExecuteFn,
  type WrappedValue,
  type ExpandedChild,
  type GraphQLCompositeOutputType,
} from "../executor";
import { flattenMiddleware } from "../utils";

const client = new Client({
  endpoint: new URL("http://localhost:8443"),
  secret: "secret",

  // NOTE: this stops jest from complaining about
  //       not exiting "one second after the test run has completed".
  http2_session_idle_ms: 100,
});

describe("backend", () => {
  const backend = createExecutorBackend(client);

  function makePath(key: string, prev?: Path): Path {
    return {
      key,
      prev,
      typename: undefined,
    };
  }

  function makeFieldNode(name: string): FieldNode {
    return {
      kind: Kind.FIELD,
      name: {
        kind: Kind.NAME,
        value: name,
      },
      alias: undefined,
      arguments: [],
      directives: [],
      selectionSet: undefined,
    };
  }

  function expandChildren(
    path: Path,
    type: GraphQLCompositeOutputType,
    value: Query,
    fields: string[],
  ): [ExpandedChild[], { value: Query }] {
    const ref: { value: Query } = { value: fql`{}` };
    const children = backend.expandChildren(
      path,
      value,
      type,
      new Map([
        [getNamedType(type) as GraphQLObjectType, fields.map(makeFieldNode)],
      ]),
      (q) => {
        ref.value = q;
      },
      {} as any,
    );

    return [Array.from(children), ref];
  }

  describe("deferred values", () => {
    test("should be recognized as such", () => {
      expect(backend.isDeferredValue(fql`"value"`)).toBe(true);
    });

    test("should be properly resolved", async () => {
      expect(
        await backend.resolveDeferredValues(
          [[fql`"value"`, makePath("value")]],
          {} as any,
        ),
      ).toStrictEqual(["value"]);
    });
  });

  describe("wrapped values", () => {
    test("wrapping a deferred value should return a wrapped value", () => {
      const original = fql`"original"`;
      const wrapped = backend.wrapSourceValue(original, () =>
        Promise.resolve(),
      );

      expect(wrapped).not.toBe(original);
      expect(backend.isWrappedValue(wrapped)).toBe(true);
    });

    test("wrapping a literal value should return the original value", () => {
      const original = Symbol("original");
      const wrapped = backend.wrapSourceValue(original, () =>
        Promise.resolve(),
      );

      expect(wrapped).toBe(original);
      expect(backend.isWrappedValue(wrapped)).toBe(false);
    });

    test("unwrapping values should return the original value", () => {
      const original = fql`"original"`;
      const wrapped = backend.wrapSourceValue(original, () =>
        Promise.resolve(),
      );
      expect(backend.unwrapResolvedValue(wrapped)).toBe(original);
    });

    test("awaiting a wrapped value should call the getValue function", async () => {
      const original = fql`"original"`;
      const resolved = Symbol("resolved");
      const wrapped = backend.wrapSourceValue(original, () =>
        Promise.resolve(resolved),
      );
      expect(await wrapped).toBe(resolved);
    });
  });

  describe("expanding children", () => {
    const objType = new GraphQLObjectType({
      name: "T",
      fields: {},
    });

    async function resolveDeferredValue(deferred: Query): Promise<any> {
      return (
        await backend.resolveDeferredValues(
          [[deferred, makePath("value")]],
          {} as any,
        )
      )[0];
    }

    function resolveChildConcat(child: ExpandedChild): Query {
      return fql(
        ["", `+${JSON.stringify(child.fieldNode.name.value)}`],
        child.sourceValue as any,
      );
    }

    function resolveChildrenConcat(children: readonly ExpandedChild[]): void {
      for (const child of children) {
        child.setData(resolveChildConcat(child));
      }
    }

    async function resolveExpandedChildren(
      type: GraphQLCompositeOutputType,
      sourceValue: Query,
      fields: string[],
    ): Promise<any> {
      const [children, deferred] = expandChildren(
        makePath("root"),
        type,
        sourceValue,
        fields,
      );
      resolveChildrenConcat(children);
      return await resolveDeferredValue(deferred.value);
    }

    const nonNullErr = {
      "@error": "Cannot return null for non-nullable field",
    };
    describe("nullable object", () => {
      test("non-null value", async () => {
        expect(
          await resolveExpandedChildren(objType, fql`"abc"`, ["foo", "bar"]),
        ).toStrictEqual({ foo: "abcfoo", bar: "abcbar" });
      });

      test("null value", async () => {
        expect(
          await resolveExpandedChildren(objType, fql`null`, ["foo", "bar"]),
        ).toStrictEqual(null);
      });
    });

    describe("non-null object", () => {
      test("non-null value", async () => {
        expect(
          await resolveExpandedChildren(
            new GraphQLNonNull(objType),
            fql`"abc"`,
            ["foo", "bar"],
          ),
        ).toStrictEqual({ foo: "abcfoo", bar: "abcbar" });
      });

      test("null value", async () => {
        expect(
          await resolveExpandedChildren(
            new GraphQLNonNull(objType),
            fql`null`,
            ["foo", "bar"],
          ),
        ).toStrictEqual(nonNullErr);
      });
    });

    describe("nullable list", () => {
      describe("nullable items", () => {
        const listType = new GraphQLList(objType);

        test("non-null array of non-null values", async () => {
          expect(
            await resolveExpandedChildren(listType, fql`["abc","def"]`, [
              "foo",
              "bar",
            ]),
          ).toStrictEqual([
            { foo: "abcfoo", bar: "abcbar" },
            { foo: "deffoo", bar: "defbar" },
          ]);
        });

        test("non-null array of null values", async () => {
          expect(
            await resolveExpandedChildren(listType, fql`[null, null]`, [
              "foo",
              "bar",
            ]),
          ).toStrictEqual([null, null]);
        });

        test("non-null array of mixed null and non-null values", async () => {
          expect(
            await resolveExpandedChildren(listType, fql`["abc", null]`, [
              "foo",
              "bar",
            ]),
          ).toStrictEqual([{ foo: "abcfoo", bar: "abcbar" }, null]);
        });

        test("null array", async () => {
          expect(
            await resolveExpandedChildren(listType, fql`null`, ["foo", "bar"]),
          ).toStrictEqual(null);
        });
      });

      describe("non-null items", () => {
        const listType = new GraphQLList(new GraphQLNonNull(objType));

        test("non-null array of non-null values", async () => {
          expect(
            await resolveExpandedChildren(listType, fql`["abc","def"]`, [
              "foo",
              "bar",
            ]),
          ).toStrictEqual([
            { foo: "abcfoo", bar: "abcbar" },
            { foo: "deffoo", bar: "defbar" },
          ]);
        });

        test("non-null array of null values", async () => {
          expect(
            await resolveExpandedChildren(listType, fql`[null, null]`, [
              "foo",
              "bar",
            ]),
          ).toStrictEqual([nonNullErr, nonNullErr]);
        });

        test("non-null array of mixed null and non-null values", async () => {
          expect(
            await resolveExpandedChildren(listType, fql`["abc", null]`, [
              "foo",
              "bar",
            ]),
          ).toStrictEqual([{ foo: "abcfoo", bar: "abcbar" }, nonNullErr]);
        });

        test("null array", async () => {
          expect(
            await resolveExpandedChildren(listType, fql`null`, ["foo", "bar"]),
          ).toStrictEqual(null);
        });
      });
    });

    describe("non-null list", () => {
      describe("nullable items", () => {
        const listType = new GraphQLNonNull(new GraphQLList(objType));

        test("non-null array of non-null values", async () => {
          expect(
            await resolveExpandedChildren(listType, fql`["abc","def"]`, [
              "foo",
              "bar",
            ]),
          ).toStrictEqual([
            { foo: "abcfoo", bar: "abcbar" },
            { foo: "deffoo", bar: "defbar" },
          ]);
        });

        test("non-null array of null values", async () => {
          expect(
            await resolveExpandedChildren(listType, fql`[null, null]`, [
              "foo",
              "bar",
            ]),
          ).toStrictEqual([null, null]);
        });

        test("non-null array of mixed null and non-null values", async () => {
          expect(
            await resolveExpandedChildren(listType, fql`["abc", null]`, [
              "foo",
              "bar",
            ]),
          ).toStrictEqual([{ foo: "abcfoo", bar: "abcbar" }, null]);
        });

        test("null array", async () => {
          expect(
            await resolveExpandedChildren(listType, fql`null`, ["foo", "bar"]),
          ).toStrictEqual(nonNullErr);
        });
      });

      describe("non-null items", () => {
        const listType = new GraphQLNonNull(
          new GraphQLList(new GraphQLNonNull(objType)),
        );

        test("non-null array of non-null values", async () => {
          expect(
            await resolveExpandedChildren(listType, fql`["abc","def"]`, [
              "foo",
              "bar",
            ]),
          ).toStrictEqual([
            { foo: "abcfoo", bar: "abcbar" },
            { foo: "deffoo", bar: "defbar" },
          ]);
        });

        test("non-null array of null values", async () => {
          expect(
            await resolveExpandedChildren(listType, fql`[null, null]`, [
              "foo",
              "bar",
            ]),
          ).toStrictEqual([nonNullErr, nonNullErr]);
        });

        test("non-null array of mixed null and non-null values", async () => {
          expect(
            await resolveExpandedChildren(listType, fql`["abc", null]`, [
              "foo",
              "bar",
            ]),
          ).toStrictEqual([{ foo: "abcfoo", bar: "abcbar" }, nonNullErr]);
        });

        test("null array", async () => {
          expect(
            await resolveExpandedChildren(listType, fql`null`, ["foo", "bar"]),
          ).toStrictEqual(nonNullErr);
        });
      });
    });
  });
});

describe("executor", () => {
  function makeSchema(
    fields: Record<string, GraphQLFieldConfig<any, any, any>>,
  ) {
    return new GraphQLSchema({
      query: new GraphQLObjectType({
        name: "Query",
        fields,
      }),
    });
  }

  async function execute(
    schema: GraphQLSchema,
    query: string,
    backendOptions: CreateExecutorBackendOptions = {},
  ): Promise<[ExecutionResult<any>, number]> {
    let count = 0;
    const executeFn = createExecuteFn(
      createExecutorBackend(client, {
        ...backendOptions,
        queryMiddleware: [
          (next) =>
            async (...args) => {
              count++;
              // console.log("query", JSON.stringify(args[1].encode(), null, 2));
              const res = await next(...args);
              // console.log("result", JSON.stringify(res, null, 2));
              return res;
            },
          flattenMiddleware(backendOptions.queryMiddleware),
        ],
      }),
    );

    return [
      JSON.parse(
        JSON.stringify(
          await executeFn({
            schema,
            document: parse(query),
          }),
        ),
      ),
      count,
    ];
  }

  test("exception in resolver", async () => {
    expect(
      await execute(
        makeSchema({
          foo: {
            type: GraphQLInt,
            resolve: () => {
              throw new Error("bar");
            },
          },
        }),
        "{ foo }",
      ),
    ).toStrictEqual([
      {
        data: { foo: null },
        errors: [
          {
            locations: [{ column: 3, line: 1 }],
            message: "bar",
            path: ["foo"],
          },
        ],
      },
      0,
    ]);
  });

  describe("primitives", () => {
    describe("literal", () => {
      test("valid", async () => {
        expect(
          await execute(
            makeSchema({
              foo: {
                type: GraphQLInt,
                resolve: () => 9001,
              },
            }),
            "{ foo }",
          ),
        ).toStrictEqual([{ data: { foo: 9001 } }, 0]);
      });

      test("invalid", async () => {
        expect(
          await execute(
            makeSchema({
              foo: {
                type: GraphQLInt,
                resolve: () => "foo",
              },
            }),
            "{ foo }",
          ),
        ).toStrictEqual([
          {
            data: { foo: null },
            errors: [
              {
                locations: [{ column: 3, line: 1 }],
                message: `Int cannot represent non-integer value: "foo"`,
                path: ["foo"],
              },
            ],
          },
          0,
        ]);
      });
    });

    describe("deferred", () => {
      test("valid", async () => {
        expect(
          await execute(
            makeSchema({
              foo: {
                type: GraphQLInt,
                resolve: () => fql`9001`,
              },
            }),
            "{ foo }",
          ),
        ).toStrictEqual([{ data: { foo: 9001 } }, 1]);
      });
      test("invalid", async () => {
        expect(
          await execute(
            makeSchema({
              foo: {
                type: GraphQLInt,
                resolve: () => fql`"foo"`,
              },
            }),
            "{ foo }",
          ),
        ).toStrictEqual([
          {
            data: { foo: null },
            errors: [
              {
                locations: [{ column: 3, line: 1 }],
                message: `Int cannot represent non-integer value: "foo"`,
                path: ["foo"],
              },
            ],
          },
          1,
        ]);
      });
    });
  });

  describe("objects", () => {
    describe("default resolver", () => {
      test("literal", async () => {
        expect(
          await execute(
            makeSchema({
              foo: {
                type: new GraphQLObjectType({
                  name: "Bar",
                  fields: {
                    bar: { type: GraphQLString },
                  },
                }),
                resolve: () => ({ bar: "baz" }),
              },
            }),
            "{ foo { bar } }",
          ),
        ).toStrictEqual([{ data: { foo: { bar: "baz" } } }, 0]);
      });

      test("deferred", async () => {
        expect(
          await execute(
            makeSchema({
              foo: {
                type: new GraphQLObjectType({
                  name: "Bar",
                  fields: {
                    bar: { type: GraphQLString },
                  },
                }),
                resolve: () => fql`{ bar: "baz" }`,
              },
            }),
            "{ foo { bar } }",
          ),
        ).toStrictEqual([{ data: { foo: { bar: "baz" } } }, 1]);
      });
    });

    test("literal outer deferred inner", async () => {
      expect(
        await execute(
          makeSchema({
            foo: {
              type: new GraphQLObjectType({
                name: "Bar",
                fields: {
                  bar: {
                    type: GraphQLString,
                    resolve: (v: { bar: string }) => fql`${v.bar}`,
                  },
                },
              }),
              resolve: () => ({ bar: "baz" }),
            },
          }),
          "{ foo { bar } }",
        ),
      ).toStrictEqual([{ data: { foo: { bar: "baz" } } }, 1]);
    });

    test("deferred outer and inner", async () => {
      expect(
        await execute(
          makeSchema({
            foo: {
              type: new GraphQLObjectType({
                name: "Bar",
                fields: {
                  bar: {
                    type: GraphQLString,
                    resolve: (v: WrappedValue<{ bar: string }>) =>
                      fql`${v.bar}`,
                  },
                },
              }),
              resolve: () => fql`{ bar: "baz" }`,
            },
          }),
          "{ foo { bar } }",
        ),
      ).toStrictEqual([{ data: { foo: { bar: "baz" } } }, 1]);
    });

    test("deferred outer and awaited inner", async () => {
      expect(
        await execute(
          makeSchema({
            foo: {
              type: new GraphQLObjectType({
                name: "Bar",
                fields: {
                  bar: {
                    type: GraphQLString,
                    resolve: async (v: WrappedValue<{ bar: string }>) =>
                      fql`${await v.bar}`,
                  },
                },
              }),
              resolve: () => fql`{ bar: "baz" }`,
            },
          }),
          "{ foo { bar } }",
        ),
      ).toStrictEqual([{ data: { foo: { bar: "baz" } } }, 2]);
    });
  });

  describe("lists", () => {
    describe("literal", () => {
      test("empty", async () => {
        expect(
          await execute(
            makeSchema({
              foo: {
                type: new GraphQLList(GraphQLString),
                resolve: () => [],
              },
            }),
            "{ foo }",
          ),
        ).toStrictEqual([{ data: { foo: [] } }, 0]);
      });

      test("non-empty", async () => {
        expect(
          await execute(
            makeSchema({
              foo: {
                type: new GraphQLList(GraphQLString),
                resolve: () => ["bar", "baz"],
              },
            }),
            "{ foo }",
          ),
        ).toStrictEqual([{ data: { foo: ["bar", "baz"] } }, 0]);
      });

      test("undefined", async () => {
        expect(
          await execute(
            makeSchema({
              foo: {
                type: new GraphQLList(GraphQLString),
                resolve: () => {},
              },
            }),
            "{ foo }",
          ),
        ).toStrictEqual([{ data: { foo: null } }, 0]);
      });

      test("null", async () => {
        expect(
          await execute(
            makeSchema({
              foo: {
                type: new GraphQLList(GraphQLString),
                resolve: () => null,
              },
            }),
            "{ foo }",
          ),
        ).toStrictEqual([{ data: { foo: null } }, 0]);
      });

      test("non-array", async () => {
        expect(
          await execute(
            makeSchema({
              foo: {
                type: new GraphQLList(GraphQLString),
                resolve: () => "bar",
              },
            }),
            "{ foo }",
          ),
        ).toStrictEqual([
          {
            data: { foo: null },
            errors: [
              {
                locations: [{ column: 3, line: 1 }],
                message: "Cannot return non-list value for list field",
                path: ["foo"],
              },
            ],
          },
          0,
        ]);
      });
    });

    describe("deferred", () => {
      test("empty", async () => {
        expect(
          await execute(
            makeSchema({
              foo: {
                type: new GraphQLList(GraphQLString),
                resolve: () => fql`[]`,
              },
            }),
            "{ foo }",
          ),
        ).toStrictEqual([{ data: { foo: [] } }, 1]);
      });

      test("non-empty", async () => {
        expect(
          await execute(
            makeSchema({
              foo: {
                type: new GraphQLList(GraphQLString),
                resolve: () => fql`["bar", "baz"]`,
              },
            }),
            "{ foo }",
          ),
        ).toStrictEqual([{ data: { foo: ["bar", "baz"] } }, 1]);
      });

      test("null", async () => {
        expect(
          await execute(
            makeSchema({
              foo: {
                type: new GraphQLList(GraphQLString),
                resolve: () => fql`null`,
              },
            }),
            "{ foo }",
          ),
        ).toStrictEqual([{ data: { foo: null } }, 1]);
      });

      test("non-array", async () => {
        expect(
          await execute(
            makeSchema({
              foo: {
                type: new GraphQLList(GraphQLString),
                resolve: () => fql`"bar"`,
              },
            }),
            "{ foo }",
          ),
        ).toStrictEqual([
          {
            data: { foo: null },
            errors: [
              {
                locations: [{ column: 3, line: 1 }],
                message: "Cannot return non-list value for list field",
                path: ["foo"],
              },
            ],
          },
          1,
        ]);
      });
    });
  });

  describe("non-null", () => {
    test("literal non-null", async () => {
      expect(
        await execute(
          makeSchema({
            foo: {
              type: new GraphQLNonNull(GraphQLString),
              resolve: () => "bar",
            },
          }),
          "{ foo }",
        ),
      ).toStrictEqual([{ data: { foo: "bar" } }, 0]);
    });

    test("literal null", async () => {
      expect(
        await execute(
          makeSchema({
            foo: {
              type: new GraphQLNonNull(GraphQLString),
              resolve: () => null,
            },
          }),
          "{ foo }",
        ),
      ).toStrictEqual([
        {
          data: null,
          errors: [
            {
              locations: [{ column: 3, line: 1 }],
              message: "Cannot return null for non-nullable field",
              path: ["foo"],
            },
          ],
        },
        0,
      ]);
    });

    test("deferred non-null", async () => {
      expect(
        await execute(
          makeSchema({
            foo: {
              type: new GraphQLNonNull(GraphQLString),
              resolve: () => fql`"bar"`,
            },
          }),
          "{ foo }",
        ),
      ).toStrictEqual([{ data: { foo: "bar" } }, 1]);
    });

    test("deferred null", async () => {
      expect(
        await execute(
          makeSchema({
            foo: {
              type: new GraphQLNonNull(GraphQLString),
              resolve: () => fql`null`,
            },
          }),
          "{ foo }",
        ),
      ).toStrictEqual([
        {
          data: null,
          errors: [
            {
              locations: [{ column: 3, line: 1 }],
              message: "Cannot return null for non-nullable field",
              path: ["foo"],
            },
          ],
        },
        1,
      ]);
    });
  });

  describe("unions", () => {
    const fooType = new GraphQLObjectType({
      name: "Foo",
      fields: {
        foo: { type: GraphQLString },
      },
    });
    const helloType = new GraphQLObjectType({
      name: "Hello",
      fields: {
        hello: { type: GraphQLString },
      },
    });
    const unionType = new GraphQLUnionType({
      name: "FooHello",
      types: [fooType, helloType],
    });

    test("literal", async () => {
      const schema = new GraphQLSchema({
        types: [fooType, helloType, unionType],
        query: new GraphQLObjectType({
          name: "Query",
          fields: {
            foohello: {
              type: new GraphQLList(unionType),
              resolve: () => [
                { __typename: "Foo", foo: "bar" },
                { __typename: "Hello", hello: "world" },
              ],
            },
          },
        }),
      });

      expect(
        await execute(
          schema,
          "{ foohello { ... on Foo { foo } ... on Hello { hello } } }",
        ),
      ).toStrictEqual([
        { data: { foohello: [{ foo: "bar" }, { hello: "world" }] } },
        0,
      ]);
    });

    test("deferred", async () => {
      const schema = new GraphQLSchema({
        types: [fooType, helloType, unionType],
        query: new GraphQLObjectType({
          name: "Query",
          fields: {
            foohello: {
              type: new GraphQLList(unionType),
              resolve: () => fql`[
                { __typename: "Foo", foo: "bar" },
                { __typename: "Hello", hello: "world" },
              ]`,
            },
          },
        }),
      });

      expect(
        await execute(
          schema,
          "{ foohello { __typename ... on Foo { foo } ... on Hello { hello } } }",
        ),
      ).toStrictEqual([
        {
          data: {
            foohello: [
              { foo: "bar", __typename: "Foo" },
              { hello: "world", __typename: "Hello" },
            ],
          },
        },
        1,
      ]);
    });
  });

  describe("interfaces", () => {
    const helloJimType = new GraphQLObjectType({
      name: "HelloJim",
      fields: {
        hello: { type: GraphQLString },
        jim: { type: GraphQLString },
      },
    });
    const helloWorldType = new GraphQLObjectType({
      name: "HelloWorld",
      fields: {
        hello: { type: GraphQLString },
        world: { type: GraphQLString },
      },
    });
    const helloType = new GraphQLInterfaceType({
      name: "Hello",
      fields: {
        hello: { type: GraphQLString },
      },
    });

    test("literal", async () => {
      const schema = new GraphQLSchema({
        types: [helloJimType, helloWorldType, helloType],
        query: new GraphQLObjectType({
          name: "Query",
          fields: {
            hello: {
              type: new GraphQLList(helloType),
              resolve: () => [
                { __typename: "HelloJim", hello: "hello", jim: "ko" },
                { __typename: "HelloWorld", hello: "hello", world: "earth" },
              ],
            },
          },
        }),
      });

      expect(
        await execute(
          schema,
          "{ hello { __typename ... on HelloJim { jim } ... on HelloWorld { world } } }",
        ),
      ).toStrictEqual([
        {
          data: {
            hello: [
              { __typename: "HelloJim", jim: "ko" },
              { __typename: "HelloWorld", world: "earth" },
            ],
          },
        },
        0,
      ]);
    });

    test("deferred", async () => {
      const schema = new GraphQLSchema({
        types: [helloJimType, helloWorldType, helloType],
        query: new GraphQLObjectType({
          name: "Query",
          fields: {
            hello: {
              type: new GraphQLList(helloType),
              resolve: () => fql`[
                { __typename: "HelloJim", hello: "hello", jim: "ko" },
                { __typename: "HelloWorld", hello: "hello", world: "earth" },
              ]`,
            },
          },
        }),
      });

      expect(
        await execute(
          schema,
          "{ hello { __typename ... on HelloJim { jim } ... on HelloWorld { world } } }",
        ),
      ).toStrictEqual([
        {
          data: {
            hello: [
              { __typename: "HelloJim", jim: "ko" },
              { __typename: "HelloWorld", world: "earth" },
            ],
          },
        },
        1,
      ]);
    });
  });
});
