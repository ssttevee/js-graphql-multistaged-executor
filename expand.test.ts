import { expect, test } from '@jest/globals';
import { expandFromObject } from './expand';
import { Path, addPath } from 'graphql/jsutils/Path';

function arrayToPath(...path: Array<string | number>) {
  return path.reduce<Path | undefined>((path, key) => addPath(path, key, undefined), undefined);
}

test("simple expand", () => {
  expect(
    expandFromObject(
      { foo: "bar" },
      ["foo"],
      arrayToPath("foo"),
      undefined,
      [],
    ),
  ).toEqual([{ path: arrayToPath("foo"), value: "bar" }]);
  expect(
    expandFromObject(
      { foo: { bar: "baz" } },
      ["foo", "bar"],
      arrayToPath("foo", "bar"),
      undefined,
      [],
    ),
  ).toEqual([{ path: arrayToPath("foo", "bar"), value: "baz" }]);
  expect(
    expandFromObject(
      [{ foo: { bar: "baz" } }],
      [0, "foo", "bar"],
      arrayToPath("asdf", "foo", "bar"),
      undefined,
      [],
    ),
  ).toEqual([{ path: arrayToPath("asdf", "foo", "bar"), value: "baz" }]);
  expect(
    expandFromObject(
      { foo: [{ bar: "baz" }] },
      ["foo", 0, "bar"],
      arrayToPath("asdf", 0),
      undefined,
      [],
    ),
  ).toEqual([{ path: arrayToPath("asdf", 0), value: "baz" }]);
});

test("1d expand", () => {
  expect(
    expandFromObject(
      [{ hello: "world" }, { hello: "jim" }],
      ["[]", "hello"],
      arrayToPath("[]", "hello"),
      undefined,
      [],
    ),
  ).toEqual([
    { path: arrayToPath(0, "hello"), value: "world" },
    { path: arrayToPath(1, "hello"), value: "jim" },
  ]);
  expect(
    expandFromObject(
      { hello: ["world", "jim"] },
      ["hello", "[]"],
      arrayToPath("hello", "[]"),
      undefined,
      [],
    ),
  ).toEqual([
    { path: arrayToPath("hello", 0), value: "world" },
    { path: arrayToPath("hello", 1), value: "jim" },
  ]);
});

test("2d expand", () => {
  expect(
    expandFromObject(
      [[{ hello: "world" }], [{ hello: "jim" }]],
      ["[]", "[]", "hello"],
      arrayToPath("[]", "[]", "hello"),
      undefined,
      []
    )
  ).toEqual([
    { path: arrayToPath(0, 0, "hello"), value: "world" },
    { path: arrayToPath(1, 0, "hello"), value: "jim" },
  ]);

  expect(
    expandFromObject(
      [[{ hello: "world" }, { hello: "jim" }]],
      ["[]", "[]", "hello"],
      arrayToPath("[]", "[]", "hello"),
      undefined,
      []
    )
  ).toEqual([
    { path: arrayToPath(0, 0, "hello"), value: "world" },
    { path: arrayToPath(0, 1, "hello"), value: "jim" },
  ]);

  expect(
    expandFromObject(
      [{ hello: ["world"] }, { hello: ["jim"] }],
      ["[]", "hello", "[]"],
      arrayToPath("[]", "hello", "[]"),
      undefined,
      []
    )
  ).toEqual([
    { path: arrayToPath(0, "hello", 0), value: "world" },
    { path: arrayToPath(1, "hello", 0), value: "jim" },
  ]);

  expect(
    expandFromObject(
      [{ hello: ["world", "jim"] }],
      ["[]", "hello", "[]"],
      arrayToPath("[]", "hello", "[]"),
      undefined,
      []
    )
  ).toEqual([
    { path: arrayToPath(0, "hello", 0), value: "world" },
    { path: arrayToPath(0, "hello", 1), value: "jim" },
  ]);

  expect(
    expandFromObject(
      { hello: [["world", "jim"]] },
      ["hello", "[]", "[]"],
      arrayToPath("hello", "[]", "[]"),
      undefined,
      []
    )
  ).toEqual([
    { path: arrayToPath("hello", 0, 0), value: "world" },
    { path: arrayToPath("hello", 0, 1), value: "jim" },
  ]);

  expect(
    expandFromObject(
      { hello: [["world"], ["jim"]] },
      ["hello", "[]", "[]"],
      arrayToPath("hello", "[]", "[]"),
      undefined,
      []
    )
  ).toEqual([
    { path: arrayToPath("hello", 0, 0), value: "world" },
    { path: arrayToPath("hello", 1, 0), value: "jim" },
  ]);
});

test("empty array in path", () => {
  expect(
    expandFromObject(
      { foo: [] },
      ["foo", "[]", "bar"],
      arrayToPath("foo", "[]", "bar"),
      undefined,
      []
    )
  ).toEqual([
    { path: arrayToPath("foo"), value: [] },
  ]);
});

test("null in path", () => {
  expect(
    expandFromObject(
      [{ foo: null }],
      [0, "foo", "bar"],
      arrayToPath("asdf", "foo", "bar"),
      undefined,
      []
    )
  ).toEqual([
    { path: arrayToPath("asdf", "foo"), value: null },
  ]);

  expect(
    expandFromObject(
      { foo: null },
      ["foo", "[]", "bar"],
      arrayToPath("foo", "[]", "bar"),
      undefined,
      []
    )
  ).toEqual([
    { path: arrayToPath("foo"), value: null },
  ]);

  expect(
    expandFromObject(
      { foo: [{ bar: null }, { bar: { baz: "asdf" } }] },
      ["foo", "[]", "bar", "baz"],
      arrayToPath("foo", "[]", "bar", "baz"),
      undefined,
      []
    )
  ).toEqual([
    { path: arrayToPath("foo", 0, "bar"), value: null },
    { path: arrayToPath("foo", 1, "bar", "baz"), value: "asdf" },
  ]);
});
