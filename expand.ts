import { GraphQLError } from "graphql";
import { Path, addPath, pathToArray } from "graphql/jsutils/Path";
import { isNullValue, selectFromObject } from "./utils";


export interface ShouldExcludeResultPredicate {
  (deferredPath: Array<string | number>, deferredValue: any): boolean;
}

function didParentError(path: Array<string | number>, errors: readonly GraphQLError[]) {
  return errors.some((error) => {
    const errorPath = error.path;
    if (!errorPath) {
      return false;
    }

    if (path.length < errorPath.length) {
      return false;
    }

    return Array.prototype.every.call(errorPath, (key, i) => key === path[i]);
  });
}

function reversePath(path: Path | undefined): Path | undefined {
  let reversed: Path | undefined;
  while (path) {
    reversed = addPath(reversed, path.key, path.typename);
    path = path.prev;
  }

  return reversed;
}

function concatPath(a: Path, b: Path): Path;
function concatPath(a: Path, b: Path | undefined): Path;
function concatPath(a: Path | undefined, b: Path): Path;
function concatPath(a: Path | undefined, b: Path | undefined): Path | undefined;
function concatPath(a: Path | undefined, b: Path | undefined): Path | undefined {
  let reversed = reversePath(b);
  while (reversed) {
    a = addPath(a, reversed.key, reversed.typename);
    reversed = reversed.prev;
  }

  return a;
}

function fixErrorPath(errPath: Array<string | number>, pathArray: Array<string | number>, deferredPath: Array<string | number>) {
  if (typeof errPath[0] === 'number') {
    errPath.shift();
  }

  const fixedDeferredPathLength = (deferredPath.length - (typeof deferredPath[0] === 'number' ? 1 : 0));
  let realPath: Array<string | number>;
  if (!errPath.length) {
    realPath = pathArray.slice(0, -fixedDeferredPathLength);
  } else {
    let pos = -1;
    do {
      pos = pathArray.indexOf(errPath[0], pos + 1);
      if (pos === -1) {
        // give up
        realPath = pathArray;
        break;
      }
    } while (pathArray.slice(pos, pos + errPath.length).some((v, i) => v !== errPath[i]));
    realPath = pathArray.slice(0, pos + errPath.length);
  }

  return realPath;
}

export function expandFromObject(obj: any, deferredPath: Array<string | number>, path: Path | undefined, shouldExcludeResult: ShouldExcludeResultPredicate | undefined, resultErrors: readonly GraphQLError[], getErrorMessage?: (value: any) => string | null): Array<{ path: Path; value: any }> {
  if (shouldExcludeResult?.(deferredPath, obj)) {
    return [];
  }

  if (didParentError(pathToArray(path), resultErrors)) {
    return [];
  }

  if (deferredPath.length === 0) {
    return [{ path: path!, value: obj }];
  }

  const pathArray = pathToArray(path);
  // pathArray must have the same number of array index placeholders (`[]`) as deferredPath
  const arrayIndexPlaceholderCount = deferredPath.filter((p) => p === '[]').length;
  if (arrayIndexPlaceholderCount !== pathArray.filter((p) => p === '[]').length) {
    throw new Error(`expandFromObject: arraysFromPath !== arraysFromDeferred: ${JSON.stringify(pathArray)} ${JSON.stringify(deferredPath)}`);
  }

  if (arrayIndexPlaceholderCount > 1) {
    // everything between the first and last array index placeholder must be identical
    const firstDeferredPathIndexPlaceholderPos = deferredPath.indexOf('[]');
    const lastDeferredPathIndexPlaceholderPos = deferredPath.lastIndexOf('[]');
    const firstPathArrayIndexPlaceholderPos = pathArray.indexOf('[]');
    const lastPathArrayIndexPlaceholderPos = pathArray.lastIndexOf('[]');

    const deferredPathMiddle = deferredPath.slice(firstDeferredPathIndexPlaceholderPos + 1, lastDeferredPathIndexPlaceholderPos);
    const pathArrayMiddle = pathArray.slice(firstPathArrayIndexPlaceholderPos + 1, lastPathArrayIndexPlaceholderPos);
    if (deferredPathMiddle.length !== pathArrayMiddle.length || deferredPathMiddle.some((p, i) => p !== pathArrayMiddle[i])) {
      throw new Error(`expandFromObject: pathArrayMiddle !== deferredPathMiddle: ${JSON.stringify(pathArray)} ${JSON.stringify(deferredPath)}`);
    }
  }

  let pathSuffix = reversePath(path);
  let pathPrefix: Path | undefined = undefined;
  try {
    let pathPos = 0;
    for (const [i, key] of deferredPath.entries()) {
      if (isNullValue(obj)) {
        return [{ path: pathPrefix!, value: obj }];
      }

      const errorMessage = getErrorMessage?.(obj);
      if (errorMessage) {
        throw new GraphQLError(errorMessage, {
          path: deferredPath.slice(0, i),
        });
      }

      if (!pathPrefix) {
        const pos = pathArray.indexOf(key);
        if (pos !== -1) {
          pathPos = pos + 1;
          for (let j = 0; j < pathPos; j++) {
            pathPrefix = addPath(pathPrefix, pathSuffix!.key, pathSuffix!.typename);
            pathSuffix = pathSuffix!.prev;
          }
        }
      } else if (pathSuffix) {
        pathPrefix = addPath(pathPrefix, pathSuffix.key, pathSuffix.typename);
        pathSuffix = pathSuffix!.prev;
        pathPos += 1;
      }

      if (key === '[]') {
        break;
      }

      obj = obj[key];
    }
  } catch (err) {
    if (!(err instanceof GraphQLError)) {
      throw err;
    }

    throw new GraphQLError(err.message, {
      path: fixErrorPath(Array.from(err.path!), pathArray, deferredPath),
    });
  }

  if (!pathPrefix && pathSuffix) {
    // no path overlap, return the original path
    return [{ path: path!, value: obj }];
  }

  if (pathPrefix?.key !== '[]') {
    if (!pathPrefix) {
      throw new Error('expandFromObject: pathPrefix is undefined');
    }

    return [{ path: pathPrefix, value: obj }];
  }

  pathPrefix = pathPrefix.prev;

  // get the first array value
  const indexPlaceholderPos = deferredPath.indexOf('[]');
  const arrayPath = pathToArray(pathPrefix);
  if (!Array.isArray(obj)) {
    throw new GraphQLError(
        `Expected array but got ${JSON.stringify(obj)}`,
        {
            path: arrayPath,
        },
    );
  }

  if (!obj.length) {
    if (!pathPrefix) {
      throw new Error('expandFromObject: pathPrefix is undefined');
    }

    return [
      {
        path: pathPrefix,
        value: obj,
      }
    ];
  }

  pathSuffix = reversePath(pathSuffix);

  // recurse for each array element
  return obj.flatMap((elem, index) => {
    // curry the shouldExcludeResult predicate to include the omitted path prefix
    let shouldExcludeResultFn = shouldExcludeResult;
    if (shouldExcludeResultFn) {
      const originalFn = shouldExcludeResultFn;
      shouldExcludeResultFn = (deferredPath) => originalFn([...arrayPath, index, ...deferredPath], obj);
    }

    try {
      return expandFromObject(elem, deferredPath.slice(indexPlaceholderPos + 1), pathSuffix, shouldExcludeResult, resultErrors, getErrorMessage)
        .map(({ path: childPath, value }) => {
          return {
            // add the path prefix with the real index back
            path: concatPath(addPath(pathPrefix, index, undefined), childPath),
            value,
          };
        });
    } catch (err) {
      if (!(err instanceof GraphQLError) || !err.path) {
          throw err;
      }

      throw new GraphQLError(err.message, {
        path: [...pathToArray(pathPrefix), index, ...err.path],
      });
    }
  });
}
