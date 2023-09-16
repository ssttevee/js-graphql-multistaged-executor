import { GraphQLError } from "graphql";
import { Path, addPath, pathToArray } from "graphql/jsutils/Path";
import { selectFromObject } from "./utils";


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

function isContained(a: Array<string | number>, b: Array<string | number>): boolean {
outer:
  for (let i = 0; i < b.length; i++) {
    if (a[a.length - 1] !== b[b.length - i - 1]) {
      continue;
    }

    if (a.length > 1) {
      for (let j = 0; j < a.length - 1; j++) {
        if (a[a.length - j - 2] !== b[b.length - i - j - 2]) {
          continue outer;
        }
      }
    }

    return true;
  }

  return false;
}

export function expandFromObject(obj: any, deferredPath: Array<string | number>, path: Path | undefined, shouldExcludeResult: ShouldExcludeResultPredicate | undefined, resultErrors: readonly GraphQLError[], getErrorMessage?: (value: any) => string | null): Array<{ path: Path | undefined; value: any }> {
  if (shouldExcludeResult?.(deferredPath, obj)) {
    return [];
  }

  if (didParentError(pathToArray(path), resultErrors)) {
    return [];
  }

  const pathArray = pathToArray(path);
  // pathArray must have the same number of array index placeholders (`[]`) as deferredPath
  const arrayCountFromDeferred = deferredPath.filter((p) => p === '[]').length;
  const arrayCountFromPath = pathArray.filter((p) => p === '[]').length;
  if (arrayCountFromPath !== arrayCountFromDeferred) {
    throw new Error(`expandFromObject: arraysFromPath !== arraysFromDeferred: ${JSON.stringify(pathArray)} ${JSON.stringify(deferredPath)}`);
  }

  if (!arrayCountFromDeferred) {
    // base case (no arrays in deferredPath)
    try {
      return [{ path, value: selectFromObject(obj, deferredPath, getErrorMessage) }];
    } catch (err) {
      if (err instanceof GraphQLError) {
        const errPath = Array.from(err.path!);
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

        throw new GraphQLError(err.message, {
          path: realPath,
        });
      }

      throw err;
    }
  }

  // suffix of pathArray, starting with the first array index placeholder if any, must be contained within deferredPath
  const firstArrayIndexPlaceholderPos = pathArray.indexOf('[]');
  if (!isContained(firstArrayIndexPlaceholderPos === -1 ? [pathArray[pathArray.length - 1]] : pathArray.slice(firstArrayIndexPlaceholderPos), deferredPath)) {
    throw new Error(`expandFromObject: suffix of pathArray is not contained within deferredPath: ${JSON.stringify(pathArray)} ${JSON.stringify(deferredPath)}`)
  }

  // get the first array value
  const indexPlaceholderPos = deferredPath.indexOf('[]');
  const arrayPath = deferredPath.slice(0, indexPlaceholderPos);
  const arrayValue = selectFromObject(obj, arrayPath);
  if (!Array.isArray(arrayValue)) {
    throw new GraphQLError(
        `Expected array but got ${JSON.stringify(arrayValue)}`,
        {
            path: arrayPath,
        },
    );
  }

  // splice the path at the first array index placeholder, so it can be rebuild with a real index for each array element
  let pathPrefix = path;
  let pathSuffix: Path | undefined;

  for (let i = 0; i < pathArray.length - indexPlaceholderPos - 1; i++) {
    if (!pathPrefix) {
      throw new Error('expandFromObject: path is too short');
    }

    pathSuffix = addPath(pathSuffix, pathPrefix.key, pathPrefix.typename);
    pathPrefix = pathPrefix.prev;
  }

  // remove the array index placeholder to make space for the real index

  if (!pathPrefix) {
    throw new Error('expandFromObject: path is too short');
  }

  pathPrefix = pathPrefix.prev;
  pathSuffix = reversePath(pathSuffix);

  // recurse for each array element
  return arrayValue.flatMap((elem, index) => {
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
