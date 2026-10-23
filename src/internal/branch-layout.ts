export interface BranchLayout {
  readonly keys: readonly (string | number)[];
  index(key: unknown): number;
  shape<T>(values: T[]): T[] | Record<string, T>;
}

/** Snapshots keys before source acquisition; named shapes have no inherited keys. */
export function branchLayout(
  branches: number | readonly string[],
  capacity: number,
): BranchLayout {
  const named = typeof branches !== "number";
  if (named && !Array.isArray(branches))
    throw new TypeError("branches must be a count or an array of string keys.");
  const count = named ? branches.length : branches;
  if (
    !Number.isSafeInteger(count) ||
    count <= 0 ||
    !Number.isSafeInteger(capacity) ||
    capacity <= 0 ||
    !Number.isSafeInteger(count * capacity)
  )
    throw new RangeError(
      "branches, capacity, and their product must be positive safe integers.",
    );
  const keys = named
    ? [...branches]
    : Array.from({ length: count }, (_, index) => index);
  const indices = new Map<unknown, number>();
  keys.forEach((key, index) => {
    if (named && typeof key !== "string")
      throw new TypeError("Branch keys must be strings.");
    if (indices.has(key)) throw new RangeError("Branch keys must be unique.");
    indices.set(key, index);
  });
  return {
    keys,
    index: (key) => indices.get(key) ?? -1,
    shape<T>(values: T[]) {
      if (!named) return values;
      const result = Object.create(null) as Record<string, T>;
      keys.forEach((key, index) => {
        result[key] = values[index]!;
      });
      return result;
    },
  };
}
