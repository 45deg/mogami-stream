// #region state
import { defer, from } from "mogami-stream";

export const totals = defer(() =>
  from([1, 2])
    .scan(
      (state, value) => {
        state.total += value;
        return state;
      },
      { total: 0 },
    )
    .map((state) => state.total),
);
console.log(await totals.toArray()); // [1, 3]
console.log(await totals.toArray()); // [1, 3]
// #endregion state

// #region promises
export const pending = Promise.resolve(2);
export const preserved = await from([pending]).toArray(); // [Promise<2>]
export const resolved = await from([pending])
  .map((value) => value)
  .toArray(); // [2]
// #endregion promises
