// #region pipeline
import { filter, from, map, take } from "mogami-stream";

type Task = { title: string; done: boolean };

const tasks: Task[] = [
  { title: " Review draft ", done: false },
  { title: "Send invoice", done: true },
  { title: " Book room ", done: false },
  { title: "Update calendar", done: false },
];

const titles = from(tasks).pipe(
  filter((task) => !task.done),
  map((task) => task.title.trim()),
  take(2),
);

const result = await titles.toArray();
console.log(result); // ["Review draft", "Book room"]
// #endregion pipeline

// #region input-limit
const firstTwoInputs = from(tasks).pipe(
  take(2),
  filter((task) => !task.done),
  map((task) => task.title.trim()),
);

console.log(await firstTwoInputs.toArray()); // ["Review draft"]
// #endregion input-limit

// #region consume
await titles.forEach((title) => console.log(title));
// Review draft
// Book room
// #endregion consume
