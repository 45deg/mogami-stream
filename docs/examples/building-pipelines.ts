// #region pipeline
import { from, type Operator } from "mogami-stream";

type Task = { title: string; done: boolean };

const tasks: Task[] = [
  { title: " Review draft ", done: false },
  { title: "Send invoice", done: true },
  { title: " Book room ", done: false },
  { title: "Update calendar", done: false },
];

const titles = from(tasks)
  .filter((task) => !task.done)
  .map((task) => task.title.trim())
  .take(2);

const result = await titles.toArray();
console.log(result); // ["Review draft", "Book room"]
// #endregion pipeline

// #region input-limit
const firstTwoInputs = from(tasks)
  .take(2)
  .filter((task) => !task.done)
  .map((task) => task.title.trim());

console.log(await firstTwoInputs.toArray()); // ["Review draft"]
// #endregion input-limit

// #region consume
await titles.forEach((title) => console.log(title));
// Review draft
// Book room
// #endregion consume

// #region custom-operators
function trimTitles(): Operator<string, string> {
  return (source) => from(source).map((title) => title.trim());
}

function titleLengths(): Operator<string, number> {
  return (source) => from(source).map((title) => title.length);
}

const lengths = from(tasks)
  .filter((task) => !task.done)
  .map((task) => task.title)
  .pipe(trimTitles(), titleLengths())
  .take(2);

console.log(await lengths.toArray()); // [12, 9]
// #endregion custom-operators
