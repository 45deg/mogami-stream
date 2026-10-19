import { fromCallback } from "mogami-stream";
const result = await fromCallback<string>(
  (observer) => {
    observer.next("A");
    observer.next("B");
    observer.complete();
  },
  { capacity: 2 },
).toArray();

export { result };
