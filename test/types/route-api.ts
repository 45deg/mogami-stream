import {
  broadcast,
  from,
  route,
  type CloseReport,
  type StreamIterator,
} from "../../dist/index.js";
import {
  broadcast as coreBroadcast,
  route as coreRoute,
} from "../../dist/core/index.js";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
    ? true
    : false;
type Expect<T extends true> = T;
type Value<T> = T extends AsyncIterable<infer U> ? U : never;

type ImageEvent = { kind: "image"; width: number };
type TextEvent = { kind: "text"; text: string };
type OtherEvent = { kind: "other"; id: number };
type Event = ImageEvent | TextEvent | OtherEvent;
declare const events: Event[];
declare const fallback: "image" | "text";

const selected = route(events, {
  branches: ["image", "other"],
  capacity: 8,
  select: (event, { index, signal }) => {
    const typedIndex: number = index;
    const typedSignal: AbortSignal = signal;
    void typedIndex;
    void typedSignal;
    return event.kind === "image" ? "image" : "other";
  },
  default: "other",
  fallback: "error",
});
export type SelectedKeys = Expect<
  Equal<keyof typeof selected.branches, "image" | "other">
>;
export type SelectedValues = Expect<
  Equal<Value<typeof selected.branches.image>, Event>
>;
const selectedMapped: Promise<number[]> = selected.branches.image
  .map((event) => event.kind.length)
  .toArray();
const selectedIterator: StreamIterator<Event> =
  selected.branches.other.iterate();
void selectedMapped;
void selectedIterator;

const guarded = route(events, {
  branches: {
    image: (event): event is ImageEvent => event.kind === "image",
    text: (event): event is TextEvent => event.kind === "text",
    other: (_event) => true,
  },
  capacity: 8,
  default: "other",
});
export type ImageNarrowed = Expect<
  Equal<Value<typeof guarded.branches.image>, ImageEvent>
>;
export type TextNarrowed = Expect<
  Equal<Value<typeof guarded.branches.text>, TextEvent>
>;
export type DefaultReceivesAny = Expect<
  Equal<Value<typeof guarded.branches.other>, Event>
>;
const imageWidths: Promise<number[]> = guarded.branches.image
  .map((event) => event.width)
  .toArray();
void imageWidths;

const defaultGuarded = route(events, {
  branches: {
    image: (event): event is ImageEvent => event.kind === "image",
    text: (event): event is TextEvent => event.kind === "text",
  },
  capacity: 8,
  default: "text",
});
export type DefaultGuardWidened = Expect<
  Equal<Value<typeof defaultGuarded.branches.text>, Event>
>;
export type NonDefaultGuardPreserved = Expect<
  Equal<Value<typeof defaultGuarded.branches.image>, ImageEvent>
>;

const unionDefault = route(events, {
  branches: {
    image: (event): event is ImageEvent => event.kind === "image",
    text: (event): event is TextEvent => event.kind === "text",
  },
  capacity: 8,
  default: fallback,
});
export type UnionDefaultImageWidened = Expect<
  Equal<Value<typeof unionDefault.branches.image>, Event>
>;
export type UnionDefaultTextWidened = Expect<
  Equal<Value<typeof unionDefault.branches.text>, Event>
>;

const noDefault = route(events, {
  branches: {
    image: (event): event is ImageEvent => event.kind === "image",
    text: (event): event is TextEvent => event.kind === "text",
    asynchronous: async (event) => event.kind === "other",
  },
  capacity: 8,
});
export type NoDefaultImageNarrowed = Expect<
  Equal<Value<typeof noDefault.branches.image>, ImageEvent>
>;
export type AsyncPredicateNotNarrowed = Expect<
  Equal<Value<typeof noDefault.branches.asynchronous>, Event>
>;

const methodGuarded = from(events).route({
  branches: {
    image: (event): event is ImageEvent => event.kind === "image",
    text: (event): event is TextEvent => event.kind === "text",
  },
  capacity: 8,
});
export type FluentGuardNarrowed = Expect<
  Equal<Value<typeof methodGuarded.branches.image>, ImageEvent>
>;
const methodSelected = from(events).route({
  branches: ["selected", "rest"],
  capacity: 8,
  select: async (event) => (event.kind === "image" ? "selected" : "rest"),
});
export type FluentSelectedNotNarrowed = Expect<
  Equal<Value<typeof methodSelected.branches.selected>, Event>
>;

const numeric = coreRoute([Promise.resolve(1)], {
  branches: 2,
  capacity: 1,
  select: (_value, { index }) => index % 2,
});
export type NumericPromisePreserved = Expect<
  Equal<Value<NonNullable<(typeof numeric.branches)[number]>>, Promise<number>>
>;
const numericReport: Promise<{
  readonly source: CloseReport;
  readonly branches: readonly CloseReport[];
}> = numeric.closed;
void numericReport;
// @ts-expect-error Core branches have no fluent map method.
numeric.branches[0]!.map(String);
// @ts-expect-error Branch arrays cannot grow after construction.
numeric.branches.push(numeric.branches[0]!);

const coreGuarded = coreRoute(events, {
  branches: {
    image: (event): event is ImageEvent => event.kind === "image",
    text: (event): event is TextEvent => event.kind === "text",
  },
  capacity: 1,
  default: "text",
});
export type CoreGuardNarrowed = Expect<
  Equal<Value<typeof coreGuarded.branches.image>, ImageEvent>
>;
export type CoreFallbackWidened = Expect<
  Equal<Value<typeof coreGuarded.branches.text>, Event>
>;
// @ts-expect-error Core branches have no fluent map method.
coreGuarded.branches.image.map(String);

const copied = broadcast(events, {
  branches: ["store", "metrics"],
  capacity: 8,
});
export type BroadcastKeys = Expect<
  Equal<keyof typeof copied.branches, "store" | "metrics">
>;
const broadcastResult: Promise<Event[]> = copied.branches.store.toArray();
void broadcastResult;
const methodBroadcast = from(events).broadcast({
  branches: ["store", "metrics"],
  capacity: 8,
});
export type FluentBroadcastValue = Expect<
  Equal<Value<typeof methodBroadcast.branches.metrics>, Event>
>;
const coreCopied = coreBroadcast(events, {
  branches: ["store", "metrics"],
  capacity: 8,
});
export type CoreBroadcastValue = Expect<
  Equal<Value<typeof coreCopied.branches.store>, Event>
>;

route(events, {
  branches: ["image", "text"],
  capacity: 1,
  // @ts-expect-error Unknown keys cannot become destinations through select inference.
  select: () => "missing",
});
// @ts-expect-error Unknown default keys cannot widen the declared key set.
route(events, {
  branches: ["image", "text"],
  capacity: 1,
  select: () => "image",
  default: "missing",
});
route(events, {
  // @ts-expect-error Predicate branches cannot also specify select.
  branches: { image: (event: Event) => event.kind === "image" },
  capacity: 1,
  select: () => "image",
});
route(events, {
  // @ts-expect-error Predicate default keys must name a configured branch.
  branches: { image: (event: Event) => event.kind === "image" },
  capacity: 1,
  default: "missing",
});
// @ts-expect-error Fallback supports only ignore or error.
route(events, { branches: 2, capacity: 1, select: () => 0, fallback: "drop" });
// @ts-expect-error Named branches reject numeric destinations.
route(events, { branches: ["image", "text"], capacity: 1, select: () => 0 });
// @ts-expect-error Numeric branches reject named destinations.
route(events, { branches: 2, capacity: 1, select: () => "image" });
// @ts-expect-error Guard narrowing does not remove values matched by earlier guards.
const badOther: AsyncIterable<OtherEvent> = guarded.branches.other;
void badOther;
// @ts-expect-error Named branches cannot be reassigned.
guarded.branches.image = guarded.branches.text;
from(events).route({
  branches: ["image"],
  capacity: 1,
  // @ts-expect-error Unknown fluent destinations are rejected.
  select: () => "missing",
});
