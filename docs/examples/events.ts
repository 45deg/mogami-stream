import { fromCallback, fromEvent } from "mogami-stream";

export function websocketMessages(socket: WebSocket, signal: AbortSignal) {
  return fromCallback<MessageEvent>(
    (observer) => {
      const message = (event: MessageEvent) => observer.next(event);
      const error = (event: Event) => observer.error(event);
      const close = () => observer.complete();
      socket.addEventListener("message", message);
      socket.addEventListener("error", error);
      socket.addEventListener("close", close);
      return () => {
        socket.removeEventListener("message", message);
        socket.removeEventListener("error", error);
        socket.removeEventListener("close", close);
      };
    },
    { capacity: 64, overflow: "drop-oldest" },
  ).iterate({ signal });
}

export function sseMessages(events: EventSource, signal: AbortSignal) {
  return fromEvent<MessageEvent>(events, "message", {
    capacity: 64,
    overflow: "drop-oldest",
    onDrop(info) {
      console.warn("Dropped SSE messages in this execution:", info.count);
    },
  }).iterate({ signal });
}
