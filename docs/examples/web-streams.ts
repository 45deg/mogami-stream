import { fromReadableStream } from "mogami-stream";

export function relay(response: Response, signal: AbortSignal): Response {
  if (!response.body) return new Response(null, { status: response.status });
  const body = fromReadableStream(response.body)
    .map((chunk) => chunk)
    .toReadableStream({ signal });
  return new Response(body, { status: response.status });
}
