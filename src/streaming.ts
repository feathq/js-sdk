// Live datafile streaming over Server-Sent Events (SSE).
//
// The server exposes `GET {url}/sdk/v1/datafile/stream` as a long-lived
// `text/event-stream`. A `put` frame carries the full datafile JSON and is
// emitted on connect and on every subsequent change:
//
//   event: put
//   id: <datafile version>
//   data: <full datafile JSON>
//
// Lines beginning with `:` are heartbeat comments and are ignored. The
// transport here decodes the wire format into structured frames; the client
// owns adoption (version ordering), reconnect, and poll fallback.

// A single decoded SSE frame. `event` defaults to "message" when the wire
// omits an explicit `event:` field. `data` is the concatenation of every
// `data:` line in the frame (joined with newlines, per the SSE spec).
export interface SseFrame {
  event: string;
  id: string | null;
  data: string;
}

// Hard cap on the bytes a single SSE frame (or an unterminated line) may
// accumulate before the parser aborts the stream. Mirrors the poll path's
// datafile size limit so a server that never emits a newline, or a runaway
// `data:` frame, cannot exhaust memory.
export const STREAM_MAX_BYTES = 10 * 1024 * 1024;

// Thrown by the transport when the server rejects the stream with a non-ok
// HTTP status. Carries the status so the client can distinguish terminal
// auth failures (401/403) from retryable ones (429/5xx).
export class SseHttpError extends Error {
  constructor(readonly status: number) {
    super(`datafile stream failed: ${status}`);
    this.name = "SseHttpError";
  }
}

export interface SseTransportOptions {
  // Fully-qualified stream URL.
  url: string;
  // Request headers (Authorization, User-Agent, Accept).
  headers: Record<string, string>;
  // The SDK's configured fetch implementation.
  fetch: typeof fetch;
  // Aborted by the client to tear the connection down.
  signal: AbortSignal;
  // Optional override of the per-frame byte cap. Defaults to STREAM_MAX_BYTES.
  maxBytes?: number;
  // Invoked once the server has accepted the connection (HTTP 200). The
  // client uses this to mark the stream healthy and relax the safety-net
  // poll.
  onOpen?: () => void;
  // Invoked for every decoded frame.
  onFrame: (frame: SseFrame) => void;
}

// A transport opens one SSE connection and pumps frames through `onFrame`.
// It resolves when the server closes the stream cleanly and rejects on a
// connection or protocol error. Reconnect/backoff is the client's job, not
// the transport's. Injectable so tests can supply a controllable mock.
export type SseTransport = (options: SseTransportOptions) => Promise<void>;

// Default transport: a minimal SSE reader over a streamed fetch body. No
// third-party dependency, works anywhere the SDK's fetch streams a response
// body (Node 18+, Bun, Deno, Workers).
export const fetchSseTransport: SseTransport = async (options) => {
  const { url, headers, fetch: fetchImpl, signal, onOpen, onFrame, maxBytes } = options;
  const res = await fetchImpl(url, {
    method: "GET",
    headers: { Accept: "text/event-stream", ...headers },
    signal,
  });
  if (!res.ok) {
    throw new SseHttpError(res.status);
  }
  if (!res.body) {
    throw new Error("datafile stream returned no body");
  }
  onOpen?.();

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  const parser = new SseParser(onFrame, maxBytes ?? STREAM_MAX_BYTES);
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      parser.push(decoder.decode(value, { stream: true }));
    }
    parser.push(decoder.decode());
    parser.flush();
  } finally {
    // Releasing the lock lets the underlying connection be reclaimed once
    // the body is no longer referenced.
    reader.releaseLock();
  }
};

// Incremental Server-Sent Events parser. Accumulates `event`/`data`/`id`
// fields until a blank line dispatches the frame. Comment lines (leading
// `:`) and unknown fields are ignored.
export class SseParser {
  private buffer = "";
  private eventType = "";
  private dataLines: string[] = [];
  private dataBytes = 0;
  private lastId: string | null = null;

  constructor(
    private readonly onFrame: (frame: SseFrame) => void,
    private readonly maxBytes: number = STREAM_MAX_BYTES,
  ) {}

  push(chunk: string): void {
    this.buffer += chunk;
    // Guard the line buffer: a server that never emits a newline would grow
    // this without bound. Aborting the stream is preferable to OOM.
    if (this.buffer.length > this.maxBytes) {
      throw new Error(`datafile stream line exceeds ${this.maxBytes} bytes`);
    }
    let newlineIndex: number;
    // Normalise CRLF and CR to LF as we go by splitting on any of them.
    while ((newlineIndex = this.indexOfLineBreak(this.buffer)) !== -1) {
      // A lone CR at the very end of the buffer may be the first half of a
      // CRLF split across a chunk boundary. Hold it back until more bytes
      // arrive (or flush() drops it at end of stream) so we never dispatch a
      // line early and then mistake the following LF for a blank line.
      if (this.buffer[newlineIndex] === "\r" && newlineIndex === this.buffer.length - 1) {
        break;
      }
      const line = this.buffer.slice(0, newlineIndex);
      this.buffer = this.buffer.slice(newlineIndex + this.lineBreakLength(newlineIndex));
      this.handleLine(line);
    }
  }

  // Leftover bytes are passed to handleLine but dispatch() is never called, so
  // a frame missing its terminating blank line is discarded. That is
  // spec-correct: an unterminated final frame is not delivered.
  flush(): void {
    // Drop a held-back trailing CR: at end of stream no LF can follow it.
    if (this.buffer.endsWith("\r")) this.buffer = this.buffer.slice(0, -1);
    if (this.buffer.length > 0) {
      this.handleLine(this.buffer);
      this.buffer = "";
    }
  }

  private indexOfLineBreak(s: string): number {
    const lf = s.indexOf("\n");
    const cr = s.indexOf("\r");
    if (lf === -1) return cr;
    if (cr === -1) return lf;
    return Math.min(lf, cr);
  }

  private lineBreakLength(index: number): number {
    // Treat "\r\n" as a single break.
    if (this.buffer[index] === "\r" && this.buffer[index + 1] === "\n") return 2;
    return 1;
  }

  private handleLine(line: string): void {
    if (line === "") {
      this.dispatch();
      return;
    }
    if (line.startsWith(":")) {
      // Heartbeat / comment.
      return;
    }
    const colon = line.indexOf(":");
    let field: string;
    let value: string;
    if (colon === -1) {
      field = line;
      value = "";
    } else {
      field = line.slice(0, colon);
      value = line.slice(colon + 1);
      // A single leading space after the colon is part of the framing, not
      // the value.
      if (value.startsWith(" ")) value = value.slice(1);
    }
    switch (field) {
      case "event":
        this.eventType = value;
        break;
      case "data":
        // Guard the accumulated frame: a runaway `data:` frame must not
        // outgrow the cap the poll path enforces.
        this.dataBytes += value.length;
        if (this.dataBytes > this.maxBytes) {
          throw new Error(`datafile stream frame exceeds ${this.maxBytes} bytes`);
        }
        this.dataLines.push(value);
        break;
      case "id":
        // Per spec, an id containing a NUL is ignored; otherwise recorded.
        if (!value.includes("\0")) this.lastId = value;
        break;
      default:
        // Unknown field (e.g. "retry") is ignored.
        break;
    }
  }

  private dispatch(): void {
    if (this.dataLines.length === 0) {
      // Blank line with no buffered data: nothing to dispatch, but reset
      // any stray event type.
      this.eventType = "";
      return;
    }
    const frame: SseFrame = {
      event: this.eventType === "" ? "message" : this.eventType,
      id: this.lastId,
      data: this.dataLines.join("\n"),
    };
    this.eventType = "";
    this.dataLines = [];
    this.dataBytes = 0;
    this.onFrame(frame);
  }
}
