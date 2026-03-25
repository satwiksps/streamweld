import type { StreamErrorEvent, StreamSequence } from "./types.js";

export type StreamExpirationCode =
  | "stream_expired"
  | "stream_not_found"
  | "stream_offset_expired"
  | "stream_not_resumable";

export class StreamweldError extends Error {
  override readonly name: string = "StreamweldError";
}

export class StreamProtocolError extends StreamweldError {
  override readonly name = "StreamProtocolError";
}

export class StreamHTTPError extends StreamweldError {
  override readonly name: string = "StreamHTTPError";

  constructor(
    message: string,
    readonly status: number,
    readonly code: string | null,
    readonly streamId: string | null,
  ) {
    super(message);
  }
}

export class StreamExpiredError extends StreamHTTPError {
  override readonly name = "StreamExpiredError";

  constructor(
    message: string,
    status: number,
    readonly expirationCode: StreamExpirationCode,
    streamId: string | null,
  ) {
    super(message, status, expirationCode, streamId);
  }
}

export class StreamTransportError extends StreamweldError {
  override readonly name = "StreamTransportError";

  constructor(message: string, readonly attempts: number, options?: ErrorOptions) {
    super(message, options);
  }
}

/** Named AbortError for compatibility, while remaining portable beyond DOMException. */
export class LocalAbortError extends StreamweldError {
  override readonly name = "AbortError";

  constructor(message = "the local stream attachment was aborted", options?: ErrorOptions) {
    super(message, options);
  }
}

export class StreamGenerationError extends StreamweldError {
  override readonly name = "StreamGenerationError";

  constructor(readonly event: StreamErrorEvent) {
    super(event.message);
  }
}

export class StreamPersistenceError extends StreamweldError {
  override readonly name = "StreamPersistenceError";
}

export class StreamBufferLimitError extends StreamweldError {
  override readonly name = "StreamBufferLimitError";

  constructor(readonly cursorAtOverflow: StreamSequence) {
    super("this async iterator exceeded its local replay-buffer limit; the shared transport and other views continue");
  }
}

export class StreamNotIdentifiedError extends StreamweldError {
  override readonly name = "StreamNotIdentifiedError";
}
