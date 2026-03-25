import { StreamProtocolError } from "./errors.js";

export interface ParsedSSEEvent {
  readonly data: string;
  readonly event: string;
  /** Present only when this frame contained an `id:` field. */
  readonly id?: string;
  readonly retry?: number;
}

/** A byte-incremental, fatal-UTF-8 implementation of the SSE field grammar. */
export class IncrementalSSEParser {
  readonly #decoder = new TextDecoder("utf-8", { fatal: true });
  readonly #encoder = new TextEncoder();
  readonly #maxEventBytes: number;
  #pending = "";
  #started = false;
  #finished = false;
  #eventBytes = 0;
  #data: string[] = [];
  #eventType = "";
  #eventId: string | undefined;
  #retry: number | undefined;

  constructor(maxEventBytes = 1 << 20) {
    if (!Number.isSafeInteger(maxEventBytes) || maxEventBytes < 1) {
      throw new StreamProtocolError("maxEventBytes must be a positive safe integer");
    }
    this.#maxEventBytes = maxEventBytes;
  }

  push(chunk: Uint8Array): ParsedSSEEvent[] {
    if (this.#finished) {
      throw new StreamProtocolError("cannot push bytes after the SSE parser reached EOF");
    }
    let decoded: string;
    try {
      decoded = this.#decoder.decode(chunk, { stream: true });
    } catch (error) {
      throw new StreamProtocolError("SSE response contains invalid UTF-8", { cause: error });
    }
    if (!this.#started) {
      this.#started = true;
      if (decoded.startsWith("\uFEFF")) decoded = decoded.slice(1);
    }
    this.#pending += decoded;
    const events = this.#consumeLines(false);
    this.#boundPendingLine();
    return events;
  }

  finish(): ParsedSSEEvent[] {
    if (this.#finished) return [];
    this.#finished = true;
    let decoded: string;
    try {
      decoded = this.#decoder.decode();
    } catch (error) {
      throw new StreamProtocolError("SSE response ends with incomplete UTF-8", { cause: error });
    }
    if (!this.#started) {
      this.#started = true;
      if (decoded.startsWith("\uFEFF")) decoded = decoded.slice(1);
    }
    this.#pending += decoded;
    const events = this.#consumeLines(true);
    // WHATWG event streams discard an event that has not been terminated by
    // a blank line. Advancing its id would skip a truncated event on resume.
    this.#pending = "";
    this.#discardEvent();
    return events;
  }

  #consumeLines(final: boolean): ParsedSSEEvent[] {
    const events: ParsedSSEEvent[] = [];
    let start = 0;
    for (let index = 0; index < this.#pending.length; index += 1) {
      const character = this.#pending[index];
      if (character !== "\r" && character !== "\n") continue;
      if (character === "\r" && index + 1 === this.#pending.length && !final) break;

      const hasLF = character === "\r" && this.#pending[index + 1] === "\n";
      const line = this.#pending.slice(start, index);
      this.#account(line);
      const event = this.#processLine(line);
      if (event !== null) events.push(event);
      if (hasLF) index += 1;
      start = index + 1;
    }
    this.#pending = this.#pending.slice(start);
    return events;
  }

  #processLine(line: string): ParsedSSEEvent | null {
    if (line.length === 0) return this.#dispatch();
    if (line.startsWith(":")) return null;

    const colon = line.indexOf(":");
    const field = colon < 0 ? line : line.slice(0, colon);
    let value = colon < 0 ? "" : line.slice(colon + 1);
    if (value.startsWith(" ")) value = value.slice(1);

    switch (field) {
      case "data":
        this.#data.push(value);
        break;
      case "event":
        this.#eventType = value;
        break;
      case "id":
        if (!value.includes("\0")) this.#eventId = value;
        break;
      case "retry":
        if (/^[0-9]+$/.test(value)) {
          const parsed = Number(value);
          if (Number.isSafeInteger(parsed)) this.#retry = parsed;
        }
        break;
      default:
        break;
    }
    return null;
  }

  #dispatch(): ParsedSSEEvent | null {
    const hasData = this.#data.length > 0;
    const eventType = this.#eventType;
    const eventId = this.#eventId;
    const retry = this.#retry;
    const data = this.#data.join("\n");
    this.#data = [];
    this.#eventType = "";
    this.#eventId = undefined;
    this.#retry = undefined;
    this.#eventBytes = 0;
    if (!hasData) return null;

    return {
      data,
      event: eventType || "message",
      ...(eventId === undefined ? {} : { id: eventId }),
      ...(retry === undefined ? {} : { retry }),
    };
  }

  #discardEvent(): void {
    this.#data = [];
    this.#eventType = "";
    this.#eventId = undefined;
    this.#retry = undefined;
    this.#eventBytes = 0;
  }

  #account(line: string): void {
    this.#eventBytes += this.#encoder.encode(line).byteLength;
    if (this.#eventBytes > this.#maxEventBytes) {
      throw new StreamProtocolError(`SSE event exceeds ${String(this.#maxEventBytes)} bytes`);
    }
  }

  #boundPendingLine(): void {
    const incompleteLine = this.#pending.endsWith("\r")
      ? this.#pending.slice(0, -1)
      : this.#pending;
    if (this.#eventBytes + this.#encoder.encode(incompleteLine).byteLength > this.#maxEventBytes) {
      throw new StreamProtocolError(`SSE event exceeds ${String(this.#maxEventBytes)} bytes`);
    }
  }
}
