import { StreamProtocolError } from "./errors.js";
import type { ParsedSSEEvent } from "./sse.js";
import type {
  StreamChunkEvent,
  StreamDoneEvent,
  StreamErrorEvent,
  StreamEvent,
  StreamMigrationEvent,
  StreamOpenEvent,
  StreamSequence,
  StreamStoppedEvent,
  StreamWarningEvent,
  TokenUsage,
} from "./types.js";

export type DecodedProtocolFrame =
  | { readonly kind: "event"; readonly value: StreamEvent }
  | { readonly kind: "done-sentinel" }
  | { readonly kind: "reader-error"; readonly code: string }
  | {
      readonly kind: "expired";
      readonly code: "stream_expired" | "stream_not_found" | "stream_offset_expired" | "stream_not_resumable";
      readonly message: string;
      readonly streamId: string | null;
    };

const expirationCodes = new Set([
  "stream_expired",
  "stream_not_found",
  "stream_offset_expired",
  "stream_not_resumable",
]);

export function decodeProtocolFrame(
  frame: ParsedSSEEvent,
  seq: StreamSequence | null,
): DecodedProtocolFrame {
  if (frame.event === "message") {
    if (frame.data === "[DONE]") return { kind: "done-sentinel" };
    return { kind: "event", value: decodeChunk(frame.data, seq) };
  }

  const object = parseObject(frame.data, frame.event);
  switch (frame.event) {
    case "streamweld.stream.open":
      return { kind: "event", value: decodeOpen(object, requireSeq(seq, frame.event)) };
    case "streamweld.stream.migration":
      return { kind: "event", value: decodeMigration(object, seq) };
    case "streamweld.stream.warning":
      return { kind: "event", value: decodeWarning(object, seq) };
    case "streamweld.stream.done":
      return { kind: "event", value: decodeDone(object, seq) };
    case "streamweld.stream.stopped":
      return { kind: "event", value: decodeStopped(object, seq) };
    case "streamweld.stream.error": {
      const code = stringField(object, "code");
      if (seq === null && expirationCodes.has(code)) {
        return {
          kind: "expired",
          code: code as "stream_expired" | "stream_not_found" | "stream_offset_expired" | "stream_not_resumable",
          message: optionalStringField(object, "message") ?? "stream journal is no longer resumable",
          streamId: optionalStringField(object, "stream_id"),
        };
      }
      return { kind: "event", value: decodeError(object, seq) };
    }
    case "streamweld.reader.error":
      return { kind: "reader-error", code: stringField(object, "code") };
    default:
      throw new StreamProtocolError(`unsupported SSE event type ${JSON.stringify(frame.event)}`);
  }
}

export function extractTextDelta(event: StreamChunkEvent): string | null {
  const root = asRecord(event.data);
  if (root === null) return null;
  const choices = root["choices"];
  if (!Array.isArray(choices)) return null;
  const indexedZero = choices.find((choice) => {
    const record = asRecord(choice);
    return record?.["index"] === 0;
  });
  const first = choices[0];
  const firstRecord = asRecord(first);
  const candidate = indexedZero ?? (firstRecord?.["index"] === undefined ? first : undefined);
  if (candidate === undefined) return null;
  const choice = asRecord(candidate);
  if (choice === null) return null;

  const legacy = choice["text"];
  if (typeof legacy === "string") return legacy;
  const delta = asRecord(choice["delta"]);
  const content = delta?.["content"];
  return typeof content === "string" ? content : null;
}

function decodeChunk(raw: string, seq: StreamSequence | null): StreamChunkEvent {
  let data: unknown;
  try {
    data = JSON.parse(raw) as unknown;
  } catch (error) {
    throw new StreamProtocolError("stream chunk data is not valid JSON", { cause: error });
  }
  return { type: "chunk", seq, raw, data };
}

function decodeOpen(object: Record<string, unknown>, seq: StreamSequence): StreamOpenEvent {
  const modelVersion = object["model_version"];
  if (modelVersion !== null && typeof modelVersion !== "string") {
    throw new StreamProtocolError("open.model_version must be a string or null");
  }
  return {
    type: "open",
    seq,
    streamId: stringField(object, "stream_id"),
    model: stringField(object, "model"),
    modelVersion,
    backendId: stringField(object, "backend_id"),
  };
}

function decodeMigration(
  object: Record<string, unknown>,
  seq: StreamSequence | null,
): StreamMigrationEvent {
  return {
    type: "migration",
    seq,
    fromBackend: stringField(object, "from_backend"),
    toBackend: stringField(object, "to_backend"),
    reason: stringField(object, "reason"),
    rescuedTokens: integerField(object, "rescued_tokens"),
    tokenCountEstimated: booleanField(object, "token_count_estimated"),
    attempt: integerField(object, "attempt"),
  };
}

function decodeWarning(
  object: Record<string, unknown>,
  seq: StreamSequence | null,
): StreamWarningEvent {
  const predicate = object["predicate"];
  if (predicate !== undefined && predicate !== null && typeof predicate !== "string") {
    throw new StreamProtocolError("warning.predicate must be a string or null");
  }
  return {
    type: "warning",
    seq,
    code: stringField(object, "code"),
    message: stringField(object, "message"),
    predicate: typeof predicate === "string" ? predicate : null,
    details: object["details"] ?? null,
  };
}

function decodeDone(object: Record<string, unknown>, seq: StreamSequence | null): StreamDoneEvent {
  const finishReason = object["finish_reason"];
  if (finishReason !== undefined && finishReason !== null && typeof finishReason !== "string") {
    throw new StreamProtocolError("done.finish_reason must be a string or null");
  }
  return {
    type: "done",
    seq,
    finishReason: typeof finishReason === "string" ? finishReason : null,
    usage: usageField(object),
  };
}

function decodeStopped(object: Record<string, unknown>, seq: StreamSequence | null): StreamStoppedEvent {
  const partialText = optionalStringField(object, "partial_text");
  return {
    type: "stopped",
    seq,
    usage: usageField(object),
    ...(partialText === null ? {} : { partialText }),
  };
}

function decodeError(
  object: Record<string, unknown>,
  seq: StreamSequence | null,
): StreamErrorEvent {
  const retriable = object["retriable"];
  if (retriable !== false) {
    throw new StreamProtocolError("error.retriable must be false in protocol v1");
  }
  return {
    type: "error",
    seq,
    code: stringField(object, "code"),
    message: stringField(object, "message"),
    reason: stringField(object, "reason"),
    retriable,
    usage: usageField(object),
  };
}

function usageField(object: Record<string, unknown>): TokenUsage {
  const usage = asRecord(object["usage"]);
  if (usage === null) throw new StreamProtocolError("terminal event is missing usage");
  return {
    promptTokens: integerField(usage, "prompt_tokens"),
    completionTokens: integerField(usage, "completion_tokens"),
    totalTokens: integerField(usage, "total_tokens"),
    estimated: booleanField(usage, "estimated"),
  };
}

function parseObject(value: string, context: string): Record<string, unknown> {
  let decoded: unknown;
  try {
    decoded = JSON.parse(value) as unknown;
  } catch (error) {
    throw new StreamProtocolError(`${context} data is not valid JSON`, { cause: error });
  }
  const object = asRecord(decoded);
  if (object === null) throw new StreamProtocolError(`${context} data must be a JSON object`);
  return object;
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function stringField(object: Record<string, unknown>, name: string): string {
  const value = object[name];
  if (typeof value !== "string" || value.length === 0) {
    throw new StreamProtocolError(`${name} must be a non-empty string`);
  }
  return value;
}

function optionalStringField(object: Record<string, unknown>, name: string): string | null {
  const value = object[name];
  if (value === undefined || value === null) return null;
  if (typeof value !== "string") throw new StreamProtocolError(`${name} must be a string when present`);
  return value;
}

function integerField(object: Record<string, unknown>, name: string): number {
  const value = object[name];
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    throw new StreamProtocolError(`${name} must be a non-negative safe integer`);
  }
  return value as number;
}

function booleanField(object: Record<string, unknown>, name: string): boolean {
  const value = object[name];
  if (typeof value !== "boolean") throw new StreamProtocolError(`${name} must be a boolean`);
  return value;
}

function requireSeq(seq: StreamSequence | null, event: string): StreamSequence {
  if (seq === null) throw new StreamProtocolError(`${event} must carry an SSE id`);
  return seq;
}
