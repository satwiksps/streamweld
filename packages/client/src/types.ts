/** A canonical unsigned 64-bit journal sequence encoded without precision loss. */
export type StreamSequence = string;

export interface TokenUsage {
  readonly promptTokens: number;
  readonly completionTokens: number;
  readonly totalTokens: number;
  readonly estimated: boolean;
}

export interface StreamOpenEvent {
  readonly type: "open";
  readonly seq: StreamSequence;
  readonly streamId: string;
  readonly model: string;
  readonly modelVersion: string | null;
  readonly backendId: string;
}

export interface StreamChunkEvent {
  readonly type: "chunk";
  /** Null only after durability degraded and sequence allocation stopped. */
  readonly seq: StreamSequence | null;
  /** The exact downstream SSE data string. */
  readonly raw: string;
  /** The parsed JSON value represented by {@link raw}. */
  readonly data: unknown;
}

export interface StreamMigrationEvent {
  readonly type: "migration";
  readonly seq: StreamSequence | null;
  readonly fromBackend: string;
  readonly toBackend: string;
  readonly reason: string;
  readonly rescuedTokens: number;
  readonly tokenCountEstimated: boolean;
  readonly attempt: number;
}

export interface StreamWarningEvent {
  readonly type: "warning";
  readonly seq: StreamSequence | null;
  readonly code: string;
  readonly message: string;
  readonly predicate: string | null;
  readonly details: unknown;
}

export interface StreamDoneEvent {
  readonly type: "done";
  readonly seq: StreamSequence | null;
  readonly finishReason: string | null;
  /** Missing only when resuming exactly at the terminal sequence yields [DONE]. */
  readonly usage?: TokenUsage;
  readonly replayedTerminal?: boolean;
}

export interface StreamStoppedEvent {
  readonly type: "stopped";
  readonly seq: StreamSequence | null;
  readonly usage: TokenUsage;
  /** Present on servers that include the already-generated prefix in this event. */
  readonly partialText?: string;
}

export interface StreamErrorEvent {
  readonly type: "error";
  readonly seq: StreamSequence | null;
  readonly code: string;
  readonly message: string;
  readonly reason: string;
  readonly retriable: false;
  readonly usage: TokenUsage;
}

export type StreamEvent =
  | StreamOpenEvent
  | StreamChunkEvent
  | StreamMigrationEvent
  | StreamWarningEvent
  | StreamDoneEvent
  | StreamStoppedEvent
  | StreamErrorEvent;

export interface DoneOutcome {
  readonly type: "done";
  readonly streamId: string | null;
  readonly seq: StreamSequence | null;
  readonly finishReason: string | null;
  readonly usage?: TokenUsage;
}

export interface StoppedOutcome {
  readonly type: "stopped";
  readonly streamId: string;
  readonly seq?: StreamSequence | null;
  readonly partialText?: string;
  readonly usage: TokenUsage;
}

export interface ErrorOutcome {
  readonly type: "error";
  readonly streamId: string | null;
  readonly seq: StreamSequence | null;
  readonly code: string;
  readonly message: string;
  readonly reason: string;
  readonly retriable: false;
  readonly usage: TokenUsage;
}

export type StreamOutcome = DoneOutcome | StoppedOutcome | ErrorOutcome;

export type DurableStreamState =
  | "connecting"
  | "streaming"
  | "reconnecting"
  | "done"
  | "stopped"
  | "error"
  | "disconnected";

export interface StreamBackoffOptions {
  /** Defaults to 250. */
  readonly initialMs?: number;
  /** Defaults to 5,000. */
  readonly maxMs?: number;
  /** Full jitter when true; defaults to true. */
  readonly jitter?: boolean;
}

export interface StreamResumeOptions {
  /** Consecutive retries after the first connection attempt. Defaults to 5. */
  readonly maxAttempts?: number;
  readonly backoff?: StreamBackoffOptions;
}

export interface StreamResumeCursor {
  readonly id: string;
  /** Canonical uint64 decimal. Defaults to zero. */
  readonly lastEventId?: StreamSequence;
}

/**
 * Persistence stores an encoded Streamweld checkpoint returned by `get()`.
 * `setExact` is used when available so cursors above Number.MAX_SAFE_INTEGER
 * remain lossless. The localStorage helper implements both methods.
 */
export interface StreamPersistence {
  get(): string | null;
  set(id: string, seq: number): void;
  setExact?(id: string, seq: StreamSequence): void;
}

export interface DurableStreamLimits {
  /** Serialized initial request limit. Defaults to 1 MiB. */
  readonly maxRequestBytes?: number;
  /** One decoded SSE event's wire-content limit. Defaults to 1 MiB. */
  readonly maxEventBytes?: number;
  /** Error response bytes retained for decoding. Defaults to 64 KiB. */
  readonly maxErrorBytes?: number;
  /** Events retained for replay-safe iterable views. Defaults to 65,536. */
  readonly maxReplayEvents?: number;
  /** Approximate retained event bytes. Defaults to 16 MiB. */
  readonly maxReplayBytes?: number;
}

export interface DurableStreamOptions {
  readonly url: string | URL;
  /** Required when no resume cursor or persisted checkpoint exists. */
  readonly body?: unknown;
  readonly headers?: HeadersInit;
  readonly credentials?: RequestCredentials;
  /** Aborts only this local attachment. It never stops the generation. */
  readonly signal?: AbortSignal;
  /** Fetch-compatible injection seam for tests and custom edge runtimes. */
  readonly fetch?: typeof globalThis.fetch;
  readonly resume?: StreamResumeOptions;
  readonly resumeFrom?: StreamResumeCursor;
  readonly persist?: StreamPersistence;
  readonly onMigration?: (event: StreamMigrationEvent) => void;
  readonly onWarning?: (event: StreamWarningEvent) => void;
  readonly limits?: DurableStreamLimits;
}

export interface DurableStream {
  /** Independent replay-safe view over the stream's one shared transport pump. */
  readonly events: AsyncIterable<StreamEvent>;
  /** Independent replay-safe view of choice-zero chat/completion text deltas. */
  readonly text: AsyncIterable<string>;
  /** Available after response headers or an open event identify the stream. */
  readonly id: string | null;
  readonly idReady: Promise<string>;
  readonly state: DurableStreamState;
  /** Resolves for protocol done/stopped/error; rejects for local failures. */
  readonly result: Promise<StreamOutcome>;
  /** Explicitly stops the remote generation, independent of the local signal. */
  stop(): Promise<StoppedOutcome>;
}
