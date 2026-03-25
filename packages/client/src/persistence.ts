import { StreamPersistenceError, StreamProtocolError } from "./errors.js";
import type { StreamPersistence, StreamSequence } from "./types.js";

const checkpointPrefix = "streamweld:v1:";
const canonicalStreamId = /^[0-7][0-9a-hjkmnp-tv-z]{25}$/;
const canonicalSequence = /^(?:0|[1-9][0-9]*)$/;
const maxUint64 = 18_446_744_073_709_551_615n;

export interface PersistedStreamCursor {
  readonly id: string;
  readonly lastEventId: StreamSequence;
}

export function isCanonicalStreamId(value: string): boolean {
  return canonicalStreamId.test(value);
}

export function validateStreamId(value: string): string {
  if (!isCanonicalStreamId(value)) {
    throw new StreamProtocolError("stream ID must be a canonical lowercase ULID");
  }
  return value;
}

export function validateSequence(value: string): StreamSequence {
  if (!canonicalSequence.test(value)) {
    throw new StreamProtocolError("stream sequence must be a canonical unsigned decimal integer");
  }
  let parsed: bigint;
  try {
    parsed = BigInt(value);
  } catch (error) {
    throw new StreamProtocolError("stream sequence is not an unsigned 64-bit integer", { cause: error });
  }
  if (parsed > maxUint64) {
    throw new StreamProtocolError("stream sequence exceeds uint64");
  }
  return value;
}

export function compareSequences(left: StreamSequence, right: StreamSequence): number {
  const a = BigInt(validateSequence(left));
  const b = BigInt(validateSequence(right));
  return a < b ? -1 : a > b ? 1 : 0;
}

export function encodePersistedCursor(id: string, lastEventId: StreamSequence): string {
  return `${checkpointPrefix}${validateStreamId(id)}:${validateSequence(lastEventId)}`;
}

export function decodePersistedCursor(value: string): PersistedStreamCursor {
  if (value.length > 128) {
    throw new StreamPersistenceError("persisted stream checkpoint is too large");
  }
  if (isCanonicalStreamId(value)) {
    return { id: value, lastEventId: "0" };
  }
  if (!value.startsWith(checkpointPrefix)) {
    throw new StreamPersistenceError("persisted stream checkpoint has an unsupported format");
  }
  const encoded = value.slice(checkpointPrefix.length);
  const separator = encoded.indexOf(":");
  if (separator < 0 || separator !== encoded.lastIndexOf(":")) {
    throw new StreamPersistenceError("persisted stream checkpoint is malformed");
  }
  try {
    return {
      id: validateStreamId(encoded.slice(0, separator)),
      lastEventId: validateSequence(encoded.slice(separator + 1)),
    };
  } catch (error) {
    throw new StreamPersistenceError("persisted stream checkpoint is invalid", { cause: error });
  }
}

interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
}

export function createLocalStoragePersistence(
  key: string,
  storage?: StorageLike,
): StreamPersistence {
  if (key.length === 0 || key.length > 256) {
    throw new StreamPersistenceError("localStorage key must contain between 1 and 256 characters");
  }
  const resolved = storage ?? resolveLocalStorage();
  return {
    get: () => resolved.getItem(key),
    set: (id, seq) => {
      if (!Number.isSafeInteger(seq) || seq < 0) {
        throw new StreamPersistenceError("numeric persisted sequence must be a non-negative safe integer");
      }
      resolved.setItem(key, encodePersistedCursor(id, String(seq)));
    },
    setExact: (id, seq) => {
      resolved.setItem(key, encodePersistedCursor(id, seq));
    },
  };
}

function resolveLocalStorage(): StorageLike {
  let candidate: StorageLike | undefined;
  try {
    candidate = globalThis.localStorage;
  } catch (error) {
    throw new StreamPersistenceError("localStorage cannot be accessed", { cause: error });
  }
  if (candidate === undefined) {
    throw new StreamPersistenceError("localStorage is unavailable in this runtime");
  }
  return candidate;
}
