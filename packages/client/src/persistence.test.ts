import { describe, expect, it } from "vitest";

import {
  StreamPersistenceError,
  StreamProtocolError,
} from "./errors.js";
import {
  createLocalStoragePersistence,
  decodePersistedCursor,
  encodePersistedCursor,
} from "./persistence.js";

const id = "01arz3ndektsv4rrffq69g5fav";

describe("stream cursor persistence", () => {
  it("round-trips exact uint64 cursors without numeric coercion", () => {
    const encoded = encodePersistedCursor(id, "18446744073709551615");
    expect(decodePersistedCursor(encoded)).toEqual({
      id,
      lastEventId: "18446744073709551615",
    });
    expect(decodePersistedCursor(id)).toEqual({ id, lastEventId: "0" });
  });

  it("rejects overflow ULIDs, noncanonical sequences, and uint64 overflow", () => {
    expect(() => encodePersistedCursor("81arz3ndektsv4rrffq69g5fav", "0")).toThrow(StreamProtocolError);
    expect(() => encodePersistedCursor(id, "01")).toThrow(StreamProtocolError);
    expect(() => encodePersistedCursor(id, "18446744073709551616")).toThrow(StreamProtocolError);
  });

  it("ships a storage helper whose opaque get value contains id and offset", () => {
    const values = new Map<string, string>();
    const storage = {
      getItem: (key: string) => values.get(key) ?? null,
      setItem: (key: string, value: string) => values.set(key, value),
    };
    const persistence = createLocalStoragePersistence("active", storage);
    persistence.set(id, 41);
    expect(decodePersistedCursor(persistence.get() ?? "")).toEqual({ id, lastEventId: "41" });
    persistence.setExact?.(id, "18446744073709551615");
    expect(decodePersistedCursor(persistence.get() ?? "").lastEventId).toBe("18446744073709551615");
    expect(() => persistence.set(id, Number.MAX_SAFE_INTEGER + 1)).toThrow(StreamPersistenceError);
  });
});
