import { describe, expect, it } from "vitest";

import { StreamProtocolError } from "./errors.js";
import { IncrementalSSEParser } from "./sse.js";

describe("IncrementalSSEParser", () => {
  it("decodes arbitrary UTF-8 splits, mixed line endings, comments, and multiline data", () => {
    const wire = new TextEncoder().encode(
      "\uFEFF: heartbeat\r\nevent: update\rid: 42\nretry: 1500\r\n" +
      "data: snow ☃\r\ndata: second\r\n\r\n",
    );
    const parser = new IncrementalSSEParser();
    const events = [];
    for (const byte of wire) events.push(...parser.push(Uint8Array.of(byte)));
    events.push(...parser.finish());

    expect(events).toEqual([{
      data: "snow ☃\nsecond",
      event: "update",
      id: "42",
      retry: 1500,
    }]);
  });

  it("discards a frame not terminated by a blank line at EOF", () => {
    const parser = new IncrementalSSEParser();
    expect(parser.push(new TextEncoder().encode("id: 2\ndata: {\"partial\":true}\n"))).toEqual([]);
    expect(parser.finish()).toEqual([]);
  });

  it("dispatches the same frame when the final blank line is present", () => {
    const parser = new IncrementalSSEParser();
    expect(parser.push(new TextEncoder().encode("id: 2\ndata: ok\n\n"))).toEqual([{
      data: "ok",
      event: "message",
      id: "2",
    }]);
    expect(parser.finish()).toEqual([]);
  });

  it("ignores NUL ids and invalid retry fields", () => {
    const parser = new IncrementalSSEParser();
    expect(parser.push(new TextEncoder().encode("id: bad\0id\nretry: +2\ndata:\n\n"))).toEqual([{
      data: "",
      event: "message",
    }]);
  });

  it("rejects invalid UTF-8 and bounds non-delimiter event bytes", () => {
    expect(() => new IncrementalSSEParser().push(Uint8Array.of(0xff))).toThrow(StreamProtocolError);

    const exact = new IncrementalSSEParser(7);
    expect(exact.push(new TextEncoder().encode("data: x\n\n"))).toHaveLength(1);
    const tooSmall = new IncrementalSSEParser(6);
    expect(() => tooSmall.push(new TextEncoder().encode("data: x\n\n"))).toThrow(/exceeds 6 bytes/);
  });
});
