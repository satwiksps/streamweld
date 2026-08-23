---
title: Resume and stop
description: Exact cursors, fan-out readers, retention failures, and why disconnect is not cancellation.
---

# Resume and stop

## Resume from an exact cursor

Every visible journal entry has an SSE `id:` equal to its unsigned 64-bit
sequence. Reconnect with the greatest ID the application has processed:

```http
GET /v1/streams/01k3example000000000000000/events HTTP/1.1
Accept: text/event-stream
Last-Event-ID: 41
```

The cursor is exclusive. Streamweld replays committed entries whose sequence is
greater than 41 and atomically switches to the live tail. Multiple readers can
start from different cursors without changing producer ownership.

Do not coerce the cursor to a JavaScript `number`; the protocol sequence is a
`uint64`. The TypeScript client preserves it as an exact decimal string.

## Resume failures

| Condition | Status | Code |
|---|---:|---|
| Journal expired but tombstone remains | 410 | `stream_expired` |
| ID was never observed | 410 | `stream_not_found` |
| Cursor predates retained ring entries | 410 | `stream_offset_expired` |
| Stream was explicitly stopped | 410 | `stream_not_resumable` |
| Cursor is malformed | 400 | `invalid_resume_cursor` |
| Cursor is ahead of the journal | 409 | `cursor_ahead` |

A client must surface a 410. It must not silently start a replacement generation.

## Stop is a protocol operation

```http
POST /v1/streams/01k3example000000000000000/stop HTTP/1.1
```

The first valid stop marks stop requested, cancels the current attempt, commits a
terminal `stopped` entry, and notifies every reader. It returns `202` only after
that terminal state is committed. Repeating the stop is idempotent.

Closing a socket, aborting a local fetch, or navigating away detaches one reader.
It never calls the stop endpoint. The route's orphan policy determines whether a
producer with no readers continues, cancels immediately, or cancels after a
timeout.

## Save a resume checkpoint

Store `{ stream_id, last_event_id }` only after the application has processed the
corresponding event. The `@streamweld/client` persistence hook and
`createLocalStoragePersistence` helper implement this ordering for browser apps.
