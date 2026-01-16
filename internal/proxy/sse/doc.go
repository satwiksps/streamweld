// Package sse incrementally decodes and encodes Server-Sent Events streams.
//
// Decoder returns only blank-line-terminated frames. This is deliberately
// stricter than implementations that turn an EOF into an implicit frame: an
// unterminated frame usually means that an upstream generation was truncated.
// It also returns ErrInvalidUTF8 instead of applying the replacement-character
// decoding used by browser EventSource implementations. Streamweld must surface
// malformed producer output rather than journal altered text.
//
// Encoder produces a canonical semantic representation, not a byte-lossless
// reproduction of the input. It uses LF endings and canonical field spacing and
// order; decoding has already discarded unknown fields and collapsed repeated
// event, id, and retry fields to their effective values.
package sse
