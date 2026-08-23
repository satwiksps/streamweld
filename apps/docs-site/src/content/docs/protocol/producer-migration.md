---
title: Producer migration
description: Failure detection, eligibility gates, continuation, and seam reconciliation.
---

# Producer migration

Producer migration continues one logical response on a different inference
backend. It is not a blind retry. Streamweld refuses the move unless it can form a
valid continuation request and reconcile the boundary.

## Triggers

An unexpected EOF, TCP reset, upstream 5xx, error chunk, failed health state,
drain, or configured stall can end an attempt. An upstream 4xx is a request
rejection and is never automatically migrated.

## Eligibility gates

All of these must pass:

- the route permits migration and has an eligible, non-draining target;
- migration count, stream duration, and generated-token limits remain in budget;
- target model version matches unless cross-version continuation is explicit;
- the cached chat-template verdict satisfies the route's strictness mode;
- the request has one continuable text choice and no unsafe tool-call boundary;
- structured output is either disabled or its accepted prefix is valid;
- the remaining token budget is positive.

If any predicate fails, Streamweld records a warning and closes with
`migration_refused`. Returning a visible error is safer than splicing an output
whose meaning may have changed.

## Continuation and seam

The next attempt receives the original normalized messages plus the accepted
assistant text as a continuation prefix. Its remaining token budget is reduced by
the already-generated amount. The proxy then compares the beginning of the new
output with a bounded suffix of accepted UTF-8 text, removes the longest safe
overlap, and commits only novel complete SSE events.

Every successful move commits a `migration` journal entry before continuation
chunks become visible. That entry records origin and target backend, trigger,
attempt number, rescued tokens, and whether token accounting was estimated.

!!! caution "Sampling still matters"

    A safe chat template makes continuation structurally possible; it does not
    make stochastic inference bit-for-bit deterministic. Evaluate application
    tolerance, sampling settings, model versions, and the conformance verdict
    before enabling migration for production traffic.
