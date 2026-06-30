# 0001 — RSocket message-size limits: fragmentation + a streaming high-watermark

Status: Proposed

## Context

A production service returned an unbounded result set (a "fetch everything" query) as a single
RSocket response. The encoded payload exceeded RSocket's maximum single-frame length and the
responder failed with:

```
io.rsocket.exceptions.CanceledException: The payload is too big to be send as a single frame
with a max frame length 16777215. Consider enabling fragmentation.
```

The gateway (the caller) received this as an RSocket ERROR frame and relayed it to the external
client as a generic `errorCode: 500`.

Two facts frame the problem:

- **The 16 MB cap is a fixed protocol constant.** RSocket's frame-length header is 24 bits, so no
  single, non-fragmented frame can exceed `2^24 - 1 = 16_777_215` bytes (`FrameLengthCodec`). It
  cannot be raised.
- **scalecube-services builds the RSocket server/connector internally** (`RSocketServerTransport`,
  `RSocketClientTransport`) and exposed no hook to configure framing — so neither fragmentation nor
  any size limit could be set by a consumer of this library. The fix has to live here.

Failure modes to address, in order of importance:

1. A legitimately large response (bigger than one frame but bounded) cannot be sent at all.
2. A response with no size bound produces a cryptic transport error (`CanceledException`) rather
   than an actionable business error.
3. An unbounded response can exhaust memory — on the sender while serializing, and on the receiver
   while reassembling.

The deeper product issue (an unbounded query) is out of scope for the transport: the transport
cannot stop a service from OOMing while building its own result objects. That requires bounding the
query (pagination/streaming) at the service. This ADR covers only what the transport can guarantee.

## Alternatives considered

1. **Do nothing in this repo; bound the response at the service (pagination / limits / streaming).**
   Buys: addresses the root cause (an unbounded result set) with zero transport change; a streamed
   `Flux<T>` never produces an oversized frame because each element is its own frame. Costs: doesn't
   generalize — every service must remember to bound every response, and a forgotten bound reproduces
   the incident with the same cryptic error; provides no defense-in-depth at the transport. *This remains the correct product fix and is complementary, not exclusive.*

2. **Enable RSocket fragmentation only (`fragment(mtu)`), no size limit.** Buys: lifts the 16 MB cap
   so large responses transmit (split into frames, reassembled on receipt). Costs: removes the only
   guardrail — RSocket then accepts payloads up to `maxInboundPayloadSize` (default ~2 GiB), so an
   unbounded response silently marches toward OOM instead of failing. Fragmentation alone trades a
   loud, early failure for a quiet, late one.

3. **Encode the whole response, then check its size and reject if too big.** Buys: a clean business
   error at a chosen ceiling. Costs: not OOM-safe — the oversized payload is fully materialized into
   a buffer *before* the size is known, so a runaway response OOMs the encoder before the check runs.

4. **Streaming-encode with an abort-early size guard, plus fragmentation, plus an inbound reassembly
   cap (chosen).** Buys: large-but-bounded responses succeed (fragmentation); anything past a
   configured high-watermark fails fast with a typed `413` business error *during* encoding, so the
   payload is never fully materialized (OOM-safe on send); inbound reassembly is capped so a peer
   that ignores the watermark cannot OOM the receiver (OOM-safe on receive). Costs: more moving parts;
   the watermark, when it also drives `maxInboundPayloadSize`, must be `>=` the 16 MB frame cap
   (RSocket forbids a smaller inbound cap), so a sub-frame-cap watermark bounds only the outbound
   encode.

## Decision

Expose two opt-in knobs on `RSocketServiceTransport`, default off (`0`), so existing behavior is
unchanged:

- **`mtu(int)`** — enable RSocket fragmentation on both the server and client (`RSocketServer
  .fragment` / `RSocketConnector.fragment`). Lets a payload larger than the single-frame cap be sent
  in pieces and reassembled.

- **`maxMessageSize(int)`** — the high-watermark. It does two things:
  - **Outbound (sender):** encoding runs through a streaming guard (`LimitedOutputStream`) that
    aborts the instant the running byte count would cross the watermark, throwing
    `MessageTooLargeException` (error code `413`). The responder converts this to a normal service
    error message via the error mapper; the client request path lets it propagate directly. The
    oversized payload is never fully materialized — OOM-safe on send — and the caller gets an
    actionable `413` instead of a cryptic `CanceledException`.
  - **Inbound (receiver):** when the watermark is `>=` the single-frame cap, it is also applied as
    RSocket's `maxInboundPayloadSize` on both ends, so reassembly of an oversized payload is rejected
    — OOM-safe on receive. Below the frame cap the inbound cap is not set (RSocket requires it to be
    at least one frame), so a small watermark bounds only the outbound encode.

This is chosen over the alternatives because it is the only option that is simultaneously
(a) able to send legitimately large responses, (b) loud-and-early rather than quiet-and-late on
abuse, and (c) OOM-safe in both directions — while staying opt-in and backward-compatible. It does
not replace alternative 1 (bounding the query at the service), which remains the right product fix
and composes with this one.

## Consequences

- **Buys:** an unbounded result set now either succeeds (if within the watermark, via
  fragmentation) or fails fast with a clear `413 "Message size exceeds limit (N bytes)"`; no cryptic
  transport error; no OOM from serialization or reassembly within the transport's control.
- **Costs / residual risk:**
  - The transport cannot prevent a service from OOMing while building its own result objects before
    encoding; pagination/streaming at the service is still required for truly unbounded queries.
  - `maxMessageSize` used as an inbound cap must be `>=` the 16 MB frame cap; smaller values bound
    only the outbound encode (documented on the setter).
  - The limit is on the encoded **data** payload, not the full RSocket payload — a message's
    headers/metadata are not counted, so the on-wire payload is slightly larger than the watermark.
  - The encode-time `413` is shaped by `DefaultErrorMapper`, not a service's configured
    `ServiceProviderErrorMapper`: the error arises in the transport (`RSocketImpl`), below the method
    invoker that holds the configured mapper. A service with a custom provider error mapper will see
    this one error use the default shape. Plumbing the configured mapper into the transport is a
    possible follow-up if it ever matters.
  - Pre-existing, out-of-scope: a `Mono<byte[]>` service method cannot receive error responses —
    `ServiceMessageCodec.decodeData` decodes a `byte[]`-typed response before checking `isError()`,
    so the client gets the error bytes as a value. Tracked for a separate PR (move the `isError`
    check before the `byte[]` branch).
- **Compatibility:** both knobs default to `0` (off); the legacy transport constructors are
  preserved as overloads, so existing callers are unaffected. Fragmentation on a sender is
  wire-compatible with any RSocket 1.1.x receiver (reassembly is always supported).

## References

- RSocket protocol — 24-bit frame-length field; `io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK
  = 16_777_215`; `RSocketServer/RSocketConnector.fragment(int)` and `.maxInboundPayloadSize(int)`
  (rsocket-core 1.1.5). Adopting the established protocol mechanism rather than inventing one (E3/E4).
- Nygard, *Release It!* — bounded resources / back-pressure / fail-fast as stability patterns; the
  watermark is the "bounded queue + shed load" pattern applied to message size (E1).
- Functional-core / injected-limits framing and "make the bad state unrepresentable by construction"
  — the streaming guard makes an oversized buffer unbuildable rather than detected-after-the-fact
  (engineering-standards D1/meta-principle 2).
- Evidence (T-family): `LimitedOutputStreamTest` proves the streaming/abort-early guard allocation-
  free; `RSocketLargePayloadTest` is the runnable case matrix (frame-cap fail/fragment, watermark
  boundary, `413` + exact message, inbound-cap rejection, request-side guard, streaming control).
- Evidence tier (E6): protocol behavior verified directly against `rsocket-core` 1.1.5 bytecode and
  exercised by integration tests against a real transport — not from documentation alone.
