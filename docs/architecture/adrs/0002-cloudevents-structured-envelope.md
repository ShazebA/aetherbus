# ADR-002: CloudEvents 1.0 structured-JSON envelope

- **Status:** Accepted
- **Date:** 2026-05-17
- **Deciders:** Shazeb
- **PRD anchor:** §5.3, §7.2 ADR-002

## Context

Every AetherBus event is a CloudEvents 1.0 envelope (PRD §5.3). CloudEvents supports two encodings over a transport that has both metadata (headers) and a body:

1. **Binary mode** — CloudEvents attributes go in transport headers (`ce-id`, `ce-source`, …), the `data` field is the body. Cleanest mapping when the transport is HTTP or AMQP; payload is opaque bytes.
2. **Structured mode** — the entire envelope (attributes + data) is a single JSON document carried as the body; headers may be empty.

NATS JetStream supports headers, so binary mode is technically available. We still picked structured-JSON for M2.

## Decision

**Structured JSON in M2.** The serialized form is a single JSON object emitted by `Envelope.to_bytes()` and parsed by `Envelope.from_bytes()`. Binary-mode revisit is deferred to **M4**, when the Kafka adapter forces a comparison.

## Rationale

- **One serialization for all transports.** NATS, Kafka, and any future SLIM transport all carry bytes. Structured mode means the wire format is the same everywhere — the conformance test suite (PRD §7.3) can hash the bytes and compare.
- **The replay UI inspects payloads.** It reads from JetStream, decodes, and renders — having a single JSON object instead of "attributes-from-headers + body-blob" simplifies the UI and the planned `aetherbus tail` CLI (PRD §6.5).
- **`dataref` is the actual payload-size optimization.** The compactness win from binary mode evaporates the moment we use a `dataref` URI — which is the default per design principle P2. Optimizing the envelope encoding before optimizing inlining is the wrong order.
- **Debugging.** A structured envelope is a one-line JSON object inspectable with `nats stream view`, `redis-cli`, or `cat`. Binary mode requires a CloudEvents-aware tool.

## Consequences

- **+** Wire format is transport-agnostic. Same bytes round-trip through NATS today, Kafka tomorrow, SLIM later.
- **+** `aetherbus tail` and the replay UI can decode every event with a single `json.loads`.
- **+** Test fixtures are human-readable.
- **−** A few extra bytes per envelope (attribute names are inlined). Negligible compared to typical `data` size.
- **−** If a future transport has strong header-based routing (e.g. Kafka headers for content-based filtering), we'd want binary mode there. **Mitigation:** the Transport ABC accepts a `headers: dict` parameter; structured-mode payload is independent of it. Routing-by-header can be added without changing the envelope format.

## Alternatives revisited

- **Binary mode now:** rejected for M2. Forces NATS-specific encoding in the spike-style code and doesn't pay off until we have a transport whose ergonomics demand it.
- **Protobuf / MessagePack:** considered for size. Adds a schema-registry surface we're not ready for. The PRD §3.3 P6 "boring infrastructure" rule says no.
- **CloudEvents SDK helpers (`to_json`, `from_json`):** considered but the M1 spike used them and the round-trip semantics were thinner than the test surface we want. Hand-rolling a 50-line Envelope dataclass lets us own the invariants (data XOR dataref, lowercase extension attr names, 4 KB inline guard) directly.

## References

- PRD §5.3 (envelope spec)
- PRD §3.3 P2 (pointers by default)
- PRD §3.3 P6 (boring infrastructure)
- PRD §6.5 (CLI `aetherbus tail`)
- PRD §7.3 (conformance tests)
- [CloudEvents 1.0 spec](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md)
