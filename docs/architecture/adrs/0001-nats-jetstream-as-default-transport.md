# ADR-001: NATS JetStream as default transport

- **Status:** Accepted
- **Date:** 2026-05-17
- **Deciders:** Shazeb
- **Supersedes:** —
- **PRD anchor:** §7.2 ADR-001

## Context

AetherBus needs a default broker. PRD §7.1 calls out NATS JetStream as the default and Kafka as a first-class adapter shipped in M4, but does not write down the justification. This ADR makes the trade-off explicit so future contributors don't relitigate it casually.

Three viable options were considered:

| Option | Pros | Cons |
|---|---|---|
| **NATS JetStream** | Single Go binary, ~15 MB. Built-in queue groups. Subject wildcards (`*` one token, `>` rest) align 1:1 with our `<domain>.<entity>.<action>.<version>` topic taxonomy. Multi-tenancy via accounts is a primitive — sandbox isolation for the Replay UI (PRD §5.6) comes for free. Python client (`nats-py`) is mature. | Smaller ecosystem than Kafka. Fewer existing examples on the agent side. Not the obvious choice for the P2 "small-team platform engineer" persona who already runs Kafka. |
| **Kafka** | Industry default. Huge ecosystem. Durable-log semantics. Matches Kai Waehner's published agent-broker blueprint (PRD §2.4). | Heavy JVM stack. ZooKeeper / KRaft ops overhead. Local dev needs Docker compose with multiple containers — friction against the P1 "MacBook + docker-compose up" persona (PRD §4.1). No native exactly-one queue-group equivalent that maps cleanly onto the hierarchical-team-of-agents pattern (PRD §4.2 UC7); requires consumer groups + manual partition assignment. |
| **Redis Streams** | Tiny, ubiquitous, very low ops cost. | Weaker durability story. No multi-tenancy primitive. Ecosystem doesn't index it for agent / event sourcing — using it would cost us framing without buying capability. |

## Decision

**NATS JetStream is the default transport.** Kafka ships in v1 as a first-class adapter (PRD §7.1 M4). Redis Streams is rejected as a built-in.

## Rationale

The deciding factor is **P1 ("solo agent-tinkerer") developer experience** (PRD §4.1). The quickstart promise (Goal G1) is fewer than 20 lines of code to publish + subscribe. The demo target is `docker-compose up` and a decorator API. NATS' single-binary footprint, native queue groups, and account-based multi-tenancy make these achievable without ops weight.

Account-based multi-tenancy is the load-bearing piece. The Replay UI (PRD §5.6, the portfolio centerpiece per §7.1) requires a sandbox bus that cannot collide with production traffic. NATS accounts give us that for free; on Kafka it's either a separate cluster or a topic-prefix discipline we'd have to enforce ourselves.

For P2 ("small-team agent platform engineer") who already runs Kafka, the M4 Kafka adapter is the integration path, not the default. The conformance test suite (PRD §7.3) runs the same scenarios against both transports, so semantic parity is enforced at CI time rather than by hope.

NATS subject wildcards map directly onto our topic naming convention (PRD §5.4). On Kafka the adapter has to translate `dev.aetherbus.research.*` to a regex consumer filter — more code, more drift between transports.

This aligns with design principles P5 (Python DX is the moat) and P6 (boring infrastructure) — NATS is unfashionable in agent circles but battle-tested everywhere else.

## Consequences

- **+** `examples/spike.py` is dependency-light: one `nats-py` import, ~15 MB broker container, sub-second cold start.
- **+** Queue groups (UC7) and sandbox accounts (§5.6) are native, not bolted on.
- **+** Streaming-replay primitives (`StartTime`, sequence numbers) are first-class in JetStream — `bus.replay(since=...)` (PRD §6.2) maps to one consumer config.
- **−** First-time users from Kafka shops will need to learn NATS subject semantics. **Mitigation:** the CLI normalizes wildcards across transports (PRD §5.4) and the docs include a "Kafka mental model → NATS" cheat sheet.
- **−** The Kafka adapter (M4) must keep semantic parity. **Mitigation:** the conformance test suite (PRD §7.3) runs the same end-to-end scenarios against both. Kafka adapter ships marked "beta" in v1.0 (risk R5).

## Alternatives revisited

If `nats-py` client gaps prove blocking — e.g. per-message ack control or pull-consumer ergonomics (PRD §8 R8) — we vendor a thin shim and contribute upstream. **We do not switch defaults.** Switching defaults is a v2 conversation, gated on the 90-day adoption review (PRD §10.4).

## References

- PRD §2.4 (why not just Kafka / NATS directly — the convention gap)
- PRD §3.3 P5, P6 (design principles)
- PRD §4.1 (personas — P1 is the wedge)
- PRD §4.2 UC7 (queue-group hierarchy)
- PRD §5.4 (topic naming + wildcards)
- PRD §5.6 (replay sandbox via NATS accounts)
- PRD §7.1 M4 (Kafka adapter milestone)
- PRD §7.3 (conformance test suite)
- PRD §8 R5, R8 (Kafka semantic-drift risk, NATS client risk)
