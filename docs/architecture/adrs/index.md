# Architecture Decision Records

ADRs capture the structural choices that drive the rest of the codebase. The number is permanent; status changes over time as decisions get superseded.

| # | Decision | Status |
|---|---|---|
| [ADR-001](0001-nats-jetstream-as-default-transport.md) | NATS JetStream as default transport | Accepted |
| [ADR-002](0002-cloudevents-structured-envelope.md) | CloudEvents 1.0 structured-JSON envelope | Accepted |
| [ADR-003](0003-pydantic-v2-for-payloads.md) | Pydantic v2 for `EventPayload` | Accepted |
| [ADR-004](0004-mcp-poke-then-read-semantics.md) | MCP adapter uses poke-then-read, not push | Accepted |
| [ADR-005](0005-uuidv7-event-ids.md) | UUIDv7 for event IDs | Accepted |

## Format

Each ADR follows a lightweight MADR-style shape: Context → Decision → Rationale → Consequences → Alternatives revisited → References. New ADRs land as separate PRs when a decision affects multiple files or otherwise deserves a record.

## When to write one

- Choosing between two reasonable libraries / patterns where the loser comes up in code review six months from now.
- Reversing a previous decision (link the superseded ADR).
- Capturing a "we explicitly chose NOT to do X" that future contributors will second-guess.

When in doubt, write one. They're cheap and they save the next reviewer a Slack thread.
