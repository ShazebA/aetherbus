# ADR-005: UUIDv7 for event IDs

- **Status:** Accepted
- **Date:** 2026-05-17
- **Deciders:** Shazeb
- **PRD anchor:** §5.3, §7.2 ADR-005

## Context

Every event needs a unique, sortable ID for: provenance (`aetherbusdependencies` carries comma-separated event IDs), replay positioning (sort by ID = sort by time), and idempotency keys in the outbox.

Candidates: UUIDv4 (random), UUIDv7 (time-prefixed, RFC 9562), ULID, KSUID, snowflake.

## Decision

**UUIDv7**, generated inline. No external dep — minted in `envelope.new_id()` using `time.time()` for the 48-bit timestamp and `os.urandom` for the 74 bits of randomness, formatted as the canonical 36-char dashed hex.

## Rationale

- **Time-sortable.** UUIDv7 prefixes the ID with a 48-bit unix-ms timestamp. Sorting IDs lexicographically equals sorting by publish time — useful for replay UI scrubbing and DB index locality.
- **RFC-standard.** UUIDv7 is part of RFC 9562 (May 2024). KSUID and ULID are de facto formats with no IETF blessing; UUIDv7 is the path that ecosystem tooling (Postgres, JSON-Schema validators, language stdlibs) will converge on. Python 3.14 added `uuid.uuid7()` to stdlib; we inline for back-compat with 3.11–3.13.
- **No dep required.** The algorithm is ~10 lines. Adding a third-party UUIDv7 package (e.g. `uuid7`, `uuid_utils`) buys nothing and costs an install dep. Inlining matches PRD §3.3 P6 ("boring infrastructure").
- **Compatibility.** Existing `uuid.UUID` parsers accept UUIDv7 strings — `from uuid import UUID; UUID(event.id)` works.

## Consequences

- **+** Event IDs sort chronologically without parsing.
- **+** No new dependency.
- **+** Forward-compatible with `uuid.uuid7()` once we set the minimum Python to 3.14 — the inline function gets replaced with a one-line call.
- **−** Two near-simultaneous events on different hosts share the same ms prefix, so collision avoidance leans on the 74 random bits (2^74 ≈ 1.9 × 10^22 possibilities). The birthday-collision probability is negligible at any realistic event rate.
- **−** UUIDv7 timestamps are extracted by anyone who reads the ID. Not a confidentiality issue here (`time` is already an envelope attribute) but worth noting for v2 if events ever become public.

## Alternatives revisited

- **UUIDv4:** rejected. Random IDs make replay-by-position painful and DB indexes spray.
- **KSUID / ULID:** rejected. Time-sortable, yes, but non-standard alphabets (base62 / Crockford base32) trip up tools that expect standard UUIDs. ULID's 80-bit timestamp is overkill.
- **Snowflake (Twitter):** rejected. Requires a worker-ID coordinator. Adds infrastructure for a pre-alpha library.

## References

- PRD §5.3 (envelope attrs — id requirement)
- PRD §7.2 ADR-005
- PRD §3.3 P6
- [RFC 9562 — UUID Formats](https://datatracker.ietf.org/doc/rfc9562/)
