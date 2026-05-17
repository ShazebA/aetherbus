# ADR-003: Pydantic v2 for `EventPayload`

- **Status:** Accepted
- **Date:** 2026-05-17
- **Deciders:** Shazeb
- **PRD anchor:** §5.7, §7.2 ADR-003

## Context

`EventPayload` is the base class users subclass to declare typed event payloads. PRD §5.7 shows Pydantic in the quickstart but doesn't lock in the choice — `attrs`, `msgspec`, and stdlib `dataclasses` are also reasonable candidates.

## Decision

**Pydantic v2.** `EventPayload` is `class EventPayload(BaseModel): pass`. Serialization in `Bus.publish` uses `payload.model_dump(mode="json")`. Deserialization in `Event.resolve(as_=Model)` uses `Model.model_validate(data)`.

## Rationale

- **Ecosystem alignment.** Our target framework adapters (LangGraph, CrewAI, OpenAI Agents SDK — PRD §7.1 M6) all use Pydantic. FastAPI for the Replay UI backend (M5) is built on it. Choosing anything else means users translate between models at the AetherBus boundary.
- **JSON Schema export is free.** PRD §5.7 commits to `aetherbus schema export` emitting AsyncAPI 3.0. AsyncAPI message schemas are JSON Schema; Pydantic v2 emits JSON Schema natively via `model_json_schema()`. Zero codegen needed.
- **Pydantic v2 is fast.** The Rust-core rewrite (`pydantic-core`) closes most of the historical perf gap with `msgspec`. Not a bottleneck on the publish path even for high-fanout topics.
- **Validation on resolve is the right place.** Producers serialize from typed models; consumers validate on `event.resolve(as_=Model)`. Schema-mismatch warn-mode in v1 (strict in v2, per PRD §5.10) maps cleanly to `model_validate` raising vs. logging.

## Consequences

- **+** Users get familiar API (`BaseModel`, `Field`, validators) with no AetherBus-specific concepts to learn.
- **+** AsyncAPI export (M3 / later milestone) is a one-liner per registered topic.
- **+** Pydantic-core is fast enough that we don't need to bypass it for throughput.
- **−** Pydantic is a heavier dependency than `dataclasses` or `msgspec` (~10 MB installed). Acceptable; it's table stakes in the agent/LLM ecosystem already.
- **−** `pydantic-core` is Rust, so wheels matter. Pydantic ships wheels for all major platforms; on a niche arch the user installs a Rust toolchain. Not blocking for v1.

## Alternatives revisited

- **`msgspec`:** faster, smaller, no JSON-Schema export by default, much less ecosystem reach in the agent space. Reconsider only if Pydantic perf shows up in profiles for a real workload — which it hasn't.
- **`attrs` + `cattrs`:** clean but the validation+JSON-Schema story is bolt-on. Not worth the assembly.
- **`dataclasses`:** no validation, no JSON-Schema. Forces us to write a schema-registry layer ourselves.

## References

- PRD §5.7 (schema + contract management)
- PRD §5.10 (schema-mismatch policy)
- PRD §3.3 P6 (boring infrastructure — Pydantic counts)
- PRD §7.1 M6 (framework adapters)
- [Pydantic v2 docs](https://docs.pydantic.dev/latest/)
