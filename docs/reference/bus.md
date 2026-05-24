# `Bus`

The public client. Construct one per process; share across producers and subscribers.

## Construction

```python
from aetherbus import Bus
from aetherbus.config import Settings

# From env (AETHERBUS_URL, AETHERBUS_SOURCE, …)
bus = Bus.from_env()

# From YAML
bus = Bus.from_config("aetherbus.yaml")

# Explicit settings
bus = Bus(Settings(
    transport_type="nats",
    transport_url="nats://localhost:4222",
    stream_name="AETHERBUS",
    stream_subjects=("dev.aetherbus.>",),
))
```

## Lifecycle

Two patterns. Use the context manager for one-shot publish, `run()` for long-running subscribers.

```python
# One-shot
async with Bus.from_env() as bus:
    await bus.publish("dev.aetherbus.t.v1", payload={"a": 1})

# Long-running
bus = Bus.from_env()
@bus.subscribe("dev.aetherbus.t.v1")
async def handler(event):
    ...
await bus.run()       # blocks until bus.close() is called from another task
```

## Publish

```python
event_id = await bus.publish(
    "dev.aetherbus.research.finding.v1",
    payload=Finding(query="x", summary="y"),   # EventPayload, dict, or None
    # — or —
    dataref="vec://chroma/findings/abc",       # one of payload OR dataref, not both
    depends_on=["evt-1", "evt-2"],             # provenance DAG
    correlation_id="trace-abc",                 # OTel-compatible
    ttl=300,                                    # cache hint (seconds) for resolvers
    headers={"x-tenant": "acme"},
)
```

Returns the **UUIDv7** event id. On transport failure the envelope is queued in the local SQLite outbox; the call still returns the id.

Inline payloads larger than `settings.inline_size_limit` (default 4 KB serialized) raise `EnvelopeError`. Use `dataref` instead.

## Subscribe

```python
@bus.subscribe(
    "dev.aetherbus.research.finding.v1",
    queue_group="workers",          # exactly-one delivery across multiple subscribers
    replay_from="earliest",          # "earliest" | "latest" (default) | datetime
    max_in_flight=16,
)
async def on_finding(event):
    finding = await event.resolve(as_=Finding)
    ...
```

Decoration only registers the handler. Handlers don't fire until `await bus.run()`.

## Replay

```python
async for event in bus.replay(
    "dev.aetherbus.research.finding.v1",
    since=datetime(2026, 5, 17, tzinfo=UTC),     # or None for all
    until=None,
    into=sandbox_bus,                             # optional — re-emit raw envelopes onto a sandbox
    idle_timeout=1.0,
):
    print(event.id, event.inline_data)
```

`replay()` is an async generator. It yields stored events between `since` and `until` and stops at an end-of-history snapshot captured before iteration. See [Replay correctness](../architecture/transports.md#replay-correctness-the-snapshot-rule).

`into=` re-emits the **raw envelope bytes** — ids, timestamps, correlation ids, signatures all preserved. The audit trail survives a fork.

## Register a schema

Optional. Enables warn-mode validation per [PRD §5.10](#) on publish and `event.resolve(as_=Model)` on subscribe.

```python
bus.register_schema("dev.aetherbus.research.finding.v1", Finding)
```

## Settings

| Field | Default | Source |
|---|---|---|
| `transport_type` | `"nats"` | env / yaml |
| `transport_url` | `nats://localhost:4222` | `AETHERBUS_URL` |
| `source` | `aetherbus://agent/${HOSTNAME}-${pid}` | `AETHERBUS_SOURCE` |
| `stream_name` | `AETHERBUS` | yaml |
| `stream_subjects` | `("aetherbus.>", "dev.aetherbus.>")` | yaml |
| `outbox_path` | `.outbox.sqlite` | `AETHERBUS_OUTBOX` |
| `inline_size_limit` | `4096` (bytes) | yaml |

See [`config.py`](https://github.com/ShazebA/aetherbus/blob/main/src/aetherbus/config.py) for the full dataclass.
