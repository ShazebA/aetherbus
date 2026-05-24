# `Event` + `EventPayload`

## `EventPayload`

A Pydantic v2 marker base class. Subclass it for typed payloads.

```python
from aetherbus import EventPayload
from pydantic import HttpUrl, Field

class Finding(EventPayload):
    query: str
    source_url: HttpUrl | None = None
    summary: str
    confidence: float = Field(ge=0, le=1, default=0.5)
```

Standard Pydantic — validators, default factories, JSON schema all work.

## `Event`

The wrapper handlers receive. Thin facade over an [`Envelope`](../architecture/envelope.md).

```python
@bus.subscribe("dev.aetherbus.research.finding.v1")
async def handler(event):
    event.id                  # UUIDv7
    event.source              # "aetherbus://agent/research-1"
    event.type                # "dev.aetherbus.research.finding.v1"
    event.time                # datetime
    event.dataref             # str | None
    event.inline_data         # any | None
    event.correlation_id      # str | None
    event.depends_on          # list[str] — provenance DAG

    # Resolve to a typed payload — inline OR dataref, same call.
    finding = await event.resolve(as_=Finding)
```

## `event.resolve()`

Returns the event payload, fetching the `dataref` if needed.

| Envelope shape | `event.resolve()` | `event.resolve(as_=Model)` |
|---|---|---|
| Inline `data` | Returns the inline value (dict / scalar / list). | `Model.model_validate(data)`. |
| `dataref` + `datacontenttype=application/json` | Fetches via resolver SDK, `json.loads(bytes)`. | `Model.model_validate_json(bytes)`. |
| `dataref` + binary content type | Returns raw `bytes`. | `Model.model_validate_json(bytes)` — assumes JSON. |

The resolver dispatches by URI scheme. Built-ins: `file://`, `https://`, `mcp://`, `git://`. See the [resolver SDK](resolver.md).

```python
# inline JSON
await event.resolve()                                # {"query": "x", "summary": "y"}
await event.resolve(as_=Finding)                     # Finding(query="x", summary="y")

# dataref with TTL hint
await event.resolve(as_=Finding)                     # one network call, cached for envelope's TTL
```

The cache + per-URI single-flight live in the resolver SDK — concurrent resolves of the same URI share one fetch.

## Provenance graph

`event.depends_on` is the list of ancestor event IDs from the `aetherbusdependencies` envelope attribute. The Replay UI renders this as a click-through breadcrumb.

```python
event_id = await bus.publish(
    "dev.aetherbus.code.diff.v1",
    payload=Diff(...),
    depends_on=[plan_event_id, lint_event_id],
)
```

Forms the audit trail PRD §1 calls out as "reproduce the exact stream that led to a bug."
