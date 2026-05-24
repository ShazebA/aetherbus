# HN submission

## Title (max 80 chars)

```
Show HN: AetherBus – event-driven context layer for AI agents, MCP-native
```

## URL

```
https://github.com/ShazebA/aetherbus
```

## Text (optional, under the URL field)

```
Hi HN — I built AetherBus to close a gap I kept hitting with multi-agent systems: MCP defines `resources/subscribe` and `notifications/resources/updated`, but the ecosystem skipped them. Claude Desktop doesn't implement subscribe. FastMCP doesn't. So every multi-agent team rebuilds the same pub/sub layer by hand.

AetherBus is a Python library + reference broker that wraps NATS JetStream (default) and Kafka in a Kafka-style pub/sub API where events carry `dataref` URIs instead of payloads. Subscribers resolve content on demand from mcp://, vec://, git://, file://, or https://. The strategic piece is the MCP bridge: `aetherbus-mcp serve` is a stdio MCP server that subscribes to bus topics and pushes `notifications/resources/updated` to Claude Desktop / Cursor / Cline. Drop it into the client's config — the bus stream shows up as a live MCP resource. Zero AetherBus-specific code on the client side.

Other bits worth knowing:

- Replay UI (FastAPI + React) with sandbox replay — fork-from-here debugging while preserving original event IDs
- Framework adapters for LangGraph, CrewAI, OpenAI Agents SDK (<100 LOC each)
- Kafka transport conforms against the same scenarios as NATS
- 95 unit tests, integration tests parametrized over both transports

The longer write-up explains why I built this — what AGNTCY, Anthropic's research-agent post, and Cognition's "Don't Build Multi-Agents" piece converge on, and why a library for it didn't exist: [link to blog post]

Apache 2.0, Python 3.11+. Happy to answer questions about the design or the trade-offs.
```

## Posting checklist

- [ ] Repo is public.
- [ ] README GIF plays on GitHub (test on desktop + mobile preview).
- [ ] Blog post is live with a stable URL.
- [ ] Docs site (`aetherbus.dev` or `shazeb.ca/aetherbus`) is reachable.
- [ ] `pip install aetherbus` works against PyPI (or note "from source" in the post).
- [ ] You have ~3 hours of clear time after posting to engage with comments.

## Hour-1 reply templates

- **"How is this different from Kafka?"** "Kafka is a transport — AetherBus is a convention layer (CloudEvents envelopes, `dataref` URIs, MCP bridge, replay UI) that ships with both NATS and Kafka adapters. You can run AetherBus on top of your existing Kafka."
- **"Why not just use A2A?"** "A2A is RPC — agent calls agent. AetherBus is pub/sub — many-to-many, replayable. They're complementary; the README has a table."
- **"What's the relationship to AGNTCY / SLIM?"** "AGNTCY/SLIM target protocol rigor for internet-scale agent routing. AetherBus is the developer-experience layer. SLIM is a planned transport for v2."
- **"Why structured-JSON CloudEvents, not binary?"** "ADR-002 in the repo. tl;dr — one serialization across NATS and Kafka, replay UI decodes everything with one `json.loads`, and the binary win evaporates when you `dataref` (which is the default)."

## Do not

- Lead with token-count comparisons (HN is allergic to "X uses Y tokens" framings; lead with the gap, not the math).
- Compare to OpenAI's Swarm or LangChain by name — invites a thread that isn't the one you want.
- Push back on early dismissals. Answer the actual technical question or move on.
