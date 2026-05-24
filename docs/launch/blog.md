# MCP has `resources/subscribe` — but nobody uses it

*Draft for Substack / personal site. ~750 words. Lead with the gap, end with the demo.*

---

Anthropic published [Model Context Protocol](https://modelcontextprotocol.io) (MCP) in November 2024. The spec describes a small set of JSON-RPC methods agents call on servers: `resources/list`, `resources/read`, `resources/subscribe`, `notifications/resources/updated`, and a handful more. The first two are everywhere — every MCP server you've installed exposes them. The last two are almost nowhere.

[MCP discussion #301](https://github.com/modelcontextprotocol/specification/discussions/301) documents the gap. Claude Desktop doesn't implement subscribe. FastMCP doesn't. Most third-party servers don't. The primitive is in the spec, marked optional, and the ecosystem skipped it.

That's the gap I built [AetherBus](https://github.com/ShazebA/aetherbus) to close.

## What's there if you don't have subscribe

If `resources/subscribe` works, MCP can act as a *push* channel — the server tells the client "this resource just changed; come read it when you're ready." That's the right shape for any agent system where multiple processes care about the same context: research subagents publishing findings, planner/implementer/tester pipelines, scheduled context refreshes.

Without subscribe, every consumer polls. Or every producer wires per-consumer HTTP calls. Or the team builds a Kafka cluster, learns Avro, and now they're back to writing connectors.

Anthropic's own [June 2025 multi-agent research post](https://www.anthropic.com/news/multi-agent-research-system) describes exactly this pattern as the solution: *"Subagents write to a shared filesystem and return a lightweight reference. The lead agent does not re-read every detail; it gets a pointer and pulls what it needs."* That's the [claim-check pattern](https://www.enterpriseintegrationpatterns.com/patterns/messaging/StoreInLibrary.html) from enterprise messaging — published in *Enterprise Integration Patterns* in 2003 — wrapped in 2025 language. Cognition's ["Don't Build Multi-Agents"](https://cognition.ai/blog/dont-build-multi-agents) post argues for shared traces. Microsoft's reference architectures mandate message brokers with correlation IDs. Everyone is solving the same problem.

Nobody had shipped the library.

## What I built

AetherBus is a Python library + reference broker that gives AI agents a Kafka-style pub/sub layer where **events carry pointers to context, not the context itself**. Producers publish CloudEvents 1.0 envelopes with a `dataref` URI; consumers resolve the actual payload on demand from `mcp://`, `vec://`, `git://`, `file://`, or `https://`.

Three pieces matter most:

**The MCP bridge.** `aetherbus-mcp serve --topic ...` is an MCP server that subscribes to bus topics and emits `notifications/resources/updated` on every event. Drop it into Claude Desktop's `claude_desktop_config.json` or Claude Code's `.mcp.json` — the bus stream shows up as a live MCP resource. The model decides when to call `resources/read`. Zero AetherBus-specific code on the client side.

**The replay UI.** A loopback FastAPI backend + React SPA. Click a stream, watch events flow live, click any event for the full envelope + resolved payload. Sandbox-replay history into a separate broker for fork-from-here debugging. Events keep their original IDs — the audit trail survives a fork. This is what every "I need to reproduce that bug" workflow looks like when you don't have it.

**The framework adapters.** `aetherbus.adapters.{langgraph,crewai,openai_agents}` — three thin shims, under 100 LOC each. LangGraph node that publishes state; CrewAI Flow decorator that subscribes; OpenAI Agents SDK tool that publishes. The bus drops into any of the three dominant agent frameworks without glue code.

The whole thing is a convention layer over [CloudEvents 1.0](https://github.com/cloudevents/spec) + [AsyncAPI 3.0](https://www.asyncapi.com/) + MCP. No new wire protocol. NATS JetStream as the default transport; Kafka adapter for teams that already run it. Both pass the same conformance suite.

## What I'm not building

I'm not building [AGNTCY](https://agntcy.org/) or [SLIM](https://github.com/agntcy/slim). Those are protocol-rigor projects targeting enterprise / internet-scale agent routing. AetherBus is the developer-experience layer: decorator API, `Bus.from_env()`, `pip install aetherbus`. AGNTCY can be a transport under AetherBus; that's a v2 conversation.

I'm not replacing [Google's A2A](https://github.com/google/A2A). A2A is RPC — agent calls agent. AetherBus is pub/sub — many-to-many, replayable. Complementary, not competitive. A2A task completions can publish to AetherBus; AetherBus events can trigger A2A tasks.

I'm not solving identity or end-to-end encryption. The v1 hands those off to broker-level ACLs. v2 will bind producer identity to A2A Agent Cards. There's only so much to ship in a v1.

## See it

[github.com/ShazebA/aetherbus](https://github.com/ShazebA/aetherbus). The README leads with a 30-second GIF of the replay UI; the [`examples/`](https://github.com/ShazebA/aetherbus/tree/main/examples) directory has a working publisher, a CrewAI flow listener, a LangGraph node, and an OpenAI Agents tool. `docker compose up -d nats && python examples/poke.py` and you're publishing CloudEvents in 20 seconds.

If MCP `resources/subscribe` was already implemented everywhere, AetherBus wouldn't need to exist. It isn't, and so it does.

---

*[Show HN discussion](#) · [Source](https://github.com/ShazebA/aetherbus) · [Docs](https://aetherbus.dev/)*
