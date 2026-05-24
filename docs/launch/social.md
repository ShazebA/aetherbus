# Social posts — r/LocalLLaMA, r/AI_Agents, LinkedIn

## r/LocalLLaMA + r/AI_Agents

Focus: the replay UI. This subreddit responds to demos, not architecture diagrams.

**Title:**

```
I built a replay UI for multi-agent systems — fork-from-here debugging with envelope IDs preserved
```

**Body:**

```
Multi-agent systems are great until something breaks at minute 47 and you have no way to reproduce
the exact event stream that led to the bug. I shipped a thing that does that, plus the bus to
back it.

AetherBus is a Python library + broker:
• Agents publish CloudEvents with a `dataref` URI (claim-check pattern)
• NATS JetStream by default, Kafka adapter for production
• Replay UI scrubs broker history into a sandbox — original event IDs preserved so the audit
  trail survives the fork
• MCP server adapter so Claude Desktop / Cursor / Cline can consume the bus stream natively
  (the bridge nobody else implemented for MCP's `resources/subscribe`)
• Drops into LangGraph, CrewAI Flows, OpenAI Agents SDK in <30 lines

Apache 2.0, runs locally, `docker compose up -d nats` to get started.

Repo: https://github.com/ShazebA/aetherbus
30-second demo GIF: in the README
Long-form why: [blog post link]

Happy to answer questions on the replay flow specifically — that's the part I'm most proud of.
```

**Engagement plan:** post mid-week, US-AM. Be present for 2 hours after. r/AI_Agents lives at ~weekly cadence; r/LocalLLaMA is much faster.

---

## LinkedIn

Career-narrative framing. Audience is Shazeb's network — mix of recruiters, senior ICs, founders. Lead with the synthesis, end with the link.

**Post:**

```
Three things I noticed building agent systems this year:

1. Anthropic's June 2025 multi-agent research post benchmarks the cost: agents use ~15× the
   tokens of single chats. The fix they describe — subagents writing to a shared filesystem,
   returning lightweight references — is the claim-check pattern from enterprise messaging,
   2003 edition.

2. MCP's spec defines `resources/subscribe` and `notifications/resources/updated`, but the
   ecosystem skipped them. Claude Desktop doesn't implement subscribe. FastMCP doesn't. The
   primitive that turns MCP from request/response into a real push channel was just sitting
   there.

3. Every multi-agent team builds the same async-pub/sub layer by hand. Kafka or NATS at the
   bottom, ad-hoc envelope schema in the middle, no replay or audit trail at the top.

So I built the missing piece. AetherBus is a Python library that gives agents Kafka-style
pub/sub with CloudEvents 1.0 envelopes, `dataref` URIs (claim-check, on-demand resolution
from MCP / vector DB / Git / HTTPS / files), an MCP bridge that finally implements
`resources/subscribe`, and a replay UI for fork-from-here debugging. Apache 2.0. Ships with
adapters for LangGraph, CrewAI Flows, and the OpenAI Agents SDK.

The thing I'm most proud of is the replay UI. Production agent run fails — open the UI,
scrub to the failure, click "fork from here," and the same agents re-execute against a
sandbox broker with the original event IDs preserved. That's the workflow every multi-agent
team builds by hand and nobody ships.

Long-form write-up on why this needed to exist, and what AGNTCY / Anthropic / Cognition all
point toward without quite shipping: [blog link]

Source: https://github.com/ShazebA/aetherbus

If you're building multi-agent systems and the "I can't reproduce this bug" sentence has
crossed your mind, this is for you.

#AI #Agents #MCP #Anthropic #OpenSource
```

**Engagement plan:** schedule for Tuesday 9 AM ET. Don't engage with low-effort comments; do reply to anything technical within 4 hours.
