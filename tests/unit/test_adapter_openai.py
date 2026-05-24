"""OpenAI Agents SDK adapter.

The ``openai-agents`` SDK isn't a dev dependency, so the success path is gated
behind ``importlib.util.find_spec("agents")``. The import-error path is
always testable and is the most actionable signal for a user.
"""
from __future__ import annotations

import importlib.util

import pytest

_AGENTS_INSTALLED = importlib.util.find_spec("agents") is not None


def test_as_tool_raises_when_agents_sdk_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """If the [openai] extra isn't installed, the error message points at it."""
    import sys

    # Force the import inside as_tool() to fail even if the SDK is installed.
    monkeypatch.setitem(sys.modules, "agents", None)

    from aetherbus.adapters.openai_agents import as_tool

    class _FakeBus:
        async def publish(self, topic, *, payload):
            return "evt-1"

    with pytest.raises(ImportError) as ei:
        as_tool(_FakeBus(), "t.v1")
    msg = str(ei.value)
    assert "[openai] extra" in msg
    assert "aetherbus[openai]" in msg


@pytest.mark.skipif(not _AGENTS_INSTALLED, reason="openai-agents SDK not installed")
@pytest.mark.asyncio
async def test_as_tool_publishes_payload_when_sdk_present() -> None:
    from aetherbus.adapters.openai_agents import as_tool

    class _FakeBus:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict]] = []

        async def publish(self, topic, *, payload):
            self.calls.append((topic, payload))
            return "evt-1"

    bus = _FakeBus()
    tool = as_tool(bus, "code.diff.v1", name="emit_diff")
    # The function_tool wrapper exposes the underlying callable; the SDK
    # invokes it with the model's JSON arguments. Call directly here.
    # Different SDK versions stash the callable under different attrs; try
    # the common ones.
    underlying = getattr(tool, "on_invoke_tool", None) or getattr(tool, "func", None) or tool
    if not callable(underlying):
        pytest.skip("openai-agents SDK version exposes the tool callable under an unexpected attr")
    result = await underlying({"a": 1})
    assert result == "evt-1"
    assert bus.calls == [("code.diff.v1", {"a": 1})]
