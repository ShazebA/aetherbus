"""AetherBus — event-driven context layer for AI agents.

See PRD_AetherBus.md for the v1 specification.
"""

from .bus import Bus
from .event import Event, EventPayload, Handler

__version__ = "0.1.0"

__all__ = ["Bus", "Event", "EventPayload", "Handler", "__version__"]
