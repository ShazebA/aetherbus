"""Run-centric observability over the bus.

A *run* is every event sharing one ``aetherbuscorrelationid``. :class:`RunIndex`
groups events into runs and serves the run-explorer surface (PRD pivot —
Direction A). The bus stays the capture mechanism; this layer is the read model.
"""

from .run_index import GraphEdge, GraphNode, RunEventRow, RunIndex, RunSummaryRow

__all__ = [
    "GraphEdge",
    "GraphNode",
    "RunEventRow",
    "RunIndex",
    "RunSummaryRow",
]
