"""Framework adapters — thin shims that wire AetherBus into existing agent libs.

Each submodule targets one framework and is gated by an optional extra:

- :mod:`aetherbus.adapters.langgraph` — needs ``aetherbus[langgraph]``
- :mod:`aetherbus.adapters.crewai` — needs ``aetherbus[crewai]``
- :mod:`aetherbus.adapters.openai_agents` — needs ``aetherbus[openai]``

Submodules are NOT imported eagerly here so installs without the framework
extras don't fail at ``import aetherbus``. Import the specific adapter you
need; the import surfaces a clear ``ImportError`` with the right install
hint if the framework isn't on the path.
"""
