"""Collection gate for the released adaptor suite.

The HTTP and WebSocket server modules are re-exports of ``hgraph_web.compat``
and raise on import without the optional hgraph-web distribution, so every
module here is unimportable then.  Skip collecting them rather than failing.
"""

try:
    import hgraph_web  # noqa: F401
except ImportError:
    collect_ignore_glob = ["test_*.py"]
