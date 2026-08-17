"""Collection gate for the released adaptor suite.

The supported install path (the ``hgraph[web]`` extra) brings hgraph-web,
whose ``hgraph_web.compat`` implements the released HTTP/WebSocket server
surface these suites exercise.  In a development checkout without the
extension built there is nothing to test against — the server modules raise
their pointed install error on import — so skip collection rather than fail;
never install this gate to mask a broken supported install.
"""

try:
    import hgraph_web  # noqa: F401
except ImportError:
    collect_ignore_glob = ["test_*.py"]
