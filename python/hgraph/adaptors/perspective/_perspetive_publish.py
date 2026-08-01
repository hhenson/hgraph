# Backward-compatible import for the misspelling in Python hgraph.
from ._perspective_publish import *
from ._perspective_publish import __all__ as __all__  # noqa: F401 — mirror the real module's surface
