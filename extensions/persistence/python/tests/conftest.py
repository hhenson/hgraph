"""Canonical suite configuration.

The repository enables the Polars compatibility boundary for normal source-tree
use. Most tests assert the canonical Arrow representation directly, so collect
them with that compatibility veneer disabled. Dedicated Polars tests enable it
explicitly and separately verify the repository default in a clean subprocess.
"""

import os


os.environ["HGRAPH_POLARS_FRAMES"] = "false"


import pytest


@pytest.fixture(autouse=True)
def _global_state_scope():
    """Give every test one GlobalState spanning its configuration and its runs.

    These tests configure record/replay with the compatibility setters and then
    wire a graph, which only works if both see the same state. The state exists
    only inside a scope, and it is the caller who owns that scope -- a runner
    that opened its own would close it before the next call, and the
    configuration would be lost between them.
    """
    import hgraph as hg

    with hg.GlobalState():
        yield
