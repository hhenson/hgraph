"""A naive dmap_ prototype validating RFC 0037's protocol shape.

Deliberately partitioned from the main tree: nothing here is imported by
``python/hgraph``, collected by pytest (``testpaths = ["python/tests"]``) or
packaged into the wheel (``wheel.packages = ["python/hgraph"]``).
"""

from .node import dmap_, dmap_shared_
from .protocol import Dispatch, Result, partition_of

__all__ = ["dmap_", "dmap_shared_", "Dispatch", "Result", "partition_of"]
