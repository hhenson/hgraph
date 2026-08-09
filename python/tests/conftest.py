"""Canonical suite configuration.

The repository enables the Polars compatibility boundary for normal source-tree
use. Most tests assert the canonical Arrow representation directly, so collect
them with that compatibility veneer disabled. Dedicated Polars tests enable it
explicitly and separately verify the repository default in a clean subprocess.
"""

import os


os.environ["HGRAPH_POLARS_FRAMES"] = "false"
