"""Optional Perspective table publishing.

Importing this package does not import ``perspective``. The client is loaded
only when a real table or web endpoint is requested; tests and applications
may inject a compatible client through :class:`PerspectiveTablesManager`.
"""

from ._perspective import *
from ._perspective_publish import *
from ._perspective_adaptor import *

__all__ = [
    "PerspectiveTablesManager",
    "perspective_web",
    "_publish_table",
    "_receive_table_edits",
    "TableEdits",
    "publish_table",
    "publish_table_editable",
    "publish_multitable",
    "publish_table_impl",
    "publish_table_editable_impl",
    "publish_multitable_impl",
    "register_perspective_adaptors",
]
