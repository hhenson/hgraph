"""Perspective publication without importing the optional client eagerly."""

from ._perspective import (
    IndexPageHandler,
    PerspectiveTablesManager,
    TablePageHandler,
    perspective_web,
)
from ._perspective_publish import TableEdits, defaultdbldict
from ._perspective_adaptor import (
    publish_multitable,
    publish_multitable_impl,
    publish_table,
    publish_table_editable,
    publish_table_editable_impl,
    publish_table_impl,
    register_perspective_adaptors,
)

__all__ = [
    "perspective_web",
    "PerspectiveTablesManager",
    "TablePageHandler",
    "IndexPageHandler",
    "TableEdits",
    "defaultdbldict",
    "publish_table",
    "publish_table_editable",
    "publish_multitable",
    "publish_table_impl",
    "publish_table_editable_impl",
    "publish_multitable_impl",
    "register_perspective_adaptors",
]
