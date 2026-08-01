# try:
import perspective

assert tuple(map(int, perspective.__version__.split("."))) >= (2, 10, 1)

from ._perspective import *
from ._perspetive_publish import *
from ._perspective_adaptor import *

# except:
#     pass

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
