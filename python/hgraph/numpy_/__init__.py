"""Native NumPy-shaped operators for hgraph.

``Array`` values retain their declared dimensions in the C++ schema and are
converted to and from ``numpy.ndarray`` at the Python boundary. Evaluation of
the operators exported here remains in the native runtime.
"""

from ._constants import ARRAY, ARRAY_1
from ._operators import as_array, corrcoef, cumsum, get_item, quantile
from ._utils import add_docs, extract_dimensions_from_array, extract_type_from_array

__all__ = (
    "ARRAY",
    "ARRAY_1",
    "add_docs",
    "as_array",
    "corrcoef",
    "cumsum",
    "extract_dimensions_from_array",
    "extract_type_from_array",
    "get_item",
    "quantile",
)
