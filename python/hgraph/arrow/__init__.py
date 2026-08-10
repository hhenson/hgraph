"""The arrow combinator DSL.

The implementation remains a Python authoring convenience over ordinary
hgraph wiring. ``__all__`` is assembled explicitly so API inventories and
type tooling see the same surface as wildcard imports.
"""
from hgraph.arrow._arrow import *
from hgraph.arrow._arrow import __all__ as _arrow_all
from hgraph.arrow._control_flow import *
from hgraph.arrow._control_flow import __all__ as _control_flow_all
from hgraph.arrow._pair_operators import *
from hgraph.arrow._pair_operators import __all__ as _pair_operators_all
from hgraph.arrow._std_operators import *
from hgraph.arrow._std_operators import __all__ as _std_operators_all
from hgraph.arrow._test_operators import *
from hgraph.arrow._test_operators import __all__ as _test_operators_all

__all__ = tuple(dict.fromkeys((
    *_arrow_all,
    *_control_flow_all,
    *_pair_operators_all,
    *_std_operators_all,
    *_test_operators_all,
)))
