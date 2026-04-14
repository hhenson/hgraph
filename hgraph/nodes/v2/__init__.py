from hgraph._feature_switch import is_feature_enabled

__all__ = ("const", "debug_print", "nothing", "null_sink")


if is_feature_enabled("use_cpp"):
    import hgraph._hgraph as _hgraph

    const = _hgraph.v2.const
    debug_print = _hgraph.v2.debug_print
    nothing = _hgraph.v2.nothing
    null_sink = _hgraph.v2.null_sink
else:
    from hgraph._operators._debug_tools import debug_print
    from hgraph._operators._graph_operators import nothing, null_sink
    from hgraph._operators._time_series_conversion import const
