from hgraph import operator_function

# Defaults and optional call shapes are registered as native overloads, so
# these remain normal subscriptable wiring operators rather than Python nodes.
as_array = operator_function("as_array")
get_item = operator_function("get_item")
cumsum = operator_function("cumsum")
corrcoef = operator_function("corrcoef")
quantile = operator_function("quantile")

__all__ = ("as_array", "get_item", "cumsum", "corrcoef", "quantile")
