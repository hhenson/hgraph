from hgraph._wiring._decorators import graph, compute_node
from hgraph._wiring._wiring import WiringPort, WiringError
from hgraph._types import TIME_SERIES_TYPE, TS

__all__ = ("add_", "sub_", "mul_", "div_")


@graph
def add_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    raise WiringError(f"operator add_ is not implemented for {lhs.output_type} and {rhs.output_type}")


WiringPort.__add__ = lambda x, y: add_(x, y)
WiringPort.__radd__ = lambda x, y: add_(y, x)


@graph
def sub_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    raise WiringError(f"operator sub_ is not implemented for {lhs.output_type} and {rhs.output_type}")


WiringPort.__sub__ = lambda x, y: sub_(x, y)
WiringPort.__rsub__ = lambda x, y: sub_(y, x)


@graph
def mul_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    raise WiringError(f"operator mul_ is not implemented for {lhs.output_type} and {rhs.output_type}")


WiringPort.__mul__ = lambda x, y: mul_(x, y)
WiringPort.__rmul__ = lambda x, y: mul_(y, x)


@graph
def div_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    raise WiringError(f"operator div_ is not implemented for {lhs.output_type} and {rhs.output_type}")


WiringPort.__truediv__ = lambda x, y: div_(x, y)
WiringPort.__rtruediv__ = lambda x, y: div_(y, y)


@compute_node
def eq_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TS[bool]:
    return lhs.value == rhs.value


WiringPort.__eq__ = lambda x, y: eq_(x, y)
