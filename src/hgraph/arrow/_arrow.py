from contextlib import nullcontext
from datetime import datetime
from functools import partial
from typing import Generic, TypeVar, Callable

from hgraph import MIN_ST, MAX_ET, WiringNodeClass
from hgraph._operators import (
    nothing,
    const,
)
from hgraph._impl._operators._record_replay_in_memory import (
    record_to_memory,
    get_recorded_value,
    replay_from_memory,
    set_replay_values,
    SimpleArrayReplaySource,
)
from hgraph._types import (
    TIME_SERIES_TYPE,
    TIME_SERIES_TYPE_1,
    TIME_SERIES_TYPE_2,
    OUT,
    TS,
    TSL,
    Size,
    TSB,
    TimeSeriesSchema,
    clone_type_var,
)
from hgraph._runtime import (
    evaluate_graph,
    GraphConfiguration,
    GlobalState,
)
from hgraph._wiring._decorators import graph, operator
from hgraph._wiring._wiring_port import WiringPort

__all__ = (
    "arrow",
    "identity",
    "i",
    "null",
    "eval_",
)

A: TypeVar = clone_type_var(TIME_SERIES_TYPE, "A")
B: TypeVar = clone_type_var(TIME_SERIES_TYPE, "B")
C: TypeVar = clone_type_var(TIME_SERIES_TYPE, "C")
D: TypeVar = clone_type_var(TIME_SERIES_TYPE, "D")


class _Arrow(Generic[A, B]):
    """Arrow function wrapper exposing support for piping operations"""

    def __init__(self, fn: Callable[[A], B], __name__=None):
        # Avoid unnecessary nesting of wrappers
        if isinstance(fn, _Arrow):
            self.fn = fn.fn
        else:
            self.fn = fn
        self._name = __name__ if __name__ is not None else \
            str(fn) if isinstance(fn, (_Arrow, _ArrowInput)) else \
                fn.signature.name if isinstance(fn, WiringNodeClass) else \
                    fn.__name__

    def __rshift__(self, other: "Arrow[B, C]") -> "Arrow[A, C]":
        """
        Support piping values through arrow functions, this is a chaining operator.
        The usage is:

        ::

            (f >> g)(x)

        Which is equivalent to:

        ::

            g(f(x))

        """
        # Unwrap the fn from the arrow wrapper
        if isinstance(other, _Arrow):
            fn = other.fn
            other_name = str(other)
        else:
            fn = other
            other_name = str(other.__name__)
        # Now re-wrap the logic
        name = f"{self} >> {other_name}"
        return _Arrow(lambda x: fn(self.fn(x)), __name__=name)

    def __lshift__(self, other: "Arrow[A, B]") -> "Arrow[A, B]":
        """
        Support binding an arrow function result to the stream.
        The usage is:
        ::

             ... >> op << op_2 ...

        This is equivalent to:
        ::

            ... >> i / op_2 >> op ...

        It helps with readability when what we are describing is a binary function and we wish to place
        the second argument to the right which can make the flow more readable.
        such as for example:
        ::

            ,,, >> eq_ << const_(10) >> ...

        """
        return arrow(i / arrow(other) >> self, __name__=f"{self} << {other}")

    def __floordiv__(self, other: "Arrow[C, D]") -> "Arrow[tuple[A, B], tuple[C, D]]":
        """
        Consumes a tuple of inputs applying the first to the first function and the second to the second function.
        This is used as follows:

        ::

            pair > f // g

        """
        f = self.fn
        if isinstance(other, _Arrow):
            g = other.fn
            other_name = str(other)
        else:
            g = other
            other_name = str(other.__name__)
        name = f"{self} // {other_name}"
        return _Arrow(lambda pair, _f=f, _g=g, _name=name: _make_tuple(_f(pair[0]), _g(pair[1])), __name__=name)

    def __truediv__(self, other):
        """
        Takes a single input and applies to the functions supplied.

        ::

            ts > f / g

        results in:

        ::

            make_tuple(f(ts), g(ts))

        """
        f = self.fn
        if isinstance(other, _Arrow):
            g = other.fn
            other_name = str(other)
        else:
            g = other
            other_name = str(other.__name__)
        name = f"{self} // {other_name}"
        return _Arrow(lambda x, _f=f, _g=g: _make_tuple(_f(x), _g(x)), __name__=name)

    def __pos__(self):
        """
        Apply only the first tuple input to this arrow function
        """
        return _Arrow(lambda pair: self.fn(pair[0]), __name__=f"+{self._name}")

    def __neg__(self):
        """
        Apply only the second tuple input to this arrow function
        """
        return _Arrow(lambda pair: self.fn(pair[1]), __name__=f"-{self._name}")

    def __call__(self, value: A) -> B:
        return self.fn(value)

    def __str__(self):
        return self._name


class _ArrowInput(Generic[A]):

    def __init__(self, ts: A, __name__=None):
        if isinstance(ts, _ArrowInput):
            self.ts = ts.ts
        else:
            self.ts = ts
        self._name = __name__ if __name__ is not None else str(ts)

    def __or__(self, other: "Arrow[A, B]") -> B:
        if not isinstance(other, _Arrow):
            raise TypeError(f"Expected Arrow function change, got {type(other)}")
        return other(self.ts)

    def __str__(self):
        return self._name


class _EvalArrowInput:

    def __init__(
            self,
            first,
            second=None,
            type_map: tuple = None,
            start_time: datetime = MIN_ST,
            end_time: datetime = MAX_ET,
    ):
        self.first = first
        self.second = second
        self.type_map = type_map if type_map is None or type(type_map) is tuple else (type_map,)
        self.start_time = start_time
        self.end_time = end_time

    def __or__(self, other: "Arrow[A, B]") -> B:
        # Evaluate the other function call passing in the value captured in this
        # Input.
        @graph
        def g():
            # For now use the limited support in arrow
            values = _build_inputs(self.first, self.second, self.type_map, 0, self.start_time)
            out = other(values)
            if out is not None:
                record_to_memory(out)
            else:
                # Place nothing into the buffer
                record_to_memory(nothing[TS[int]]())

        with GlobalState() if GlobalState._instance is None else nullcontext():
            evaluate_graph(g, GraphConfiguration(start_time=self.start_time, end_time=self.end_time))
            results = get_recorded_value()
        return [result[1] for result in results]


def _build_inputs(
        first,
        second=None,
        type_map: tuple = None,
        level: int = 0,
        start_time: datetime = MIN_ST,
):
    if type(first) is tuple:
        first = _build_inputs(*first, type_map=type_map[0] if type_map else None, level=level + 1)
    elif isinstance(first, list):
        set_replay_values(ts_arg := f"{level}:0", SimpleArrayReplaySource(first, start_time=start_time))
        first = replay_from_memory(ts_arg, TS[type(first[0])] if type_map is None else type_map[0])
    else:
        first = const(first) if type_map is None else const(first, type_map[0])

    if second is None:
        return first
    elif type(second) is tuple:
        second = _build_inputs(*second, type_map=type_map[1] if type_map else None, level=level + 1)
    elif isinstance(second, list):
        set_replay_values(ts_arg := f"{level}:1", SimpleArrayReplaySource(second, start_time=start_time))
        second = replay_from_memory(ts_arg, TS[type(second[0])] if type_map is None else type_map[1])
    else:
        second = const(second) if type_map is None else const(second, type_map[1])
    return make_tuple(first, second).ts


def eval_(
        first,
        second=None,
        type_map: tuple = None,
        start_time: datetime = MIN_ST,
        end_time: datetime = MAX_ET,
):
    """
    Wraps inputs to the graph that can be used to evaluate the graph
    in simulation mode. If the values are lists then the input is assumed
    to be a time-series of values to feed the graph with, otherwise it is assumed
    that the values are constants.

    For this to work correctly, tuples must be used to represent tuples.
    lists are then used to express the inputs.

    If the types of the inputs are not just TS[SCALAR], then the user
    must supply the appropriate types to use for each input stream.
    """
    return _EvalArrowInput(first, second, type_map, start_time, end_time)


def arrow(
        input_: Callable[[A], B] | A = None,
        input_2: C = None,
        __name__=None
) -> _Arrow[A, B] | _ArrowInput[A] | _ArrowInput[tuple[A, C]]:
    """
    Converts the supplied graph / node / time-series value into an arrow suitable wrapper.
    This allows monoid functions to be used in a chainable manner as well as to wrap
    inputs to the chain.
    For example:

    ::

        my_ts: TS[int] = ...
        result = arrow(my_ts) > arrow(lambda x: x*3) >> arrow(lambda x: x+5)

    """
    if input_ is None:
        return partial(arrow, __name__=__name__)
    if input_2 is not None:
        # Then input_ must be a TimeSeries or _ArrowInput
        return make_tuple(input_, input_2)
    if isinstance(input_, _Arrow):
        return input_ if __name__ is None else _Arrow(input_.fn, __name__=__name__)
    if isinstance(input_, _ArrowInput):
        return input_ if __name__ is None else _ArrowInput(input_.ts, __name__=__name__)
    elif isinstance(input_, WiringPort):
        return _ArrowInput(input_, __name__=__name__)
    elif isinstance(input_, tuple):
        return _ArrowInput(make_tuple(*input_), __name__=__name__)
    elif callable(input_):
        return _Arrow(input_, __name__=__name__)
    else:
        # Assume this is a constant and attempt to convert to a const
        return _ArrowInput(const(input_), __name__=__name__)


def make_tuple(ts1: TIME_SERIES_TYPE_1, ts2: TIME_SERIES_TYPE_2, __name__=None):
    """
    Makes an arrow tuple input. An arrow input is a value that can be piped into an arrow function chain
    using the ``>`` operator.
    """
    # Unpack time-series values if they are already arrow inputs
    if isinstance(ts1, tuple):
        # Assume we are creating nested tuples
        ts1 = make_tuple(*ts1, __name__=None if __name__ is None else f"{__name__}[0]")
    if isinstance(ts2, tuple):
        ts2 = make_tuple(*ts2, __name__=None if __name__ is None else f"{__name__}[1]")
    if isinstance(ts1, _ArrowInput):
        ts1 = ts1.ts
    if isinstance(ts2, _ArrowInput):
        ts2 = ts2.ts
    if not isinstance(ts1, WiringPort):
        ts1 = const(ts1)
    if not isinstance(ts2, WiringPort):
        ts2 = const(ts2)
    if isinstance(ts1, WiringPort) and isinstance(ts2, WiringPort):
        # Create the tuple and then return the result wrapped as an _ArrowInput to allow this to create
        # a left to right application of values.
        return _ArrowInput(_make_tuple(ts1, ts2), __name__=__name__)
    else:
        raise TypeError(f"Expected TimeSeriesInput's, got {type(ts1)} and {type(ts2)}")


@operator
def _make_tuple(ts1: TIME_SERIES_TYPE_1, ts2: TIME_SERIES_TYPE_2) -> OUT:
    """
    Create a tuple from the two time-series values.
    If the types are the same OUT will be TSL[TIME_SERIES_TYPE, Size[2]]
    If the types are different OUT will be TSB[{"ts1": TIME_SERIES_TYPE_1, "ts2": TIME_SERIES_TYPE_2}]

    The idea is that tuples are also composable, so it is possible to create construct reasonable complex
    input streams as a collection of tuples, of tuples.
    """


@graph(overloads=_make_tuple)
def _make_tuple_tsl(ts1: TIME_SERIES_TYPE, ts2: TIME_SERIES_TYPE) -> TSL[TIME_SERIES_TYPE, Size[2]]:
    """When both input types match return a TSL"""
    from hgraph._operators import combine

    return combine[TSL](ts1, ts2)


class _TupleSchema(TimeSeriesSchema, Generic[TIME_SERIES_TYPE_1, TIME_SERIES_TYPE_2]):
    ts1: TIME_SERIES_TYPE_1
    ts2: TIME_SERIES_TYPE_2


@graph(overloads=_make_tuple, requires=lambda m, s: m[TIME_SERIES_TYPE_1] != m[TIME_SERIES_TYPE_2])
def _make_tuple_tsb(
        ts1: TIME_SERIES_TYPE_1, ts2: TIME_SERIES_TYPE_2
) -> TSB[_TupleSchema[TIME_SERIES_TYPE_1, TIME_SERIES_TYPE_2]]:
    """When the input types do not match return a TSB"""
    from hgraph._operators import combine

    return combine[TSB](ts1=ts1, ts2=ts2)


@arrow(__name__="i")
def identity(x):
    """The identity function, does nothing."""
    return x


i = identity


@arrow
def null(x):
    return nothing(x.output_type.py_type)
