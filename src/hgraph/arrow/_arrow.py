from contextlib import nullcontext
from dataclasses import dataclass
from typing import Generic, TypeVar, Callable

from hgraph._operators import (
    nothing,
    const,
)
from hgraph._impl._operators._record_replay_in_memory import record_to_memory, get_recorded_value
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
    STATE,
    CompoundScalar,
)
from hgraph._runtime import (
    evaluate_graph,
    GraphConfiguration,
    GlobalState,
)
from hgraph._wiring._decorators import graph, operator, compute_node
from hgraph._wiring._wiring_port import WiringPort

__all__ = ("arrow", "first", "second", "swap", "apply_", "assoc", "identity", "i")

A: TypeVar = clone_type_var(TIME_SERIES_TYPE, "A")
B: TypeVar = clone_type_var(TIME_SERIES_TYPE, "B")
C: TypeVar = clone_type_var(TIME_SERIES_TYPE, "C")
D: TypeVar = clone_type_var(TIME_SERIES_TYPE, "D")


class _Arrow(Generic[A, B]):
    """Arrow function wrapper exposing support for piping operations"""

    def __init__(self, fn: Callable[[A], B]):
        # Avoid unnecessary nesting of wrappers
        if isinstance(fn, _Arrow):
            self.fn = fn.fn
        else:
            self.fn = fn

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
        else:
            fn = other
        # Now re-wrap the logic
        return _Arrow(lambda x: fn(self.fn(x)))

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
        else:
            g = other
        return _Arrow(lambda pair, _f=f, _g=g: _make_tuple(_f(pair[0]), _g(pair[1])))

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
        else:
            g = other
        return _Arrow(lambda x, _f=f, _g=g: _make_tuple(_f(x), _g(x)))

    def __call__(self, value: A) -> B:
        return self.fn(value)


class _ArrowInput(Generic[A]):

    def __init__(self, ts: A):
        if isinstance(ts, _ArrowInput):
            self.ts = ts.ts
        else:
            self.ts = ts

    def __or__(self, other: "Arrow[A, B]") -> B:
        if not isinstance(other, _Arrow):
            raise TypeError(f"Expected Arrow function change, got {type(other)}")
        return other(self.ts)


class _ConstArrowInput:

    def __init__(self, first, second=None):
        self.first = first
        self.second = second

    def __or__(self, other: "Arrow[A, B]") -> B:
        # Evaluate the other function call passing in the value captured in this
        # Input.
        @graph
        def g():
            # For now use the limited support in arrow
            values = arrow(self.first, self.second).ts
            out = other(values)
            if out is not None:
                record_to_memory(out)
            else:
                # Place nothing into the buffer
                record_to_memory(nothing[TS[int]]())

        with GlobalState() if GlobalState._instance is None else nullcontext():
            evaluate_graph(g, GraphConfiguration())
            results = get_recorded_value()
        return [result[1] for result in results]


def eval_(first, second=None):
    """
    Wraps inputs to the graph that can be used to evaluate the graph
    in simulation mode. If the values are lists then the input is assumed
    to be a time-series of values to feed the graph with, otherwise it is assumed
    that the values are constants.

    For this to work correctly, tuples must be used to represent tuples.
    lists are then used to express the inputs.
    """
    return _ConstArrowInput(first, second)


def arrow(input_: Callable[[A], B] | A, input_2: C = None) -> _Arrow[A, B] | _ArrowInput[A] | _ArrowInput[tuple[A, C]]:
    """
    Converts the supplied graph / node / time-series value into an arrow suitable wrapper.
    This allows monoid functions to be used in a chainable manner as well as to wrap
    inputs to the chain.
    For example:

    ::

        my_ts: TS[int] = ...
        result = arrow(my_ts) > arrow(lambda x: x*3) >> arrow(lambda x: x+5)

    """
    if input_2 is not None:
        # Then input_ must be a TimeSeries or _ArrowInput
        return make_tuple(input_, input_2)
    if isinstance(input_, _Arrow) or isinstance(input_, _ArrowInput):
        return input_
    elif isinstance(input_, WiringPort):
        return _ArrowInput(input_)
    elif isinstance(input_, tuple):
        return _ArrowInput(make_tuple(*input_))
    elif callable(input_):
        return _Arrow(input_)
    else:
        # Assume this is a constant and attempt to convert to a const
        return _ArrowInput(const(input_))


def make_tuple(ts1: TIME_SERIES_TYPE_1, ts2: TIME_SERIES_TYPE_2):
    """
    Makes an arrow tuple input. An arrow input is a value that can be piped into an arrow function chain
    using the ``>`` operator.
    """
    # Unpack time-series values if they are already arrow inputs
    if isinstance(ts1, tuple):
        # Assume we are creating nested tuples
        ts1 = make_tuple(*ts1)
    if isinstance(ts2, tuple):
        ts2 = make_tuple(*ts2)
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
        return _ArrowInput(_make_tuple(ts1, ts2))
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


@arrow
def first(pair) -> A:
    """
    Returns the first element of a tuple
    """
    return pair[0]


@arrow
def swap(pair):
    """
    Swaps the values in a tuple.
    """
    return _make_tuple(pair[1], pair[0])


@arrow
def second(pair) -> B:
    """Returns the second element of a tuple"""
    return pair[1]


def binary_op(fn):
    """
    A simple arrow wrapper of an HGraph function that takes two args as input.
    Useful to wrap binary operators.
    """
    return arrow(lambda pair, fn_=fn: fn_(pair[0], pair[1]))


@arrow
def assoc(pair):
    """
    Adjust the associativity of a pair.
    Converts ((a, b), c) -> (a, (b, c)).
    """
    return _make_tuple(pair[0][0], _make_tuple(pair[0][1], pair[1]))


def apply_(tp: OUT):
    """
    Applies the function in the first element to the value in the second element.
    The tp is the output type of the function.
    """
    from hgraph import apply

    return arrow(lambda pair, tp_=tp: apply[tp_](pair[0], pair[1]))


@arrow
def identity(x):
    """The identity function, does nothing."""
    return x


i = identity


@dataclass
class _AssertState(CompoundScalar):
    count: int = 0


def assert_(*args, message: str = None):
    """
    Provides a simple assertion operator that can use used for simple tests.
    Provide the sequence of expected values, this acts as a pass through node,
    that will raise an AssertionError if the value does not match the expected value.
    """
    if message is None:
        message = ""
    else:
        message = f": ({message})"

    @compute_node
    def _assert(ts: A, _state: STATE[_AssertState] = None) -> A:
        if (c := _state.count) >= (l := len(args)):
            raise AssertionError(f"Expected {l} ticks, but still getting results{message}")
        expected = args[c]
        _state.count += 1
        if ts.value != expected:
            raise AssertionError(f"Expected '{expected}' but got '{ts.value}' on tick count: {_state.count}{message}")
        return ts.delta_value

    @_assert.stop
    def _assert_stop(_state: STATE[_AssertState]):
        if (l := len(args)) != (c := _state.count):
            raise AssertionError(f"Expected {l} values but got {c} results{message}")

    return arrow(_assert)
