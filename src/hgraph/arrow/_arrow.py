from contextlib import nullcontext
from datetime import datetime
from functools import partial, wraps
from typing import Generic, TypeVar, Callable

from hgraph import (
    MIN_ST,
    MAX_ET,
    WiringNodeClass,
    HgTimeSeriesTypeMetaData,
    AUTO_RESOLVE,
    null_sink,
    is_subclass_generic,
    compute_node,
    WiringGraphContext,
    TS_SCHEMA,
    EvaluationMode,
)
from hgraph._impl._operators._record_replay_in_memory import (
    record_to_memory,
    get_recorded_value,
    replay_from_memory,
    set_replay_values,
    SimpleArrayReplaySource,
)
from hgraph._operators import (
    nothing,
    const,
)
from hgraph._runtime import (
    evaluate_graph,
    GraphConfiguration,
    GlobalState,
)
from hgraph._types import (
    TIME_SERIES_TYPE,
    TIME_SERIES_TYPE_1,
    TIME_SERIES_TYPE_2,
    TS,
    TSB,
    TimeSeriesSchema,
    clone_type_var,
)
from hgraph._wiring._decorators import graph
from hgraph._wiring._wiring_port import WiringPort

__all__ = (
    "arrow",
    "identity",
    "i",
    "null",
    "eval_",
    "make_pair",
    "PairSchema",
    "Pair",
    "extract_delta_value",
    "extract_value",
    "convert_pairs_to_delta_tuples",
    "convert_pairs_to_tuples",
    "a",
)

A: TypeVar = clone_type_var(TIME_SERIES_TYPE, "A")
B: TypeVar = clone_type_var(TIME_SERIES_TYPE, "B")
C: TypeVar = clone_type_var(TIME_SERIES_TYPE, "C")
D: TypeVar = clone_type_var(TIME_SERIES_TYPE, "D")


class _Arrow(Generic[A, B]):
    """Arrow function wrapper exposing support for piping operations"""

    def __init__(
        self, fn: Callable[[A], B], __name__=None, __has_side_effects__=False, bound_args=None, bound_kwargs=None
    ):
        # Avoid unnecessary nesting of wrappers
        self._bound_args = bound_args if bound_args is not None else ()
        self._bound_kwargs = bound_kwargs if bound_kwargs is not None else {}
        self._has_side_effects = __has_side_effects__
        if isinstance(fn, _Arrow):
            self._fn = fn.fn
        else:
            self._fn = fn
        self._name = (
            __name__
            if __name__ is not None
            else (
                str(fn)
                if isinstance(fn, (_Arrow, _ArrowInput))
                else fn.signature.name if isinstance(fn, WiringNodeClass) else fn.__name__
            )
        )

    @property
    def fn(self):
        # Calling fn will finalise the structure
        f = _wrap_for_side_effects(self._fn) if self._has_side_effects else self._fn
        if self._bound_args or self._bound_kwargs:
            return partial(f, *self._bound_args, **self._bound_kwargs)
        else:
            return f

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
        # First make sure this is wrapped as an arrow function (by calling arrow any additional uplift work is done if necissary)
        other = arrow(other)

        # Unwrap the fn from the arrow wrapper
        # Since we have ensured the other is an arrow we can just go ahead and unwrap
        fn = other.fn
        other_name = str(other)

        # Now re-wrap the logic
        name = f"{self} >> {other_name}"
        return _Arrow(lambda x: fn(self.fn(x)), __name__=name)

    def __rrshift__(self, other):
        """
        If the first item is not an arrow function then we try and catch if the second is. This won't help with
        chains of non-arrow functions, but will catch the odd scenario where the first is a normal function.
        """
        return arrow(other) >> self

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

    def __rlshift__(self, other):
        """Catch the case where the left is a normal function"""
        return arrow(other) << self

    def __floordiv__(self, other: "Arrow[C, D]") -> "Arrow[tuple[A, B], tuple[C, D]]":
        """
        Consumes a tuple of inputs applying the first to the first function and the second to the second function.
        This is used as follows:

        ::

            pair > f // g

        """
        other = arrow(other)
        f = self.fn
        g = other.fn
        other_name = str(other)
        name = f"{self} // {other_name}"
        return _Arrow(lambda pair, _f=f, _g=g, _name=name: make_pair(_f(pair[0]), _g(pair[1])), __name__=name)

    def __truediv__(self, other):
        """
        Takes a single input and applies to the functions supplied.

        ::

            ts > f / g

        results in:

        ::

            make_tuple(f(ts), g(ts))

        """
        other = arrow(other)
        f = self.fn
        g = other.fn
        other_name = str(other)
        name = f"{self} // {other_name}"
        return _Arrow(lambda x, _f=f, _g=g: make_pair(_f(x), _g(x)), __name__=name)

    def __pos__(self):
        """
        Apply only the first tuple input to this arrow function
        """
        return _Arrow(lambda pair: make_pair(self.fn(pair[0]), pair[1]), __name__=f"+{self._name}")

    def __neg__(self):
        """
        Apply only the second tuple input to this arrow function
        """
        return _Arrow(lambda pair: make_pair(pair[0], self.fn(pair[1])), __name__=f"-{self._name}")

    def __call__(self, *args, **kwargs) -> B:
        if len(args) == 1 and len(kwargs) == 0 and isinstance(args[0], WiringPort):
            # We are not binding and are in-fact processing now
            f = _wrap_for_side_effects(self._fn) if self._has_side_effects else self._fn
            # We need to be consistent with a bound function, where the input value is supplied last
            return f(*self._bound_args, args[0], **self._bound_kwargs)
        else:
            return _Arrow(
                self._fn,
                __name__=self._name,
                bound_args=args,
                bound_kwargs=kwargs,
                __has_side_effects__=self._has_side_effects,
            )

    def __str__(self):
        return self._name


def _wrap_for_side_effects(fn):
    """
    Ensure that the wrapped function is not wired out if the results are not used. This is because we have marked the
    function as having side effects, which is not normally the case, but for some special cases it is.
    """

    @wraps(fn)
    def _wrapper(*args, **kwargs):
        result = fn(*args, **kwargs)
        if result is not None:
            null_sink(result)
        return result

    return _wrapper


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
        trace: bool | dict = False,
        trace_wiring: bool | dict = False,
        profile: bool | dict = False,
        run_mode: EvaluationMode = EvaluationMode.SIMULATION,
    ):
        self.first = first
        self.second = second
        self.type_map = type_map if type_map is None or type(type_map) is tuple else (type_map,)
        self.start_time = start_time
        self.end_time = end_time
        self.trace = trace
        self.trace_wiring = trace_wiring
        self.profile = profile
        self.run_mode = run_mode

    def __or__(self, other: "Arrow[A, B]") -> B:
        # Evaluate the other function call passing in the value captured in this
        # Input.
        @graph
        def g():
            # For now use the limited support in arrow
            values = _build_inputs(self.first, self.second, self.type_map, 0, self.start_time)
            out = other(values)
            if out is not None:
                out = convert_pairs_to_delta_tuples(out)
                record_to_memory(out)
            else:
                # Place nothing into the buffer
                record_to_memory(nothing[TS[int]]())

        with GlobalState() if not GlobalState.has_instance() else nullcontext():
            evaluate_graph(
                g,
                GraphConfiguration(
                    start_time=self.start_time,
                    end_time=self.end_time,
                    trace=self.trace,
                    trace_wiring=self.trace_wiring,
                    run_mode=self.run_mode,
                ),
            )
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
    return _make_pair(first, second).ts


def eval_(
    first,
    second=None,
    type_map: tuple = None,
    start_time: datetime = MIN_ST,
    end_time: datetime = MAX_ET,
    trace: bool | dict = False,
    trace_wiring: bool | dict = False,
    profile: bool | dict = False,
    run_mode: EvaluationMode = EvaluationMode.SIMULATION,
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
    return _EvalArrowInput(first, second, type_map, start_time, end_time, trace, trace_wiring, profile, run_mode)


def arrow(
    input_: Callable[[A], B] | A = None,
    input_2: C = None,
    __name__=None,
    __has_side_effects__=False,
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
        return partial(arrow, __name__=__name__, __has_side_effects__=__has_side_effects__)
    if input_2 is not None:
        # Then input_ must be a TimeSeries or _ArrowInput
        return _make_pair(input_, input_2)
    if isinstance(input_, _Arrow):
        return (
            input_
            if __name__ is None
            else _Arrow(input_.fn, __name__=__name__, __has_side_effects__=__has_side_effects__)
        )
    if isinstance(input_, _ArrowInput):
        return input_ if __name__ is None else _ArrowInput(input_.ts, __name__=__name__)
    elif isinstance(input_, WiringPort):
        return _ArrowInput(input_, __name__=__name__)
    elif isinstance(input_, WiringNodeClass):
        return _Arrow(_flatten_wrapper(input_), __name__=__name__, __has_side_effects__=__has_side_effects__)
    elif isinstance(input_, tuple):
        return _ArrowInput(_make_pair(*input_), __name__=__name__)
    elif callable(input_):
        return _Arrow(input_, __name__=__name__, __has_side_effects__=__has_side_effects__)
    else:
        # Assume this is a constant and attempt to convert to a const
        if WiringGraphContext.instance():
            return _ArrowInput(const(input_), __name__=__name__)
        else:
            # Assume if there is no wiring context then we must be chaining operators
            from hgraph.arrow._std_operators import const_

            return const_(input_)


a = arrow  # Shortcut to mark as arrow


def _make_pair(ts1: TIME_SERIES_TYPE_1, ts2: TIME_SERIES_TYPE_2, __name__=None):
    """
    Makes an arrow tuple input. An arrow input is a value that can be piped into an arrow function chain
    using the ``>`` operator.
    """
    # Unpack time-series values if they are already arrow inputs
    if isinstance(ts1, tuple):
        # Assume we are creating nested tuples
        ts1 = _make_pair(*ts1, __name__=None if __name__ is None else f"{__name__}[0]")
    if isinstance(ts2, tuple):
        ts2 = _make_pair(*ts2, __name__=None if __name__ is None else f"{__name__}[1]")
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
        return _ArrowInput(make_pair(ts1, ts2), __name__=__name__)
    else:
        raise TypeError(f"Expected TimeSeriesInput's, got {type(ts1)} and {type(ts2)}")


class PairSchema(TimeSeriesSchema, Generic[TIME_SERIES_TYPE_1, TIME_SERIES_TYPE_2]):
    first: TIME_SERIES_TYPE_1
    second: TIME_SERIES_TYPE_2


class _PairGenerator:

    def __getitem__(self, item):
        if isinstance(item, tuple):
            if (l := len(item)) == 1:
                item = item[0], item[0]
            if len(item) != 2:
                raise ValueError(f"Expected a tuple of length 2, got {item}")
            first, second = item
            first = self.__getitem__(first) if isinstance(first, tuple) else first
            second = self.__getitem__(second) if isinstance(second, tuple) else second
            return TSB[PairSchema[first, second]]
        else:
            return TSB[PairSchema[item, item]]


Pair = _PairGenerator()


@graph
def make_pair(
    first: TIME_SERIES_TYPE_1,
    second: TIME_SERIES_TYPE_2,
    _first_tp: type[TIME_SERIES_TYPE_1] = AUTO_RESOLVE,
    _second_tp: type[TIME_SERIES_TYPE_2] = AUTO_RESOLVE,
) -> TSB[PairSchema[TIME_SERIES_TYPE_1, TIME_SERIES_TYPE_2]]:
    """
    Create a tuple from the two time-series values.
    If the types are the same OUT will be TSL[TIME_SERIES_TYPE, Size[2]]
    If the types are different OUT will be TSB[{"ts1": TIME_SERIES_TYPE_1, "ts2": TIME_SERIES_TYPE_2}]

    The idea is that tuples are also composable, so it is possible to create construct reasonable complex
    input streams as a collection of tuples, of tuples.
    """
    from hgraph._operators import combine

    return combine[TSB[PairSchema[_first_tp, _second_tp]]](first=first, second=second)


@arrow(__name__="i")
def identity(x):
    """The identity function, does nothing."""
    return x


i = identity  # Expose as both i and identity, use i as a rule, but identity when i would be confusing


@arrow
def null(x):
    """
    The null function, returns nothing and will terminal the received input. This ensures we don't accidentally wire out
    a result.
    """
    null_sink(x)
    return nothing(x.output_type.py_type)


def _flatten_wrapper(node: WiringNodeClass) -> Callable[[A], B]:
    """Attempts to convert the inputs to match the signature of the node"""

    @wraps(node.fn)
    def _wrapper(*args, **kwargs):
        x = args[-1]
        args = args[:-1]
        sz = len(node.signature.time_series_args)
        # Unpack left to right
        if len(node.signature.time_series_args) > 1 and not _MATCH_PAIR.matches(tp := x.output_type.dereference()):
            if not _MATCH_TSB.matches(tp):
                raise ValueError(f"Expected a Pair or TSB but got {tp}")
            kwargs |= x.as_dict()
        else:
            args = _unpack(x, sz) + args

        try:
            return node(*args, **kwargs)
        except:
            print(f"Failed to call {node} with {args} and {kwargs}")
            raise

    return _wrapper


_MATCH_PAIR = HgTimeSeriesTypeMetaData.parse_type(TSB[PairSchema[TIME_SERIES_TYPE_1, TIME_SERIES_TYPE_2]])
_MATCH_TSB = HgTimeSeriesTypeMetaData.parse_type(TSB[TS_SCHEMA])


def _unpack(x: WiringPort, sz: int, _check: bool = True) -> tuple:
    """
    Recursively unpacks the tuple until we have the desired size, or we are not able to do so.
    This unpacks using a depth first search. There is a risk that this will accidentally unpack a TSL or TSB of size
    2 by accident.
    """
    if sz == 0:
        return tuple()

    if sz == 1:
        return (x,)

    if not _MATCH_PAIR.matches(tp := x.output_type.dereference()):
        if _check:
            raise ValueError(f"Expected an Arrow tuple, got {tp}")
        else:
            return (x,)

    if sz == 2:
        return x[0], x[1]
    else:
        left = _unpack(x[0], sz - 1, _check=False)
        right = _unpack(x[1], sz - len(left), _check=False)
    result = left + right
    if _check:
        if len(result) != sz:
            raise ValueError(f"Expected {sz} inputs but was only able to extract {len(result)}")
    return result


def _flatten(x: WiringPort) -> tuple:
    """Expand the input into a tuple of non-pair values."""
    if not _MATCH_PAIR.matches(x.output_type.dereference()):
        return (x,)
    left = _flatten(x[0])
    right = _flatten(x[1])
    return left + right


def extract_value(ts, tp):
    """Extracts the value from a time-series where the type is a pair and returns the value as a tuple."""
    if is_subclass_generic(tp, TSB) and issubclass(tp.__args__[0], PairSchema):
        return (
            extract_value(ts.first, tp.__args__[0].__args__[0]),
            extract_value(ts.second, tp.__args__[0].__args__[1]),
        )
    else:
        return ts.value


def extract_delta_value(ts, tp):
    """Extracts the delta-value from the time-series, for a pair the non-ticked value will be represented as None"""
    if is_subclass_generic(tp, TSB) and issubclass(tp.__args__[0], PairSchema):
        return (
            extract_delta_value(ts.first, tp.__args__[0].__args__[0]) if ts.first.modified else None,
            extract_delta_value(ts.second, tp.__args__[0].__args__[1]) if ts.second.modified else None,
        )
    else:
        return ts.delta_value


@compute_node
def convert_pairs_to_delta_tuples(ts: TIME_SERIES_TYPE, _tp: type[TIME_SERIES_TYPE] = AUTO_RESOLVE) -> TS[object]:
    """Converts the incoming time-series to a tuple of values."""
    return extract_delta_value(ts, _tp)


@compute_node
def convert_pairs_to_tuples(ts: TIME_SERIES_TYPE, _tp: type[TIME_SERIES_TYPE] = AUTO_RESOLVE) -> TS[object]:
    """Converts the incoming time-series to a tuple of values."""
    return extract_value(ts, _tp)
