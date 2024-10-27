from collections import defaultdict
from dataclasses import dataclass, field

from hgraph import MIN_ST
from hgraph._impl._types._ref import PythonTimeSeriesReference
from hgraph._operators._flow_control import all_, any_, merge, index_of
from hgraph._operators._flow_control import race, BoolResult, if_, route_by_index, if_true, if_then_else
from hgraph._operators._operators import bit_and, bit_or
from hgraph._runtime._constants import MAX_DT, MIN_DT
from hgraph._runtime._evaluation_clock import EvaluationClock
from hgraph._types._ref_type import REF, REF_OUT
from hgraph._types._scalar_types import CompoundScalar, STATE, SCALAR, SIZE, SIZE_1
from hgraph._types._time_series_types import OUT, TIME_SERIES_TYPE, K
from hgraph._types._ts_type import TS, TS_OUT
from hgraph._types._tsb_type import TSB, TS_SCHEMA
from hgraph._types._tsd_type import TSD
from hgraph._types._tsl_type import TSL
from hgraph._types._type_meta_data import AUTO_RESOLVE
from hgraph._wiring._decorators import graph, compute_node
from hgraph._wiring._reduce import reduce

__all__ = tuple()


@graph(overloads=all_)
def all_default(*args: TSL[TS[bool], SIZE]) -> TS[bool]:
    return reduce(bit_and, args, False)


@graph(overloads=any_)
def any_default(*args: TSL[TS[bool], SIZE]) -> TS[bool]:
    """
    Graph version of python `any` operator
    """
    return reduce(bit_or, args, False)


@compute_node(overloads=merge, valid=())
def merge_ts_scalar(*tsl: TSL[TS[SCALAR], SIZE], _output: TS[SCALAR] = None) -> TS[SCALAR]:
    """
    Selects and returns the first of the values that tick (are modified) in the list provided.
    If more than one input is modified in the engine-cycle, it will return the first one that ticked in order of the
    list.
    """
    if tsl.modified:
        return next(tsl.modified_values()).value
    else:
        # This implies a value has gone away, if this is the last ticked value, revert to the last ticked value that
        # is still valid
        out = None
        last_ticked = MIN_DT
        for ts in tsl:
            if ts.valid and ts.last_modified_time > last_ticked:
                last_ticked = ts.last_modified_time
                out = ts.value
        if out is not None and out != _output.value:
            return out


@graph(overloads=merge)
def merge_tsb(*tsl: TSL[TSB[TS_SCHEMA], SIZE], _schema_tp: type[TS_SCHEMA] = AUTO_RESOLVE) -> TSB[TS_SCHEMA]:
    """
    Applies merge to each element of the schema.
    """
    kwargs = {k: merge(*[v[k] for v in tsl]) for k in _schema_tp._schema_keys()}
    return TSB[_schema_tp].from_ts(**kwargs)


@graph(overloads=merge)
def merge_tsl(
    *tsl: TSL[TSL[TIME_SERIES_TYPE, SIZE_1], SIZE], sz_1: type[SIZE_1] = AUTO_RESOLVE
) -> TSL[TIME_SERIES_TYPE, SIZE_1]:
    return TSL[TIME_SERIES_TYPE, SIZE_1].from_ts(*[merge(item[i] for item in tsl) for i in range(sz_1.SIZE)])


@dataclass
class _RaceState(CompoundScalar):
    first_valid_times: dict = field(default_factory=lambda: defaultdict(lambda: MAX_DT))
    winner: REF[OUT] = None


@compute_node(overloads=race, active=("tsl",))
def race_default(
    *tsl: TSL[REF[OUT], SIZE],
    _values: TSL[OUT, SIZE] = None,
    _state: STATE[_RaceState] = None,
    _ec: EvaluationClock = None,
    _sz: type[SIZE] = AUTO_RESOLVE,
) -> REF[OUT]:

    # Keep track of the first time each input goes valid (and invalid)
    pending_refs = [None] * _sz.SIZE

    for i in range(_sz.SIZE):
        if _tsl_ref_item_valid(tsl, i):
            if i not in _state.first_valid_times:
                _state.first_valid_times[i] = _ec.now
        elif tsl[i].valid:  # valid reference but invalid referee
            pending_refs[i] = tsl[i].value
            _state.first_valid_times.pop(i, None)
        else:
            _state.first_valid_times.pop(i, None)

    # Forward the input with the earliest valid time
    winner = _state.winner
    if winner not in _state.first_valid_times:
        # Find a new winner - old one has gone invalid
        winner = min(_state.first_valid_times.items(), default=None, key=lambda item: item[1])
        if winner is not None:
            _state.winner = winner[0]
            for v in _values:  # make all values passive and disconnect now that we have a winner
                v.make_passive()
                PythonTimeSeriesReference().bind_input(v)
            return tsl[_state.winner].value
        else:  # if no winner, track timeseries where we have reference but no value
            for i, r in enumerate(pending_refs):
                if r is not None and not _values[i].active:
                    r.bind_input(_values[i])
                    _values[i].make_active()
    elif tsl[_state.winner].modified:
        # Forward the winning value
        return tsl[_state.winner].delta_value


def _tsl_ref_item_valid(tsl, i):
    if not tsl.valid:
        return False
    return _ref_input_valid(tsl[i])


@compute_node(overloads=race, active=("tsd",))
def race_tsd(
    tsd: TSD[K, REF[OUT]], _values: TSD[K, OUT] = None, _state: STATE[_RaceState] = None, _ec: EvaluationClock = None
) -> REF[OUT]:
    # Keep track of the first time each input goes valid (and invalid)
    pending_refs = {}

    for k, v in tsd.modified_items():
        if _ref_input_valid(v):
            if k not in _state.first_valid_times:
                _state.first_valid_times[k] = _ec.now
        elif v.valid:  # valid reference but invalid referee
            pending_refs[k] = v.value
            _state.first_valid_times.pop(k, None)
        else:
            _state.first_valid_times.pop(k, None)

    for k in tsd.removed_keys():
        _state.first_valid_times.pop(k, None)

    # Forward the input with the earliest valid time
    winner = _state.winner
    if winner not in _state.first_valid_times:
        # Find a new winner - old one has gone invalid or gone altogether
        winner = min(_state.first_valid_times.items(), default=None, key=lambda item: item[1])
        if winner is not None:
            _state.winner = winner[0]
            for v in _values.valid_values():  # make all values passive and disconnect now that we have a winner
                v.make_passive()
                PythonTimeSeriesReference().bind_input(v)
            return tsd[_state.winner].value
        else:  # if no winner, track timeseries where we have reference but no value
            for i, r in pending_refs.items():
                if i not in _values:
                    _values._create(i)
                if not _values[i].active:
                    r.bind_input(_values[i])
                    _values[i].make_active()
    elif tsd[_state.winner].modified:
        # Forward the winning value
        return tsd[_state.winner].delta_value


def _ref_input_valid(v):
    if not v.valid:
        return False
    return _ref_valid(v.value)


def _ref_valid(value):
    if (output := value.output) is not None:
        return output.valid
    elif (items := getattr(value, "items", None)) is not None:
        return any(i.valid for i in items if i is not None)
    else:
        return False


@dataclass
class _RaceTsdOfBundlesState(CompoundScalar):
    first_valid_hashes: dict = field(default_factory=lambda: defaultdict(lambda: defaultdict(lambda: MAX_DT)))
    first_valid_times: dict = field(default_factory=lambda: defaultdict(lambda: defaultdict(lambda: MAX_DT)))
    winners: list[K] = None


@compute_node(overloads=race, active=("tsd",))
def race_tsd_of_bundles(
    tsd: TSD[K, REF[TSB[TS_SCHEMA]]],
    _values: TSD[K, TSB[TS_SCHEMA]] = None,
    _state: STATE[_RaceTsdOfBundlesState] = None,
    _ec: EvaluationClock = None,
    _schema: type[TS_SCHEMA] = TS_SCHEMA,
    _output: TS_OUT[REF[TSB[TS_SCHEMA]]] = None,
) -> REF[TSB[TS_SCHEMA]]:
    # Keep track of the first time each input goes valid (and invalid)
    pending_values = {}
    pending_items = defaultdict(dict)

    for k in tsd.removed_keys():
        for i in _schema.__meta_data_schema__:
            _state.first_valid_times.get(i, {}).pop(k, None)
        _values.on_key_removed(k)

    for k in tsd.valid_keys():
        v = tsd[k]
        if _ref_input_valid(v):
            ref = v.value
            if ref.output:
                for n, r in ref.output.items():
                    if r.valid:
                        if _state.first_valid_hashes.get(n, {}).get(k, None) != id(r):
                            _state.first_valid_hashes[n][k] = id(r)
                            _state.first_valid_times[n][k] = _ec.now
                    else:
                        _state.first_valid_times.get(n, {}).pop(k, None)
                        _state.first_valid_hashes.get(n, {}).pop(k, None)
                        pending_items[n][k] = PythonTimeSeriesReference(r)
                if k in _values:
                    _values[k].make_passive()
                    PythonTimeSeriesReference().bind_input(_values[k])
            else:
                for i, n in enumerate(_schema.__meta_data_schema__):
                    if (r := ref.items[i]) and _ref_valid(r):
                        if _state.first_valid_hashes.get(n, {}).get(k, None) != r:
                            _state.first_valid_hashes[n][k] = r
                            _state.first_valid_times[n][k] = _ec.now
                    elif r:
                        _state.first_valid_times.get(n, {}).pop(k, None)
                        _state.first_valid_hashes.get(n, {}).pop(k, None)
                        pending_items[n][k] = ref.items[i]
                    else:
                        _state.first_valid_times.get(n, {}).pop(k, None)
                        _state.first_valid_hashes.get(n, {}).pop(k, None)
        elif v.valid and v.value.output:  # valid reference but invalid referee
            pending_values[k] = v.value
            for n in _schema.__meta_data_schema__:
                _state.first_valid_times.get(n, {}).pop(k, None)
                _state.first_valid_hashes.get(n, {}).pop(k, None)
        else:
            for n in _schema.__meta_data_schema__:
                _state.first_valid_times.get(n, {}).pop(k, None)
                _state.first_valid_hashes.get(n, {}).pop(k, None)

    # Forward the input with the earliest valid time
    winners = _state.winners
    if winners is None:
        winners = [None] * len(_schema.__meta_data_schema__)

    new_winners = [None] * len(_schema.__meta_data_schema__)

    for i, n in enumerate(_schema.__meta_data_schema__):
        if winners[i] not in _state.first_valid_times.get(n, {}):
            # Find a new winner - old one has gone invalid or gone altogether
            winner = min(_state.first_valid_times.get(n, {}).items(), default=None, key=lambda item: item[1])
            if winner is not None:
                new_winners[i] = winner[0]
                for v in _values.valid_values():  # make all values passive and disconnect now that we have a winner
                    if v[n].bound:
                        v[n].make_passive()
                        PythonTimeSeriesReference().bind_input(v[n])
            else:  # if no winner, track timeseries where we have reference but no value
                new_winners[i] = None
                for k, r in pending_items.get(n, {}).items():
                    if k not in _values:
                        _values._create(k)
                    if not (v := _values[k][n]).active:
                        r.bind_input(v)
                        v.make_active()
        else:
            new_winners[i] = winners[i]

    if any(n is None for n in new_winners):
        for k, v in pending_values.items():
            if k not in _values:
                _values._create(k)
            if not _values[k].active:
                v.bind_input(_values[k])
                _values[k].make_active()

    _state.winners = new_winners

    ref_items = [None] * len(_schema.__meta_data_schema__)
    for i, (n, o) in enumerate(zip(_schema.__meta_data_schema__, _state.winners)):
        if o is not None:
            value = tsd[o].value
            if value.output:
                ref_items[i] = PythonTimeSeriesReference(value.output[n])
            else:
                ref_items[i] = value.items[i]
        else:
            ref_items[i] = PythonTimeSeriesReference()

    result = PythonTimeSeriesReference(from_items=ref_items)
    if _output.valid:
        if _output.value != result:
            return result
    else:
        return result


@compute_node(valid=("condition",), overloads=if_)
def if_impl(condition: TS[bool], ts: REF[TIME_SERIES_TYPE]) -> TSB[BoolResult[TIME_SERIES_TYPE]]:
    """
    Forwards a timeseries value to one of two bundle outputs: "true" or "false", according to whether
    the condition is true or false
    """
    if condition.value:
        return {"true": ts.value if ts.valid else PythonTimeSeriesReference(), "false": PythonTimeSeriesReference()}
    else:
        return {"false": ts.value if ts.valid else PythonTimeSeriesReference(), "true": PythonTimeSeriesReference()}


@compute_node(valid=("index_ts",), overloads=route_by_index)
def route_by_index_impl(
    index_ts: TS[int], ts: REF[TIME_SERIES_TYPE], _sz: type[SIZE] = AUTO_RESOLVE
) -> TSL[REF[TIME_SERIES_TYPE], SIZE]:
    """
    Forwards a timeseries value to the 'nth' output according to the value of index_ts
    """
    out = [PythonTimeSeriesReference()] * _sz.SIZE
    index_ts = index_ts.value
    if 0 <= index_ts < _sz.SIZE and ts.valid:
        out[index_ts] = ts.value
    return out


@compute_node(overloads=if_true)
def if_true_impl(condition: TS[bool], tick_once_only: bool = False) -> TS[bool]:
    """
    Emits a tick with value True when the input condition ticks with True.
    If tick_once_only is True then this will only tick once, otherwise this will tick with every tick of the condition,
    when the condition is True.
    """
    if condition.value:
        if tick_once_only:
            condition.make_passive()
        return True


@compute_node(overloads=if_then_else, valid=("condition",))
def if_then_else_impl(
    condition: TS[bool],
    true_value: REF[TIME_SERIES_TYPE],
    false_value: REF[TIME_SERIES_TYPE],
    _output: REF_OUT[TIME_SERIES_TYPE] = None,
) -> REF[TIME_SERIES_TYPE]:
    """
    If the condition is true the output ticks with the true_value, otherwise it ticks with the false_value.
    """
    condition_value = condition.value
    if condition.modified:
        if condition_value:
            if true_value.valid and _output.value != true_value.value:
                return true_value.value
        else:
            if false_value.valid and _output.value != false_value.value:
                return false_value.value

    if condition_value and true_value.modified:
        return true_value.value

    if not condition_value and false_value.modified:
        return false_value.value


@compute_node(overloads=index_of)
def index_of_impl(tsl: TSL[TIME_SERIES_TYPE, SIZE], ts: TIME_SERIES_TYPE) -> TS[int]:
    """
    Return the index of the leftmost time-series in the TSL with value equal to ts
    """
    return next((i for i, t in enumerate(tsl) if t.valid and t.value == ts.value), -1)
