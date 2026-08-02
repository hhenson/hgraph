"""Comparative benchmark scenarios for the three hgraph implementations.

Written in standard Python hgraph syntax only — the SAME source must wire and
run on:
  1. upstream hgraph, Python runtime           (pip install hgraph)
  2. upstream hgraph, old C++ runtime          (HGRAPH_USE_CPP=true)
  3. hg_cpp                                    (this repository's package)

Two flavours per scenario family:
  *_std — mostly-graph, standard operators (python nodes only where a tick
          source is unavoidable);
  *_py  — the same shape but with the work done in custom @compute_node
          python nodes.

Value-type axes: int, float, str (std::string vs PyStr cost in the new
runtime) and CompoundScalar (new compound-scalar value vs old python object).
Key-type axis for TSD: int keys vs str keys.

Each scenario returns (graph_fn, n_cycles). Scenario metadata at the bottom of
the module controls grouping, supported runtimes, and whether collection size
is scaled independently from the number of engine cycles.
"""
from dataclasses import dataclass
from typing import Callable

import hgraph as hg
from hgraph import (
    TS, TSB, TSD, TSL, TSS, CompoundScalar, Size, TimeSeriesSchema,
    compute_node, feedback, generator, graph, map_, mesh_, null_sink, reduce,
    sink_node,
)

MIN_TD = hg.MIN_TD

# ---------------------------------------------------------------------------
# Compat shims (bench-local; the two implementations differ at the edges)
# ---------------------------------------------------------------------------

# TSD key removal sentinel: both export REMOVE today; keep the lookup soft so
# the bench degrades with a clear message rather than an import error.
REMOVE = getattr(hg, "REMOVE")

ALL_MODES = ("upstream-py", "upstream-cpp", "hg-cpp")
HG_CPP_ONLY = ("hg-cpp",)


@dataclass(frozen=True)
class BenchmarkScenario:
    """Description and construction policy for one timed workload."""

    group: str
    label: str
    factory: Callable
    suite: str = "core"
    modes: tuple[str, ...] = ALL_MODES
    independent_size: bool = False

    def build(self, cycle_scale: float, size_scale: float):
        if self.independent_size:
            return self.factory(cycle_scale, size_scale)
        return self.factory(cycle_scale)


# ---------------------------------------------------------------------------
# Shared sources
# ---------------------------------------------------------------------------

@generator
def _int_pulse(cycles: int) -> TS[int]:
    """One int tick per engine cycle for `cycles` cycles."""
    for i in range(cycles):
        yield MIN_TD, i


@generator
def _float_pulse(cycles: int) -> TS[float]:
    for i in range(cycles):
        yield MIN_TD, i * 1.000001


@generator
def _str_pulse(cycles: int) -> TS[str]:
    values = [f"payload-{i:06d}" for i in range(64)]
    for i in range(cycles):
        yield MIN_TD, values[i & 63]


@generator
def _symbol_pulse(cycles: int) -> TS[str]:
    values = [f"symbol-{i:03d}" for i in range(64)]
    for i in range(cycles):
        yield MIN_TD, values[i & 63]


@generator
def _bool_pulse(cycles: int) -> TS[bool]:
    for i in range(cycles):
        yield MIN_TD, bool(i & 1)


@generator
def _service_int_pulse(requests: int) -> TS[int]:
    """Requests separated by the idle cycle required by service forwarding."""
    for i in range(requests):
        yield MIN_TD if i == 0 else 2 * MIN_TD, i


@generator
def _service_symbol_pulse(requests: int) -> TS[str]:
    values = [f"symbol-{i:03d}" for i in range(64)]
    for i in range(requests):
        yield MIN_TD if i == 0 else 2 * MIN_TD, values[i & 63]


@dataclass(frozen=True)
class BenchCS(CompoundScalar):
    ident: int
    price: float
    label: str


@dataclass(frozen=True)
class BenchPythonRecord:
    """Python-owned counterpart to ``BenchCS`` for storage-policy benches."""

    ident: int
    price: float
    label: str


class BenchBundle(TimeSeriesSchema):
    fast: TS[int]
    medium: TS[int]
    slow: TS[int]


@generator
def _cs_pulse(cycles: int) -> TS[BenchCS]:
    values = [BenchCS(ident=i, price=i * 1.5, label=f"cs-{i:04d}") for i in range(64)]
    for i in range(cycles):
        yield MIN_TD, values[i & 63]


@generator
def _python_record_pulse(cycles: int) -> TS[BenchPythonRecord]:
    values = [
        BenchPythonRecord(ident=i, price=i * 1.5, label=f"record-{i:04d}")
        for i in range(64)
    ]
    for i in range(cycles):
        yield MIN_TD, values[i & 63]


@generator
def _cs_equal_pair_pulse(cycles: int) -> TS[BenchCS]:
    values = [
        BenchCS(ident=i // 2, price=(i // 2) * 1.5, label=f"cs-{i // 2:04d}")
        for i in range(128)
    ]
    for i in range(cycles):
        yield MIN_TD, values[i & 127]


@generator
def _python_record_equal_pair_pulse(cycles: int) -> TS[BenchPythonRecord]:
    values = [
        BenchPythonRecord(
            ident=i // 2,
            price=(i // 2) * 1.5,
            label=f"record-{i // 2:04d}",
        )
        for i in range(128)
    ]
    for i in range(cycles):
        yield MIN_TD, values[i & 127]


@generator
def _tsd_dense_pulse(cycles: int, keys: int) -> TSD[int, TS[int]]:
    """All `keys` keys tick every cycle (dense)."""
    for i in range(cycles):
        yield MIN_TD, {k: i + k for k in range(keys)}


@generator
def _tsd_dense_pulse_strkeys(cycles: int, keys: int) -> TSD[str, TS[int]]:
    names = [f"key-{k:05d}" for k in range(keys)]
    for i in range(cycles):
        yield MIN_TD, {names[k]: i + k for k in range(keys)}


@generator
def _tsd_sparse_pulse(
    cycles: int, keys: int, per_cycle: int, offset: int = 0
) -> TSD[int, TS[int]]:
    """Universe of `keys` keys, created up front; only `per_cycle` tick each
    cycle (sparse)."""
    yield MIN_TD, {offset + k: 0 for k in range(keys)}
    for i in range(1, cycles):
        base = (i * per_cycle) % keys
        yield MIN_TD, {
            offset + (base + j) % keys: i for j in range(per_cycle)
        }


@generator
def _tsd_churn_pulse(cycles: int, live: int, churn: int) -> TSD[int, TS[int]]:
    """A sliding window of `live` keys; each cycle `churn` keys are removed
    at the back and `churn` new keys appear at the front (items come and go —
    exercises map_/reduce instance creation and teardown)."""
    yield MIN_TD, {k: k for k in range(live)}
    front = live
    for i in range(1, cycles):
        delta = {}
        for j in range(churn):
            delta[front - live + j] = REMOVE
            delta[front + j] = i
        front += churn
        yield MIN_TD, delta


@generator
def _tsd_reactivate_pulse(
    cycles: int, live: int, churn: int
) -> TSD[int, TS[int]]:
    """Remove and later recreate the same keys to exercise slot reuse."""
    yield MIN_TD, {key: key for key in range(live)}
    keys = tuple(range(churn))
    for i in range(1, cycles):
        if i & 1:
            yield MIN_TD, {key: REMOVE for key in keys}
        else:
            yield MIN_TD, {key: i for key in keys}


@generator
def _tsd_growing_pulse(cycles: int, per_cycle: int) -> TSD[int, TS[int]]:
    """Grow monotonically to exercise slot-store capacity boundaries."""
    front = 0
    for i in range(cycles):
        yield MIN_TD, {front + j: i for j in range(per_cycle)}
        front += per_cycle


@generator
def _tsd_clear_repopulate_pulse(cycles: int, keys: int) -> TSD[int, TS[int]]:
    """Alternately erase every live key and repopulate a fresh key range."""
    base = 0
    live = tuple(range(keys))
    for i in range(cycles):
        if i & 1:
            yield MIN_TD, {key: REMOVE for key in live}
        else:
            live = tuple(range(base, base + keys))
            yield MIN_TD, {key: i + key for key in live}
            base += keys


@generator
def _tsd_cardinality_pulse(cycles: int) -> TSD[int, TS[int]]:
    """Cycle repeatedly through empty, singleton, and two-value states."""
    yield MIN_TD, {}
    for i in range(1, cycles):
        phase = i % 4
        if phase == 1:
            yield MIN_TD, {0: i}
        elif phase == 2:
            yield MIN_TD, {1: i}
        elif phase == 3:
            yield MIN_TD, {0: REMOVE}
        else:
            yield MIN_TD, {1: REMOVE}


@generator
def _tss_churn_pulse(cycles: int, live: int, churn: int) -> TSS[int]:
    """Maintain a fixed-size set using explicit add/remove deltas."""
    yield MIN_TD, set(range(live))
    front = live
    for _ in range(1, cycles):
        removed = range(front - live, front - live + churn)
        added = range(front, front + churn)
        yield MIN_TD, {*added, *(hg.Removed(key) for key in removed)}
        front += churn


@generator
def _dynamic_tsl_sparse_pulse(
    cycles: int, initial_size: int, per_cycle: int
) -> TSL[TS[int], Size[0]]:
    """hg_cpp-only unbounded TSL source with sparse positional updates."""
    yield MIN_TD, {index: index for index in range(initial_size)}
    for i in range(1, cycles):
        base = (i * per_cycle) % initial_size
        yield MIN_TD, {
            (base + offset) % initial_size: initial_size + i
            for offset in range(per_cycle)
        }


# ---------------------------------------------------------------------------
# A. Large graph construction
# ---------------------------------------------------------------------------

def _chain_std(x, depth):
    for _ in range(depth):
        x = x + 1
    return x


@compute_node
def _add_one_py(ts: TS[int]) -> TS[int]:
    return ts.value + 1


def _chain_py(x, depth):
    for _ in range(depth):
        x = _add_one_py(x)
    return x


def _wide_chain_std(x, width, depth):
    branches = [_chain_std(x + branch, depth) for branch in range(width)]
    total = branches[0]
    for branch in branches[1:]:
        total = total + branch
    return total


def _wide_chain_py(x, width, depth):
    branches = [_chain_py(x + branch, depth) for branch in range(width)]
    total = branches[0]
    for branch in branches[1:]:
        total = total + branch
    return total


def construct_std(_cycle_scale: float, size_scale: float):
    width = max(2, int(30 * size_scale))
    depth = max(2, int(100 * size_scale))

    @graph
    def g():
        src = _int_pulse(1)
        null_sink(_wide_chain_std(src, width, depth))

    return g, 1  # 1 cycle: measured time ~= wiring + build cost


def construct_py(_cycle_scale: float, size_scale: float):
    width = max(2, int(30 * size_scale))
    depth = max(2, int(100 * size_scale))

    @graph
    def g():
        src = _int_pulse(1)
        null_sink(_wide_chain_py(src, width, depth))

    return g, 1


# ---------------------------------------------------------------------------
# B. Fast ticking (hot loop)
# ---------------------------------------------------------------------------

def tick_std(scale: float):
    cycles = int(100_000 * scale)

    @graph
    def g():
        fb = feedback(TS[int], 0)
        nxt = fb() + 1
        fb(nxt)
        null_sink(nxt)

    return g, cycles


def tick_py(scale: float):
    cycles = int(20_000 * scale)

    @graph
    def g():
        src = _int_pulse(cycles)
        x = src
        for _ in range(5):
            x = _add_one_py(x)
        null_sink(x)

    return g, cycles


@sink_node
def _discard_py(value: TS[int]):
    pass


@compute_node
def _read_injected_global_state(
    value: TS[int], global_state: hg.GlobalState = None
) -> TS[int]:
    return value.value + global_state.get("benchmark_offset", 0)


def scheduler_fan_out_std(cycle_scale: float, size_scale: float):
    cycles = int(20_000 * cycle_scale)
    width = max(2, int(32 * size_scale))

    @graph
    def g():
        source = _int_pulse(cycles)
        for branch in range(width):
            null_sink(source + branch)

    return g, cycles


def scheduler_fan_in_std(cycle_scale: float, size_scale: float):
    cycles = int(20_000 * cycle_scale)
    width = max(2, int(32 * size_scale))

    @graph
    def g():
        source = _int_pulse(cycles)
        branches = [source + branch for branch in range(width)]
        total = branches[0]
        for branch in branches[1:]:
            total = total + branch
        null_sink(total)

    return g, cycles


def scheduler_conflated_fixed_tsl_std(scale: float):
    """Eight inputs notify one lifted reducer during each engine cycle."""
    cycles = int(20_000 * scale)

    @graph
    def g():
        source = _int_pulse(cycles)
        values = TSL.from_ts(*(source + offset for offset in range(8)))
        null_sink(reduce(hg.add_, values, 0))

    return g, cycles


def python_generator_boundary(scale: float):
    cycles = int(20_000 * scale)

    @graph
    def g():
        null_sink(_int_pulse(cycles))

    return g, cycles


def python_sink_boundary(scale: float):
    cycles = int(20_000 * scale)

    @graph
    def g():
        _discard_py(_int_pulse(cycles))

    return g, cycles


def python_global_state_boundary(scale: float):
    cycles = int(20_000 * scale)

    @graph
    def g():
        null_sink(_read_injected_global_state(_int_pulse(cycles)))

    return g, cycles


# ---------------------------------------------------------------------------
# C. Value-type axes (std chains; one py variant for compound scalars)
# ---------------------------------------------------------------------------

def type_int_std(scale: float):
    cycles = int(20_000 * scale)

    @graph
    def g():
        x = _int_pulse(cycles)
        null_sink(((x + 1) * 3) - x)

    return g, cycles


def type_float_std(scale: float):
    cycles = int(20_000 * scale)

    @graph
    def g():
        x = _float_pulse(cycles)
        null_sink(((x + 1.5) * 1.000001) - x)

    return g, cycles


def type_str_std(scale: float):
    cycles = int(20_000 * scale)

    @graph
    def g():
        x = _str_pulse(cycles)
        null_sink((x + "-suffix") + (x + "!"))

    return g, cycles


def type_cs_std(scale: float):
    """Compound-scalar traffic through std field access + arithmetic."""
    cycles = int(20_000 * scale)

    @graph
    def g():
        x = _cs_pulse(cycles)
        null_sink(x.price + x.ident)

    return g, cycles


@compute_node
def _cs_work_py(ts: TS[BenchCS]) -> TS[BenchCS]:
    v = ts.value
    return BenchCS(ident=v.ident + 1, price=v.price * 1.000001, label=v.label)


def type_cs_py(scale: float):
    """Compound scalars crossing the python-node boundary every tick — the
    conversion cost axis (new C++ compound value vs old python object)."""
    cycles = int(10_000 * scale)

    @graph
    def g():
        x = _cs_pulse(cycles)
        x = _cs_work_py(_cs_work_py(x))
        null_sink(x.price)

    return g, cycles


def python_owned_pass_through_native(scale: float):
    """Whole-value storage traffic for a native-layout structured scalar."""
    cycles = int(20_000 * scale)

    @graph
    def g():
        null_sink(_cs_pulse(cycles))

    return g, cycles


def python_owned_pass_through_python(scale: float):
    """Whole-value storage traffic for a Python-owned structured scalar."""
    cycles = int(20_000 * scale)

    @graph
    def g():
        null_sink(_python_record_pulse(cycles))

    return g, cycles


def python_owned_project_one_native(scale: float):
    cycles = int(20_000 * scale)

    @graph
    def g():
        null_sink(_cs_pulse(cycles).price)

    return g, cycles


def python_owned_project_one_python(scale: float):
    cycles = int(20_000 * scale)

    @graph
    def g():
        null_sink(_python_record_pulse(cycles).price)

    return g, cycles


def python_owned_project_several_native(scale: float):
    cycles = int(20_000 * scale)

    @graph
    def g():
        value = _cs_pulse(cycles)
        null_sink(value.ident)
        null_sink(value.price)
        null_sink(value.label)

    return g, cycles


def python_owned_project_several_python(scale: float):
    cycles = int(20_000 * scale)

    @graph
    def g():
        value = _python_record_pulse(cycles)
        null_sink(value.ident)
        null_sink(value.price)
        null_sink(value.label)

    return g, cycles


def python_owned_construct_native(scale: float):
    cycles = int(20_000 * scale)

    @graph
    def g():
        ident = _int_pulse(cycles)
        null_sink(hg.combine[TS[BenchCS]](
            ident=ident,
            price=ident * 1.5,
            label=hg.convert[TS[str]](ident),
        ))

    return g, cycles


def python_owned_construct_python(scale: float):
    cycles = int(20_000 * scale)

    @graph
    def g():
        ident = _int_pulse(cycles)
        null_sink(hg.combine[TS[BenchPythonRecord]](
            ident=ident,
            price=ident * 1.5,
            label=hg.convert[TS[str]](ident),
        ))

    return g, cycles


def python_owned_dedup_native(scale: float):
    cycles = int(20_000 * scale)

    @graph
    def g():
        null_sink(hg.drop_dups(_cs_equal_pair_pulse(cycles)))

    return g, cycles


def python_owned_dedup_python(scale: float):
    cycles = int(20_000 * scale)

    @graph
    def g():
        null_sink(hg.drop_dups(_python_record_equal_pair_pulse(cycles)))

    return g, cycles


def type_tsb_partial_fields_std(scale: float):
    """A structural bundle where only a subset of fields tick each cycle."""
    cycles = int(20_000 * scale)

    @graph
    def g():
        source = _int_pulse(cycles)
        medium = hg.if_(source % 2 == 0, source).true
        slow = hg.if_(source % 8 == 0, source).true
        bundle = TSB[BenchBundle].from_ts(
            fast=source,
            medium=medium,
            slow=slow,
        )
        null_sink(bundle.fast + bundle.medium + bundle.slow)

    return g, cycles


def type_tsw_append_evict_std(scale: float):
    cycles = int(20_000 * scale)

    @graph
    def g():
        null_sink(hg.to_window(_int_pulse(cycles), 64, 1))

    return g, cycles


@graph
def _switch_short(value: TS[int]) -> TS[int]:
    return value + 1


@graph
def _switch_deep(value: TS[int]) -> TS[int]:
    return _chain_std(value, 12)


def switch_alternating_branch_sizes_std(scale: float):
    cycles = int(20_000 * scale)

    @graph
    def g():
        selected = hg.switch_(
            _bool_pulse(cycles),
            {False: _switch_short, True: _switch_deep},
            value=_int_pulse(cycles),
        )
        null_sink(selected)

    return g, cycles


@graph
def _switch_tsd_double(values: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
    return map_(_mapped_std, values)


@graph
def _switch_tsd_identity(values: TSD[int, TS[int]]) -> TSD[int, TS[int]]:
    return values


def switch_keyed_collection_std(cycle_scale: float, size_scale: float):
    cycles = int(2_000 * cycle_scale)
    live = max(8, int(200 * size_scale))

    @graph
    def g():
        selected = hg.switch_(
            _bool_pulse(cycles),
            {False: _switch_tsd_identity, True: _switch_tsd_double},
            values=_tsd_churn_pulse(cycles, live, 5),
        )
        null_sink(reduce(hg.add_, selected, 0))

    return g, cycles


# ---------------------------------------------------------------------------
# D/E/F. TSD ticking: dense / sparse / churn through map_ + reduce
# ---------------------------------------------------------------------------

@graph
def _mapped_std(v: TS[int]) -> TS[int]:
    return (v + 1) * 2


@compute_node
def _mapped_py(v: TS[int]) -> TS[int]:
    return (v.value + 1) * 2


@graph
def _map_reduce_std(tsd: TSD[int, TS[int]]) -> TS[int]:
    return reduce(hg.add_, map_(_mapped_std, tsd), 0)


@graph
def _map_reduce_py(tsd: TSD[int, TS[int]]) -> TS[int]:
    return reduce(hg.add_, map_(_mapped_py, tsd), 0)


def tsd_dense_std(cycle_scale: float, size_scale: float):
    cycles, keys = int(1_000 * cycle_scale), max(4, int(200 * size_scale))

    @graph
    def g():
        null_sink(_map_reduce_std(_tsd_dense_pulse(cycles, keys)))

    return g, cycles


def tsd_dense_source_std(cycle_scale: float, size_scale: float):
    """Dense diagnostic: Python generator and native sink only."""
    cycles, keys = int(1_000 * cycle_scale), max(4, int(200 * size_scale))

    @graph
    def g():
        null_sink(_tsd_dense_pulse(cycles, keys))

    return g, cycles


def tsd_dense_map_std(cycle_scale: float, size_scale: float):
    """Dense diagnostic: source plus the native nested map graph."""
    cycles, keys = int(1_000 * cycle_scale), max(4, int(200 * size_scale))

    @graph
    def g():
        null_sink(map_(_mapped_std, _tsd_dense_pulse(cycles, keys)))

    return g, cycles


def tsd_dense_reduce_std(cycle_scale: float, size_scale: float):
    """Dense diagnostic: source plus the native reduce tree."""
    cycles, keys = int(1_000 * cycle_scale), max(4, int(200 * size_scale))

    @graph
    def g():
        null_sink(reduce(hg.add_, _tsd_dense_pulse(cycles, keys), 0))

    return g, cycles


def tsd_dense_py(cycle_scale: float, size_scale: float):
    cycles, keys = int(1_000 * cycle_scale), max(4, int(200 * size_scale))

    @graph
    def g():
        null_sink(_map_reduce_py(_tsd_dense_pulse(cycles, keys)))

    return g, cycles


def tsd_dense_strkeys_std(cycle_scale: float, size_scale: float):
    """Key-type axis: identical to tsd_dense_std but with str keys."""
    cycles, keys = int(1_000 * cycle_scale), max(4, int(200 * size_scale))

    @graph
    def g():
        mapped = map_(_mapped_std, _tsd_dense_pulse_strkeys(cycles, keys))
        null_sink(reduce(hg.add_, mapped, 0))

    return g, cycles


def tsd_sparse_std(cycle_scale: float, size_scale: float):
    cycles, keys = int(2_000 * cycle_scale), max(16, int(2_000 * size_scale))

    @graph
    def g():
        null_sink(_map_reduce_std(_tsd_sparse_pulse(cycles, keys, 5)))

    return g, cycles


def tsd_churn_std(cycle_scale: float, size_scale: float):
    cycles, live, churn = int(2_000 * cycle_scale), max(8, int(200 * size_scale)), 5

    @graph
    def g():
        null_sink(_map_reduce_std(_tsd_churn_pulse(cycles, live, churn)))

    return g, cycles


def tsd_churn_py(cycle_scale: float, size_scale: float):
    cycles, live, churn = int(2_000 * cycle_scale), max(8, int(200 * size_scale)), 5

    @graph
    def g():
        null_sink(_map_reduce_py(_tsd_churn_pulse(cycles, live, churn)))

    return g, cycles


def _tsd_sparse_variant(cycle_scale: float, size_scale: float, stage: str):
    cycles = int(2_000 * cycle_scale)
    keys = max(16, int(2_000 * size_scale))

    @graph
    def g():
        values = _tsd_sparse_pulse(cycles, keys, 5)
        if stage == "map":
            values = map_(_mapped_std, values)
        elif stage == "reduce":
            values = reduce(hg.add_, values, 0)
        null_sink(values)

    return g, cycles


def tsd_sparse_source_std(cycle_scale: float, size_scale: float):
    return _tsd_sparse_variant(cycle_scale, size_scale, "source")


def tsd_sparse_map_std(cycle_scale: float, size_scale: float):
    return _tsd_sparse_variant(cycle_scale, size_scale, "map")


def tsd_sparse_reduce_std(cycle_scale: float, size_scale: float):
    return _tsd_sparse_variant(cycle_scale, size_scale, "reduce")


def _tsd_churn_variant(cycle_scale: float, size_scale: float, stage: str):
    cycles = int(2_000 * cycle_scale)
    live = max(8, int(200 * size_scale))

    @graph
    def g():
        values = _tsd_churn_pulse(cycles, live, 5)
        if stage == "map":
            values = map_(_mapped_std, values)
        elif stage == "reduce":
            values = reduce(hg.add_, values, 0)
        null_sink(values)

    return g, cycles


def tsd_churn_source_std(cycle_scale: float, size_scale: float):
    return _tsd_churn_variant(cycle_scale, size_scale, "source")


def tsd_churn_map_std(cycle_scale: float, size_scale: float):
    return _tsd_churn_variant(cycle_scale, size_scale, "map")


def tsd_churn_reduce_std(cycle_scale: float, size_scale: float):
    return _tsd_churn_variant(cycle_scale, size_scale, "reduce")


def tsd_sparse_large_capacity_std(cycle_scale: float, size_scale: float):
    """Regression guard for accidental O(capacity) work on a tiny delta."""
    cycles = int(5_000 * cycle_scale)
    keys = max(128, int(20_000 * size_scale))

    @graph
    def g():
        null_sink(_map_reduce_std(_tsd_sparse_pulse(cycles, keys, 2)))

    return g, cycles


def tsd_capacity_growth_std(cycle_scale: float, size_scale: float):
    cycles = int(1_000 * cycle_scale)
    per_cycle = max(1, int(4 * size_scale))

    @graph
    def g():
        values = _tsd_growing_pulse(cycles, per_cycle)
        null_sink(_map_reduce_std(values))

    return g, cycles


def tsd_clear_repopulate_std(cycle_scale: float, size_scale: float):
    cycles = int(200 * cycle_scale)
    keys = max(8, int(1_000 * size_scale))

    @graph
    def g():
        values = _tsd_clear_repopulate_pulse(cycles, keys)
        null_sink(_map_reduce_std(values))

    return g, cycles


def tsd_key_reactivation_std(cycle_scale: float, size_scale: float):
    cycles = int(2_000 * cycle_scale)
    live = max(8, int(200 * size_scale))

    @graph
    def g():
        values = _tsd_reactivate_pulse(cycles, live, 5)
        null_sink(_map_reduce_std(values))

    return g, cycles


@graph
def _sum_optional(lhs: TS[int], rhs: TS[int]) -> TS[int]:
    return hg.default(lhs, 0) + hg.default(rhs, 0)


def tsd_two_input_union_std(cycle_scale: float, size_scale: float):
    cycles = int(2_000 * cycle_scale)
    keys = max(16, int(1_000 * size_scale))

    @graph
    def g():
        lhs = _tsd_sparse_pulse(cycles, keys, 3)
        rhs = _tsd_sparse_pulse(cycles, keys, 3, offset=keys // 2)
        combined = map_(_sum_optional, lhs, rhs)
        null_sink(reduce(hg.add_, combined, 0))

    return g, cycles


def tsd_two_input_intersection_std(cycle_scale: float, size_scale: float):
    cycles = int(2_000 * cycle_scale)
    keys = max(16, int(1_000 * size_scale))

    @graph
    def g():
        lhs = _tsd_sparse_pulse(cycles, keys, 3)
        rhs = _tsd_sparse_pulse(cycles, keys, 3, offset=keys // 2)
        combined = map_(
            _sum_optional,
            lhs,
            rhs,
            __keys__=lhs.key_set & rhs.key_set,
        )
        null_sink(reduce(hg.add_, combined, 0))

    return g, cycles


@graph
def _key_identity(key: TS[int]) -> TS[int]:
    return key


def tsd_explicit_key_set_std(cycle_scale: float, size_scale: float):
    cycles = int(2_000 * cycle_scale)
    live = max(8, int(200 * size_scale))

    @graph
    def g():
        source = _tsd_churn_pulse(cycles, live, 5)
        keyed = map_(_key_identity, __keys__=source.key_set)
        null_sink(reduce(hg.add_, keyed, 0))

    return g, cycles


@graph
def _add_graph(lhs: TS[int], rhs: TS[int]) -> TS[int]:
    return lhs + rhs


@compute_node
def _add_py(lhs: TS[int], rhs: TS[int]) -> TS[int]:
    return lhs.value + rhs.value


def reduce_tsd_nested_graph_std(cycle_scale: float, size_scale: float):
    cycles = int(1_000 * cycle_scale)
    keys = max(4, int(200 * size_scale))

    @graph
    def g():
        null_sink(reduce(_add_graph, _tsd_dense_pulse(cycles, keys), 0))

    return g, cycles


def reduce_tsd_python_combiner(cycle_scale: float, size_scale: float):
    cycles = int(1_000 * cycle_scale)
    keys = max(4, int(200 * size_scale))

    @graph
    def g():
        null_sink(reduce(_add_py, _tsd_dense_pulse(cycles, keys), 0))

    return g, cycles


@graph
def _subtract_graph(lhs: TS[int], rhs: TS[int]) -> TS[int]:
    return lhs - rhs


def reduce_fixed_tsl_ordered_std(scale: float):
    cycles = int(10_000 * scale)

    @graph
    def g():
        source = _int_pulse(cycles)
        values = TSL.from_ts(*(source + offset for offset in range(8)))
        reduced = reduce(
            _subtract_graph,
            values,
            hg.const(0, tp=TS[int]),
            is_associative=False,
        )
        null_sink(reduced)

    return g, cycles


def reduce_dynamic_tsl_std(cycle_scale: float, size_scale: float):
    cycles = int(5_000 * cycle_scale)
    initial_size = max(4, int(128 * size_scale))

    @graph
    def g():
        values = _dynamic_tsl_sparse_pulse(cycles, initial_size, 2)
        null_sink(reduce(hg.add_, map_(_mapped_std, values), 0))

    return g, cycles


def reduce_tsd_without_zero_std(scale: float):
    cycles = int(20_000 * scale)

    @graph
    def g():
        null_sink(reduce(_add_graph, _tsd_cardinality_pulse(cycles)))

    return g, cycles


def tss_add_remove_std(cycle_scale: float, size_scale: float):
    cycles = int(5_000 * cycle_scale)
    live = max(8, int(500 * size_scale))

    @graph
    def g():
        values = _tss_churn_pulse(cycles, live, 3)
        null_sink(hg.len_(values))

    return g, cycles


# ---------------------------------------------------------------------------
# G. mesh_ with inter-key dependencies, items coming and going
# ---------------------------------------------------------------------------

# Self-reference inside a mesh: upstream spells it ``mesh_(fn)[key]``, hg_cpp
# spells it ``mesh_ref(key)`` — the one API divergence the bench shims over.
if hasattr(hg, "mesh_ref"):
    def _mesh_self(cell_fn, key_port):
        return hg.mesh_ref(key_port)
else:
    def _mesh_self(cell_fn, key_port):
        return mesh_(cell_fn)[key_port]


@graph
def _mesh_cell(key: TS[int], v: TS[int]) -> TS[int]:
    # Each key depends on its predecessor through the mesh; every 8th key is
    # a chain root so on-demand instantiation stays bounded (referencing
    # key-1 unconditionally would cascade instance creation to -infinity).
    dep = hg.switch_(
        key % 8 == 0,
        {
            True: lambda k: hg.const(0),
            False: lambda k: hg.default(_mesh_self(_mesh_cell, k - 1), hg.const(0)),
        },
        k=key,
    )
    return v + dep


def mesh_std(cycle_scale: float, size_scale: float):
    """Inter-key dependency mesh; the key window slides forward so instances
    come and go (creation, dependency ranking, teardown)."""
    cycles, live, churn = int(500 * cycle_scale), max(8, int(50 * size_scale)), 2

    @graph
    def g():
        src = _tsd_churn_pulse(cycles, live, churn)
        meshed = mesh_(_mesh_cell, src)
        null_sink(reduce(hg.add_, meshed, 0))

    return g, cycles


# ---------------------------------------------------------------------------
# H/I. Services and adaptors
# ---------------------------------------------------------------------------

@hg.reference_service
def _benchmark_reference(path: str = "benchmark") -> TS[int]: ...


@hg.service_impl(interfaces=_benchmark_reference)
def _benchmark_reference_std_impl(path: str, cycles: int) -> TS[int]:
    return _int_pulse(cycles)


@hg.service_impl(interfaces=_benchmark_reference)
def _benchmark_reference_py_impl(path: str, cycles: int) -> TS[int]:
    return _add_one_py(_int_pulse(cycles))


def service_reference_std(cycle_scale: float, size_scale: float):
    cycles = int(20_000 * cycle_scale)
    clients = max(1, int(4 * size_scale))

    @graph
    def g():
        hg.register_service("benchmark", _benchmark_reference_std_impl, cycles=cycles)
        for _ in range(clients):
            null_sink(_benchmark_reference(path="benchmark"))

    return g, cycles


def service_reference_py(cycle_scale: float, size_scale: float):
    cycles = int(20_000 * cycle_scale)
    clients = max(1, int(4 * size_scale))

    @graph
    def g():
        hg.register_service("benchmark", _benchmark_reference_py_impl, cycles=cycles)
        for _ in range(clients):
            null_sink(_benchmark_reference(path="benchmark"))

    return g, cycles


@hg.request_reply_service
def _benchmark_request_reply(request: TS[int], path: str = "benchmark") -> TS[int]: ...


@hg.service_impl(interfaces=_benchmark_request_reply)
def _benchmark_request_reply_std_impl(
    request: TSD[int, TS[int]], path: str
) -> TSD[int, TS[int]]:
    return map_(_mapped_std, request)


@hg.service_impl(interfaces=_benchmark_request_reply)
def _benchmark_request_reply_py_impl(
    request: TSD[int, TS[int]], path: str
) -> TSD[int, TS[int]]:
    return map_(_mapped_py, request)


def service_request_reply_std(cycle_scale: float, size_scale: float):
    requests = int(10_000 * cycle_scale)
    cycles = requests * 2
    clients = max(1, int(4 * size_scale))

    @graph
    def g():
        source = _service_int_pulse(requests)
        hg.register_service("benchmark", _benchmark_request_reply_std_impl)
        for _ in range(clients):
            null_sink(_benchmark_request_reply(source, path="benchmark"))

    return g, cycles


def service_request_reply_py(cycle_scale: float, size_scale: float):
    requests = int(10_000 * cycle_scale)
    cycles = requests * 2
    clients = max(1, int(4 * size_scale))

    @graph
    def g():
        source = _service_int_pulse(requests)
        hg.register_service("benchmark", _benchmark_request_reply_py_impl)
        for _ in range(clients):
            null_sink(_benchmark_request_reply(source, path="benchmark"))

    return g, cycles


def service_request_reply_multiple_paths_std(
    cycle_scale: float, size_scale: float
):
    requests = int(5_000 * cycle_scale)
    cycles = requests * 2
    paths = max(2, int(4 * size_scale))

    @graph
    def g():
        source = _service_int_pulse(requests)
        for path_index in range(paths):
            path = f"benchmark-{path_index}"
            hg.register_service(path, _benchmark_request_reply_std_impl)
            null_sink(_benchmark_request_reply(source, path=path))

    return g, cycles


@hg.subscription_service
def _benchmark_subscription(key: TS[str], path: str = "benchmark") -> TS[int]: ...


@graph
def _subscription_value_std(key: TS[str]) -> TS[int]:
    return hg.const(1, tp=TS[int])


@compute_node
def _subscription_value_py(key: TS[str]) -> TS[int]:
    return len(key.value)


@hg.service_impl(interfaces=_benchmark_subscription)
def _benchmark_subscription_std_impl(
    key: TSS[str], path: str
) -> TSD[str, TS[int]]:
    return map_(_subscription_value_std, __keys__=key)


@hg.service_impl(interfaces=_benchmark_subscription)
def _benchmark_subscription_py_impl(
    key: TSS[str], path: str
) -> TSD[str, TS[int]]:
    return map_(_subscription_value_py, __keys__=key)


def service_subscription_std(cycle_scale: float, size_scale: float):
    requests = int(10_000 * cycle_scale)
    cycles = requests * 2
    clients = max(1, int(1 * size_scale))

    @graph
    def g():
        hg.register_service("benchmark", _benchmark_subscription_std_impl)
        source = _service_symbol_pulse(requests)
        for _ in range(clients):
            null_sink(_benchmark_subscription(source, path="benchmark"))

    return g, cycles


def service_subscription_py(cycle_scale: float, size_scale: float):
    requests = int(10_000 * cycle_scale)
    cycles = requests * 2
    clients = max(1, int(1 * size_scale))

    @graph
    def g():
        hg.register_service("benchmark", _benchmark_subscription_py_impl)
        source = _service_symbol_pulse(requests)
        for _ in range(clients):
            null_sink(_benchmark_subscription(source, path="benchmark"))

    return g, cycles


@hg.adaptor
def _benchmark_adaptor(value: TS[int], path: str = "benchmark") -> TS[int]: ...


@hg.adaptor_impl(interfaces=_benchmark_adaptor)
def _benchmark_adaptor_std_impl(value: TS[int], path: str) -> TS[int]:
    return value + 1


@hg.adaptor_impl(interfaces=_benchmark_adaptor)
def _benchmark_adaptor_py_impl(value: TS[int], path: str) -> TS[int]:
    return _add_one_py(value)


def adaptor_std(scale: float):
    cycles = int(20_000 * scale)

    @graph
    def g():
        hg.register_adaptor("benchmark", _benchmark_adaptor_std_impl)
        null_sink(_benchmark_adaptor(_int_pulse(cycles), path="benchmark"))

    return g, cycles


def adaptor_py(scale: float):
    cycles = int(20_000 * scale)

    @graph
    def g():
        hg.register_adaptor("benchmark", _benchmark_adaptor_py_impl)
        null_sink(_benchmark_adaptor(_int_pulse(cycles), path="benchmark"))

    return g, cycles


@hg.service_adaptor
def _benchmark_service_adaptor(
    request: TS[int], path: str = "benchmark"
) -> TS[int]: ...


@hg.service_adaptor_impl(interfaces=_benchmark_service_adaptor)
def _benchmark_service_adaptor_std_impl(
    request: TSD[int, TS[int]], path: str
) -> TSD[int, TS[int]]:
    return map_(_mapped_std, request)


@hg.service_adaptor_impl(interfaces=_benchmark_service_adaptor)
def _benchmark_service_adaptor_py_impl(
    request: TSD[int, TS[int]], path: str
) -> TSD[int, TS[int]]:
    return map_(_mapped_py, request)


def service_adaptor_std(cycle_scale: float, size_scale: float):
    requests = int(10_000 * cycle_scale)
    cycles = requests * 2
    clients = max(1, int(4 * size_scale))

    @graph
    def g():
        source = _service_int_pulse(requests)
        hg.register_adaptor("benchmark", _benchmark_service_adaptor_std_impl)
        for client in range(clients):
            null_sink(_benchmark_service_adaptor(source + client, path="benchmark"))

    return g, cycles


def service_adaptor_py(cycle_scale: float, size_scale: float):
    requests = int(10_000 * cycle_scale)
    cycles = requests * 2
    clients = max(1, int(4 * size_scale))

    @graph
    def g():
        source = _service_int_pulse(requests)
        hg.register_adaptor("benchmark", _benchmark_service_adaptor_py_impl)
        for client in range(clients):
            null_sink(_benchmark_service_adaptor(source + client, path="benchmark"))

    return g, cycles


def _scenario(
    group: str,
    label: str,
    factory: Callable,
    *,
    suite: str = "core",
    modes: tuple[str, ...] = ALL_MODES,
    independent_size: bool = False,
) -> BenchmarkScenario:
    return BenchmarkScenario(
        group=group,
        label=label,
        factory=factory,
        suite=suite,
        modes=modes,
        independent_size=independent_size,
    )


# Stable IDs remain suitable for command-line filtering and historical result
# matching. Reports use the group and label fields instead of exposing these
# implementation-oriented names as the primary description.
SCENARIOS = {
    "construct_std": _scenario(
        "Graph construction", "Wide/deep graph - native operators",
        construct_std, independent_size=True),
    "construct_py": _scenario(
        "Graph construction", "Wide/deep graph - Python nodes",
        construct_py, independent_size=True),

    "tick_std": _scenario(
        "Scheduler", "Feedback hot loop - native add", tick_std),
    "tick_py": _scenario(
        "Scheduler", "Five-node Python compute chain", tick_py),
    "scheduler_fan_out_std": _scenario(
        "Scheduler", "One source fanning out to many sinks",
        scheduler_fan_out_std, suite="diagnostic", independent_size=True),
    "scheduler_fan_in_std": _scenario(
        "Scheduler", "Many branches joining one output",
        scheduler_fan_in_std, suite="diagnostic", independent_size=True),
    "scheduler_conflated_fixed_tsl_std": _scenario(
        "Scheduler", "Eight notifications conflated into one reducer",
        scheduler_conflated_fixed_tsl_std, suite="diagnostic"),

    "python_generator_boundary": _scenario(
        "Python boundary", "Python scalar generator to native sink",
        python_generator_boundary, suite="diagnostic"),
    "python_sink_boundary": _scenario(
        "Python boundary", "Python scalar generator to Python sink",
        python_sink_boundary, suite="diagnostic"),
    "python_global_state_boundary": _scenario(
        "Python boundary", "Python compute with injected GlobalState",
        python_global_state_boundary, suite="diagnostic", modes=HG_CPP_ONLY),

    "type_int_std": _scenario(
        "Value types", "Integer arithmetic", type_int_std),
    "type_float_std": _scenario(
        "Value types", "Floating-point arithmetic", type_float_std),
    "type_str_std": _scenario(
        "Value types", "String concatenation", type_str_std),
    "type_cs_std": _scenario(
        "Value types", "CompoundScalar field access - native operators",
        type_cs_std),
    "type_cs_py": _scenario(
        "Value types", "CompoundScalar crossing Python nodes", type_cs_py),
    "python_owned_pass_through_native": _scenario(
        "Python-owned structured scalars",
        "Whole-object pass-through - native layout",
        python_owned_pass_through_native,
        suite="diagnostic", modes=HG_CPP_ONLY),
    "python_owned_pass_through_python": _scenario(
        "Python-owned structured scalars",
        "Whole-object pass-through - Python-owned",
        python_owned_pass_through_python,
        suite="diagnostic", modes=HG_CPP_ONLY),
    "python_owned_project_one_native": _scenario(
        "Python-owned structured scalars",
        "One field projection - native layout",
        python_owned_project_one_native,
        suite="diagnostic", modes=HG_CPP_ONLY),
    "python_owned_project_one_python": _scenario(
        "Python-owned structured scalars",
        "One field projection - Python-owned",
        python_owned_project_one_python,
        suite="diagnostic", modes=HG_CPP_ONLY),
    "python_owned_project_several_native": _scenario(
        "Python-owned structured scalars",
        "Three field projections - native layout",
        python_owned_project_several_native,
        suite="diagnostic", modes=HG_CPP_ONLY),
    "python_owned_project_several_python": _scenario(
        "Python-owned structured scalars",
        "Three field projections - Python-owned",
        python_owned_project_several_python,
        suite="diagnostic", modes=HG_CPP_ONLY),
    "python_owned_construct_native": _scenario(
        "Python-owned structured scalars",
        "Construction from fields - native layout",
        python_owned_construct_native,
        suite="diagnostic", modes=HG_CPP_ONLY),
    "python_owned_construct_python": _scenario(
        "Python-owned structured scalars",
        "Construction from fields - Python-owned",
        python_owned_construct_python,
        suite="diagnostic", modes=HG_CPP_ONLY),
    "python_owned_dedup_native": _scenario(
        "Python-owned structured scalars",
        "Equality and deduplication - native layout",
        python_owned_dedup_native,
        suite="diagnostic", modes=HG_CPP_ONLY),
    "python_owned_dedup_python": _scenario(
        "Python-owned structured scalars",
        "Equality and deduplication - Python-owned",
        python_owned_dedup_python,
        suite="diagnostic", modes=HG_CPP_ONLY),
    "type_tsb_partial_fields_std": _scenario(
        "Value types", "Bundle with partial field updates",
        type_tsb_partial_fields_std, suite="diagnostic"),
    "type_tsw_append_evict_std": _scenario(
        "Value types", "Tick window append and eviction",
        type_tsw_append_evict_std, suite="diagnostic"),
    "tss_add_remove_std": _scenario(
        "Value types", "Set add/remove deltas", tss_add_remove_std,
        suite="diagnostic", independent_size=True),

    "tsd_dense_std": _scenario(
        "TSD - dense", "Map and reduce - native child graph",
        tsd_dense_std, independent_size=True),
    "tsd_dense_py": _scenario(
        "TSD - dense", "Map and reduce - Python map child",
        tsd_dense_py, independent_size=True),
    "tsd_dense_source_std": _scenario(
        "TSD - dense", "Source only", tsd_dense_source_std,
        suite="diagnostic", independent_size=True),
    "tsd_dense_map_std": _scenario(
        "TSD - dense", "Map only", tsd_dense_map_std,
        suite="diagnostic", independent_size=True),
    "tsd_dense_reduce_std": _scenario(
        "TSD - dense", "Reduce only", tsd_dense_reduce_std,
        suite="diagnostic", independent_size=True),
    "tsd_dense_strkeys_std": _scenario(
        "TSD - dense", "String-key map and reduce", tsd_dense_strkeys_std,
        suite="diagnostic", independent_size=True),

    "tsd_sparse_std": _scenario(
        "TSD - sparse", "Map and reduce with five updates per cycle",
        tsd_sparse_std, independent_size=True),
    "tsd_sparse_source_std": _scenario(
        "TSD - sparse", "Source only", tsd_sparse_source_std,
        suite="diagnostic", independent_size=True),
    "tsd_sparse_map_std": _scenario(
        "TSD - sparse", "Map only", tsd_sparse_map_std,
        suite="diagnostic", independent_size=True),
    "tsd_sparse_reduce_std": _scenario(
        "TSD - sparse", "Reduce only", tsd_sparse_reduce_std,
        suite="diagnostic", independent_size=True),
    "tsd_sparse_large_capacity_std": _scenario(
        "TSD - sparse", "Large retained capacity with two updates per cycle",
        tsd_sparse_large_capacity_std, suite="diagnostic",
        independent_size=True),

    "tsd_churn_std": _scenario(
        "TSD - key lifecycle", "Map and reduce with key replacement",
        tsd_churn_std, independent_size=True),
    "tsd_churn_py": _scenario(
        "TSD - key lifecycle", "Python map with key replacement",
        tsd_churn_py, independent_size=True),
    "tsd_churn_source_std": _scenario(
        "TSD - key lifecycle", "Key replacement - source only",
        tsd_churn_source_std, suite="diagnostic", independent_size=True),
    "tsd_churn_map_std": _scenario(
        "TSD - key lifecycle", "Key replacement - map only",
        tsd_churn_map_std, suite="diagnostic", independent_size=True),
    "tsd_churn_reduce_std": _scenario(
        "TSD - key lifecycle", "Key replacement - reduce only",
        tsd_churn_reduce_std, suite="diagnostic", independent_size=True),
    "tsd_capacity_growth_std": _scenario(
        "TSD - key lifecycle", "Monotonic key growth across capacity boundaries",
        tsd_capacity_growth_std, suite="diagnostic", independent_size=True),
    "tsd_clear_repopulate_std": _scenario(
        "TSD - key lifecycle", "Full clear followed by repopulation",
        tsd_clear_repopulate_std, suite="diagnostic", independent_size=True),
    "tsd_key_reactivation_std": _scenario(
        "TSD - key lifecycle", "Remove and later recreate the same keys",
        tsd_key_reactivation_std, suite="diagnostic", independent_size=True),
    "tsd_two_input_union_std": _scenario(
        "TSD - key lifecycle", "Two-input map with union membership",
        tsd_two_input_union_std, suite="diagnostic", independent_size=True),
    "tsd_two_input_intersection_std": _scenario(
        "TSD - key lifecycle", "Two-input map with intersection membership",
        tsd_two_input_intersection_std, suite="diagnostic",
        independent_size=True),
    "tsd_explicit_key_set_std": _scenario(
        "TSD - key lifecycle", "Map driven by an explicit key set",
        tsd_explicit_key_set_std, suite="diagnostic", independent_size=True),

    "reduce_tsd_nested_graph_std": _scenario(
        "Reduce", "Dense TSD with nested graph combiner",
        reduce_tsd_nested_graph_std, suite="diagnostic", independent_size=True),
    "reduce_tsd_python_combiner": _scenario(
        "Reduce", "Dense TSD with Python node combiner",
        reduce_tsd_python_combiner, suite="diagnostic", independent_size=True),
    "reduce_fixed_tsl_ordered_std": _scenario(
        "Reduce", "Ordered non-associative fixed-list reduction",
        reduce_fixed_tsl_ordered_std, suite="diagnostic"),
    "reduce_tsd_without_zero_std": _scenario(
        "Reduce", "Empty/singleton/two-value TSD without zero",
        reduce_tsd_without_zero_std, suite="diagnostic", modes=HG_CPP_ONLY),
    "reduce_dynamic_tsl_std": _scenario(
        "hg_cpp - dynamic TSL", "Sparse map and reduce over an unbounded list",
        reduce_dynamic_tsl_std, suite="diagnostic", modes=HG_CPP_ONLY,
        independent_size=True),

    "switch_alternating_branch_sizes_std": _scenario(
        "Nested graphs", "Switch alternating small and large branches",
        switch_alternating_branch_sizes_std),
    "switch_keyed_collection_std": _scenario(
        "Nested graphs", "Switch returning a churning keyed collection",
        switch_keyed_collection_std, independent_size=True),
    "mesh_std": _scenario(
        "Nested graphs", "Mesh with predecessor dependencies and key churn",
        mesh_std, independent_size=True),

    "service_reference_std": _scenario(
        "Services", "Reference service - native implementation",
        service_reference_std, independent_size=True),
    "service_reference_py": _scenario(
        "Services", "Reference service - Python implementation",
        service_reference_py, independent_size=True),
    "service_request_reply_std": _scenario(
        "Services", "Request/reply service - native implementation",
        service_request_reply_std, independent_size=True),
    "service_request_reply_py": _scenario(
        "Services", "Request/reply service - Python implementation",
        service_request_reply_py, independent_size=True),
    "service_request_reply_multiple_paths_std": _scenario(
        "Services", "Request/reply service across multiple paths",
        service_request_reply_multiple_paths_std, suite="diagnostic",
        independent_size=True),
    "service_subscription_std": _scenario(
        "Services", "Subscription service - native implementation",
        service_subscription_std, independent_size=True),
    "service_subscription_py": _scenario(
        "Services", "Subscription service - Python implementation",
        service_subscription_py, independent_size=True),

    "adaptor_std": _scenario(
        "Adaptors", "Duplex adaptor - native implementation", adaptor_std),
    "adaptor_py": _scenario(
        "Adaptors", "Duplex adaptor - Python implementation", adaptor_py),
    "service_adaptor_std": _scenario(
        "Adaptors", "Multiplexed service adaptor - native implementation",
        service_adaptor_std, independent_size=True),
    "service_adaptor_py": _scenario(
        "Adaptors", "Multiplexed service adaptor - Python implementation",
        service_adaptor_py, independent_size=True),
}
