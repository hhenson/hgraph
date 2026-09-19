"""Public-wiring restart matrix: every cut must match an uninterrupted run.

Sources emit future deltas at runtime, so a resumed removal is interpreted
against the restored source baseline rather than a fresh test conversion cache.
"""
from datetime import date, datetime

import hgraph as hg
import hgraph_persistence as persistence
import pytest


class Quote(hg.TimeSeriesSchema):
    price: hg.TS[float]
    quantity: hg.TS[int]


class Portfolio(hg.TimeSeriesSchema):
    positions: hg.TSD[str, hg.TSB[Quote]]
    flags: hg.TSS[str]
    samples: hg.TSL[hg.TS[int], hg.Size[2]]


class Total(hg.TimeSeriesSchema):
    value: hg.TS[int]


@hg.compute_node
def running_total(ts: hg.TS[int], state: hg.RECORDABLE_STATE[Total] = None) -> hg.TS[int]:
    state.value.value = (state.value.value if state.value.valid else 0) + ts.value
    return state.value.value


def _source(schema, events, offset):
    @hg.generator
    def observations() -> schema:
        for index, value in enumerate(events):
            if value is not None:
                yield hg.MIN_ST + (offset + index) * hg.MIN_TD, value
    return observations


def _run(component, schemas, output_schema, inputs, offset, store=None, previous=None):
    sources = [_source(schema, values, offset) for schema, values in zip(schemas, inputs)]

    @hg.graph
    def application() -> output_schema:
        return component(*(source() for source in sources))

    count = len(inputs[0])
    with hg.GlobalState() as state:
        if store is not None:
            persistence.configure_component_recovery(
                store, component.recordable_id, f"cut-{offset + count}", previous,
                revision="scenario-v1", global_state=state)
        actual = hg.eval_node(application, __start_time__=hg.MIN_ST + offset * hg.MIN_TD,
                              __end_time__=hg.MIN_ST + (offset + count) * hg.MIN_TD)
    # The harness omits trailing unticked cycles; preserve them when joining days.
    actual = list(actual or ())
    assert len(actual) <= count
    return actual + [None] * (count - len(actual))


def compare_restarts(tmp_path, component, schemas, output_schema, inputs, cuts):
    expected = _run(component, schemas, output_schema, inputs, 0)
    resumed = []
    previous = None
    begin = 0
    for end in (*cuts, len(inputs[0])):
        store = persistence.ComponentCheckpointStore(tmp_path)
        resumed.extend(_run(component, schemas, output_schema,
                            [values[begin:end] for values in inputs], begin, store, previous))
        previous = f"cut-{end}"
        assert store.contains(previous)
        begin = end
    assert resumed == expected
    return resumed


SHAPES = [
    ("int", hg.TS[int], [None, 1, 2, None, -1, 0, 7, None]),
    ("float", hg.TS[float], [None, -0.0, 1.25, None, -2.5, 0.0, 1e-12, None]),
    ("str", hg.TS[str], [None, "", "alpha", None, "β", "", "omega", None]),
    ("tuple", hg.TS[tuple[int, ...]], [None, (), (1, 2), None, (3,), (), (4, 5), None]),
    ("date", hg.TS[date], [None, date(2026, 1, 1), date(2026, 1, 2), None,
                          date(2026, 2, 1), date(2026, 2, 2), date(2026, 3, 1), None]),
    ("datetime", hg.TS[datetime], [None, datetime(2026, 1, 1), datetime(2026, 1, 2), None,
                                  datetime(2026, 2, 1), datetime(2026, 2, 2), datetime(2026, 3, 1), None]),
    ("signal", hg.SIGNAL, [None, True, True, None, True, None, True, None]),
    ("set", hg.TSS[str], [None, {"a", "b"}, {hg.Removed("a")}, None,
                          {"c"}, {hg.Removed("b"), hg.Removed("c")}, {"a"}, None]),
    ("fixed-list", hg.TSL[hg.TS[int], hg.Size[3]],
     [None, {0: 1}, {2: 3}, None, {1: 2}, {0: -1, 2: 0}, {1: 7}, None]),
    ("dynamic-list", hg.TSL[hg.TS[int], hg.Size[-1]],
     [None, {0: 1, 1: 2, 2: 3}, {1: hg.REMOVE}, None, {1: 4, 2: 5}, {0: hg.REMOVE}, {0: 7}, None]),
    ("dict", hg.TSD[str, hg.TS[int]],
     [None, {"a": 1, "b": 2}, {"a": hg.REMOVE}, None, {"c": 3},
      {"b": hg.REMOVE, "c": hg.REMOVE}, {"a": 7}, None]),
    ("bundle", hg.TSB[Quote],
     [None, {"price": 1.5}, {"quantity": 2}, None, {"price": 2.5},
      {"quantity": 0}, {"price": -1.0, "quantity": 7}, None]),
    ("dict-bundle", hg.TSD[str, hg.TSB[Quote]],
     [None, {"a": {"price": 1.5}, "b": {"quantity": 2}}, {"a": {"quantity": 4}}, None,
      {"a": hg.REMOVE}, {"b": {"price": 2.5}}, {"a": {"price": 7.0}}, None]),
    ("dict-set", hg.TSD[str, hg.TSS[str]],
     [None, {"a": {"x", "y"}, "b": {"z"}}, {"a": {hg.Removed("x")}}, None,
      {"b": hg.REMOVE}, {"a": {"z"}}, {"b": {"new"}}, None]),
    ("mixed-bundle", hg.TSB[Portfolio],
     [None, {"positions": {"a": {"price": 1.0}}, "flags": {"open"}, "samples": {0: 1}},
      {"positions": {"a": {"quantity": 2}}, "samples": {1: 3}}, None,
      {"positions": {"a": hg.REMOVE}, "flags": {hg.Removed("open")}},
      {"samples": {0: 4}}, {"positions": {"b": {"price": 7.0}}, "flags": {"closed"}}, None]),
]
CUTS = [(1,), (2,), (3,), (4,), (5,), (6,), (7,), tuple(range(1, 8))]


@pytest.mark.parametrize("name,schema,events", SHAPES, ids=[item[0] for item in SHAPES])
@pytest.mark.parametrize("cuts", CUTS)
def test_schema_restart_at_every_boundary(tmp_path, name, schema, events, cuts):
    @hg.compute_node
    def copy_delta(ts: schema) -> schema:
        return ts.delta_value

    @hg.component
    def scenario(ts: schema) -> schema:
        return copy_delta(ts)

    compare_restarts(tmp_path, scenario, (schema,), schema, (events,), cuts)


@pytest.mark.parametrize("cuts", CUTS)
@pytest.mark.parametrize("kind", ["dict", "fixed-list", "dynamic-list", "nested-dict"])
def test_stateful_maps_restart_at_every_boundary(tmp_path, cuts, kind):
    if kind == "nested-dict":
        schema = hg.TSD[str, hg.TSD[str, hg.TS[int]]]
        events = [None, {"x": {"a": 1, "b": 2}, "y": {"c": 10}}, {"x": {"a": 3}}, None,
                  {"x": {"b": hg.REMOVE}}, {"y": hg.REMOVE}, {"x": {"b": 7}, "y": {"c": 4}}, None]

        @hg.graph
        def inner(ts: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
            return hg.map_(running_total, ts)

        @hg.component
        def scenario(ts: schema) -> schema:
            return hg.map_(inner, ts)
    else:
        shape_name = {"dict": "dict", "fixed-list": "fixed-list", "dynamic-list": "dynamic-list"}[kind]
        _, schema, events = next(item for item in SHAPES if item[0] == shape_name)

        @hg.component
        def scenario(ts: schema) -> schema:
            return hg.map_(running_total, ts)

    compare_restarts(tmp_path, scenario, (schema,), schema, (events,), cuts)


@pytest.mark.parametrize("cuts", CUTS)
@pytest.mark.parametrize("duration", [False, True])
@pytest.mark.parametrize("reset", [False, True])
def test_window_aggregates_restart_at_every_boundary(tmp_path, cuts, duration, reset):
    period = hg.MIN_TD * 3 if duration else 3
    minimum = hg.MIN_TD if duration else 2
    values = [None, 1.0, 1e16, None, -1e16, 2.0, -3.0, 4.0]
    resets = [None, None, None, True, None, None, True, None]

    class WindowSummary(hg.TimeSeriesSchema):
        total: hg.TS[float]
        average: hg.TS[float]
        minimum: hg.TS[float]
        maximum: hg.TS[float]

    @hg.component
    def scenario(ts: hg.TS[float], clear: hg.SIGNAL) -> hg.TSB[WindowSummary]:
        window = hg.to_window(ts, period, minimum, reset=clear) if reset else hg.to_window(ts, period, minimum)
        return hg.combine[hg.TSB[WindowSummary]](total=hg.sum_(window), average=hg.mean(window),
                                                minimum=hg.min_(window), maximum=hg.max_(window))

    compare_restarts(tmp_path, scenario, (hg.TS[float], hg.SIGNAL), hg.TSB[WindowSummary], (values, resets), cuts)


@pytest.mark.parametrize("cuts", CUTS)
@pytest.mark.parametrize("duration", [False, True])
def test_windows_inside_stateful_maps_restart(tmp_path, cuts, duration):
    period = hg.MIN_TD * 3 if duration else 3
    minimum = hg.MIN_TD if duration else 2
    schema = hg.TSD[str, hg.TS[float]]
    events = [None, {"a": 1.0, "b": 10.0}, {"a": 2.0}, None,
              {"a": 4.0, "b": hg.REMOVE}, {"b": 3.0}, {"a": 8.0, "b": 5.0}, None]

    @hg.graph
    def average(ts: hg.TS[float]) -> hg.TS[float]:
        return hg.mean(hg.to_window(ts, period, minimum))

    @hg.component
    def scenario(ts: schema) -> schema:
        return hg.map_(average, ts)

    compare_restarts(tmp_path, scenario, (schema,), schema, (events,), cuts)


@pytest.mark.parametrize("seed", range(12))
def test_seeded_key_churn_across_many_completed_days(tmp_path, seed):
    import random
    randomizer = random.Random(seed)
    live = set()
    events = []
    for _ in range(48):
        if randomizer.randrange(5) == 0:
            events.append(None)
            continue
        key = str(randomizer.randrange(8))
        if key in live and randomizer.randrange(3) == 0:
            events.append({key: hg.REMOVE})
            live.remove(key)
        else:
            events.append({key: randomizer.randrange(-100, 101)})
            live.add(key)

    @hg.component
    def scenario(ts: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
        return hg.map_(running_total, ts)

    cuts = tuple(sorted(randomizer.sample(range(1, 48), 15)))
    compare_restarts(tmp_path, scenario, (hg.TSD[str, hg.TS[int]],), hg.TSD[str, hg.TS[int]], (events,), cuts)


KEYED = hg.TSD[str, hg.TS[int]]
KEYED_EVENTS = [None, {"a": 1, "b": 2}, {"a": 3}, None, {"b": hg.REMOVE}, {"a": 4, "c": 5}, {"b": 6}, None]


def test_a_recordable_id_is_honoured_inside_a_mapped_child(tmp_path):
    # A map_ child is wired on a wiring of its own, whose own state is empty;
    # the recovery configuration is the root's. The binding asked the child's
    # state, so a ``__recordable_id__`` was honoured at the top of a component
    # and silently dropped one level down (found by adversarial review). A
    # duplicate is the difference that shows: honoured ids collide.
    @hg.graph
    def clashing(ts: hg.TS[int]) -> hg.TS[int]:
        first = running_total(ts, __recordable_id__="total")
        return running_total(first, __recordable_id__="total")

    @hg.component
    def scenario(ts: KEYED) -> KEYED:
        return hg.map_(clashing, ts)

    with pytest.raises(Exception, match="duplicate node id 'total'"):
        _run(scenario, (KEYED,), KEYED, (KEYED_EVENTS,), 0, persistence.ComponentCheckpointStore(tmp_path))
    # With nothing to recover there is no id to honour, here as at the top.
    assert _run(scenario, (KEYED,), KEYED, (KEYED_EVENTS,), 0)[1] == {"a": 1, "b": 2}


@pytest.mark.parametrize("cuts", [(3,), tuple(range(1, 8))])
def test_named_nodes_inside_a_mapped_child_restart(tmp_path, cuts):
    @hg.graph
    def named(ts: hg.TS[int]) -> hg.TS[int]:
        return running_total(ts, __recordable_id__="total")

    @hg.component
    def scenario(ts: KEYED) -> KEYED:
        return hg.map_(named, ts)

    actual = compare_restarts(tmp_path, scenario, (KEYED,), KEYED, (KEYED_EVENTS,), cuts)
    assert actual[5] == {"a": 8, "c": 5}
