"""Durable recovery through ``dmap_`` and ``spawn_`` from public Python wiring.

A ``dmap_`` owner's children live in worker processes, so its checkpoint is one
graph image per worker (RFC 0039). The contract is the one every recoverable
owner has: a run restarted at any completed day is indistinguishable from one
that was never interrupted.

For ``spawn_`` what recovers is a component INSIDE a stage. Everything else in
the stage is processed, the pipeline's sink first of all: it acts in a worker
process, no checkpoint could replay that, and it declares nothing.
"""

import os
import pickle
from pathlib import Path

import hgraph as hg
import hgraph_persistence as persistence
import pytest

from .test_component_recovery_scenarios import CUTS, _run, _source, compare_restarts, running_total
from .test_reduce_recovery_scenarios import historical_combine


# Key 1 accumulates across every boundary, key 2 leaves and returns (its state
# must NOT survive its removal), and the quiet cycles leave boundaries with
# nothing in flight. Three workers, so the keys are spread.
EVENTS = [None, {1: 2, 2: 10, 3: 100}, {1: 3}, None, {1: 4, 2: hg.REMOVE},
          {2: 1, 4: 5}, {1: 7, 3: 1}, None]
SCHEMA = hg.TSD[int, hg.TS[int]]

# Raising a process per worker per day is slow, so the process-hosted form takes
# the two cut sets that matter most: one boundary mid-run, and every boundary.
PROCESS_CUTS = [(3,), tuple(range(1, 8))]


@hg.compute_node
def settle(ts: hg.TS[int]) -> hg.TS[int]:
    return ts.value


@hg.graph
def nested_owner(ts: hg.TSD[str, hg.TS[int]]) -> hg.TS[int]:
    # The worker's child is itself a dynamic owner: a reduction whose combiners
    # each keep recordable state, so both its topology and its history have to
    # come back from the worker's image. It ends in a node that writes its own
    # output, which is the map_ form that can be checkpointed today.
    return settle(hg.reduce(historical_combine, ts, 0))


@hg.graph
def forwarding_terminal(ts: hg.TSD[str, hg.TS[int]]) -> hg.TS[int]:
    return hg.reduce(historical_combine, ts, 0)


@pytest.mark.parametrize("cuts", CUTS)
def test_dmap_state_and_membership_survive_every_restart_boundary_in_process(tmp_path, cuts):
    @hg.component
    def scenario(values: SCHEMA) -> SCHEMA:
        return hg.dmap_(running_total, values, __workers__=3, in_process=True)

    actual = compare_restarts(tmp_path, scenario, (SCHEMA,), SCHEMA, (EVENTS,), cuts)
    # Pinned so the comparison cannot pass on silence or on reset totals.
    assert actual[6] == {1: 16, 3: 101}


@pytest.mark.parametrize("cuts", PROCESS_CUTS)
def test_dmap_state_and_membership_survive_restarts_across_processes(tmp_path, cuts):
    @hg.component
    def scenario(values: SCHEMA) -> SCHEMA:
        return hg.dmap_(running_total, values, __workers__=3)

    actual = compare_restarts(tmp_path, scenario, (SCHEMA,), SCHEMA, (EVENTS,), cuts)
    assert actual[6] == {1: 16, 3: 101}


NESTED = hg.TSD[str, hg.TSD[str, hg.TS[int]]]
NESTED_RESULT = hg.TSD[str, hg.TS[int]]
NESTED_EVENTS = [None, {"x": {"a": 1, "b": 2}, "y": {"c": 10}}, {"x": {"a": 3}}, None,
                 {"x": {"b": hg.REMOVE}}, {"y": hg.REMOVE}, {"x": {"b": 7}, "y": {"c": 4}}, None]


@pytest.mark.parametrize("cuts", PROCESS_CUTS)
@pytest.mark.parametrize("in_process", [True, False], ids=["in-process", "processes"])
def test_dmap_with_a_dynamic_owner_nested_in_the_worker_survives_restarts(tmp_path, cuts, in_process):
    @hg.component
    def scenario(values: NESTED) -> NESTED_RESULT:
        return hg.dmap_(nested_owner, values, __workers__=2, in_process=in_process)

    actual = compare_restarts(tmp_path, scenario, (NESTED,), NESTED_RESULT, (NESTED_EVENTS,), cuts)
    # Pinned so the comparison cannot pass on silence: the day "x" and "y" both tick.
    assert actual[6] is not None and set(actual[6]) == {"x", "y"}


def test_dmap_refusal_reaches_through_the_worker_and_says_which_node_and_why(tmp_path):
    # Not a dmap_ limit: a child that ENDS in a reduce gives the worker's map_
    # a forwarding terminal, a form map_ itself cannot checkpoint yet. What
    # dmap_ adds is that the refusal names the node inside the worker, carries
    # map_'s own reason, and arrives at wiring. Flip this when map_ learns.
    @hg.component
    def scenario(values: NESTED) -> NESTED_RESULT:
        return hg.dmap_(forwarding_terminal, values, __workers__=2, in_process=True)

    store = persistence.ComponentCheckpointStore(tmp_path)
    with pytest.raises(Exception, match=r"hosts 'map_', which cannot be recovered: .*forwarding outputs"):
        _run(scenario, (NESTED,), NESTED_RESULT, (NESTED_EVENTS[:2],), 0, store, None)


@hg.compute_node
def forgetful_total(ts: hg.TS[int], _state: hg.STATE = None) -> hg.TS[int]:
    _state.total = getattr(_state, "total", 0) + ts.value
    return _state.total


def test_dmap_over_an_unrecoverable_child_is_refused_at_wiring_and_still_runs_without_recovery(tmp_path):
    @hg.component
    def scenario(values: SCHEMA) -> SCHEMA:
        return hg.dmap_(forgetful_total, values, __workers__=2, in_process=True)

    # Recovery configured: the owner walks its worker plans at wiring, where a
    # component learns everything else, and names what cannot be recovered.
    store = persistence.ComponentCheckpointStore(tmp_path)
    with pytest.raises(Exception, match="cannot be recovered"):
        _run(scenario, (SCHEMA,), SCHEMA, (EVENTS[:3],), 0, store, None)

    # Recovery not configured: the same child wires and runs, as most do.
    assert _run(scenario, (SCHEMA,), SCHEMA, (EVENTS[:3],), 0) == [None, {1: 2, 2: 10, 3: 100}, {1: 5}]


@hg.sink_node
def record(value: hg.TS[int], path: str, clock: hg.CLOCK = None):
    # An ordinary sink: it acts in the worker process, asks for the clock, and
    # declares nothing about recovery. One file per process, so rows from the
    # worker of each day stay apart until they are read back in time order.
    with open(f"{path}.{os.getpid()}", "ab") as stream:
        pickle.dump((clock.evaluation_time, value.value), stream)


@hg.component
def running(ts: hg.TS[int]) -> hg.TS[int]:
    return running_total(ts)


@hg.graph
def priced_and_published(ts: hg.TS[int], path: str) -> None:
    # What recovers is the component. The sink beside it is processed.
    record(running(ts), path=path)


@hg.graph
def priced(ts: hg.TS[int]) -> hg.TS[int]:
    return running(ts)


def _rows(path):
    rows = []
    for file in sorted(Path(path).parent.glob(Path(path).name + ".*")):
        with file.open("rb") as stream:
            while True:
                try:
                    rows.append(pickle.load(stream))
                except EOFError:
                    break
    assert all(int(file.suffix[1:]) != os.getpid() for file in Path(path).parent.glob(Path(path).name + ".*"))
    return sorted(rows)


def _run_pipeline(pipeline, events, offset, store=None, previous=None):
    source = _source(hg.TS[int], events, offset)

    @hg.graph
    def application() -> None:
        hg.spawn_(pipeline, source())

    with hg.GlobalState() as state:
        if store is not None:
            persistence.configure_component_recovery(
                store, running.recordable_id, f"cut-{offset + len(events)}", previous,
                revision="scenario-v1", global_state=state)
        hg.eval_node(application, __start_time__=hg.MIN_ST + offset * hg.MIN_TD,
                     __end_time__=hg.MIN_ST + (offset + len(events)) * hg.MIN_TD)


PIPELINE_EVENTS = [None, 1, 2, None, 3, 4, None, 5]
PIPELINES = {
    "one-stage": lambda path: hg.bind_(priced_and_published, path=path),
    "sink-in-the-next-stage": lambda path: hg.pipeline_([priced, hg.bind_(record, path=path)]),
}


@pytest.mark.parametrize("shape", list(PIPELINES))
@pytest.mark.parametrize("cuts", PROCESS_CUTS)
def test_spawn_component_inside_a_stage_survives_restarts_and_its_sink_declares_nothing(tmp_path, cuts, shape):
    expected_path, resumed_path = str(tmp_path / "expected"), str(tmp_path / "resumed")
    _run_pipeline(PIPELINES[shape](expected_path), PIPELINE_EVENTS, 0)
    expected = _rows(expected_path)
    # Pinned so the comparison cannot pass on silence or on reset totals.
    assert [value for _, value in expected] == [1, 3, 6, 10, 15]

    previous, begin = None, 0
    for end in (*cuts, len(PIPELINE_EVENTS)):
        store = persistence.ComponentCheckpointStore(tmp_path / "store")
        _run_pipeline(PIPELINES[shape](resumed_path), PIPELINE_EVENTS[begin:end], begin, store, previous)
        previous = f"cut-{end}"
        assert store.contains(previous)
        begin = end
    assert _rows(resumed_path) == expected


def test_spawn_restarts_without_recovery_lose_the_component_and_the_sink_sees_it(tmp_path):
    # The control for the test above: the same restarts with nothing configured.
    path = str(tmp_path / "forgetful")
    for begin, end in ((0, 3), (3, len(PIPELINE_EVENTS))):
        _run_pipeline(PIPELINES["one-stage"](path), PIPELINE_EVENTS[begin:end], begin)
    assert [value for _, value in _rows(path)] == [1, 3, 3, 7, 12]


@hg.graph
def forgetful_and_published(ts: hg.TS[int], path: str) -> None:
    record(forgetful_component(ts), path=path)


@hg.component
def forgetful_component(ts: hg.TS[int]) -> hg.TS[int]:
    return forgetful_total(ts)


def test_spawn_with_an_unrecoverable_node_inside_the_hosted_component_is_refused_at_wiring(tmp_path):
    source = _source(hg.TS[int], [None, 1], 0)

    @hg.graph
    def application() -> None:
        hg.spawn_(hg.bind_(forgetful_and_published, path=str(tmp_path / "trace")), source())

    with hg.GlobalState() as state:
        persistence.configure_component_recovery(
            persistence.ComponentCheckpointStore(tmp_path / "store"), forgetful_component.recordable_id,
            "cut-2", None, revision="scenario-v1", global_state=state)
        with pytest.raises(Exception, match=r"spawn_ worker 0 .* cannot be recovered"):
            hg.eval_node(application, __start_time__=hg.MIN_ST, __end_time__=hg.MIN_ST + 2 * hg.MIN_TD)
