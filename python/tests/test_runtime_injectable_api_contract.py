"""0.5.41-derived contract for Python-visible runtime and injectable APIs.

The ordinary module-surface audit cannot see objects created only while a
Python node callback is running.  These tests therefore exercise both the
live callback objects and the generated native stub.  Runtime topology and
lifecycle mutation are classified separately in the migration guide rather
than being mistaken for missing user APIs here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from pathlib import Path
import re

import _hgraph
import hgraph as hg
import pytest


SCHEDULER_API = {
    "has_tag",
    "is_scheduled",
    "is_scheduled_now",
    "next_scheduled_time",
    "pop_tag",
    "reset",
    "schedule",
    "un_schedule",
}

CLOCK_API = {
    "cycle_time",
    "evaluation_time",
    "next_cycle_evaluation_time",
    "now",
}

ENGINE_API = {
    "add_after_evaluation_notification",
    "add_before_evaluation_notification",
    "end_time",
    "evaluation_clock",
    "evaluation_mode",
    "is_stop_requested",
    "request_engine_stop",
    "start_time",
}

GLOBAL_STATE_API = {
    "__bool__",
    "__contains__",
    "__delitem__",
    "__getitem__",
    "__iter__",
    "__len__",
    "__setitem__",
    "get",
    "items",
    "keys",
    "pop",
    "setdefault",
    "values",
}

STATE_API = {
    "as_schema",
    "is_updated",
    "items",
    "keys",
    "reset_updated",
    "values",
}

TRAITS_API = {"get_trait", "get_trait_or"}

RECORDABLE_STATE_API = {
    "as_schema",
    "modified",
    "valid",
    "value",
}

NODE_API = {
    "graph",
    "has_input",
    "has_output",
    "is_started",
    "label",
    "node_id",
    "node_index",
    "node_ndx",
    "node_type",
    "notify_next_cycle",
    "owning_graph_id",
    "started",
}

GRAPH_API = {
    "evaluation_clock",
    "evaluating",
    "graph_id",
    "is_started",
    "label",
    "nodes",
    "parent_node",
    "started",
}

LOGGER_API = {
    "critical",
    "debug",
    "error",
    "exception",
    "info",
    "log",
    "warning",
}


class _AuditRecordableState(hg.TimeSeriesSchema):
    count: hg.TS[int]


def _assert_api(value, expected):
    missing = sorted(expected.difference(dir(value)))
    assert not missing, f"{type(value).__name__} is missing {missing}"


def _assert_declared_api(value, expected):
    """Inspect a dynamic-attribute view without invoking its field lookup."""
    missing = sorted(expected.difference(type(value).__dict__))
    assert not missing, f"{type(value).__name__} is missing {missing}"


def test_public_injectable_annotations_identify_the_runtime_types():
    assert hg.SCHEDULER is _hgraph.Scheduler
    assert hg.CLOCK is hg.EvaluationClock is _hgraph.EvaluationClock
    assert hg.NODE is hg.Node is _hgraph.Node
    assert hg.EvaluationEngineApi is _hgraph.EvaluationEngineApi
    assert hg.Traits is _hgraph.Traits
    assert hg.LOGGER is logging.Logger

    _assert_api(hg.STATE, STATE_API)
    _assert_api(hg.SCHEDULER, SCHEDULER_API)
    _assert_api(hg.CLOCK, CLOCK_API)
    _assert_api(hg.NODE, NODE_API)
    _assert_api(hg.EvaluationEngineApi, ENGINE_API)
    _assert_api(hg.Traits, TRAITS_API)
    _assert_api(hg.LOGGER, LOGGER_API)


def test_live_injected_objects_expose_and_execute_the_supported_0_5_api():
    observations = []
    retained_traits = []

    @hg.compute_node
    def inspect(
        trigger: hg.TS[int],
        scheduler: hg.SCHEDULER = None,
        clock: hg.CLOCK = None,
        engine: hg.EvaluationEngineApi = None,
        global_state: hg.GlobalState = None,
        state: hg.STATE = None,
        traits: hg.Traits = None,
        logger: hg.LOGGER = None,
        node: hg.NODE = None,
    ) -> hg.TS[int]:
        _assert_api(scheduler, SCHEDULER_API)
        _assert_api(clock, CLOCK_API)
        _assert_api(engine, ENGINE_API)
        _assert_api(global_state, GLOBAL_STATE_API)
        _assert_api(state, STATE_API)
        _assert_api(traits, TRAITS_API)
        _assert_api(logger, LOGGER_API)
        _assert_api(node, NODE_API)
        _assert_api(node.graph, GRAPH_API)

        assert isinstance(scheduler, hg.SCHEDULER)
        assert isinstance(clock, hg.CLOCK)
        assert isinstance(engine, hg.EvaluationEngineApi)
        assert isinstance(logger, hg.LOGGER)
        assert isinstance(traits, hg.Traits)
        assert isinstance(node, hg.NODE)
        assert isinstance(node.graph, hg.Graph)

        assert clock.evaluation_time == hg.MIN_ST
        assert clock.next_cycle_evaluation_time == hg.MIN_ST + hg.MIN_TD
        assert isinstance(clock.now, datetime)
        assert isinstance(clock.cycle_time, timedelta)
        assert engine.evaluation_clock.evaluation_time == clock.evaluation_time
        assert engine.evaluation_mode == hg.EvaluationMode.SIMULATION
        assert engine.start_time <= clock.evaluation_time < engine.end_time
        assert not engine.is_stop_requested
        assert traits.get_trait_or(trait="missing", default=42) == 42
        with pytest.raises(ValueError, match="Trait missing not found"):
            traits.get_trait("missing")
        retained_traits.append(traits)

        assert not state.is_updated()
        state.count = getattr(state, "count", 0) + 1
        assert state.is_updated()
        assert state["count"] == state.as_schema["count"] == 1
        assert list(state.keys()) == ["count"]
        assert list(state.items()) == [("count", 1)]
        assert list(state.values()) == [1]
        state.reset_updated()
        assert not state.is_updated()

        graph = node.graph
        assert graph.graph_id == node.owning_graph_id == ()
        assert graph.parent_node is None
        assert graph.started == graph.is_started
        assert graph.evaluating
        assert graph.evaluation_clock.evaluation_time == clock.evaluation_time
        assert any(candidate.node_id == node.node_id for candidate in graph.nodes)
        assert node.node_index == node.node_ndx == node.node_id[-1]
        assert node.started == node.is_started
        assert node.has_input and node.has_output
        assert not hasattr(node, "notify")

        assert not scheduler.is_scheduled
        assert not scheduler.is_scheduled_now
        assert scheduler.next_scheduled_time == hg.MIN_DT
        assert scheduler.pop_tag("missing") is None
        fallback = object()
        assert scheduler.pop_tag("missing", fallback) is fallback

        scheduler.schedule(when=3 * hg.MIN_TD, tag="later")
        scheduler.schedule(
            when=clock.evaluation_time + 2 * hg.MIN_TD, tag="first"
        )
        assert scheduler.is_scheduled
        assert scheduler.next_scheduled_time == clock.evaluation_time + 2 * hg.MIN_TD
        assert scheduler.has_tag(tag="later") and scheduler.has_tag(tag="first")
        assert (
            scheduler.pop_tag(tag="later")
            == clock.evaluation_time + 3 * hg.MIN_TD
        )
        assert not scheduler.has_tag("later")
        scheduler.schedule(4 * hg.MIN_TD, "cancel")
        scheduler.un_schedule(tag="cancel")
        assert not scheduler.has_tag("cancel")
        scheduler.un_schedule()
        assert not scheduler.is_scheduled
        scheduler.schedule(5 * hg.MIN_TD, "reset")
        scheduler.reset()
        assert scheduler.next_scheduled_time == hg.MIN_DT

        assert global_state["seed"] == 1
        initial_state_size = len(global_state)
        assert global_state.get("missing", 10) == 10
        assert global_state.setdefault("seed", 99) == 1
        assert global_state.setdefault("added", 2) == 2
        global_state["mutated"] = 3
        global_state["deleted"] = 4
        del global_state["deleted"]
        assert "deleted" not in global_state
        assert "seed" in global_state and bool(global_state)
        assert len(global_state) == initial_state_size + 2
        assert "seed" in list(global_state)
        assert dict(global_state.items())["added"] == 2
        assert 3 in global_state.values()
        assert global_state.pop("added") == 2
        assert global_state.pop("missing", 11) == 11
        with pytest.raises(KeyError):
            global_state.pop("missing")

        observations.append((node.node_id, graph.label, logger.name))
        return trigger.value

    with hg.GlobalState(seed=1) as state:
        assert hg.eval_node(
            inspect,
            [7],
            __end_time__=hg.MIN_ST + hg.MIN_TD,
        ) == [7]
        assert state["mutated"] == 3
        assert "added" not in state

    assert len(observations) == 1
    with pytest.raises(RuntimeError, match="outside its node's evaluation"):
        retained_traits[0].get_trait_or("missing")


def test_node_notify_is_excluded_but_next_cycle_notification_remains():
    evaluations = []

    @hg.compute_node
    def reschedule(
        trigger: hg.TS[int], clock: hg.CLOCK = None, node: hg.NODE = None
    ) -> hg.TS[int]:
        evaluations.append(clock.evaluation_time)
        assert not hasattr(node, "notify")
        if len(evaluations) == 1:
            node.notify_next_cycle()
        return trigger.value

    assert hg.eval_node(
        reschedule,
        [7],
        __end_time__=hg.MIN_ST + 2 * hg.MIN_TD,
    ) == [7, 7]
    assert evaluations == [hg.MIN_ST, hg.MIN_ST + hg.MIN_TD]


def test_recordable_state_exposes_the_0_5_schema_and_value_api():
    observations = []

    @hg.compute_node
    def count(
        trigger: hg.TS[int],
        state: hg.RECORDABLE_STATE[_AuditRecordableState] = None,
    ) -> hg.TS[int]:
        _assert_declared_api(state, RECORDABLE_STATE_API)
        assert state.as_schema is state
        count_state = state["count"]
        _assert_declared_api(count_state, RECORDABLE_STATE_API)
        assert count_state.valid == state.count.valid
        previous = count_state.value if count_state.valid else 0
        count_state.value = previous + trigger.value
        observations.append(
            (count_state.valid, count_state.modified, count_state.value)
        )
        return count_state.value

    assert hg.eval_node(count, [1, 2]) == [1, 3]
    assert observations == [(True, True, 1), (True, True, 3)]


def test_compound_scalar_keeps_its_public_conversion_contract():
    @dataclass(frozen=True)
    class Point(hg.CompoundScalar):
        x: int
        label: str | None = None

    assert hg.CompoundScalar.__module__ == "hgraph._compat"
    value = Point(x=3)
    assert value.to_dict() == {"x": 3}
    assert Point.from_dict({"x": 3, "unknown": "ignored"}) == value


def test_native_stub_declares_runtime_api_with_user_facing_signatures():
    source = Path(_hgraph.__file__).with_name("_hgraph.pyi").read_text(
        encoding="utf-8"
    )

    def class_declaration(name):
        match = re.search(
            rf"^class {name}:.*?(?=^class |^def |\Z)",
            source,
            flags=re.MULTILINE | re.DOTALL,
        )
        assert match is not None, name
        return match.group(0)

    scheduler = class_declaration("Scheduler")
    for name in SCHEDULER_API:
        assert f"def {name}(" in scheduler, name
    assert (
        "def schedule(self, when: datetime.datetime, tag: str | None = None, "
        "on_wall_clock: bool = False) -> None:" in scheduler
    )
    assert (
        "def schedule(self, when: datetime.timedelta, tag: str | None = None, "
        "on_wall_clock: bool = False) -> None:" in scheduler
    )
    assert "def pop_tag(self, tag: str, default: object | None = None)" in scheduler
    assert "def has_tag(self, tag: str) -> bool:" in scheduler
    assert "def un_schedule(self, tag: str | None = None) -> None:" in scheduler
    assert "def next_scheduled_time(self) -> datetime.datetime:" in scheduler

    runtime_state = class_declaration("RuntimeGlobalState")
    for name in GLOBAL_STATE_API:
        assert f"def {name}(" in runtime_state, name
    assert "def keys(self) -> list[str]:" in runtime_state
    assert "def values(self) -> list[object]:" in runtime_state
    assert "def items(self) -> list[tuple[str, object]]:" in runtime_state
    assert "def __iter__(self) -> Iterator[str]:" in runtime_state

    node = class_declaration("Node")
    graph = class_declaration("Graph")
    traits = class_declaration("Traits")
    recordable_state = class_declaration("RecordableStateView")
    for name in NODE_API:
        assert f"def {name}(" in node, name
    for name in GRAPH_API:
        assert f"def {name}(" in graph, name
    for name in TRAITS_API:
        assert f"def {name}(" in traits, name
    for name in RECORDABLE_STATE_API:
        assert f"def {name}(" in recordable_state, name
    assert "def as_schema(self) -> RecordableStateView:" in recordable_state
    assert (
        "def __getitem__(self, arg: object, /) -> RecordableStateView:"
        in recordable_state
    )
    assert "def notify(" not in node
