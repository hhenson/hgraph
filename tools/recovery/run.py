"""Run one scenario and judge it."""

# No ``from __future__ import annotations`` here: the graphs below are annotated with LOCAL
# schemas, and hgraph evaluates annotations by name.
import pickle
import tempfile
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path

import hgraph as hg

from . import graphs
from .generate import thaw
from .model import MESH_EMPTY_INPUT, Scenario, expected, known_defect


@dataclass
class Result:
    scenario: Scenario
    status: str                     # pass | known | fail | error
    detail: str = ""
    seconds: float = 0.0
    sensitive: object = None        # did the same restarts WITHOUT recovery differ?
    extra: dict = field(default_factory=dict)

    def to_json(self) -> dict:
        family = known_defect(self.scenario)
        return {"status": self.status, "detail": self.detail, "seconds": round(self.seconds, 3),
                "sensitive": self.sensitive, "family": family.id if family else None,
                "scenario": self.scenario.to_json(), **self.extra}


def _source(schema, events, offset):
    @hg.generator
    def observations() -> schema:
        for index, value in enumerate(events):
            if value is not None:
                yield hg.MIN_ST + (offset + index) * hg.MIN_TD, thaw(value)

    return observations


def _rows(path: str):
    rows = []
    for file in sorted(Path(path).parent.glob(Path(path).name + ".*")):
        with file.open("rb") as stream:
            while True:
                try:
                    rows.append(pickle.load(stream))
                except EOFError:
                    break
    return sorted(rows, key=repr)


def _application(scenario: Scenario, source, trace: str):
    """The main graph of one day, and whether it has an output of its own."""
    entry = graphs.resolve(scenario.chain)

    def body(ts):
        if scenario.host == "graph":
            return entry.fn(ts)
        hg.spawn_(hg.pipeline_([entry.fn, hg.bind_(graphs.record, path=trace)]), ts)
        return ts

    if scenario.placement == "outer":
        body.__annotations__ = {"ts": entry.input,
                                "return": entry.output if scenario.host == "graph" else entry.input}
        body.__name__ = body.__qualname__ = "scenario"
        wrapped = hg.component(body, recordable_id="scenario")
    else:
        wrapped = body

    if scenario.host == "graph":
        @hg.graph
        def application() -> entry.output:
            return wrapped(source())
    else:
        @hg.graph
        def application() -> None:
            wrapped(source())
    return application


def _days(scenario: Scenario, workdir: Path, label: str, recover: bool, cuts):
    """The observable result of running ``scenario`` broken at ``cuts``."""
    import hgraph_persistence as persistence

    entry = graphs.resolve(scenario.chain)
    trace = str(workdir / f"trace-{label}")
    observed, previous, begin = [], None, 0
    for end in (*cuts, len(scenario.events)):
        events = scenario.events[begin:end]
        application = _application(scenario, _source(entry.input, events, begin), trace)
        with hg.GlobalState() as state:
            if recover:
                store = persistence.ComponentCheckpointStore(workdir / f"store-{label}")
                persistence.configure_component_recovery(
                    store, scenario.component, f"cut-{end}", previous,
                    revision="campaign-v1", global_state=state)
                previous = f"cut-{end}"
            day = hg.eval_node(application, __start_time__=hg.MIN_ST + begin * hg.MIN_TD,
                               __end_time__=hg.MIN_ST + end * hg.MIN_TD)
            if recover and not store.contains(previous):
                raise AssertionError(f"day ending at {end} completed and published no checkpoint")
        if scenario.host == "graph":
            day = [graphs.canonical(item) if item is not None else None for item in (day or ())]
            observed.extend(day + [None] * (len(events) - len(day)))
        begin = end
    return _rows(trace) if scenario.host == "spawn" else observed


def _fold(value, delta):
    """``value`` after ``delta``: what a consumer that folds what it is sent would hold."""
    if delta is None:
        return value
    if hasattr(delta, "items"):
        merged = dict(value) if isinstance(value, dict) else {}
        for key, item in delta.items():
            if item is hg.REMOVE or item is getattr(hg, "REMOVE_IF_EXISTS", None):
                merged.pop(key, None)
            else:
                merged[key] = _fold(merged.get(key), item)
        return merged
    return delta


def _values(scenario: Scenario, cuts, recover: bool):
    """What a consumer holds after each cycle of ``scenario`` broken at ``cuts``.

    The consumer starts with the day, as the graph does: nothing is carried over a cut but
    what the run itself sends. In RECOVER mode that is the re-seeded inputs flowing through
    again, and it has to be the WHOLE value; with no recovery it is nothing, which is what
    the control relies on. One GlobalState spans the days because the in-memory recording
    model lives in it, standing in for a durable store."""
    entry = graphs.resolve(scenario.chain)
    held, begin = [], 0
    with hg.GlobalState():
        hg.set_record_replay_config(hg.IN_MEMORY)
        for end in (*cuts, len(scenario.events)):
            events = scenario.events[begin:end]
            application = _application(scenario, _source(entry.input, events, begin), trace="")
            mode = hg.RecordReplayEnum.RECORD
            if recover and begin != 0:
                mode = hg.RecordReplayEnum.RECOVER | hg.RecordReplayEnum.RECORD
            with hg.RecordReplayContext(mode=mode):
                day = hg.eval_node(application, __start_time__=hg.MIN_ST + begin * hg.MIN_TD,
                                   __end_time__=hg.MIN_ST + end * hg.MIN_TD)
            day = list(day or ())
            value = None
            for delta in day + [None] * (len(events) - len(day)):
                value = _fold(value, delta)
                held.append(graphs.canonical(value) if value is not None else None)
            begin = end
    return held


def _without_empties(value):
    """``value`` with every empty collection, and the key that held it, taken out."""
    if not isinstance(value, tuple) or not all(isinstance(item, tuple) and len(item) == 2 for item in value):
        return value
    kept = tuple((key, _without_empties(item)) for key, item in value)
    return tuple((key, item) for key, item in kept if item != ())


def _confirmed(scenario: Scenario, family, workdir: Path, oracle, actual) -> bool:
    """Is THIS mismatch the one ``family`` describes?

    Membership is a relation over the recipe -- "this scenario can reach the defect" -- and
    says nothing about what actually went wrong. Without this check every mismatch in a
    member would be filed as known, and an unrelated regression in a reduction, a map_, a
    dmap_ or a pipeline that happened to sit in a member would leave the nightly green.
    """
    if family is MESH_EMPTY_INPUT:
        # The consequence is exactly this: keys holding an EMPTY collection in the unbroken
        # run are absent afterwards. Take the empties out of both and nothing else may differ.
        return [_without_empties(row) for row in oracle] == [_without_empties(row) for row in actual]
    return False


def _classify(scenario: Scenario, workdir: Path, oracle, actual, prefix: str, seconds) -> "Result":
    detail = prefix + _first_difference(oracle, actual)
    family = known_defect(scenario)
    if family is None:
        return Result(scenario, "fail", detail, seconds())
    if _confirmed(scenario, family, workdir, oracle, actual):
        return Result(scenario, "known", f"{family.id}: {detail}", seconds())
    return Result(scenario, "fail", f"in family {family.id}, but the mismatch does NOT have its signature, "
                                    f"so it is something else: {detail}", seconds())


def _run_recover(scenario: Scenario, control: bool, started: float, workdir: Path) -> "Result":
    oracle = _values(scenario, cuts=(), recover=False)
    if not any(row is not None for row in oracle):
        return Result(scenario, "error", "the uninterrupted run produced nothing to compare",
                      time.monotonic() - started)
    actual = _values(scenario, cuts=scenario.cuts, recover=True)
    if actual != oracle:
        return _classify(scenario, workdir, oracle, actual, "values: ", lambda: time.monotonic() - started)
    sensitive = None
    if control:
        sensitive = _values(scenario, cuts=scenario.cuts, recover=False) != oracle
    return Result(scenario, "pass", "", time.monotonic() - started, sensitive=sensitive)


def _first_difference(expected_rows, actual_rows) -> str:
    for index, (want, got) in enumerate(zip(expected_rows, actual_rows)):
        if want != got:
            return f"first difference at {index}: expected {want!r}, got {got!r}"
    return f"lengths differ: expected {len(expected_rows)}, got {len(actual_rows)}"


def run(scenario: Scenario, *, control: bool = False) -> Result:
    """Judge one scenario against the uninterrupted run and against what is expected of it."""
    started = time.monotonic()
    expectation = expected(scenario)
    with tempfile.TemporaryDirectory(prefix="hgraph-recovery-") as directory:
        workdir = Path(directory)
        try:
            if scenario.mode == "recover":
                return _run_recover(scenario, control, started, workdir)
            oracle = _days(scenario, workdir, "oracle", recover=False, cuts=())
            if not any(row is not None for row in oracle):
                return Result(scenario, "error", "the uninterrupted run produced nothing to compare",
                              time.monotonic() - started)
            if scenario.mode == "clean":
                return Result(scenario, "pass", "", time.monotonic() - started)
            try:
                actual = _days(scenario, workdir, "restarted", recover=True, cuts=scenario.cuts)
            except Exception as error:  # a refusal is an outcome, judged below
                seconds = time.monotonic() - started
                if expectation.outcome == "refused" and expectation.reason in str(error):
                    return Result(scenario, "pass", f"refused as expected: {expectation.reason}", seconds)
                return Result(scenario, "fail", f"unexpected refusal: {error}", seconds,
                              extra={"traceback": traceback.format_exc(limit=6)})
            seconds = time.monotonic() - started
            if expectation.outcome == "refused":
                return Result(scenario, "fail",
                              "expected a refusal (" + expectation.reason + ") and it ran; if a limit was "
                              "lifted, update tools/recovery/model.py:expected", seconds)
            if actual != oracle:
                return _classify(scenario, workdir, oracle, actual, "", lambda: time.monotonic() - started)
            sensitive = None
            if control:
                # The same restarts with nothing configured. Where state crosses a cut this
                # HAS to differ, or the comparison above proves nothing; the campaign reports
                # how many scenarios were sensitive so it cannot quietly go vacuous.
                try:
                    sensitive = _days(scenario, workdir, "control", recover=False, cuts=scenario.cuts) != oracle
                except Exception:
                    sensitive = True        # without a baseline the day could not even be expressed
            return Result(scenario, "pass", "", time.monotonic() - started, sensitive=sensitive)
        except Exception as error:
            return Result(scenario, "error", f"{type(error).__name__}: {error}", time.monotonic() - started,
                          extra={"traceback": traceback.format_exc(limit=8)})
