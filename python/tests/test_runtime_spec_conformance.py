"""Accepted runtime rules from runtime_spec/validation.md, DV-01/02/05/06/08/10."""
import hgraph as hg
from hgraph.test import eval_node


def test_dictionary_membership_and_key_set_clock():
    observations = []

    @hg.compute_node
    def source(step: hg.TS[int], _output: hg.TSD = None) -> hg.TSD[str, hg.TS[int]]:
        if step.value == 1:
            _output.get_or_create("X")
        elif step.value in (2, 6):
            _output.get_or_create("X").value = 7
        elif step.value == 3:
            _output["X"].invalidate()
        elif step.value == 4:
            del _output["X"]

    @hg.compute_node(valid=("step",), active=("step",))
    def observe(step: hg.TS[int], ts: hg.TSD[str, hg.TS[int]]) -> hg.TS[int]:
        observations.append((set(ts.added_keys()), set(ts.removed_keys()), ts.key_set.modified))
        if ts.key_set.modified:
            expected = {hg.Removed("X")} if step.value == 4 else {"X"}
            assert set(ts.key_set.delta_value) == expected
        return step.value

    @hg.graph
    def run(step: hg.TS[int]) -> hg.TS[int]:
        return observe(step, source(step))

    eval_node(run, list(range(8)))
    assert observations == [
        (set(), set(), False), ({"X"}, set(), True),
        (set(), set(), False), (set(), set(), False),
        (set(), {"X"}, True), (set(), set(), False),
        ({"X"}, set(), True), (set(), set(), False),
    ]


def test_dictionary_reference_sampling_and_withdrawal():
    observations = []

    @hg.compute_node(valid=("selector",), active=("selector",))
    def choose(selector: hg.TS[int], a: hg.REF[hg.TSD[str, hg.TS[int]]],
               b: hg.REF[hg.TSD[str, hg.TS[int]]]) -> hg.REF[hg.TSD[str, hg.TS[int]]]:
        return [a.value, b.value, hg.TimeSeriesReference.make()][selector.value]

    @hg.compute_node(valid=("step",), active=("step",))
    def observe(step: hg.TS[int], ts: hg.TSD[str, hg.TS[int]]) -> hg.TS[int]:
        observations.append({
            "valid": ts.valid,
            "modified": ts.modified,
            "delta": dict(ts.delta_value) if ts.modified else None,
            "removed": {k: v.value for k, v in ts.removed_items()},
            "times": {k: v.last_modified_time for k, v in ts.items()},
        })
        return step.value

    @hg.graph
    def run(step: hg.TS[int], a: hg.TSD[str, hg.TS[int]], b: hg.TSD[str, hg.TS[int]],
            selector: hg.TS[int]) -> hg.TS[int]:
        return observe(step, choose(selector, a, b))

    eval_node(run, list(range(6)), [{"X": 1, "Z": 9}, None, None, None, None, None],
              [{"Y": 2, "Z": 3}, {"Y": 4}, None, None, None, None], [0, None, 1, 2, 0, None])
    assert [o["delta"] for o in observations] == [
        {"X": 1, "Z": 9}, None, {"X": hg.REMOVE, "Y": 4, "Z": 3},
        {"Y": hg.REMOVE, "Z": hg.REMOVE}, {"X": 1, "Z": 9}, None,
    ]
    assert observations[2]["removed"] == {"X": 1}
    assert observations[3]["removed"] == {"Y": 4, "Z": 3}
    assert observations[3]["valid"] is False
    assert observations[3]["modified"] is True
    assert observations[2]["times"] == {"Y": hg.MIN_ST + 2 * hg.MIN_TD, "Z": hg.MIN_ST + 2 * hg.MIN_TD}
    assert observations[4]["times"] == {"X": hg.MIN_ST + 4 * hg.MIN_TD, "Z": hg.MIN_ST + 4 * hg.MIN_TD}


def test_scalar_reference_unbind_resets_observed_time():
    observations = []

    @hg.compute_node
    def choose(step: hg.TS[int], a: hg.REF[hg.TS[int]]) -> hg.REF[hg.TS[int]]:
        return a.value if step.value == 0 else hg.TimeSeriesReference.make()

    @hg.compute_node(valid=("step",), active=("step",))
    def observe(step: hg.TS[int], ts: hg.TS[int]) -> hg.TS[int]:
        observations.append((ts.valid, ts.modified, ts.last_modified_time))
        return step.value

    @hg.graph
    def run(step: hg.TS[int], a: hg.TS[int]) -> hg.TS[int]:
        return observe(step, choose(step, a))

    eval_node(run, [0, 1], [7, None])
    assert observations == [(True, True, hg.MIN_ST), (False, False, hg.MIN_DT)]


def test_saved_reference_expires_before_next_dictionary_mutation():
    observations = []

    @hg.compute_node(valid=("step",), active=("step",))
    def save(step: hg.TS[int], ref: hg.REF[hg.TS[int]], _state: hg.STATE = None) -> hg.REF[hg.TS[int]]:
        if step.value == 0:
            _state.saved = ref.value
        if step.value in (0, 3, 5):
            return _state.saved

    @hg.compute_node(valid=("step",), active=("step",))
    def observe(step: hg.TS[int], ts: hg.TS[int]) -> hg.TS[int]:
        observations.append((ts.value if ts.valid else None, ts.modified))
        return step.value

    @hg.graph
    def run(step: hg.TS[int], values: hg.TSD[str, hg.TS[int]]) -> hg.TS[int]:
        return observe(step, save(step, values["X"]))

    eval_node(run, list(range(7)), [{"X": 7}, None, {"X": hg.REMOVE}, None, {"X": 9}, None, None])
    assert observations == [(7, True), (7, False), (7, False), (None, False),
                            (None, False), (None, False), (None, False)]
