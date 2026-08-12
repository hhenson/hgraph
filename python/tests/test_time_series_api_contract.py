"""Python runtime-view compatibility contract for every time-series kind.

The release/0.5 runtime exposed a common observational API plus specialised
collection/window views.  These tests deliberately exercise the methods from
inside Python node callbacks: a method present only in C++, or present in a
stub but absent on the live callback object, does not satisfy this contract.

Read-only parent and bound-peer topology is part of the diagnostic contract.
Endpoint mutation, subscription control and ``REF.value.output`` traversal are
intentionally excluded and remain native runtime responsibilities.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import _hgraph
import numpy as np

from hgraph import (
    MIN_ST,
    MIN_TD,
    REF,
    REMOVE,
    Removed,
    SIGNAL,
    Size,
    TS,
    TSB,
    TSB_OUT,
    TSD,
    TSD_OUT,
    TSL,
    TSS,
    TSS_OUT,
    TSW,
    TSW_OUT,
    TS_OUT,
    TimeSeriesSchema,
    WindowSize,
    combine,
    compute_node,
    graph,
    to_window,
)
from hgraph.test import eval_node


BASE_INPUT_API = {
    "active",
    "all_valid",
    "bound",
    "delta_value",
    "has_parent_input",
    "has_peer",
    "is_reference",
    "last_modified_time",
    "make_active",
    "make_passive",
    "modified",
    "owning_graph",
    "owning_node",
    "output",
    "parent_input",
    "valid",
    "value",
}

ITERABLE_API = {
    "items",
    "key_from_value",
    "keys",
    "modified_items",
    "modified_keys",
    "modified_values",
    "valid_items",
    "valid_keys",
    "valid_values",
    "values",
}

TSD_API = {
    "added_items",
    "added_keys",
    "added_values",
    "get",
    "items",
    "key_set",
    "key_from_value",
    "keys",
    "modified_items",
    "modified_keys",
    "modified_values",
    "removed_items",
    "removed_keys",
    "removed_values",
    "valid_items",
    "valid_keys",
    "valid_values",
    "values",
}

TSS_API = {"added", "removed", "values", "was_added", "was_removed"}

TSW_API = {
    "first_modified_time",
    "has_removed_value",
    "min_size",
    "removed_value",
    "size",
    "value_times",
}

REFERENCE_TOKEN_API = {
    "has_output",
    "is_empty",
    "is_valid",
    "items",
}

BASE_OUTPUT_API = {
    "all_valid",
    "can_apply_result",
    "clear",
    "delta_value",
    "invalidate",
    "is_reference",
    "last_modified_time",
    "modified",
    "owning_graph",
    "owning_node",
    "valid",
    "value",
}

READ_ONLY_OUTPUT_API = (
    (BASE_OUTPUT_API - {"can_apply_result", "clear", "invalidate"})
    | ITERABLE_API
    | TSD_API
    | TSS_API
    | TSW_API
)

ENDPOINT_MUTATION_API = {
    "apply_result",
    "bind_input",
    "bind_output",
    "copy_from_input",
    "copy_from_output",
    "do_bind_output",
    "do_un_bind_output",
    "parent_output",
    "re_parent",
    "subscribe",
    "un_bind_output",
    "unsubscribe",
}


def _assert_api(value, expected):
    missing = sorted(expected.difference(dir(value)))
    assert not missing, f"{type(value).__name__} is missing {missing}"


def _declared_public_api(value):
    return {name for name in value.__dict__ if not name.startswith("_")}


def _assert_input_topology(value):
    assert value.bound
    assert value.has_peer
    assert not value.has_parent_input
    assert value.parent_input is None
    assert isinstance(value.output, _hgraph.TimeSeriesOutput)
    _assert_api(value.output, READ_ONLY_OUTPUT_API)
    for name in ENDPOINT_MUTATION_API | {
        "add",
        "can_apply_result",
        "clear",
        "get_or_create",
        "invalidate",
        "pop",
        "remove",
    }:
        assert not hasattr(value.output, name), name
    for name in ENDPOINT_MUTATION_API:
        assert not hasattr(value, name), name


def _assert_read_only_output(value):
    assert isinstance(value, _hgraph.TimeSeriesOutput)
    for name in ENDPOINT_MUTATION_API | {
        "add",
        "can_apply_result",
        "clear",
        "get_or_create",
        "invalidate",
        "pop",
        "remove",
    }:
        assert not hasattr(value, name), name


@dataclass
class _ApiContractPair(TimeSeriesSchema):
    left: TS[int]
    right: TS[str]


def test_live_input_views_expose_the_complete_supported_api_inventory():
    @compute_node(valid=())
    def inspect_ts(value: TS[int]) -> TS[bool]:
        _assert_api(value, BASE_INPUT_API)
        _assert_input_topology(value)
        assert not hasattr(value, "key_set")
        assert not hasattr(value, "size")
        assert not hasattr(value, "value_times")
        return True

    @compute_node(valid=())
    def inspect_ref(value: REF[TS[int]]) -> TS[bool]:
        _assert_api(value, BASE_INPUT_API)
        _assert_input_topology(value)
        _assert_api(value.value, REFERENCE_TOKEN_API)
        assert value.value.has_output
        assert value.value.is_valid
        assert not value.value.is_empty
        # Returning the referenced output would expose mutable endpoint
        # topology. This is a deliberate, permanent compatibility exclusion.
        assert not hasattr(value.value, "output")
        return True

    @compute_node(valid=())
    def inspect_signal(value: SIGNAL) -> TS[bool]:
        _assert_api(value, BASE_INPUT_API)
        _assert_input_topology(value)
        assert value.value is True
        assert value.delta_value is True
        return True

    @compute_node(valid=())
    def inspect_tss(value: TSS[int]) -> TS[bool]:
        _assert_api(value, BASE_INPUT_API | TSS_API)
        _assert_input_topology(value)
        assert set(value.output.values()) == {1}
        assert set(value.output.added()) == {1}
        assert value.output.was_added(1)
        return True

    @compute_node(valid=())
    def inspect_tsd(value: TSD[str, TS[int]]) -> TS[bool]:
        _assert_api(value, BASE_INPUT_API | TSD_API)
        _assert_input_topology(value)
        peer = value.output
        assert peer.keys() == ["a"]
        child = peer.get("a")
        _assert_read_only_output(child)
        assert child.value == 1
        assert peer.key_from_value(child) == "a"
        assert peer.get("missing") is None
        assert peer.key_set.values() == ["a"]
        _assert_read_only_output(peer.key_set)
        return True

    @compute_node(valid=())
    def inspect_tsl(value: TSL[TS[int], Size[2]]) -> TS[bool]:
        _assert_api(value, BASE_INPUT_API | ITERABLE_API)
        _assert_input_topology(value)
        assert value.output.keys() == [0, 1]
        child = value.output[0]
        _assert_read_only_output(child)
        assert child.value == 1
        assert value.output.key_from_value(child) == 0
        return True

    @compute_node(valid=())
    def inspect_tsb(value: TSB[_ApiContractPair]) -> TS[bool]:
        _assert_api(value, BASE_INPUT_API | ITERABLE_API | {"as_schema"})
        _assert_input_topology(value)
        assert value.left.has_parent_input
        assert value.left.parent_input.key_from_value(value.left) == "left"
        assert value.left.bound and value.left.has_peer
        assert isinstance(value.left.output, _hgraph.TimeSeriesOutput)
        assert value.output.left.value == 1
        _assert_read_only_output(value.output.left)
        return True

    @compute_node(valid=())
    def inspect_tsw(value: TSW[int, WindowSize[2], WindowSize[1]]) -> TS[bool]:
        _assert_api(value, BASE_INPUT_API | TSW_API)
        _assert_input_topology(value)
        assert isinstance(value.output.value_times, np.ndarray)
        assert value.output.value_times.dtype == np.dtype("datetime64[us]")
        return True

    @graph
    def inspect_window(value: TS[int]) -> TS[bool]:
        return inspect_tsw(to_window(value, 2, 1))

    assert eval_node(inspect_ts, [1]) == [True]
    assert eval_node(inspect_ref, [1]) == [True]
    assert eval_node(inspect_signal, [True]) == [True]
    assert eval_node(inspect_tss, [{1}]) == [True]
    assert eval_node(inspect_tsd, [{"a": 1}]) == [True]
    assert eval_node(inspect_tsl, [{0: 1}]) == [True]
    assert eval_node(inspect_tsb, [{"left": 1}]) == [True]
    assert eval_node(inspect_window, [1]) == [True]


def test_compound_reference_exposes_items_but_never_output_traversal():
    @compute_node
    def inspect(value: REF[TSL[TS[int], Size[2]]]) -> TS[bool]:
        reference = value.value
        assert not reference.has_output
        assert not hasattr(reference, "output")
        assert len(reference) == 2
        assert reference.items == tuple(reference)
        assert reference[0] == reference.items[0]
        assert all(item.has_output and item.is_valid for item in reference.items)
        return True

    @graph
    def app(left: TS[int], right: TS[int]) -> TS[bool]:
        return inspect(combine[TSL[TS[int], Size[2]]](left, right))

    assert eval_node(app, [1], [2]) == [True]


def test_live_output_views_expose_the_complete_supported_api_inventory():
    @compute_node
    def inspect_ts(value: TS[int], _output: TS_OUT[int] = None) -> TS[int]:
        _assert_api(_output, BASE_OUTPUT_API)
        assert not hasattr(_output, "key_set")
        assert not hasattr(_output, "size")
        assert not hasattr(_output, "value_times")
        return value.value

    @compute_node
    def inspect_ref(
        value: REF[TS[int]], _output: REF[TS[int]] = None
    ) -> REF[TS[int]]:
        _assert_api(_output, BASE_OUTPUT_API)
        return value.value

    @compute_node
    def inspect_signal(value: SIGNAL, _output: SIGNAL = None) -> SIGNAL:
        _assert_api(_output, BASE_OUTPUT_API)
        return value.value

    @compute_node
    def inspect_tss(value: TS[int], _output: TSS_OUT[int] = None) -> TSS[int]:
        _assert_api(_output, BASE_OUTPUT_API | TSS_API | {"add", "remove"})
        return {value.value}

    @compute_node
    def inspect_tsd(
        value: TS[int], _output: TSD_OUT[str, TS[int]] = None
    ) -> TSD[str, TS[int]]:
        _assert_api(
            _output,
            BASE_OUTPUT_API
            | TSD_API
            | {"get_or_create"},
        )
        return {"value": value.value}

    @compute_node
    def inspect_tsl(
        value: TSL[TS[int], Size[2]],
        _output: TS_OUT[TSL[TS[int], Size[2]]] = None,
    ) -> TSL[TS[int], Size[2]]:
        _assert_api(_output, BASE_OUTPUT_API | ITERABLE_API)
        return value.delta_value

    @compute_node
    def inspect_tsb(
        value: TSB[_ApiContractPair], _output: TSB_OUT[_ApiContractPair] = None
    ) -> TSB[_ApiContractPair]:
        _assert_api(_output, BASE_OUTPUT_API | ITERABLE_API | {"as_schema"})
        return value.delta_value

    @compute_node
    def inspect_tsw(
        value: TS[int],
        _output: TSW_OUT[int, WindowSize[2], WindowSize[1]] = None,
    ) -> TSW[int, WindowSize[2], WindowSize[1]]:
        _assert_api(_output, BASE_OUTPUT_API | TSW_API)
        return value.value

    assert eval_node(inspect_ts, [1]) == [1]
    assert eval_node(inspect_ref, [1]) == [1]
    assert eval_node(inspect_signal, [True]) == [True]
    assert eval_node(inspect_tss, [1]) == [{1}]
    assert eval_node(inspect_tsd, [1]) == [{"value": 1}]
    assert eval_node(inspect_tsl, [{0: 1}]) == [{0: 1}]
    assert eval_node(inspect_tsb, [{"left": 1}]) == [{"left": 1}]
    assert eval_node(inspect_tsw, [1]) == [1]


def test_mutable_tsd_and_key_set_output_views_report_exact_native_changes():
    observations = []

    @compute_node
    def mutate(
        command: TS[int], _output: TSD_OUT[str, TS[int]] = None
    ) -> TSD[str, TS[int]]:
        if command.value == 1:
            _output["a"] = 10
            popped = None
        elif command.value == 2:
            _output["a"] = 20
            _output.get_or_create("b").value = 30
            popped = None
        else:
            popped = _output.pop("a")
            assert _output.pop("missing") is None

        key_set = _output.key_set
        current_a = _output.get("a")
        observations.append(
            {
                "keys": tuple(sorted(_output.keys())),
                "values": tuple(sorted(child.value for child in _output.values())),
                "items": _child_snapshot(sorted(_output.items())),
                "modified_keys": tuple(sorted(_output.modified_keys())),
                "valid_keys": tuple(sorted(_output.valid_keys())),
                "added_keys": tuple(sorted(_output.added_keys())),
                "removed_keys": tuple(sorted(_output.removed_keys())),
                "keys_from_children": tuple(
                    sorted(
                        _output.key_from_value(child)
                        for child in _output.values()
                    )
                ),
                "get": current_a.value if current_a is not None else None,
                "missing": _output.get("missing"),
                "popped_key": (
                    _output.key_from_value(popped) if popped is not None else None
                ),
                "key_set": (
                    tuple(sorted(key_set.values())),
                    tuple(sorted(key_set.added())),
                    tuple(sorted(key_set.removed())),
                ),
            }
        )

    assert eval_node(mutate, [1, 2, 3]) == [
        {"a": 10},
        {"a": 20, "b": 30},
        {"a": REMOVE},
    ]
    first, update, remove = observations
    assert first["added_keys"] == first["modified_keys"] == ("a",)
    assert first["valid_keys"] == ("a",)
    assert first["keys_from_children"] == ("a",)
    assert first["key_set"] == (("a",), ("a",), ())
    assert update["added_keys"] == ("b",)
    assert update["modified_keys"] == ("a", "b")
    assert update["valid_keys"] == ("a", "b")
    assert update["keys_from_children"] == ("a", "b")
    assert update["key_set"] == (("a", "b"), ("b",), ())
    assert remove["keys"] == ("b",)
    assert remove["modified_keys"] == ()
    assert remove["removed_keys"] == ("a",)
    assert remove["keys_from_children"] == ("b",)
    assert remove["popped_key"] == "a"
    assert remove["get"] is None
    assert remove["missing"] is None
    assert remove["key_set"] == (("b",), (), ("a",))


def test_mutable_tss_output_views_report_membership_and_exact_changes():
    observations = []

    @compute_node
    def mutate(command: TS[int], _output: TSS_OUT[int] = None) -> TSS[int]:
        if command.value == 1:
            assert _output.add(1)
            assert _output.add(2)
            assert not _output.add(2)
        else:
            assert _output.remove(1)
            assert _output.add(3)

        observations.append(
            (
                tuple(sorted(_output)),
                tuple(sorted(_output.values())),
                tuple(sorted(_output.added())),
                tuple(sorted(_output.removed())),
                _output.was_added(3),
                _output.was_removed(1),
                2 in _output,
                len(_output),
            )
        )

    assert eval_node(mutate, [1, 2]) == [{1, 2}, {Removed(1), 3}]
    assert observations == [
        ((1, 2), (1, 2), (1, 2), (), False, False, True, 2),
        ((2, 3), (2, 3), (3,), (1,), True, True, True, 2),
    ]


def test_mutable_structural_outputs_expose_complete_child_ranges():
    list_observations = []
    bundle_observations = []

    @compute_node
    def mutate_list(
        command: TS[int],
        _output: TS_OUT[TSL[TS[int], Size[2]]] = None,
    ) -> TSL[TS[int], Size[2]]:
        _output[0].value = command.value
        if command.value == 2:
            _output[1].value = 20
        list_observations.append(
            {
                "keys": tuple(_output.keys()),
                "values": tuple(child.value for child in _output.values()),
                "modified_keys": tuple(_output.modified_keys()),
                "valid_keys": tuple(_output.valid_keys()),
                "key_from_values": tuple(
                    _output.key_from_value(child) for child in _output.values()
                ),
                "key_from_items": tuple(
                    _output.key_from_value(child) for _, child in _output.items()
                ),
            }
        )

    @compute_node
    def mutate_bundle(
        command: TS[int], _output: TSB_OUT[_ApiContractPair] = None
    ) -> TSB[_ApiContractPair]:
        _output.left.value = command.value
        if command.value == 2:
            _output.right.value = "two"
        bundle_observations.append(
            {
                "keys": tuple(_output.keys()),
                "values": tuple(child.value for child in _output.values()),
                "modified_keys": tuple(_output.modified_keys()),
                "valid_keys": tuple(_output.valid_keys()),
                "key_from_values": tuple(
                    _output.key_from_value(child) for child in _output.values()
                ),
                "key_from_items": tuple(
                    _output.key_from_value(child) for _, child in _output.items()
                ),
                "schema": _output.as_schema is _output,
            }
        )

    assert eval_node(mutate_list, [1, 2]) == [{0: 1}, {0: 2, 1: 20}]
    assert list_observations == [
        {
            "keys": (0, 1),
            "values": (1, None),
            "modified_keys": (0,),
            "valid_keys": (0,),
            "key_from_values": (0, 1),
            "key_from_items": (0, 1),
        },
        {
            "keys": (0, 1),
            "values": (2, 20),
            "modified_keys": (0, 1),
            "valid_keys": (0, 1),
            "key_from_values": (0, 1),
            "key_from_items": (0, 1),
        },
    ]

    assert eval_node(mutate_bundle, [1, 2]) == [
        {"left": 1},
        {"left": 2, "right": "two"},
    ]
    assert bundle_observations == [
        {
            "keys": ("left", "right"),
            "values": (1, None),
            "modified_keys": ("left",),
            "valid_keys": ("left",),
            "key_from_values": ("left", "right"),
            "key_from_items": ("left", "right"),
            "schema": True,
        },
        {
            "keys": ("left", "right"),
            "values": (2, "two"),
            "modified_keys": ("left", "right"),
            "valid_keys": ("left", "right"),
            "key_from_values": ("left", "right"),
            "key_from_items": ("left", "right"),
            "schema": True,
        },
    ]


def _child_snapshot(items):
    return tuple(
        (key, child.value, child.valid, child.modified)
        for key, child in items
    )


def test_tsd_input_views_distinguish_added_modified_valid_and_removed_items():
    observations = []

    @compute_node(valid=())
    def inspect(value: TSD[str, TS[int]]) -> TS[int]:
        key_set = value.key_set
        observations.append(
            {
                "iter": tuple(sorted(value)),
                "keys": tuple(sorted(value.keys())),
                "values": tuple(sorted(child.value for child in value.values())),
                "items": _child_snapshot(sorted(value.items())),
                "modified": _child_snapshot(sorted(value.modified_items())),
                "modified_keys": tuple(sorted(value.modified_keys())),
                "modified_values": tuple(
                    sorted(child.value for child in value.modified_values())
                ),
                "valid": _child_snapshot(sorted(value.valid_items())),
                "valid_keys": tuple(sorted(value.valid_keys())),
                "valid_values": tuple(
                    sorted(child.value for child in value.valid_values())
                ),
                "added": _child_snapshot(sorted(value.added_items())),
                "added_keys": tuple(sorted(value.added_keys())),
                "added_values": tuple(
                    sorted(child.value for child in value.added_values())
                ),
                "removed": _child_snapshot(sorted(value.removed_items())),
                "removed_keys": tuple(sorted(value.removed_keys())),
                "removed_values": tuple(
                    child.value for child in value.removed_values()
                ),
                "removed_value_keys": tuple(
                    sorted(
                        value.key_from_value(child)
                        for child in value.removed_values()
                    )
                ),
                "removed_item_value_keys": tuple(
                    sorted(
                        value.key_from_value(child)
                        for _, child in value.removed_items()
                    )
                ),
                "get": value.get("a").value if value.get("a") is not None else None,
                "missing": value.get("missing"),
                "key_set": (
                    tuple(sorted(key_set.values())),
                    tuple(sorted(key_set.added())),
                    tuple(sorted(key_set.removed())),
                ),
                "keys_from_children": tuple(
                    sorted(
                        value.key_from_value(child) for child in value.values()
                    )
                ),
            }
        )
        return len(value)

    assert eval_node(
        inspect,
        [
            {"a": 1},
            {"a": 2, "b": 3},
            {"a": REMOVE},
            {"a": 4},
        ],
    ) == [1, 2, 1, 2]

    first, update, remove, readd = observations
    assert first["added_keys"] == ("a",)
    assert first["modified_keys"] == ("a",)
    assert first["added"] == first["modified"]
    assert first["keys_from_children"] == ("a",)
    assert first["key_set"] == (("a",), ("a",), ())

    assert update["keys"] == ("a", "b")
    assert update["added_keys"] == ("b",)
    assert update["modified_keys"] == ("a", "b")
    assert update["added_values"] == (3,)
    assert update["modified_values"] == (2, 3)
    assert update["keys_from_children"] == ("a", "b")
    assert update["valid_keys"] == ("a", "b")
    assert update["key_set"] == (("a", "b"), ("b",), ())

    assert remove["keys"] == ("b",)
    assert remove["added_keys"] == ()
    assert remove["modified_keys"] == ()
    assert remove["removed_keys"] == ("a",)
    assert tuple(key for key, *_ in remove["removed"]) == ("a",)
    assert remove["removed_value_keys"] == ("a",)
    assert remove["removed_item_value_keys"] == ("a",)
    assert remove["get"] is None
    assert remove["keys_from_children"] == ("b",)
    assert remove["missing"] is None
    assert remove["key_set"] == (("b",), (), ("a",))

    assert readd["added_keys"] == ("a",)
    assert readd["modified_keys"] == ("a",)
    assert readd["removed_keys"] == ()


def test_tsl_input_views_expose_ordered_complete_modified_and_valid_ranges():
    observations = []

    @compute_node(valid=())
    def inspect(value: TSL[TS[int], Size[3]]) -> TS[int]:
        observations.append(
            {
                "iter": tuple(child.value for child in value),
                "keys": tuple(value.keys()),
                "items": _child_snapshot(value.items()),
                "modified": _child_snapshot(value.modified_items()),
                "modified_keys": tuple(value.modified_keys()),
                "modified_values": tuple(child.value for child in value.modified_values()),
                "valid": _child_snapshot(value.valid_items()),
                "valid_keys": tuple(value.valid_keys()),
                "valid_values": tuple(child.value for child in value.valid_values()),
                "keys_from_children": (
                    value.key_from_value(value[1]),
                    value.key_from_value(value.values()[1]),
                    value.key_from_value(dict(value.items())[1]),
                ),
            }
        )
        return len(value)

    assert eval_node(inspect, [{0: 1}, {1: 2}, {0: 3}]) == [3, 3, 3]
    assert observations[0]["keys"] == (0, 1, 2)
    assert observations[0]["iter"] == (1, None, None)
    assert observations[0]["modified_keys"] == (0,)
    assert observations[0]["valid_keys"] == (0,)
    assert observations[0]["keys_from_children"] == (1, 1, 1)
    assert observations[1]["modified_keys"] == (1,)
    assert observations[1]["valid_keys"] == (0, 1)
    assert observations[2]["modified_values"] == (3,)


def test_tsb_input_views_expose_named_complete_modified_and_valid_ranges():
    observations = []

    @compute_node(valid=())
    def inspect(value: TSB[_ApiContractPair]) -> TS[int]:
        observations.append(
            {
                "keys": tuple(value.keys()),
                "items": _child_snapshot(value.items()),
                "modified": _child_snapshot(value.modified_items()),
                "modified_keys": tuple(value.modified_keys()),
                "modified_values": tuple(child.value for child in value.modified_values()),
                "valid": _child_snapshot(value.valid_items()),
                "valid_keys": tuple(value.valid_keys()),
                "valid_values": tuple(child.value for child in value.valid_values()),
                "keys_from_children": (
                    value.key_from_value(value.right),
                    value.key_from_value(value.values()[1]),
                    value.key_from_value(dict(value.items())["right"]),
                ),
                "schema": value.as_schema is value,
            }
        )
        return len(value)

    assert eval_node(inspect, [{"left": 1}, {"right": "two"}]) == [2, 2]
    assert observations[0]["keys"] == ("left", "right")
    assert observations[0]["modified_keys"] == ("left",)
    assert observations[0]["valid_keys"] == ("left",)
    assert observations[0]["keys_from_children"] == ("right", "right", "right")
    assert observations[0]["schema"]
    assert observations[1]["modified_keys"] == ("right",)
    assert observations[1]["valid_keys"] == ("left", "right")


def test_key_from_value_rejects_foreign_unresolved_structural_children():
    @compute_node(valid=())
    def inspect(
        left_list: TSL[TS[int], Size[2]],
        right_list: TSL[TS[int], Size[2]],
        left_bundle: TSB[_ApiContractPair],
        right_bundle: TSB[_ApiContractPair],
    ) -> TS[bool]:
        assert left_list.key_from_value(left_list[1]) == 1
        assert left_bundle.key_from_value(left_bundle.right) == "right"
        assert left_list.key_from_value(right_list[1]) is None
        assert left_bundle.key_from_value(right_bundle.right) is None
        return True

    assert eval_node(
        inspect,
        [{0: 1}],
        [{0: 2}],
        [{"left": 3}],
        [{"left": 4}],
    ) == [True]


def test_tss_input_views_expose_membership_and_exact_change_classification():
    observations = []

    @compute_node(valid=())
    def inspect(value: TSS[int]) -> TS[int]:
        observations.append(
            (
                tuple(sorted(value)),
                tuple(sorted(value.values())),
                tuple(sorted(value.added())),
                tuple(sorted(value.removed())),
                value.was_added(1),
                value.was_added(3),
                value.was_removed(1),
                2 in value,
                len(value),
            )
        )
        return len(value)

    assert eval_node(inspect, [{1, 2}, {Removed(1), 3}]) == [2, 2]
    assert observations == [
        ((1, 2), (1, 2), (1, 2), (), True, False, False, True, 2),
        ((2, 3), (2, 3), (3,), (1,), False, True, True, True, 2),
    ]


def test_tsw_input_views_expose_configuration_population_and_time_metadata():
    observations = []

    @compute_node
    def inspect(value: TSW[int, WindowSize[3], WindowSize[2]]) -> TS[int]:
        observations.append(
            {
                "size": value.size,
                "min_size": value.min_size,
                "length": len(value),
                "values": value.value,
                "times": value.value_times,
                "first_modified_time": value.first_modified_time,
                "has_removed_value": value.has_removed_value,
                "removed_value": value.removed_value,
            }
        )
        return len(value)

    @graph
    def app(value: TS[int]) -> TS[int]:
        return inspect(to_window(value, 3, 2))

    assert eval_node(app, [1, 2, 3, 4]) == [1, 2, 3, 3]
    assert [
        (item["size"], item["min_size"], item["length"])
        for item in observations
    ] == [
        (3, 2, 1),
        (3, 2, 2),
        (3, 2, 3),
        (3, 2, 3),
    ]
    assert all(isinstance(item["values"], np.ndarray) for item in observations)
    assert all(isinstance(item["times"], np.ndarray) for item in observations)
    assert all(
        item["times"].dtype == np.dtype("datetime64[us]")
        for item in observations
    )
    assert observations[0]["values"].tolist() == [1]
    assert observations[2]["values"].tolist() == [1, 2, 3]
    assert observations[3]["values"].tolist() == [2, 3, 4]
    assert observations[0]["times"].tolist() == [MIN_ST]
    assert observations[2]["times"].tolist() == [
        MIN_ST,
        MIN_ST + MIN_TD,
        MIN_ST + 2 * MIN_TD,
    ]
    assert observations[3]["times"].tolist() == [
        MIN_ST + MIN_TD,
        MIN_ST + 2 * MIN_TD,
        MIN_ST + 3 * MIN_TD,
    ]
    assert observations[0]["first_modified_time"] == MIN_ST
    assert observations[3]["first_modified_time"] == MIN_ST + MIN_TD
    assert observations[3]["has_removed_value"]
    assert observations[3]["removed_value"] == 1


def test_tsw_output_views_expose_numpy_compatible_value_and_time_buffers():
    observations = []

    @compute_node
    def mutate(
        value: TS[int],
        _output: TSW_OUT[int, WindowSize[3], WindowSize[2]] = None,
    ) -> TSW[int, WindowSize[3], WindowSize[2]]:
        _output.value = value.value
        observations.append(
            {
                "size": _output.size,
                "min_size": _output.min_size,
                "length": len(_output),
                "values": _output.value,
                "times": _output.value_times,
                "first_modified_time": _output.first_modified_time,
                "has_removed_value": _output.has_removed_value,
                "removed_value": _output.removed_value,
            }
        )

    assert eval_node(mutate, [1, 2, 3, 4]) == [1, 2, 3, 4]
    assert all(isinstance(item["values"], np.ndarray) for item in observations)
    assert all(isinstance(item["times"], np.ndarray) for item in observations)
    assert all(
        item["times"].dtype == np.dtype("datetime64[us]")
        for item in observations
    )
    assert observations[0]["values"].tolist() == [1]
    assert observations[3]["values"].tolist() == [2, 3, 4]
    assert observations[3]["times"].tolist() == [
        MIN_ST + MIN_TD,
        MIN_ST + 2 * MIN_TD,
        MIN_ST + 3 * MIN_TD,
    ]
    assert observations[3]["size"] == 3
    assert observations[3]["min_size"] == 2
    assert observations[3]["length"] == 3
    assert observations[3]["first_modified_time"] == MIN_ST + MIN_TD
    assert observations[3]["has_removed_value"]
    assert observations[3]["removed_value"] == 1


def test_duration_tsw_exposes_timedelta_configuration():
    period = 3 * MIN_TD
    minimum = MIN_TD
    observations = []

    @compute_node
    def inspect(
        value: TSW[int, WindowSize[MIN_TD * 3], WindowSize[MIN_TD]],
    ) -> TS[int]:
        observations.append((value.size, value.min_size))
        return len(value)

    @graph
    def app(value: TS[int]) -> TS[int]:
        return inspect(to_window(value, period, minimum))

    assert eval_node(app, [1, 2]) == [1, 2]
    assert observations == [(period, minimum), (period, minimum)]


def test_runtime_stub_declares_the_supported_time_series_contract():
    source = Path(_hgraph.__file__).with_name("_hgraph.pyi").read_text(
        encoding="utf-8"
    )

    def class_declaration(name):
        match = re.search(
            rf"^class {name}:.*?(?=^class |\Z)",
            source,
            flags=re.MULTILINE | re.DOTALL,
        )
        assert match is not None, name
        return match.group(0)

    input_declaration = class_declaration("TimeSeries")
    output_declaration = class_declaration("OutputView")
    read_only_output_declaration = class_declaration("TimeSeriesOutput")
    reference_declaration = class_declaration("TimeSeriesRef")
    for name in sorted(BASE_INPUT_API | ITERABLE_API | TSD_API | TSS_API | TSW_API):
        assert f"def {name}(" in input_declaration, name
    for name in sorted(
        BASE_OUTPUT_API
        | ITERABLE_API
        | TSD_API
        | TSS_API
        | TSW_API
        | {"get_or_create", "pop", "add", "remove"}
    ):
        assert f"def {name}(" in output_declaration, name
    for name in sorted(REFERENCE_TOKEN_API):
        assert f"def {name}(" in reference_declaration, name
    for name in sorted(READ_ONLY_OUTPUT_API):
        assert f"def {name}(" in read_only_output_declaration, name
    for name in sorted(
        ENDPOINT_MUTATION_API
        | {"add", "can_apply_result", "clear", "get_or_create", "invalidate", "pop", "remove"}
    ):
        assert f"def {name}(" not in read_only_output_declaration, name
    assert "def output(" not in reference_declaration

    assert "def size(self) -> int | datetime.timedelta:" in source
    assert "def min_size(self) -> int | datetime.timedelta:" in source
    assert "def value_times(self) -> numpy.ndarray:" in source
    assert "def get(self, key: object) -> TimeSeries | None:" in source
    assert "def get(self, key: object) -> OutputView | None:" in source
    assert "def get_or_create(self, key: object) -> OutputView:" in source
    assert "def pop(self, key: object) -> OutputView | None:" in source
    assert "def add(self, value: object) -> bool:" in source
    assert "def remove(self, value: object) -> bool:" in source
    assert "def key_from_value(self, value: TimeSeries) -> object | None:" in source
    assert "def key_from_value(self, value: OutputView) -> object | None:" in source
    assert "def parent_input(self) -> TimeSeries | None:" in source
    assert "def output(self) -> TimeSeriesOutput | None:" in input_declaration
    assert "def items(self) -> tuple[TimeSeriesRef, ...]:" in reference_declaration
    assert "def get(self, key: object) -> TimeSeriesOutput | None:" in source
    assert (
        "def key_from_value(self, value: TimeSeriesOutput) -> object | None:"
        in source
    )
    assert "def __setitem__(self, key: object, value: object) -> None:" in source


def test_native_time_series_types_declare_only_the_supported_surface():
    assert _declared_public_api(_hgraph.TimeSeries) == (
        BASE_INPUT_API | ITERABLE_API | TSD_API | TSS_API | TSW_API | {"as_schema"}
    )
    assert _declared_public_api(_hgraph.TimeSeriesOutput) == (
        READ_ONLY_OUTPUT_API | {"as_schema"}
    )
    assert _declared_public_api(_hgraph.TimeSeriesRef) == REFERENCE_TOKEN_API
