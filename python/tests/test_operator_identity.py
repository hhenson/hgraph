"""Python declarations must never inherit an unrelated operator's overloads."""

import gc
import importlib
import weakref

import _hgraph

from hgraph import TS, compute_node, operator
from hgraph.test import eval_node


def _declare_score(offset):
    @operator
    def score(value: TS[int]) -> TS[int]: ...

    @compute_node(overloads=score)
    def score_int(value: TS[int]) -> TS[int]:
        return value.value + offset

    return score


def test_collected_operator_identity_is_never_reused(monkeypatch):
    module = importlib.import_module("hgraph._wiring._operator")
    # Force the former implementation's collision deterministically. Do not
    # depend on CPython choosing a particular freed allocation for the wrapper.
    monkeypatch.setattr(module, "id", lambda _: 0x661, raising=False)
    first = _declare_score(1)
    first_name = first._registry_name
    reference = weakref.ref(first)
    assert eval_node(first, [1, 2]) == [2, 3]
    del first
    gc.collect()
    assert reference() is None
    assert first_name in _hgraph.operator_names()  # no GC-driven deregistration

    second = _declare_score(10)
    assert second._registry_name != first_name
    assert eval_node(second, [1, 2]) == [11, 12]


def test_live_same_named_operators_keep_separate_overload_families(monkeypatch):
    module = importlib.import_module("hgraph._wiring._operator")
    monkeypatch.setattr(module, "id", lambda _: 0x662, raising=False)
    first = _declare_score(2)
    second = _declare_score(20)
    assert first._registry_name != second._registry_name
    assert eval_node(first, [1, 2]) == [3, 4]
    assert eval_node(second, [1, 2]) == [21, 22]
