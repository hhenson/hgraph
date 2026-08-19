"""The missing-extension wiring diagnosis must accuse the right thing.

``record``/``replay``/``compare`` had install advice appended to EVERY
wiring failure while ``hgraph_persistence`` was unloaded — including plain
argument errors, which sent the caller after a distribution they did not
need. Selecting a durable backend is itself the extension load point and
raises a pointed error there, so an unloaded extension means no durable
backend was ever selected (RFC 0025).

Every test here states the load state it is about. The diagnosis is
deliberately silent once the extension is imported, and the wheel test
workflow installs it and imports it from an earlier test module — so a test
that inherited the state from suite order would pass locally and fail there.
"""

import sys
import types

import pytest

import hgraph as hg
from hgraph._wiring._core import _durable_wiring_hint

_INSTALL_ADVICE = "hgraph-persistence"
_RECORD_FAILURE = "no matching overload for operator 'record' with 2 argument(s)"
_REPLAY_CONST_FAILURE = (
    "no matching overload for operator 'replay_const' with 1 argument(s)"
)


@pytest.fixture
def extension_unloaded(monkeypatch):
    """Force the not-loaded state, whatever the rest of the suite did."""
    for name in [
        name
        for name in sys.modules
        if name == "hgraph_persistence" or name.startswith("hgraph_persistence.")
    ]:
        monkeypatch.delitem(sys.modules, name)


@pytest.fixture
def extension_loaded(monkeypatch):
    """Force the loaded state without requiring the distribution."""
    monkeypatch.setitem(
        sys.modules, "hgraph_persistence", types.ModuleType("hgraph_persistence")
    )


def test_argument_errors_do_not_advertise_the_extension(extension_unloaded):
    # Not a resolution failure at all: nothing about it implicates a backend.
    assert _durable_wiring_hint("record", {}, "key must be a str, got int") == ""


def test_core_backend_resolution_failure_does_not_advertise_the_extension(
    extension_unloaded,
):
    # A resolution failure with no durable backend selected is an ordinary
    # signature mismatch against the built-in backends.
    assert _durable_wiring_hint("record", {}, _RECORD_FAILURE) == ""


def test_non_durable_operator_never_advertises_the_extension(extension_unloaded):
    assert _durable_wiring_hint("add_", {}, _RECORD_FAILURE) == ""


@pytest.mark.parametrize("model", ["hgraph.persistence.frame", "DataFrame"])
def test_durable_selection_still_gets_the_diagnosis(model, extension_unloaded):
    # Both the backend id and the legacy model constant count as a durable
    # selection; the hint is the whole point when one is in play.
    assert _INSTALL_ADVICE in _durable_wiring_hint("record", {"model": model}, _RECORD_FAILURE)


def test_another_vendors_backend_is_not_blamed_on_persistence(extension_unloaded):
    # Backend selection is open. An independently registered "hgraph.*" id is
    # not persistence's, and installing persistence would not supply it.
    assert _durable_wiring_hint("record", {"model": "hgraph.custom"}, _RECORD_FAILURE) == ""


def test_extension_only_operator_is_diagnosed_without_a_selection(extension_unloaded):
    # replay_const has no in-memory implementation, so it can fail this way
    # with no durable backend configured at all.
    assert _INSTALL_ADVICE in _durable_wiring_hint(
        "replay_const", {}, _REPLAY_CONST_FAILURE
    )


def test_a_loaded_extension_is_never_blamed(extension_loaded):
    # Once the overloads are registered, a failure cannot be their absence.
    assert _durable_wiring_hint("replay_const", {}, _REPLAY_CONST_FAILURE) == ""
    assert (
        _durable_wiring_hint(
            "record", {"model": "hgraph.persistence.frame"}, _RECORD_FAILURE
        )
        == ""
    )


def test_bad_record_signature_reports_only_the_signature(extension_unloaded):
    # End to end, and the exact case the gates exist for: a wrong `key` type
    # on `record` with no durable backend selected. This raised a real
    # resolution failure with "install hgraph-persistence" appended, sending
    # the caller after a distribution that would not have helped.
    @hg.graph
    def bad_record():
        hg.record(hg.const(1), key=5)

    with pytest.raises(Exception) as caught:
        hg.eval_node(bad_record)
    message = str(caught.value)
    # Pins the native wording the hint gates on: if it changes, the gate
    # silently stops firing and this test says so.
    assert "no matching overload" in message
    assert _INSTALL_ADVICE not in message
