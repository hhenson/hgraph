"""The missing-extension wiring diagnosis must accuse the right thing.

``record``/``replay``/``compare`` had install advice appended to EVERY
wiring failure while ``hgraph_persistence`` was unloaded — including plain
argument errors, which sent the caller after a distribution they did not
need. Selecting a durable backend is itself the extension load point and
raises a pointed error there, so an unloaded extension means no durable
backend was ever selected (RFC 0025).
"""

import pytest

import hgraph as hg
from hgraph._wiring._core import _durable_wiring_hint

_INSTALL_ADVICE = "hgraph-persistence"
_RESOLUTION_FAILURE = "no matching overload for operator 'record' with 2 argument(s)"


def test_argument_errors_do_not_advertise_the_extension():
    # Not a resolution failure at all: nothing about it implicates a backend.
    assert _durable_wiring_hint("record", {}, "key must be a str, got int") == ""


def test_core_backend_resolution_failure_does_not_advertise_the_extension():
    # A resolution failure with no durable backend selected is an ordinary
    # signature mismatch against the built-in backends.
    assert _durable_wiring_hint("record", {}, _RESOLUTION_FAILURE) == ""


def test_non_durable_operator_never_advertises_the_extension():
    assert _durable_wiring_hint("add_", {}, _RESOLUTION_FAILURE) == ""


@pytest.mark.parametrize(
    "model",
    ["hgraph.persistence.frame", "DataFrame"],
)
def test_durable_selection_still_gets_the_diagnosis(model):
    # Both the backend id and the legacy model constant count as a durable
    # selection; the hint is the whole point when one is in play.
    hint = _durable_wiring_hint("record", {"model": model}, _RESOLUTION_FAILURE)
    assert _INSTALL_ADVICE in hint


def test_extension_only_operator_is_diagnosed_without_a_selection():
    # replay_const has no in-memory implementation, so it can fail this way
    # with no durable backend configured at all.
    hint = _durable_wiring_hint(
        "replay_const",
        {},
        "no matching overload for operator 'replay_const' with 1 argument(s)",
    )
    assert _INSTALL_ADVICE in hint


def test_bad_record_signature_reports_only_the_signature():
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
