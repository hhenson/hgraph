"""RecordReplayContext's public surface (issue #816 items 1.6 and 1.7).

The surface audit compares signatures and the ``repr()`` of every default, so
these pin both the shape and the values behind it.
"""

import _hgraph
import pytest

import hgraph as hg


def test_default_mode_records():
    """``with RecordReplayContext():`` recorded upstream and silently recorded
    nothing here -- the default was NONE. Silent, and the default."""
    assert hg.RecordReplayContext().mode == hg.RecordReplayEnum.RECORD


def test_recordable_id_defaults_to_none():
    """The released default is None, not "". Part of the audited surface."""
    assert hg.RecordReplayContext().recordable_id is None
    assert hg.RecordReplayContext(recordable_id="abc").recordable_id == "abc"


def test_modes_are_a_real_enum_sourced_from_the_native_constants():
    """A plain class of int constants reprs as ``1``; the released enum reprs
    as ``<RecordReplayEnum.RECORD: 1>``, so changing the default alone would
    have left the finding standing with different content.

    Values come from the native constants so the two cannot drift.
    """
    assert repr(hg.RecordReplayEnum.RECORD) == "<RecordReplayEnum.RECORD: 1>"
    for name in ("NONE", "RECORD", "REPLAY", "COMPARE", "REPLAY_OUTPUT", "RESET", "RECOVER"):
        assert int(getattr(hg.RecordReplayEnum, name)) == getattr(_hgraph, f"MODE_{name}")


def test_flags_combine():
    combined = hg.RecordReplayEnum.REPLAY | hg.RecordReplayEnum.RECORD
    assert combined & hg.RecordReplayEnum.RECORD
    assert combined & hg.RecordReplayEnum.REPLAY
    assert not (combined & hg.RecordReplayEnum.COMPARE)


def test_instance_never_returns_none():
    """Unlike ``DebugContext.instance()``, which does. Two different released
    contracts, and matching each is the right thing."""
    outside = hg.RecordReplayContext.instance()
    assert outside is not None
    assert outside.mode == hg.RecordReplayEnum.NONE


def test_instance_reflects_the_ambient_scope():
    with hg.RecordReplayContext(mode=hg.RecordReplayEnum.REPLAY, recordable_id="abc"):
        active = hg.RecordReplayContext.instance()
        assert active.mode == hg.RecordReplayEnum.REPLAY
        assert active.recordable_id == "abc"

    assert hg.RecordReplayContext.instance().mode == hg.RecordReplayEnum.NONE


def test_instance_sees_the_native_scope_too():
    """instance() reads the native scope stack rather than a second Python
    one, so a push made through ``record_replay_scope`` -- or by a native
    caller -- is visible. A private Python list could not see it."""
    with hg.record_replay_scope(hg.RecordReplayEnum.RECORD, "native-id"):
        active = hg.RecordReplayContext.instance()
        assert active.mode == hg.RecordReplayEnum.RECORD
        assert active.recordable_id == "native-id"
