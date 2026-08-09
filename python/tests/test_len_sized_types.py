"""Pin issue #81: len_ works for every type that can produce a size.

Covers the gaps found in triage: real TSW windows had no len_ overload,
len_tsl only accepted scalar TS elements, and eval_node degraded generic
``TSL[..., SIZE]`` annotations to dict scalars instead of resolving the size
from the samples. ``len_(window(...))`` returning the WindowResult bundle's
field count and ``len_(window(...).buffer)`` are pre-existing behaviour,
pinned here so changes are deliberate.
"""

import pytest

import hgraph as hg
from hgraph import TS
from hgraph.test import eval_node
from hgraph._wiring._core import WiringError


def test_len_over_tsw_window_buffer_growth():
    # The current buffer length: grows until the window fills, then stays
    # put (no-change means no tick, the 2026-07-17 ruling).
    @hg.graph
    def tsw_len(ts: TS[int]) -> TS[int]:
        return hg.len_(hg.to_window(ts, 3))

    assert eval_node(tsw_len, [1, 2, 3, 4, 5]) == [1, 2, 3, None, None]


def test_len_over_nested_dynamic_tsl():
    # The issue #81 repro: previously died with AttributeError inside
    # eval_node; the generic SIZE now resolves from the sample.
    @hg.graph
    def nested(ts: hg.TSL[hg.TSL[TS[int], hg.Size[2]], hg.SIZE]) -> TS[int]:
        return hg.len_(ts)

    assert eval_node(nested, [{0: {0: 1, 1: 2}}]) == [1]


def test_len_over_dynamic_tsl_is_a_real_tsl():
    @hg.graph
    def dyn(ts: hg.TSL[TS[int], hg.SIZE]) -> TS[int]:
        return hg.len_(ts)

    assert eval_node(dyn, [{0: 1, 1: 2, 2: 3}]) == [3]


def test_len_over_fixed_tsl_of_composite_elements():
    # len_tsl accepts any element kind (upstream TSL[TIME_SERIES_TYPE, SIZE]).
    @hg.graph
    def tsl_of_tss(a: hg.TSS[int], b: hg.TSS[int]) -> TS[int]:
        return hg.len_(hg.TSL.from_ts(a, b))

    assert eval_node(tsl_of_tss, [{1}], [{2}]) == [2]


def test_len_over_window_result_bundle_and_buffer():
    # hg.window returns the WindowResult TSB: len_ of the bundle is its
    # field count (len_tsb), and len_ of .buffer is the tuple length.
    @hg.graph
    def bundle_len(ts: TS[int]) -> TS[int]:
        return hg.len_(hg.window(ts, 3))

    assert eval_node(bundle_len, [1, 2, 3, 4]) == [2, None, None, None]

    @hg.graph
    def buffer_len(ts: TS[int]) -> TS[int]:
        return hg.len_(hg.window(ts, 3).buffer)

    assert eval_node(buffer_len, [1, 2, 3, 4]) == [None, None, 3, 3]


def test_len_over_unsized_type_raises_resolution_error():
    with pytest.raises(WiringError, match="no matching overload"):
        @hg.graph
        def bad(ts: TS[int]) -> TS[int]:
            return hg.len_(ts)

        eval_node(bad, [1])


def test_len_family_never_valid_input_never_ticks():
    # Issue #116/#137/#138: upstream parity — a NEVER-VALID input produces
    # no tick; the first publish comes with the first real delta.
    @hg.graph
    def tsd_len(ts: hg.TSD[str, hg.TS[int]]) -> TS[int]:
        return hg.len_(ts)

    assert eval_node(tsd_len, [None, None]) is None

    @hg.graph
    def tss_len(ts: hg.TSS[int]) -> TS[int]:
        return hg.len_(ts)

    assert eval_node(tss_len, [None, None]) is None

    # is_empty differs deliberately: upstream's is_empty READS a never-valid
    # input as empty and ticks True at start (ported test_is_empty pins it).
    @hg.graph
    def tsd_empty(ts: hg.TSD[str, hg.TS[int]]) -> TS[bool]:
        return hg.is_empty(ts)

    assert eval_node(tsd_empty, [None, None]) == [True, None]


def test_tsd_removal_reticks_and_none_is_a_per_key_no_op():
    # Ruling 2026-07-28: None means NOTHING ticked for that key — the
    # per-key analogue of the top-level no-tick, consistent across all
    # keyed structures and matching upstream. Removals are the explicit
    # sentinels, and the len family re-evaluates on them (both runtimes
    # agree).
    @hg.graph
    def tsd_len(ts: hg.TSD[str, hg.TS[int]]) -> TS[int]:
        return hg.len_(ts)

    assert eval_node(tsd_len, [{"a": 1, "b": 2}, {"a": hg.REMOVE_IF_EXISTS}]) == [2, 1]
    assert eval_node(tsd_len, [{"a": 1, "b": 2}, {"a": None}]) == [2, None]

    @hg.graph
    def tsd_empty(ts: hg.TSD[str, hg.TS[int]]) -> TS[bool]:
        return hg.is_empty(ts)

    assert eval_node(tsd_empty, [{"a": 1}, {"a": hg.REMOVE_IF_EXISTS}, {"b": 2}]) == [
        False, True, False]

    @hg.graph
    def tsd_contains(ts: hg.TSD[str, hg.TS[int]]) -> TS[bool]:
        return hg.contains_(ts, "c")

    assert eval_node(tsd_contains, [{"c": 0}, {"c": hg.REMOVE_IF_EXISTS}]) == [True, False]

    # Consistency across keyed structures: a None entry per TSL index is
    # likewise a no-tick for that index.
    @hg.graph
    def tsl_first(ts: hg.TSL[TS[int], hg.Size[2]]) -> TS[int]:
        return ts[0]

    assert eval_node(tsl_first, [[1, 2], [None, 5]]) == [1, None]
