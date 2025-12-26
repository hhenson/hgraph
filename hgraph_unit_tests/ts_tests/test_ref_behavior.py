"""
Reference (REF) Behavior Tests

This file tests all behaviors of the REF[T] time-series reference type.
REF represents a reference to another time-series that can be dynamically bound.

Test Dependencies: TS, TSL, TSB, TSD, TSS (most types must work first)
Implementation Order: 7 (last)

Behaviors Tested:
1. REF output value setting
2. REF value property and delta_value
3. valid, modified properties
4. TimeSeriesReference types (Empty, Bound, UnBound)
5. REF binding to reference output (peered)
6. REF binding to non-reference output
7. REF with scalar types (REF[TS[T]])
8. REF with list types (REF[TSL[...]])
9. REF with bundle types (REF[TSB[...]])
10. REF with set types (REF[TSS[...]])
11. REF with dict types (REF[TSD[...]])
12. REF switching (merge/race patterns)
13. TimeSeriesReference.make() factory
"""
import pytest
from frozendict import frozendict as fd

from hgraph import (
    compute_node,
    graph,
    TS,
    TSL,
    TSB,
    TSD,
    TSS,
    REF,
    Size,
    SIZE,
    REMOVE,
    Removed,
    TIME_SERIES_TYPE,
    TimeSeriesSchema,
    TimeSeriesReference,
    if_,
)
from hgraph.test import eval_node


# =============================================================================
# BASIC REF OUTPUT TESTS
# =============================================================================


# Test REF output value setting and retrieval.

def test_output_ref_passthrough():
    """Test passing through a reference value."""
    @compute_node
    def create_ref(ts: REF[TIME_SERIES_TYPE]) -> REF[TIME_SERIES_TYPE]:
        return ts.value

    result = eval_node(create_ref[TIME_SERIES_TYPE: TS[int]], ts=[1, 2, 3])
    assert result == [1, 2, 3]


def test_output_empty_ref():
    """Test creating empty reference - the TimeSeriesReference is empty."""
    @compute_node
    def empty_ref(ts: TS[bool]) -> REF[TS[int]]:
        return TimeSeriesReference.make()

    @compute_node
    def check_ref_is_empty(ref: REF[TS[int]]) -> TS[bool]:
        v = ref.value
        return v.is_empty if v else True

    @graph
    def g(ts: TS[bool]) -> TS[bool]:
        return check_ref_is_empty(empty_ref(ts))

    # Empty reference's is_empty should be True
    result = eval_node(g, [True])
    assert result == [True]


# =============================================================================
# REF STATE PROPERTY TESTS
# =============================================================================


# Test REF valid and modified properties.

def test_valid_after_binding():
    """Test REF is valid when bound to valid output - passthrough value."""
    @compute_node
    def passthrough_if_valid(ref: REF[TS[int]]) -> REF[TS[int]]:
        # Passthrough the ref value if valid (tests valid property)
        if ref.valid:
            return ref.value
        return None

    @graph
    def g(ts: TS[int]) -> REF[TS[int]]:
        return passthrough_if_valid(ts)

    # Both ticks should pass through since ref is valid
    assert eval_node(g, [1, 2]) == [1, 2]


def test_modified_on_ref_tick():
    """Test REF is modified when underlying value changes."""
    @compute_node
    def passthrough_if_modified(ref: REF[TS[int]]) -> REF[TS[int]]:
        # Passthrough only if modified
        return ref.value if ref.modified else None

    @graph
    def g(ts: TS[int]) -> REF[TS[int]]:
        return passthrough_if_modified(ts)

    # Each tick should pass through since ref is modified
    assert eval_node(g, [1, 2, 3]) == [1, 2, 3]


def test_value_access():
    """Test accessing REF value returns TimeSeriesReference."""
    @compute_node
    def get_value_type(ref: REF[TS[int]]) -> TS[bool]:
        return TimeSeriesReference.is_instance(ref.value)

    @graph
    def g(ts: TS[int]) -> TS[bool]:
        return get_value_type(ts)

    assert eval_node(g, [1]) == [True]


# =============================================================================
# TIME SERIES REFERENCE TYPE TESTS
# =============================================================================


# Test different TimeSeriesReference types.

def test_bound_reference_is_valid():
    """Test BoundTimeSeriesReference is_valid returns True when output valid."""
    @compute_node
    def check_ref_is_valid(ref: REF[TS[int]]) -> TS[bool]:
        v = ref.value
        return v.is_valid if v else False

    @graph
    def g(ts: TS[int]) -> TS[bool]:
        return check_ref_is_valid(ts)

    assert eval_node(g, [1]) == [True]


def test_bound_reference_has_output():
    """Test BoundTimeSeriesReference has_output returns True."""
    @compute_node
    def check_has_output(ref: REF[TS[int]]) -> TS[bool]:
        v = ref.value
        return v.has_output if v else False

    @graph
    def g(ts: TS[int]) -> TS[bool]:
        return check_has_output(ts)

    assert eval_node(g, [1]) == [True]


def test_empty_reference_not_valid():
    """Test EmptyTimeSeriesReference is_valid returns False."""
    @compute_node
    def make_empty(ts: TS[bool]) -> REF[TS[int]]:
        return TimeSeriesReference.make()

    @compute_node
    def check_ref_is_valid(ref: REF[TS[int]]) -> TS[bool]:
        v = ref.value
        return v.is_valid if v else False

    @graph
    def g(ts: TS[bool]) -> TS[bool]:
        return check_ref_is_valid(make_empty(ts))

    assert eval_node(g, [True]) == [False]


def test_empty_reference_is_empty():
    """Test EmptyTimeSeriesReference is_empty returns True."""
    @compute_node
    def make_empty(ts: TS[bool]) -> REF[TS[int]]:
        return TimeSeriesReference.make()

    @compute_node
    def check_is_empty(ref: REF[TS[int]]) -> TS[bool]:
        v = ref.value
        return v.is_empty if v else True

    @graph
    def g(ts: TS[bool]) -> TS[bool]:
        return check_is_empty(make_empty(ts))

    assert eval_node(g, [True]) == [True]


# =============================================================================
# REF BINDING TESTS
# =============================================================================


# Test REF binding behaviors.

def test_bind_to_ts_output():
    """Test REF binding to TS output."""
    @compute_node
    def passthrough_ref(ref: REF[TS[int]]) -> REF[TS[int]]:
        return ref.value

    @graph
    def g(ts: TS[int]) -> REF[TS[int]]:
        return passthrough_ref(ts)

    result = eval_node(g, [1, 2, 3])
    assert result == [1, 2, 3]


def test_bind_to_tsl_output():
    """Test REF binding to TSL output."""
    @compute_node
    def passthrough_ref(ref: REF[TSL[TS[int], SIZE]]) -> REF[TSL[TS[int], SIZE]]:
        return ref.value

    @graph
    def g(tsl: TSL[TS[int], Size[2]]) -> REF[TSL[TS[int], Size[2]]]:
        return passthrough_ref(tsl)

    result = eval_node(g, [(1, 2), (3, None), (None, 5)])
    assert result == [{0: 1, 1: 2}, {0: 3}, {1: 5}]


# =============================================================================
# REF WITH DIFFERENT TIME-SERIES TYPES
# =============================================================================


# Test REF[TS[T]] - reference to scalar time-series.

def test_ref_ts_int():
    """Test REF[TS[int]]."""
    @compute_node
    def create_ref(ts: REF[TS[int]]) -> REF[TS[int]]:
        return ts.value

    assert eval_node(create_ref, ts=[1, 2, 3]) == [1, 2, 3]


def test_ref_ts_string():
    """Test REF[TS[str]]."""
    @compute_node
    def create_ref(ts: REF[TS[str]]) -> REF[TS[str]]:
        return ts.value

    assert eval_node(create_ref, ts=["a", "b", "c"]) == ["a", "b", "c"]


def test_ref_ts_float():
    """Test REF[TS[float]]."""
    @compute_node
    def create_ref(ts: REF[TS[float]]) -> REF[TS[float]]:
        return ts.value

    assert eval_node(create_ref, ts=[1.0, 2.5, 3.7]) == [1.0, 2.5, 3.7]


# Test REF[TSL[...]] - reference to list time-series.

def test_ref_tsl_passthrough():
    """Test REF[TSL] passthrough."""
    @compute_node
    def create_ref(ts: REF[TSL[TS[int], SIZE]]) -> REF[TSL[TS[int], SIZE]]:
        return ts.value

    result = eval_node(create_ref[SIZE: Size[2]], ts=[(1, 2), (3, 4)])
    assert result == [{0: 1, 1: 2}, {0: 3, 1: 4}]


def test_ref_tsl_from_ts():
    """Test REF[TSL] created from TSL.from_ts()."""
    @compute_node
    def merge_ref(index: TS[int], ts: TSL[REF[TIME_SERIES_TYPE], SIZE]) -> REF[TIME_SERIES_TYPE]:
        from typing import cast
        return cast(REF, ts[index.value].value)

    @graph
    def g(index: TS[int], ts1: TS[int], ts2: TS[int]) -> REF[TS[int]]:
        return merge_ref(index, TSL.from_ts(ts1, ts2))

    result = eval_node(g, index=[0, None, 1], ts1=[1, 2, 3], ts2=[-1, -2, -3])
    assert result == [1, 2, -3]


# Test REF[TSB[...]] - reference to bundle time-series.

def test_ref_tsb_signal():
    """Test REF[TSB] used as signal."""
    class AB(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]

    @compute_node
    def ref_signal(ts: REF[TSB[AB]]) -> TS[bool]:
        return ts.valid

    @graph
    def g(a: TS[int], b: TS[int]) -> TS[bool]:
        from hgraph import combine
        return ref_signal(combine[TSB[AB]](a=a, b=b))

    assert eval_node(g, a=[1, 2], b=[3, 4]) == [True, None]


# Test REF[TSS[...]] - reference to set time-series.

def test_ref_tss_simple():
    """Test REF[TSS] passthrough preserves delta."""
    @compute_node
    def create_ref(ts: REF[TSS[int]]) -> REF[TSS[int]]:
        return ts.value

    # Passthrough preserves the original delta semantics
    result = eval_node(create_ref, ts=[{1, 2}, {3}, {Removed(1), 4}])
    # First tick adds {1, 2}, second adds {3}, third adds {4} and removes {1}
    assert result[0].added == frozenset({1, 2})
    assert result[1].added == {3}
    assert result[2].added == {4}
    assert Removed(1) in result[2].removed or 1 in result[2].removed


# Test REF[TSD[...]] - reference to dict time-series.

def test_ref_tsd_simple():
    """Test REF[TSD] passthrough preserves delta."""
    @compute_node
    def create_ref(ts: REF[TSD[int, TS[int]]]) -> REF[TSD[int, TS[int]]] :
        return ts.value

    # Passthrough preserves the original delta semantics
    result = eval_node(create_ref, ts=[{1: 10, 2: 20}, {3: 30}, {1: REMOVE, 4: 40}])
    assert result[0] == {1: 10, 2: 20}
    assert result[1] == {3: 30}
    assert result[2] == {1: REMOVE, 4: 40}


# =============================================================================
# REF SWITCHING/ROUTING TESTS
# =============================================================================


# Test REF switching and routing patterns.

def test_if_routes_ref():
    """Test if_ operator routes REF based on condition."""
    result = eval_node(
        if_[TIME_SERIES_TYPE: TS[int]],
        condition=[True, None, False, None],
        ts=[1, 2, None, 4],
    )
    assert result == [{"true": 1}, {"true": 2}, {"false": 2}, {"false": 4}]


def test_merge_ref_from_tsl():
    """Test merging REF from TSL based on index."""
    @compute_node
    def merge_ref(index: TS[int], ts: TSL[REF[TIME_SERIES_TYPE], SIZE]) -> REF[TIME_SERIES_TYPE]:
        from typing import cast
        return cast(REF, ts[index.value].value)

    result = eval_node(
        merge_ref[TIME_SERIES_TYPE: TS[int], SIZE: Size[2]],
        index=[0, None, 1, None],
        ts=[(1, -1), (2, -2), None, (4, -4)],
    )
    assert result == [1, 2, -2, -4]


def test_merge_ref_non_peer():
    """Test merging non-peered REF."""
    @compute_node
    def merge_ref(index: TS[int], ts: TSL[REF[TIME_SERIES_TYPE], SIZE]) -> REF[TIME_SERIES_TYPE]:
        from typing import cast
        return cast(REF, ts[index.value].value)

    @graph
    def g(index: TS[int], ts1: TIME_SERIES_TYPE, ts2: TIME_SERIES_TYPE) -> REF[TIME_SERIES_TYPE]:
        return merge_ref(index, TSL.from_ts(ts1, ts2))

    result = eval_node(
        g[TIME_SERIES_TYPE: TS[int]],
        index=[0, None, 1, None],
        ts1=[1, 2, None, 4],
        ts2=[-1, -2, None, -4],
    )
    assert result == [1, 2, -2, -4]


# =============================================================================
# REF WITH COMPLEX INNER TYPES
# =============================================================================


# Test REF with complex inner time-series types.

def test_ref_tsl_of_ts():
    """Test REF[TSL[TS[int], Size]]."""
    @compute_node
    def merge_ref(index: TS[int], ts: TSL[REF[TIME_SERIES_TYPE], SIZE]) -> REF[TIME_SERIES_TYPE]:
        from typing import cast
        return cast(REF, ts[index.value].value)

    @graph
    def g(index: TS[int], ts1: TSL[TS[int], Size[2]], ts2: TSL[TS[int], Size[2]]) -> REF[TSL[TS[int], Size[2]]]:
        return merge_ref(index, TSL.from_ts(ts1, ts2))

    result = eval_node(
        g,
        index=[0, None, 1, None],
        ts1=[(1, 1), (2, None), None, (None, 4)],
        ts2=[(-1, -1), (-2, -2), None, (-4, None)],
    )
    assert result == [{0: 1, 1: 1}, {0: 2}, {0: -2, 1: -2}, {0: -4}]


# =============================================================================
# REF SET SWITCHING TESTS
# =============================================================================


# Test REF with TSS switching behavior.

def test_merge_ref_set():
    """Test merging REF[TSS] preserves set delta semantics."""
    @compute_node
    def merge_ref(index: TS[int], ts: TSL[REF[TIME_SERIES_TYPE], SIZE]) -> REF[TIME_SERIES_TYPE]:
        from typing import cast
        return cast(REF, ts[index.value].value)

    @graph
    def g(index: TS[int], ts1: TSS[int], ts2: TSS[int]) -> REF[TSS[int]]:
        return merge_ref(index, TSL.from_ts(ts1, ts2))

    result = eval_node(
        g,
        index=[0, None, 1, None],
        ts1=[{1, 2}, None, None, {4}],
        ts2=[{-1}, {-2}, {-3, Removed(-1)}, {-4}],
    )
    assert result == [{1, 2}, None, {-2, -3, Removed(1), Removed(2)}, {-4}]


def test_merge_ref_set_overlap():
    """Test merging REF[TSS] with overlapping values."""
    @compute_node
    def merge_ref(index: TS[int], ts: TSL[REF[TIME_SERIES_TYPE], SIZE]) -> REF[TIME_SERIES_TYPE]:
        from typing import cast
        return cast(REF, ts[index.value].value)

    @graph
    def g(index: TS[int], ts1: TSS[int], ts2: TSS[int]) -> REF[TSS[int]]:
        return merge_ref(index, TSL.from_ts(ts1, ts2))

    result = eval_node(
        g,
        index=[0, None, 1, None],
        ts1=[{1, 2}, None, None, {4}],
        ts2=[{1}, None, {2}, {4}],
    )
    # When switching from ts1 to ts2, values not in ts2 are removed
    assert result == [{1, 2}, None, set(), {4}]


# =============================================================================
# REF TSD SWITCHING TESTS
# =============================================================================


# Test REF with TSD switching behavior.

def test_merge_with_tsd():
    """Test merging REF[TSD] preserves dict delta semantics."""
    @compute_node
    def merge_ref(index: TS[int], ts: TSL[REF[TIME_SERIES_TYPE], SIZE]) -> REF[TIME_SERIES_TYPE]:
        from typing import cast
        return cast(REF, ts[index.value].value)

    @graph
    def g(index: TS[int], ts1: TSD[int, TS[int]], ts2: TSD[int, TS[int]]) -> REF[TSD[int, TS[int]]]:
        return merge_ref(index, TSL.from_ts(ts1, ts2))

    result = eval_node(
        g,
        index=[0, None, 1, None],
        ts1=[{1: 1, 2: 2}, None, None, {4: 4}],
        ts2=[{-1: -1}, {-2: -2}, {-3: -3, -1: REMOVE}, {-4: -4}],
    )
    assert result == [{1: 1, 2: 2}, None, {-2: -2, -3: -3, 1: REMOVE, 2: REMOVE}, {-4: -4}]


# =============================================================================
# EDGE CASE TESTS
# =============================================================================


# Test REF edge cases and boundary conditions.

def test_ref_with_none_ticks():
    """Test REF behavior with None ticks in source."""
    @compute_node
    def create_ref(ts: REF[TS[int]]) -> REF[TS[int]]:
        return ts.value

    result = eval_node(create_ref, ts=[1, None, 3, None])
    assert result == [1, None, 3, None]


def test_ref_delta_value_equals_value():
    """Test that delta_value equals value for REF - passthrough if equal."""
    @compute_node
    def passthrough_if_delta_eq(ref: REF[TS[int]]) -> REF[TS[int]]:
        # delta_value and value should return the same TimeSeriesReference
        if ref.value == ref.delta_value:
            return ref.value
        return None

    @graph
    def g(ts: TS[int]) -> REF[TS[int]]:
        return passthrough_if_delta_eq(ts)

    # All ticks should pass through since delta_value == value
    assert eval_node(g, [10, 20, 30]) == [10, 20, 30]


def test_ref_all_valid_passthrough():
    """Test all_valid property on REF - passthrough if all_valid."""
    @compute_node
    def passthrough_if_all_valid(ref: REF[TS[int]]) -> REF[TS[int]]:
        return ref.value if ref.all_valid else None

    @graph
    def g(ts: TS[int]) -> REF[TS[int]]:
        return passthrough_if_all_valid(ts)

    # Both ticks should pass through since all_valid is True
    assert eval_node(g, [1, 2]) == [1, 2]


# =============================================================================
# REF LAST MODIFIED TIME TESTS
# =============================================================================


# Test REF last_modified_time property.

def test_last_modified_time_accessible():
    """Test that last_modified_time is accessible on each tick."""
    @compute_node
    def passthrough_if_lmt_exists(ref: REF[TS[int]]) -> REF[TS[int]]:
        # Passthrough if last_modified_time is accessible
        return ref.value if ref.last_modified_time is not None else None

    @graph
    def g(ts: TS[int]) -> REF[TS[int]]:
        return passthrough_if_lmt_exists(ts)

    assert eval_node(g, [1, 2, 3]) == [1, 2, 3]
