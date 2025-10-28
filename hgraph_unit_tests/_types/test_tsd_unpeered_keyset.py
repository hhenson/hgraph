import pytest
from frozendict import frozendict

from hgraph import graph, TS, TSD, TSS, REF, REMOVE
from hgraph.test import eval_node
from hgraph._operators import keys_
from hgraph import set_delta


# This test exercises the non-peered TSD input path where the consumer expects REF-valued children
# but the producer provides non-REF children. In this case, the TSD input should manage a local
# key_set whose membership mirrors its internal _ts_values, ensuring consistency.


@graph
def consumer_keys(tsd: TSD[str, REF[TS[int]]]) -> TSS[str]:
    # Return the keys as observed by the consumer's TSD input
    return keys_(tsd)


@graph
def expose_unpeered_keys(producer: TSD[str, TS[int]]) -> TSS[str]:
    # Wire producer (non-REF values) into a consumer that expects REF values -> forces peer=False
    return consumer_keys(producer)


def test_tsd_unpeered_keyset_tracks_adds_and_removals():
    # Sequence: add 'a', add 'b', remove 'a', remove 'b'
    inputs = [
        frozendict({"a": 1}),
        frozendict({"b": 2}),
        frozendict({"a": REMOVE}),
        frozendict({"b": REMOVE}),
    ]

    # Expect the consumer's key set to mirror the internal _ts_values of its unpeered TSD input
    # after each tick: {'a'}, {'a','b'}, {'b'}, {}
    expected = [
        set_delta(added={"a"}, removed=set(), tp=str),
        set_delta(added={"b"}, removed=set(), tp=str),
        set_delta(added=set(), removed={"a"}, tp=str),
        set_delta(added=set(), removed={"b"}, tp=str),
    ]

    assert eval_node(expose_unpeered_keys, inputs) == expected
