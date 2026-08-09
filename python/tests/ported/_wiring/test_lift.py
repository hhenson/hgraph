from typing import Mapping

from frozendict import frozendict as fd

from hgraph import TSD, TS, drop_dups, graph, lift
from hgraph.test import eval_node


def test_lift_dedup_output_uses_native_dedup():
    def as_mapping(value: Mapping[str, int]) -> Mapping[str, int]:
        return value

    lifted = lift(as_mapping, output=TSD[str, TS[int]], dedup_output=True)

    assert eval_node(lifted, [fd(a=1), fd(a=1, b=2)]) == [fd(a=1), fd(b=2)]


def test_drop_dups_is_the_dedup_compatibility_alias():
    @graph
    def deduplicated(ts: TS[int]) -> TS[int]:
        return drop_dups(ts)

    assert eval_node(deduplicated, [1, 1, 2, 2]) == [1, None, 2, None]
