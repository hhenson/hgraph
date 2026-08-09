from typing import Any, Callable

import pytest

import hgraph as hg
from hgraph.test import eval_node


@pytest.mark.parametrize("higher_order", [hg.map_, hg.mesh_], ids=["map", "mesh"])
def test_higher_order_overload_matches_lambda_shape(higher_order):
    @hg.compute_node(
        overloads=higher_order,
        requires=lambda m, func: hg.equal_lambdas(func, lambda value: value + 1),
    )
    def custom(
        func: Callable[..., Any], values: hg.TSD[str, hg.TS[int]],
    ) -> hg.TSD[str, hg.TS[int]]:
        return {
            key: value.value * 10
            for key, value in values.modified_items()
        }

    @hg.graph
    def matching(values: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
        return higher_order(lambda renamed: renamed + 1, values)

    @hg.graph
    def different(values: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
        return higher_order(lambda value: value + 2, values)

    assert eval_node(matching, [{"a": 1, "b": 2}]) == [{"a": 10, "b": 20}]
    assert eval_node(different, [{"a": 1, "b": 2}]) == [{"a": 3, "b": 4}]
