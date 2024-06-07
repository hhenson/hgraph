import pytest

from hgraph import add_, WiringError, sub_, and_, or_, not_
from hgraph.test import eval_node


def test_add_booleans_attempt():
    with pytest.raises(WiringError) as e:
        eval_node(add_, [True], [False])
    assert "Cannot add two booleans" in str(e)


def test_subtract_booleans_attempt():
    with pytest.raises(WiringError) as e:
        eval_node(sub_, [True], [False])
    assert "Cannot subtract two booleans" in str(e)


def test_and_booleans():
    assert eval_node(and_, [True], [False]) == [False]
    assert eval_node(and_, [False], [True]) == [False]
    assert eval_node(and_, [True], [True]) == [True]
    assert eval_node(and_, [False], [False]) == [False]


def test_or_booleans():
    assert eval_node(or_, [True], [False]) == [True]
    assert eval_node(or_, [False], [True]) == [True]
    assert eval_node(or_, [True], [True]) == [True]
    assert eval_node(or_, [False], [False]) == [False]


def test_not_booleans():
    assert eval_node(not_, [True]) == [False]
    assert eval_node(not_, [False]) == [True]
