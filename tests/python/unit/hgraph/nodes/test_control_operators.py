from hgraph import graph, TS, all_, any_
from hgraph.nodes import const
from hgraph.test import eval_node


def test_all_false():
    @graph
    def app() -> TS[bool]:
        return all_(const(True), const(False), const(True))

    assert eval_node(app) == [False]


def test_all_true():
    @graph
    def app() -> TS[bool]:
        return all_(const(True), const(True), const(True))

    assert eval_node(app) == [True]


def test_any_false():
    @graph
    def app() -> TS[bool]:
        return any_(const(False), const(False), const(False))

    assert eval_node(app) == [False]


def test_any_true():
    @graph
    def app() -> TS[bool]:
        return any_(const(True), const(False), const(True))

    assert eval_node(app) == [True]
