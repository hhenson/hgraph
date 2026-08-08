from hgraph import TS, graph
from hgraph.nodes import request_id
from hgraph.test import eval_node


def test_request_id_uses_unique_native_service_ids():
    @graph
    def app() -> TS[int]:
        return request_id(1)

    first = eval_node(app)[0]
    second = eval_node(app)[0]
    assert isinstance(first, int)
    assert isinstance(second, int)
    assert first != second
