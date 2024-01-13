import pytest

from hgraph import reference_service, TSD, TS, service_impl, graph, register_service, default_path
from hgraph.nodes import const
from hgraph.test import eval_node


@pytest.mark.xfail(reason="Not implemented yet", strict=True)
def test_reference_service():

    @reference_service
    def my_service(path: str = None) -> TSD[str, TS[str]]:
        """The service description"""

    @service_impl(my_service)
    def my_service_impl() -> TSD[str, TS[str]]:
        return const({"test": "a value"}, TSD[str, TS[str]])

    @graph
    def main() -> TS[str]:
        register_service(default_path, my_service, my_service_impl)
        return my_service()["test"]

    assert eval_node(main) == ["a value"]