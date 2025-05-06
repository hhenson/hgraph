from hgraph._types._ts_type import TS
from hgraph._wiring._wiring_node_signature import extract_signature, WiringNodeType


def test_extract_fn():

    def my_fn(a: TS[int], b: str = "const") -> TS[str]:
        """Stub function to test extraction logic"""

    signature = extract_signature(my_fn, WiringNodeType.GRAPH)

    assert signature.args == ("a", "b")
    assert len(signature.defaults) == 1
    assert signature.name == "my_fn"
    assert len(signature.input_types) == 2
    assert not signature.unresolved_args
    assert signature.time_series_args == frozenset({"a"})

    assert not signature.input_types["a"].is_scalar
    assert signature.input_types["a"].is_resolved
    assert not signature.input_types["a"].is_atomic
    assert signature.input_types["a"].py_type == TS[int]

    assert signature.input_types["b"].is_scalar
    assert signature.input_types["b"].is_resolved
    assert signature.input_types["b"].is_atomic
    assert signature.input_types["b"].py_type == str
    assert signature.defaults["b"] == "const"

    assert not signature.output_type.is_scalar
    assert not signature.output_type.is_atomic
    assert signature.output_type.is_resolved
    assert signature.output_type.py_type == TS[str]
