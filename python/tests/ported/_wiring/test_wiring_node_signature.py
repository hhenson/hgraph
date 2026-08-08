# Ported from ext/main/hgraph_unit_tests/_wiring/test_wiring_node_signature.py
# ported: private-module imports corrected to the public hgraph surface
# deviation: HgTypeMetaData is not part of the public API (recorded ruling) —
# input_types/output_type carry the raw annotations; the scalar/time-series
# split is asserted through signature.time_series_args instead.
from hgraph import TS, extract_signature, WiringNodeType


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

    assert signature.input_types["a"] == TS[int]
    assert "b" not in signature.time_series_args
    assert signature.input_types["b"] is str
    assert signature.defaults["b"] == "const"

    assert signature.output_type == TS[str]
