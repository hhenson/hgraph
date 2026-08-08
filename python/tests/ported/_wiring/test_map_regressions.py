# Public regression from hgraph 275215c5. The C++ runtime represents the same
# shape directly; no private Python builder behavior is part of this test.
import hgraph as hg
from hgraph.test import eval_node


def test_map_invalid_reference_bundle_field_output():
    class AB(hg.TimeSeriesSchema):
        a: hg.TS[int]

    @hg.graph
    def child(value: hg.TS[int], condition: hg.TS[bool]) -> hg.TSB[AB]:
        return hg.TSB[AB].from_ts(a=hg.if_(condition, value).true)

    @hg.graph
    def app(
        values: hg.TSD[str, hg.TS[int]], condition: hg.TS[bool],
    ) -> hg.TSD[str, hg.TSB[AB]]:
        return hg.map_(child, values, condition)

    assert eval_node(app, [{"x": 1}], [False]) == [{}]
