from hgraph import generator, compute_node, sink_node, graph, WiringGraphContext, SCALAR, WiringPort, WiringNodeInstance
from hgraph._types import HgTypeMetaData
from hgraph._types._ts_type import TS
from hgraph._wiring._wiring_node_instance import WiringNodeInstanceContext


def test_simple_wiring():

    @generator
    def t_src_n(c1: str) -> TS[str]:
        yield c1

    @compute_node
    def t_cn(ts1: TS[str]) -> TS[str]:
        return ts1.value

    @sink_node
    def t_sink_n(ts1: TS[str]):
        print(ts1.value)

    @graph
    def g():
        out = t_src_n("Test")
        out = t_cn(out)
        t_sink_n(out)

    with WiringGraphContext(None) as context, WiringNodeInstanceContext():
        g()
        assert context.has_sink_nodes()
        sink_nodes = context.sink_nodes
        assert len(sink_nodes) == 1
        assert len(sn_inputs := sink_nodes[0].inputs) == 1
        parent: WiringPort = sn_inputs["ts1"]
        assert len(pn_inputs := parent.node_instance.inputs) == 1
        parent = pn_inputs["ts1"]
        assert len(pn_inputs := parent.node_instance.inputs) == 1
        assert pn_inputs["c1"] == "Test"


def test_un_resolved_wiring():

    @generator
    def t_src_n(c1: SCALAR) -> TS[SCALAR]:
        yield c1

    @sink_node
    def t_sink_n(ts: TS[SCALAR]):
        print(ts.value)

    @graph
    def g():
        out = t_src_n("Test")
        t_sink_n(out)

    with WiringGraphContext(None) as context, WiringNodeInstanceContext():
        g()
        assert context.has_sink_nodes()
        sink_nodes = context.sink_nodes
        assert len(sink_nodes) == 1
        sn: WiringNodeInstance = sink_nodes[0]
        assert sn.resolved_signature.input_types["ts"] == HgTypeMetaData.parse_type(TS[str])
