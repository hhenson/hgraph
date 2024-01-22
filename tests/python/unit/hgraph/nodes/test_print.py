from hgraph import graph, TSL, SIZE, TS, Size
from hgraph.nodes import debug_print
from hgraph.nodes._tsl_operators import tsl_to_tsd
from hgraph.test import eval_node


def test_debug_print(capsys):

    @graph
    def main(tsl: TSL[TS[int], Size[3]], keys: tuple[str, ...]):
        tsd = tsl_to_tsd(tsl, keys)
        debug_print("tsd", tsd)

    eval_node(main, [(1, 2, 3), {1: 3}], ('a', 'b', 'c'))

    assert "tsd" in capsys.readouterr().out
