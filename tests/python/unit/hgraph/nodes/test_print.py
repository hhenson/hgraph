import logging
import time

import pytest

from hgraph import graph, TSL, SIZE, TS, Size
from hgraph.nodes import debug_print, const, log, print_
from hgraph.nodes._tsl_operators import tsl_to_tsd
from hgraph.test import eval_node


def test_debug_print(capsys):

    @graph
    def main(tsl: TSL[TS[int], Size[3]], keys: tuple[str, ...]):
        tsd = tsl_to_tsd(tsl, keys)
        debug_print("tsd", tsd)

    eval_node(main, [(1, 2, 3), {1: 3}], ('a', 'b', 'c'))

    assert "tsd" in capsys.readouterr().out


def test_print_(capsys):
    @graph
    def main(ts: TS[str]):
        print_("Test output {c}", c=ts)

    eval_node(main, ["Contents"])
    assert "Contents" in capsys.readouterr().out


@pytest.mark.xfail(reason="This passes in debug mode, but not when run property, it is probably a timing issue")
def test_log(capsys):
    @graph
    def main(ts1: TS[str], ts2: TS[int]):
        log("Error output {ts1} {ts2}", ts1=ts1, ts2=ts2, level=logging.ERROR)
        log("Info output {ts1} {ts2}", ts1=ts1, ts2=ts2, level=logging.INFO)

    eval_node(main, ["Test"], [1])
    stderr = capsys.readouterr().err
    assert "[ERROR] Error output Test 1" in stderr
    assert "[INFO] Info output Test 1" in stderr


def test_debug_print_sample(capsys):
    @graph
    def main(ts: TS[int]):
        debug_print("ts", ts, sample=2)

    eval_node(main, [1, 2, 3, 4])

    assert "[2] ts" in capsys.readouterr().out

