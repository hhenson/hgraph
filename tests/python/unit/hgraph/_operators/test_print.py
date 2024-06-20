import logging

import pytest

from hgraph import graph, TSL, TS, Size, debug_print, log_, print_
from hgraph.nodes._tsl_operators import tsl_to_tsd
from hgraph.test import eval_node


def test_debug_print(capsys):

    @graph
    def main(tsl: TSL[TS[int], Size[3]], keys: tuple[str, ...]):
        tsd = tsl_to_tsd(tsl, keys)
        debug_print("tsd", tsd)

    eval_node(main, [(1, 2, 3), {1: 3}], ('a', 'b', 'c'))

    assert "tsd" in capsys.readouterr().out


def test_print_kwargs(capsys):
    @graph
    def main(ts: TS[str]):
        print_("Test output {c}", c=ts)

    eval_node(main, ["Contents"])
    assert "Contents" in capsys.readouterr().out


def test_print_args(capsys):
    @graph
    def main(ts: TS[str]):
        print_("Test output {}", ts)

    eval_node(main, ["Contents"])
    assert "Contents" in capsys.readouterr().out


def test_print_no_args_or_kwargs(capsys):
    @graph
    def main():
        print_("Test output Contents")

    eval_node(main)
    assert "Contents" in capsys.readouterr().out


def test_print_stderr(capsys):
    @graph
    def main():
        print_("Test output Contents", __std_out__=False)

    eval_node(main)
    assert "Contents" in capsys.readouterr().err


@pytest.mark.xfail(reason="This passes when run on its own but not part of a suite. Something not cleaned up")
def test_log_kwargs(capsys):
    @graph
    def main(ts1: TS[str], ts2: TS[int]):
        log_("Error output {ts1} {ts2}", ts1=ts1, ts2=ts2, level=logging.ERROR)
        log_("Info output {ts1} {ts2}", ts1=ts1, ts2=ts2, level=logging.INFO)

    eval_node(main, ["Test"], [1])
    stderr = capsys.readouterr().err
    assert "[ERROR] Error output Test 1" in stderr
    assert "[INFO] Info output Test 1" in stderr


@pytest.mark.xfail(reason="This passes when run on its own but not part of a suite. Something not cleaned up")
def test_log_args(capsys):
    @graph
    def main(ts1: TS[str], ts2: TS[int]):
        log_("Error output {} {}", ts1, ts2, level=logging.ERROR)

    eval_node(main, ["Test"], [1])
    stderr = capsys.readouterr().err
    assert "[ERROR] Error output Test 1" in stderr


@pytest.mark.xfail(reason="This passes when run on its own but not part of a suite. Something not cleaned up")
def test_log_no_args_or_kwargs(capsys):
    @graph
    def main():
        log_("Error output Test 1", level=logging.ERROR)

    eval_node(main)
    stderr = capsys.readouterr().err
    assert "[ERROR] Error output Test 1" in stderr


def test_debug_print_sample(capsys):
    @graph
    def main(ts: TS[int]):
        debug_print("ts", ts, sample=2)

    eval_node(main, [1, 2, 3, 4])

    assert "[2] ts" in capsys.readouterr().out

