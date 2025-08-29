import logging
from contextlib import nullcontext

import pytest

from hgraph import LOGGER, compute_node, graph, TSL, TS, Size, debug_print, log_, print_, assert_, NodeException, DebugContext, null_sink
from hgraph.nodes._tsl_operators import tsl_to_tsd
from hgraph.test import eval_node


def test_debug_print(capsys):

    @graph
    def main(tsl: TSL[TS[int], Size[3]], keys: tuple[str, ...]):
        tsd = tsl_to_tsd(tsl, keys)
        debug_print("tsd", tsd)

    eval_node(main, [(1, 2, 3), {1: 3}], ("a", "b", "c"))

    assert "tsd" in capsys.readouterr().out


@pytest.mark.parametrize("debug_on, no_context_manager", [(False, False), (True, False), (False, True)])
def test_debug_context(capsys, debug_on: bool, no_context_manager: bool):

    @graph
    def main(tsl: TSL[TS[int], Size[3]], keys: tuple[str, ...]):
        with nullcontext() if no_context_manager else DebugContext(prefix="[test]", debug=debug_on):
            tsd = tsl_to_tsd(tsl, keys)
            DebugContext.print("tsd", tsd)
            null_sink(tsd)  # When false this still needs a sink node

    eval_node(main, [(1, 2, 3), {1: 3}], ("a", "b", "c"))

    if debug_on:
        assert "[test] tsd" in capsys.readouterr().out
    else:
        assert "[test] tsd" not in capsys.readouterr().out


def test_debug_context_no_prefix(capsys):

    @graph
    def main(tsl: TSL[TS[int], Size[3]], keys: tuple[str, ...]):
        with DebugContext():
            tsd = tsl_to_tsd(tsl, keys)
            DebugContext.print("tsd", tsd)

    eval_node(main, [(1, 2, 3), {1: 3}], ("a", "b", "c"))

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
    out = capsys.readouterr()
    if out.err:
        output = out.err
    else:
        output = out.out
    assert "Error output Test 1" in output
    # TODO: It seems the default logging level was raised so this does not come out at the moment
    # assert "[INFO] Info output Test 1" in output


@pytest.mark.xfail(reason="This passes when run on its own but not part of a suite. Something not cleaned up")
def test_log_args(capsys):
    @graph
    def main(ts1: TS[str], ts2: TS[int]):
        log_("Error output {} {}", ts1, ts2, level=logging.ERROR)

    eval_node(main, ["Test"], [1])
    out = capsys.readouterr()
    if out.err:
        output = out.err
    else:
        output = out.out
    assert "Error output Test 1" in output


@pytest.mark.xfail(reason="This passes when run on its own but not part of a suite. Something not cleaned up")
def test_log_no_args_or_kwargs(capsys):
    @graph
    def main():
        log_("Error output Test 1", level=logging.ERROR)

    eval_node(main)
    out = capsys.readouterr()
    if out.err:
        output = out.err
    else:
        output = out.out
    assert "Error output Test 1" in output


@pytest.mark.xfail(reason="This passes when run on its own but not part of a suite. Something not cleaned up")
def test_log_sample(capsys):

    @graph
    def g(ts: TS[str]):
        log_("Sample output {}", ts, sample_count=3, level=logging.ERROR)

    eval_node(g, ["a", "b", "c", "d", "e"])
    out = capsys.readouterr()
    if out.err:
        output = out.err
    else:
        output = out.out
    assert "Sample output c" in output
    assert "Sample output d" not in output


def test_debug_print_sample(capsys):
    @graph
    def main(ts: TS[int]):
        debug_print("ts", ts, sample=2)

    eval_node(main, [1, 2, 3, 4])

    assert "[2] ts" in capsys.readouterr().out


def test_assert():
    @graph
    def main(condition: TS[bool], ts: TS[int]):
        assert_(condition, "assertion {} {sample}", ts, sample=2)

    with pytest.raises(NodeException, match="assertion 3 2"):
        eval_node(main, [True, None, False], [1, 2, 3, 4])


def test_custom_label(capsys):
    @compute_node(label="custom_label {i}")
    def g(ts: TS[str], i: str, logger_: LOGGER = None) -> TS[str]:
        return ts.value

    eval_node(g, ["Contents"], i="one")
    out = capsys.readouterr().out
    if out:  # capture fixture is flaky, so check if out is not empty
        assert "custom_label one:g" in out