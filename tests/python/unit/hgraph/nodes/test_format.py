from hgraph import graph, TS, TIME_SERIES_TYPE_1, TIME_SERIES_TYPE_2, TIME_SERIES_TYPE
from hgraph.nodes import format_
from hgraph.test import eval_node


def test_format_args():
    @graph
    def format_test(format_str: TS[str], ts1: TIME_SERIES_TYPE_1, ts2: TIME_SERIES_TYPE_2) -> TS[str]:
        return format_(format_str, ts1, ts2)

    f_str = "{} is a test {}"
    ts1 = [1, 2]
    ts2 = ["a", "b"]

    expected = [f_str.format(ts1, ts2) for ts1, ts2 in zip(ts1, ts2)]

    assert eval_node(format_test, [f_str], ts1, ts2) == expected


def test_format_kwargs():
    @graph
    def format_test(format_str: TS[str], ts1: TIME_SERIES_TYPE_1, ts2: TIME_SERIES_TYPE_2) -> TS[str]:
        return format_(format_str, ts1=ts1, ts2=ts2)

    f_str = "{ts1} is a test {ts2}"
    ts1 = [1, 2]
    ts2 = ["a", "b"]

    expected = [f_str.format(ts1=ts1, ts2=ts2) for ts1, ts2 in zip(ts1, ts2)]

    assert eval_node(format_test, [f_str], ts1, ts2) == expected


def test_format_mixed():
    @graph
    def format_test(format_str: TS[str], ts: TIME_SERIES_TYPE, ts1: TIME_SERIES_TYPE_1, ts2: TIME_SERIES_TYPE_2) \
            -> TS[str]:
        return format_(format_str, ts, ts1=ts1, ts2=ts2)

    f_str = "{ts1} is a test {ts2}"
    ts = [1.1, 1.2]
    ts1 = [1, 2]
    ts2 = ["a", "b"]

    expected = [f_str.format(ts, ts1=ts1, ts2=ts2) for ts, ts1, ts2 in zip(ts, ts1, ts2)]

    assert eval_node(format_test, [f_str], ts, ts1, ts2) == expected


def test_format_sampled():
    @graph
    def format_test(format_str: TS[str], ts1: TIME_SERIES_TYPE_1, ts2: TIME_SERIES_TYPE_2) -> TS[str]:
        return format_(format_str, ts1, ts2, __sample__=3)

    f_str = "{} is a test {}"
    ts1 = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    ts2 = ["a", "b", "c", "d", "e", "f", "g", "h", "i"]

    expected = [None if (ndx + 1) % 3 != 0 else f_str.format(ts1, ts2) for ndx, (ts1, ts2) in enumerate(zip(ts1, ts2))]

    assert eval_node(format_test, [f_str], ts1, ts2) == expected