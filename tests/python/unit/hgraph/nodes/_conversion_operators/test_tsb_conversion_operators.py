import pytest
from frozendict import frozendict as fd

from hgraph import TimeSeriesSchema, TS, TSB, TSD, WiringError, graph
from hgraph.nodes import convert
from hgraph.test import eval_node


class ATimeSeriesSchema(TimeSeriesSchema):
    p1: TS[float]
    p2: TS[float]
    p3: TS[float]


def test_tsb_convert_to_bool():
    assert eval_node(
        convert,
        [dict(p1=1.0)],
        TS[bool],
        resolution_dict=dict(ts=TSB[ATimeSeriesSchema])
    ) == [True]


def test_tsb_convert_to_tsd():
    assert eval_node(
        convert,
        [dict(p1=1.0)],
        TSD[str, TS[float]],
        resolution_dict=dict(ts=TSB[ATimeSeriesSchema])
    ) == [fd(p1=1.0)]


class AnotherTimeSeriesSchema(ATimeSeriesSchema):
    p4: TS[bool]


def test_tsb_convert_to_tsb_failure():
    with pytest.raises(WiringError):
        eval_node(
            convert,
            [dict(p1=1.0)],
            TSD[str, TS[float]],
            resolution_dict=dict(ts=TSB[AnotherTimeSeriesSchema])
        )


def test_tsb_convert_to_tsd_keys():

    @graph
    def convert_g(ts: TSB[AnotherTimeSeriesSchema]) -> TSD[str, TS[float]]:
        return convert(ts=ts, to=TSD[str, TS[float]], keys=("p1", "p2", "p3"))

    assert eval_node(
        convert_g,
        [dict(p1=1.0)]
    ) == [fd(p1=1.0)]
