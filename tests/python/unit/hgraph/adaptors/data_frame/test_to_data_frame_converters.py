from datetime import datetime, date

from frozendict import frozendict as fd

from hgraph import compound_scalar, COMPOUND_SCALAR, MIN_ST, MIN_TD, TSB, ts_schema, TS
from hgraph.adaptors.data_frame._to_data_frame_converters import to_frame_ts, to_frame_tsb
from hgraph.test import eval_node


def test_to_frame_ts_value():
    result = eval_node(to_frame_ts[COMPOUND_SCALAR: compound_scalar(value=int)], [1, 2, 3])
    assert len(result) == 3
    assert [r['value'][0] for r in result] == [1, 2, 3]


def test_to_frame_ts_value_resolver():
    result = eval_node(to_frame_ts, [1, 2, 3], value_col='value')
    assert len(result) == 3
    assert [r['value'][0] for r in result] == [1, 2, 3]


def test_to_frame_ts_dt_value():
    result = eval_node(to_frame_ts[COMPOUND_SCALAR: compound_scalar(dt=datetime, value=int)], [1, 2, 3])
    assert len(result) == 3
    assert [r['value'][0] for r in result] == [1, 2, 3]
    assert [r['dt'][0] for r in result] == [MIN_ST, MIN_ST + MIN_TD, MIN_ST + MIN_TD * 2]


def test_to_frame_ts_dt_value_resolver():
    result = eval_node(to_frame_ts, [1, 2, 3], value_col='value', dt_col='dt')
    assert len(result) == 3
    assert [r['value'][0] for r in result] == [1, 2, 3]
    assert [r['dt'][0] for r in result] == [MIN_ST, MIN_ST + MIN_TD, MIN_ST + MIN_TD * 2]


def test_to_frame_ts_date_value():
    result = eval_node(to_frame_ts[COMPOUND_SCALAR: compound_scalar(dt=date, value=int)], [1, 2, 3])
    assert len(result) == 3
    assert [r['value'][0] for r in result] == [1, 2, 3]
    assert [r['dt'][0] for r in result] == [MIN_ST.date(), MIN_ST.date(), MIN_ST.date()]


def test_to_frame_ts_date_value_resolver():
    result = eval_node(to_frame_ts, [1, 2, 3], value_col='value', dt_col='dt', dt_is_date=True)
    assert len(result) == 3
    assert [r['value'][0] for r in result] == [1, 2, 3]
    assert [r['dt'][0] for r in result] == [MIN_ST.date(), MIN_ST.date(), MIN_ST.date()]


def test_to_frame_tsb():
    ts_schema_ = ts_schema(p1=TS[int], p2=TS[str])
    schema = compound_scalar(p1=int, p2=str)
    result = eval_node(to_frame_tsb[COMPOUND_SCALAR: schema], [fd(p1=1, p2="a"), fd(p1=2, p2="b"), fd(p1=3, p2="c")],
                       resolution_dict={"ts": TSB[ts_schema_]})
    assert len(result) == 3
    assert [r['p1'][0] for r in result] == [1, 2, 3]
    assert [r['p2'][0] for r in result] == ['a', 'b', 'c']


def test_to_frame_tsb_dt():
    ts_schema_ = ts_schema(p1=TS[int], p2=TS[str])
    schema = compound_scalar(dt=datetime, p1=int, p2=str)
    result = eval_node(to_frame_tsb[COMPOUND_SCALAR: schema], [fd(p1=1, p2="a"), fd(p1=2, p2="b"), fd(p1=3, p2="c")],
                       resolution_dict={"ts": TSB[ts_schema_]})
    assert len(result) == 3
    assert [r['dt'][0] for r in result] == [MIN_ST, MIN_ST + MIN_TD, MIN_ST + MIN_TD * 2]
    assert [r['p1'][0] for r in result] == [1, 2, 3]
    assert [r['p2'][0] for r in result] == ['a', 'b', 'c']


def test_to_frame_tsb_date():
    ts_schema_ = ts_schema(p1=TS[int], p2=TS[str])
    schema = compound_scalar(dt=date, p1=int, p2=str)
    result = eval_node(to_frame_tsb[COMPOUND_SCALAR: schema], [fd(p1=1, p2="a"), fd(p1=2, p2="b"), fd(p1=3, p2="c")],
                       resolution_dict={"ts": TSB[ts_schema_]})
    assert len(result) == 3
    assert [r['dt'][0] for r in result] == [MIN_ST.date(), MIN_ST.date(), MIN_ST.date()]
    assert [r['p1'][0] for r in result] == [1, 2, 3]
    assert [r['p2'][0] for r in result] == ['a', 'b', 'c']


def test_to_frame_tsb_guess_schema():
    ts_schema_ = ts_schema(p1=TS[int], p2=TS[str])
    result = eval_node(to_frame_tsb, [fd(p1=1, p2="a"), fd(p1=2, p2="b"), fd(p1=3, p2="c")], dt_col='dt',
                       dt_is_date=True, resolution_dict={"ts": TSB[ts_schema_]})
    assert len(result) == 3
    assert [r['dt'][0] for r in result] == [MIN_ST.date(), MIN_ST.date(), MIN_ST.date()]
    assert [r['p1'][0] for r in result] == [1, 2, 3]
    assert [r['p2'][0] for r in result] == ['a', 'b', 'c']


def test_to_frame_tsb_guess_schema_with_mapping():
    ts_schema_ = ts_schema(p1_=TS[int], p2=TS[str])
    result = eval_node(to_frame_tsb, [fd(p1_=1, p2="a"), fd(p1_=2, p2="b"), fd(p1_=3, p2="c")], dt_col='dt',
                       map_=fd(p1_="p1"), dt_is_date=True, resolution_dict={"ts": TSB[ts_schema_]})
    assert len(result) == 3
    assert [r['dt'][0] for r in result] == [MIN_ST.date(), MIN_ST.date(), MIN_ST.date()]
    assert [r['p1'][0] for r in result] == [1, 2, 3]
    assert [r['p2'][0] for r in result] == ['a', 'b', 'c']


def test_to_frame_tsb_guess_schema_with_mapping_date():
    ts_schema_ = ts_schema(p1_=TS[int], p2=TS[str])
    result = eval_node(to_frame_tsb, [fd(p1_=1, p2="a"), fd(p1_=2, p2="b"), fd(p1_=3, p2="c")], dt_col='dt',
                       map_=fd(p1_="p1"), resolution_dict={"ts": TSB[ts_schema_]})
    assert len(result) == 3
    assert [r['dt'][0] for r in result] == [MIN_ST, MIN_ST + MIN_TD, MIN_ST + MIN_TD * 2]
    assert [r['p1'][0] for r in result] == [1, 2, 3]
    assert [r['p2'][0] for r in result] == ['a', 'b', 'c']
