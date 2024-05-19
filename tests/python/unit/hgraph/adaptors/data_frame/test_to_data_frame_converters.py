from datetime import datetime, date

from hgraph import compound_scalar, COMPOUND_SCALAR, MIN_ST, MIN_DT, MIN_TD
from hgraph.adaptors.data_frame._to_data_frame_converters import to_frame_ts
from hgraph.test import eval_node


def test_to_frame_ts_value():

    result = eval_node(to_frame_ts[COMPOUND_SCALAR: compound_scalar(value=int)], [1, 2, 3])
    assert len(result) == 3
    assert [r['value'][0] for r in result] == [1, 2, 3]


def test_to_frame_ts_dt_value():

    result = eval_node(to_frame_ts[COMPOUND_SCALAR: compound_scalar(dt=datetime, value=int)], [1, 2, 3])
    assert len(result) == 3
    assert [r['value'][0] for r in result] == [1, 2, 3]
    assert [r['dt'][0] for r in result] == [MIN_ST, MIN_ST+MIN_TD, MIN_ST+MIN_TD*2]


def test_to_frame_ts_date_value():
    result = eval_node(to_frame_ts[COMPOUND_SCALAR: compound_scalar(dt=date, value=int)], [1, 2, 3])
    assert len(result) == 3
    assert [r['value'][0] for r in result] == [1, 2, 3]
    assert [r['dt'][0] for r in result] == [MIN_ST.date(), MIN_ST.date(), MIN_ST.date()]
