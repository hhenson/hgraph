"""Pin issue #80: the polars_frames compatibility switch.

When enabled (feature flag ``polars_frames`` — ``HGRAPH_POLARS_FRAMES=true``
or the hgraph_features config file), outbound Frame/Series values surface as
``polars.DataFrame``/``polars.Series`` instead of ``pyarrow.Table``/``Array``.
Inbound polars is accepted regardless of the switch (anything exposing
``__arrow_c_stream__`` converts on ingest). Arrow remains the canonical
runtime substrate; the switch is a Python-boundary veneer only.
"""

import sys
from dataclasses import dataclass

import pyarrow as pa
import pytest

import _hgraph
from hgraph import CompoundScalar, Frame, Series, TS, pass_through
from hgraph.test import eval_node

polars = pytest.importorskip("polars")


@dataclass(frozen=True)
class PriceRow(CompoundScalar):
    instrument: str
    value: float


@pytest.fixture
def polars_frames():
    _hgraph.set_polars_frames(True)
    try:
        yield
    finally:
        _hgraph.set_polars_frames(False)


def _price_table():
    return pa.table({"instrument": ["A", "B"], "value": [101.5, 7.25]})


def test_switch_defaults_off_and_frames_stay_pyarrow():
    assert _hgraph.polars_frames() is False
    result = eval_node(
        pass_through,
        [_price_table()],
        resolution_dict={"ts": TS[Frame[PriceRow]]},
    )[0]
    assert isinstance(result, pa.Table)


def test_frames_surface_as_polars_dataframes(polars_frames):
    result = eval_node(
        pass_through,
        [_price_table()],
        resolution_dict={"ts": TS[Frame[PriceRow]]},
    )[0]
    assert isinstance(result, polars.DataFrame)
    assert result.equals(polars.DataFrame(
        {"instrument": ["A", "B"], "value": [101.5, 7.25]}))


def test_polars_dataframes_accepted_inbound_and_round_trip(polars_frames):
    # Inbound polars always converts (arrow C stream); with the switch on
    # the same object shape comes back out.
    frame = polars.DataFrame({"instrument": ["A"], "value": [1.5]})
    result = eval_node(
        pass_through,
        [frame],
        resolution_dict={"ts": TS[Frame[PriceRow]]},
    )[0]
    assert isinstance(result, polars.DataFrame)
    assert result.equals(frame)


def test_series_surface_as_polars_series(polars_frames):
    result = eval_node(
        pass_through,
        [pa.array([1.0, 2.5])],
        resolution_dict={"ts": TS[Series[float]]},
    )[0]
    assert isinstance(result, polars.Series)
    assert result.to_list() == [1.0, 2.5]


def test_enabled_without_polars_raises_clearly(polars_frames):
    saved = {name: module for name, module in sys.modules.items()
             if name == "polars" or name.startswith("polars.")}
    for name in saved:
        sys.modules[name] = None
    try:
        with pytest.raises(Exception, match="polars is not installed"):
            eval_node(
                pass_through,
                [_price_table()],
                resolution_dict={"ts": TS[Frame[PriceRow]]},
            )
    finally:
        sys.modules.update(saved)
