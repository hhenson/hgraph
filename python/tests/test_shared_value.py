from dataclasses import dataclass

import _hgraph
import hgraph as hg
import pytest

from hgraph import CompoundScalar, Shared, TS
from hgraph._types import _time_series_full_value_type, _value_type


@dataclass(frozen=True)
class SharedQuote(CompoundScalar):
    instrument: str
    price: float


@dataclass(frozen=True)
class SharedTag(CompoundScalar):
    pass


def test_shared_value_type_is_transparent_to_python_conversion_and_json():
    target = _value_type(SharedQuote)
    shared = _hgraph.shared_vt(target)

    assert shared.is_shared
    assert shared.name == f"Shared[{target.name}]"
    assert _hgraph.vt_element(shared) == target
    assert shared.fields == target.fields

    value = SharedQuote(instrument="EURUSD", price=1.25)
    encoded = _hgraph.value_to_json(shared, value)
    assert encoded == '{"instrument": "EURUSD", "price": 1.25}'
    assert _hgraph.value_from_json(shared, encoded) == value


def test_shared_time_series_annotation_retains_storage_and_exposes_concrete_python_type():
    shared_ts = TS[Shared[SharedQuote]]

    assert "Shared" in hg.__all__
    assert _hgraph.ts_value_vt(shared_ts.handle).is_shared
    assert _hgraph.vt_element(_hgraph.ts_value_vt(shared_ts.handle)) == _value_type(SharedQuote)
    assert _time_series_full_value_type(shared_ts) is SharedQuote


def test_shared_value_type_supports_empty_bundle_payloads():
    target = _value_type(SharedTag)
    shared = _hgraph.shared_vt(target)

    target_encoded = _hgraph.value_to_json(target, SharedTag())
    encoded = _hgraph.value_to_json(shared, SharedTag())
    assert encoded == target_encoded
    assert _hgraph.value_from_json(shared, encoded) == SharedTag()


def test_shared_value_type_rejects_non_bundle_targets():
    int_type = _hgraph.value_type("int")
    with pytest.raises(ValueError, match="direct Bundle"):
        _hgraph.shared_vt(int_type)
