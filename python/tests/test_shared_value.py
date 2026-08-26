from dataclasses import dataclass

import _hgraph
import pytest

from hgraph import CompoundScalar
from hgraph._types import _value_type


@dataclass(frozen=True)
class SharedQuote(CompoundScalar):
    instrument: str
    price: float


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


def test_shared_value_type_rejects_non_bundle_targets():
    with pytest.raises(ValueError, match="direct Bundle"):
        _hgraph.shared_vt(_hgraph.value_type("int"))
