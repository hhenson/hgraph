"""RFC 0036: the bridge exposes the owners of REF transparency.

``value_element_ts`` mirrors ``TypeRegistry::value_element_ts``; ``value_port``
is the port as a value consumer observes it (the top-level reference followed,
the structural descent below). The wiring machinery calls these instead of
spelling ``is_ref`` / ``dereferenced`` (RFC 0036, PR 4).
"""

import _hgraph as hg
import pytest


def _ts(text: str):
    return hg.ts_type(text)


def test_value_element_ts_follows_every_reference():
    assert hg.value_element_ts(_ts("TSD[str, REF[TS[int]]]")) == _ts("TS[int]")
    assert hg.value_element_ts(_ts("TSD[str, TS[int]]")) == _ts("TS[int]")
    assert hg.value_element_ts(_ts("TSL[REF[TS[int]], Size[2]]")) == _ts("TS[int]")
    assert hg.value_element_ts(_ts("REF[TSD[str, REF[TS[int]]]]")) == _ts("TS[int]")
    assert hg.value_element_ts(_ts("TSD[str, REF[TSD[str, REF[TS[int]]]]]")) == _ts("TSD[str, TS[int]]")


def test_value_element_ts_refuses_a_schema_without_an_element():
    with pytest.raises(ValueError):
        hg.value_element_ts(_ts("TS[int]"))


def test_value_port_observes_the_referenced_value():
    w = hg.Wiring()
    src = w.wire("const", (1,), {}, output_type=_ts("TS[int]"))
    ref = hg.ref_port(w, src)
    assert ref.ts_type.is_ref

    observed = hg.value_port(w, ref)
    assert observed.ts_type == _ts("TS[int]")
    assert not observed.ts_type.is_ref
    # A plain port is observed as it is.
    assert hg.value_port(w, src).ts_type == _ts("TS[int]")
