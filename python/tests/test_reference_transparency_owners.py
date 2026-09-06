"""RFC 0036: the bridge exposes the owners of REF transparency.

``value_element_ts`` mirrors ``TypeRegistry::value_element_ts``; ``value_port``
is the port as a value consumer observes it (the top-level reference followed,
the structural descent below). The wiring machinery calls these instead of
spelling ``is_ref`` / ``dereferenced`` (RFC 0036, PR 4).
"""

import _hgraph as hg
import pytest

INT = hg.value_type("int")
STR = hg.value_type("str")
TS_INT = hg.ts(INT)
REF_INT = hg.ref_ts(TS_INT)


def test_value_element_ts_follows_every_reference():
    assert hg.value_element_ts(hg.tsd(STR, REF_INT)) == TS_INT
    assert hg.value_element_ts(hg.tsd(STR, TS_INT)) == TS_INT
    assert hg.value_element_ts(hg.tsl(REF_INT, 2)) == TS_INT
    assert hg.value_element_ts(hg.ref_ts(hg.tsd(STR, REF_INT))) == TS_INT
    nested = hg.tsd(STR, hg.ref_ts(hg.tsd(STR, REF_INT)))
    assert hg.value_element_ts(nested) == hg.tsd(STR, TS_INT)


def test_value_element_ts_refuses_a_schema_without_an_element():
    with pytest.raises(ValueError):
        hg.value_element_ts(TS_INT)


def test_value_port_observes_the_referenced_value():
    w = hg.Wiring()
    src = w.wire("const", (1,), {}, output_type=TS_INT)
    bundle_type = hg.un_named_tsb_type([("a", TS_INT)])
    bundle = hg.tsb_port(bundle_type, {"a": src})
    # A structural bundle materialized as a reference: REF[TSB[a: TS[int]]].
    ref = hg.ref_port(w, bundle)
    assert ref.ts_type.is_ref

    observed = hg.value_port(w, ref)
    assert observed.ts_type == bundle_type
    assert not observed.ts_type.is_ref
    # A plain port is observed as it is.
    assert hg.value_port(w, src).ts_type == TS_INT
