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


def test_value_ts_and_contains_ref_follow_the_declaration():
    assert hg.value_ts(REF_INT) == TS_INT
    assert hg.value_ts(TS_INT) == TS_INT
    assert hg.value_ts(hg.tsd(STR, REF_INT)) == hg.tsd(STR, TS_INT)
    assert hg.contains_ref(REF_INT)
    assert hg.contains_ref(hg.tsd(STR, REF_INT))
    assert not hg.contains_ref(hg.tsd(STR, TS_INT))
    assert not hg.contains_ref(TS_INT)


def test_value_port_keeps_a_reference_the_declaration_asks_for():
    w = hg.Wiring()
    src = w.wire("const", (1,), {}, output_type=TS_INT)
    bundle_type = hg.un_named_tsb_type([("a", TS_INT)])
    ref = hg.ref_port(w, hg.tsb_port(bundle_type, {"a": src}))
    assert ref.ts_type.is_ref

    # Declared REF: the reference as supplied. Declared value: observed.
    assert hg.value_port(w, ref, hg.ref_ts(bundle_type)).ts_type.is_ref
    assert hg.value_port(w, ref, bundle_type).ts_type == bundle_type
    assert hg.value_port(w, ref, None).ts_type == bundle_type


def test_value_port_observes_a_bundle_of_references_child_by_child():
    # if_ publishes TSB[BoolResult] whose fields are references; observed
    # without a declaration, it is a structural bundle of per-field value
    # projections, so a consumer sees each field's own ticks.
    w = hg.Wiring()
    condition = w.wire("const", (True,), {}, output_type=hg.ts(hg.value_type("bool")))
    ts = w.wire("const", (1,), {}, output_type=TS_INT)
    routed = w.wire("if_", (condition, ts), {})
    assert routed.ts_type.is_tsb and hg.tsb_has_ref_fields(routed.ts_type)
    assert not routed.is_structural

    observed = hg.value_port(w, routed)
    assert observed.is_structural
    assert observed.ts_type == hg.value_ts(routed.ts_type)
    assert not hg.tsb_has_ref_fields(observed.ts_type)
