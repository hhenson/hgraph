# Public regression from hgraph 275215c5. The C++ runtime represents the same
# shape directly; no private Python builder behavior is part of this test.
import hgraph as hg
from hgraph.test import eval_node


def test_map_invalid_reference_bundle_field_output():
    class AB(hg.TimeSeriesSchema):
        a: hg.TS[int]

    @hg.graph
    def child(value: hg.TS[int], condition: hg.TS[bool]) -> hg.TSB[AB]:
        return hg.TSB[AB].from_ts(a=hg.if_(condition, value).true)

    @hg.graph
    def app(
        values: hg.TSD[str, hg.TS[int]], condition: hg.TS[bool],
    ) -> hg.TSD[str, hg.TSB[AB]]:
        return hg.map_(child, values, condition)

    assert eval_node(app, [{"x": 1}], [False]) == [{}]


def test_map_preserves_typed_keyed_bundle_reference_and_materialization():
    class Row(hg.TimeSeriesSchema):
        value: hg.TS[int]
        label: hg.TS[str]

    class Projection(hg.TimeSeriesSchema):
        value: hg.TS[int]
        label: hg.TS[str]

    @hg.graph
    def keyed_lookup(
        lookup: hg.TS[str], rows: hg.TSD[str, hg.TSB[Row]],
    ) -> hg.TSB[Row]:
        # A dynamic lookup is physically REF[TSB[Row]], even though the typed
        # graph presents its dereferenced TSB contract to callers.
        return rows[lookup]

    @hg.graph
    def materialize_lookup(
        lookup: hg.TS[str], rows: hg.TSD[str, hg.TSB[Row]],
    ) -> hg.TSB[Projection]:
        selected = rows[lookup]
        return hg.combine[hg.TSB[Projection]](
            value=selected.value, label=selected.label,
        )

    @hg.graph
    def reference_app(
        lookups: hg.TSD[str, hg.TS[str]],
        rows: hg.TSD[str, hg.TSB[Row]],
    ) -> hg.TSD[str, hg.REF[hg.TSB[Row]]]:
        return hg.map_(keyed_lookup, lookups, hg.pass_through(rows))

    @hg.graph
    def materialized_app(
        lookups: hg.TSD[str, hg.TS[str]],
        rows: hg.TSD[str, hg.TSB[Row]],
    ) -> hg.TSD[str, hg.TSB[Projection]]:
        return hg.map_(materialize_lookup, lookups, hg.pass_through(rows))

    lookups = [{"left": "a", "right": "b"}]
    rows = [{
        "a": {"value": 1, "label": "one"},
        "b": {"value": 2, "label": "two"},
    }]
    expected = [{
        "left": {"value": 1, "label": "one"},
        "right": {"value": 2, "label": "two"},
    }]

    assert eval_node(reference_app, lookups, rows) == expected
    assert eval_node(materialized_app, lookups, rows) == expected
