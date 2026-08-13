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


def test_map_removed_typed_bundle_reference_does_not_retick_with_sibling():
    class Row(hg.TimeSeriesSchema):
        value: hg.TS[int]
        label: hg.TS[str]

    @hg.graph
    def keyed_lookup(
        lookup: hg.TS[str], rows: hg.TSD[str, hg.TSB[Row]],
    ) -> hg.TSB[Row]:
        return rows[lookup]

    @hg.graph
    def app(
        lookups: hg.TSD[str, hg.TS[str]],
        rows: hg.TSD[str, hg.TSB[Row]],
    ) -> hg.TSD[str, hg.REF[hg.TSB[Row]]]:
        return hg.map_(keyed_lookup, lookups, hg.pass_through(rows))

    assert eval_node(
        app,
        [
            {"extra": "b", "right": "b"},
            {"extra": hg.REMOVE},
            None,
        ],
        [
            {"b": {"value": 2, "label": "beta"}},
            None,
            {"b": {"value": 3, "label": "updated"}},
        ],
    ) == [
        {
            "extra": {"value": 2, "label": "beta"},
            "right": {"value": 2, "label": "beta"},
        },
        {"extra": hg.REMOVE},
        {"right": {"value": 3, "label": "updated"}},
    ]


def test_map_generic_bundle_child_can_dereference_its_element():
    class Params(hg.TimeSeriesSchema):
        value: hg.TS[int]

    @hg.graph
    def materialize(params: hg.TSB[hg.TS_SCHEMA]) -> hg.TSB[hg.TS_SCHEMA]:
        return hg.dereference(params)

    @hg.graph
    def app(
        params: hg.TSD[str, hg.TSB[Params]],
    ) -> hg.TSD[str, hg.TSB[Params]]:
        return hg.map_(materialize, params=params)

    assert eval_node(
        app,
        [
            {"left": {"value": 1}},
            {"left": {"value": 2}},
            {"right": {"value": 3}},
            {"left": hg.REMOVE},
        ],
    ) == [
        {"left": {"value": 1}},
        {"left": {"value": 2}},
        {"right": {"value": 3}},
        {"left": hg.REMOVE},
    ]


def test_map_generic_bundle_parameter_accepts_keyword_mapping():
    @hg.graph
    def handler(
        value: hg.TS[int], flag: hg.TS[bool], limit: hg.TS[int],
    ) -> hg.TS[int]:
        return hg.if_then_else(flag, value + limit, value - limit)

    @hg.graph
    def child(
        value: hg.TS[int], params: hg.TSB[hg.TS_SCHEMA],
    ) -> hg.TS[int]:
        resolved = hg.dereference(params)
        return handler(value=value, **resolved)

    @hg.graph
    def app(
        values: hg.TSD[str, hg.TS[int]],
        flag: hg.TS[bool],
        limit: hg.TS[int],
    ) -> hg.TSD[str, hg.TS[int]]:
        return hg.map_(
            child,
            value=values,
            params={"flag": flag, "limit": limit},
            __keys__=values.key_set,
        )

    assert eval_node(
        app,
        [{"a": 2, "b": 3}, None],
        [True, False],
        [10, 5],
    ) == [
        {"a": 12, "b": 13},
        {"a": -3, "b": -2},
    ]


def test_map_passive_structural_bundle_materializes_before_child_binding():
    class Params(hg.TimeSeriesSchema):
        flag: hg.TS[bool]
        limit: hg.TS[int]

    @hg.graph
    def child(
        value: hg.TS[int], params: hg.TSB[hg.TS_SCHEMA],
    ) -> hg.TS[int]:
        resolved = hg.dereference(params)
        return hg.if_then_else(
            resolved.flag, value + resolved.limit, value - resolved.limit,
        )

    @hg.graph
    def app(
        values: hg.TSD[str, hg.TS[int]],
        flag: hg.TS[bool],
        limit: hg.TS[int],
    ) -> hg.TSD[str, hg.TS[int]]:
        params = hg.passive(hg.TSB[Params].from_ts(flag=flag, limit=limit))
        return hg.map_(
            child, value=values, params=params, __keys__=values.key_set,
        )

    assert eval_node(
        app,
        [{"a": 2, "b": 3}, {"a": 4, "b": 5}],
        [True, False],
        [10, 5],
    ) == [
        {"a": 12, "b": 13},
        {"a": -1, "b": 0},
    ]


def test_key_only_map_combines_captured_reference_fields():
    @hg.graph
    def app(
        statuses: hg.TSD[str, hg.TS[bool]],
        rejects: hg.TSD[str, hg.TS[bool]],
    ) -> hg.TSD[
        str,
        hg.TSB["status": hg.TS[bool], "rejected": hg.TS[bool]],
    ]:
        return hg.map_(
            lambda key: hg.combine(
                status=statuses[key], rejected=rejects[key],
            ),
            __keys__=statuses.key_set,
        )

    assert eval_node(
        app,
        [
            {"left": True},
            {"right": False},
            {"left": False},
            {"left": hg.REMOVE},
        ],
        [
            {"left": False},
            {"right": True},
            None,
            None,
        ],
    ) == [
        {"left": {"status": True, "rejected": False}},
        {"right": {"status": False, "rejected": True}},
        {"left": {"status": False}},
        {"left": hg.REMOVE},
    ]
