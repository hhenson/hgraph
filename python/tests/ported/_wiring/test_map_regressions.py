# Public regression from hgraph 275215c5. The C++ runtime represents the same
# shape directly; no private Python builder behavior is part of this test.
from dataclasses import dataclass

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


def test_map_bundle_output_exposes_aggregate_tsd_value_to_python_nodes():
    class Status(hg.TimeSeriesSchema):
        value: hg.TS[int]
        ok: hg.TS[bool]

    aggregate_values = []
    aggregate_deltas = []
    bound_output_values = []
    bound_output_deltas = []
    individual_values = []

    @hg.graph
    def make_status(value: hg.TS[int]) -> hg.TSB[Status]:
        return hg.combine[hg.TSB[Status]](value=value, ok=value > 0)

    @hg.sink_node
    def observe(values: hg.TSD[str, hg.TSB[Status]]):
        individual_values.append(values["item"].value)
        aggregate_values.append(values.value)
        aggregate_deltas.append(values.delta_value)
        bound_output_values.append(values.output.value)
        bound_output_deltas.append(values.output.delta_value)

    @hg.graph
    def app(
        values: hg.TSD[str, hg.TS[int]],
    ) -> hg.TSD[str, hg.TSB[Status]]:
        statuses = hg.map_(make_status, values)
        observe(statuses)
        return statuses

    expected = {"item": {"value": 1, "ok": True}}
    assert eval_node(app, [{"item": 1}]) == [expected]
    assert individual_values == [expected["item"]]
    assert aggregate_values == [expected]
    assert aggregate_deltas == [expected]
    assert bound_output_values == [expected]
    assert bound_output_deltas == [expected]


def test_map_bundle_output_exposes_aggregate_fixed_tsl_value_and_delta():
    class Status(hg.TimeSeriesSchema):
        value: hg.TS[int]
        ok: hg.TS[bool]

    aggregate_values = []
    aggregate_deltas = []

    @hg.graph
    def make_status(value: hg.TS[int]) -> hg.TSB[Status]:
        return hg.combine[hg.TSB[Status]](value=value, ok=value > 0)

    @hg.sink_node
    def observe(values: hg.TSL[hg.TSB[Status], hg.Size[2]]):
        aggregate_values.append(values.value)
        aggregate_deltas.append(values.delta_value)

    @hg.graph
    def app(
        values: hg.TSL[hg.TS[int], hg.Size[2]],
    ) -> hg.TSL[hg.TSB[Status], hg.Size[2]]:
        statuses = hg.map_(make_status, values)
        observe(statuses)
        return statuses

    first = {"value": 1, "ok": True}
    second = {"value": 2, "ok": True}
    assert eval_node(app, [(1, 2)]) == [{0: first, 1: second}]
    assert aggregate_values == [(first, second)]
    assert aggregate_deltas == [{0: first, 1: second}]


def test_map_bundle_output_exposes_dynamic_tsl_bound_output_aggregates():
    class Status(hg.TimeSeriesSchema):
        value: hg.TS[int]
        ok: hg.TS[bool]

    aggregate_values = []
    aggregate_deltas = []
    bound_output_values = []
    bound_output_deltas = []

    @hg.graph
    def make_status(value: hg.TS[int]) -> hg.TSB[Status]:
        return hg.combine[hg.TSB[Status]](value=value, ok=value > 0)

    @hg.sink_node
    def observe(values: hg.TSL[hg.TSB[Status], hg.Size[0]]):
        aggregate_values.append(values.value)
        aggregate_deltas.append(values.delta_value)
        bound_output_values.append(values.output.value)
        bound_output_deltas.append(values.output.delta_value)

    @hg.graph
    def app(
        values: hg.TSL[hg.TS[int], hg.Size[0]],
    ) -> hg.TSL[hg.TSB[Status], hg.Size[0]]:
        statuses = hg.map_(make_status, values)
        observe(statuses)
        return statuses

    status = {"value": 1, "ok": True}
    expected_delta = {0: status}
    assert eval_node(app, [{0: 1}]) == [expected_delta]
    assert aggregate_values == [(status,)]
    assert aggregate_deltas == [expected_delta]
    assert bound_output_values == [(status,)]
    assert bound_output_deltas == [expected_delta]


def test_type_operator_supports_projected_structural_values():
    class Status(hg.TimeSeriesSchema):
        value: hg.TS[int]
        ok: hg.TS[bool]

    @hg.graph
    def make_status(value: hg.TS[int]) -> hg.TSB[Status]:
        return hg.combine[hg.TSB[Status]](value=value, ok=value > 0)

    @hg.graph
    def bundle_type(value: hg.TS[int]) -> hg.TS[type]:
        return hg.type_(make_status(value))

    @hg.graph
    def dict_type(values: hg.TSD[str, hg.TS[int]]) -> hg.TS[type]:
        return hg.type_(hg.map_(make_status, values))

    @hg.graph
    def list_type(
        values: hg.TSL[hg.TS[int], hg.Size[2]],
    ) -> hg.TS[type]:
        return hg.type_(hg.map_(make_status, values))

    assert eval_node(bundle_type, [1]) == [dict]
    assert eval_node(dict_type, [{"item": 1}]) == [dict]
    assert eval_node(list_type, [(1, 2)]) == [tuple]


def test_map_compound_scalar_bundle_preserves_aggregate_python_values():
    @dataclass(frozen=True)
    class Status(hg.CompoundScalar):
        value: int
        ok: bool

    aggregate_values = []
    bound_output_values = []

    @hg.graph
    def make_status(value: hg.TS[int]) -> hg.TSB[Status]:
        return hg.combine[hg.TSB[Status]](value=value, ok=value > 0)

    @hg.sink_node
    def observe(values: hg.TSD[str, hg.TSB[Status]]):
        aggregate_values.append(values.value)
        bound_output_values.append(values.output.value)

    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TSB[Status]]:
        statuses = hg.map_(make_status, values)
        observe(statuses)
        return statuses

    expected = {"item": Status(value=1, ok=True)}
    assert eval_node(app, [{"item": 1}]) == [{"item": {"value": 1, "ok": True}}]
    assert aggregate_values == [expected]
    assert bound_output_values == [expected]


def test_map_nested_fixed_tsl_bundle_preserves_aggregate_python_values():
    class Status(hg.TimeSeriesSchema):
        value: hg.TS[int]

    aggregate_values = []
    aggregate_deltas = []

    @hg.graph
    def make_status(value: hg.TS[int]) -> hg.TSB[Status]:
        return hg.combine[hg.TSB[Status]](value=value)

    @hg.graph
    def make_pair(value: hg.TS[int]) -> hg.TSL[hg.TSB[Status], hg.Size[2]]:
        return hg.combine[hg.TSL[hg.TSB[Status], hg.Size[2]]](
            make_status(value), make_status(value + 1)
        )

    @hg.sink_node
    def observe(values: hg.TSD[str, hg.TSL[hg.TSB[Status], hg.Size[2]]]):
        aggregate_values.append(values.value)
        aggregate_deltas.append(values.delta_value)

    @hg.graph
    def app(
        values: hg.TSD[str, hg.TS[int]],
    ) -> hg.TSD[str, hg.TSL[hg.TSB[Status], hg.Size[2]]]:
        pairs = hg.map_(make_pair, values)
        observe(pairs)
        return pairs

    pair = ({"value": 1}, {"value": 2})
    assert eval_node(app, [{"item": 1}]) == [{"item": {0: pair[0], 1: pair[1]}}]
    assert aggregate_values == [{"item": pair}]
    assert aggregate_deltas == [{"item": {0: pair[0], 1: pair[1]}}]


def test_proxy_lag_of_mapped_bundle_uses_erased_aggregate_conversion():
    class Status(hg.TimeSeriesSchema):
        value: hg.TS[int]
        ok: hg.TS[bool]

    aggregate_values = []
    aggregate_deltas = []
    bound_output_values = []
    bound_output_deltas = []

    @hg.graph
    def make_status(value: hg.TS[int]) -> hg.TSB[Status]:
        return hg.combine[hg.TSB[Status]](value=value, ok=value > 0)

    @hg.sink_node
    def observe(values: hg.TSD[str, hg.TSB[Status]]):
        aggregate_values.append(values.value)
        aggregate_deltas.append(values.delta_value)
        bound_output_values.append(values.output.value)
        bound_output_deltas.append(values.output.delta_value)

    @hg.graph
    def app(
        values: hg.TSD[str, hg.TS[int]], proxy: hg.TS[bool]
    ) -> hg.TSD[str, hg.TSB[Status]]:
        delayed = hg.lag(hg.map_(make_status, values), 1, proxy)
        observe(delayed)
        return delayed

    first = {"value": 1, "ok": True}
    second = {"value": 2, "ok": True}
    assert eval_node(
        app, [{"item": 1}, {"item": 2}], [True, True, True]
    ) == [{}, {"item": first}, {"item": second}]
    expected = [{}, {"item": first}, {"item": second}]
    assert aggregate_values == expected
    assert aggregate_deltas == expected
    assert bound_output_values == expected
    assert bound_output_deltas == expected


def test_bundle_with_nested_tsd_and_tsl_uses_child_conversion_strategies():
    class Nested(hg.TimeSeriesSchema):
        keyed: hg.TSD[str, hg.TS[int]]
        listed: hg.TSL[hg.TS[int], hg.Size[2]]

    aggregate_values = []
    aggregate_deltas = []
    bound_output_values = []
    bound_output_deltas = []

    @hg.sink_node
    def observe(values: hg.TSB[Nested]):
        aggregate_values.append(values.value)
        aggregate_deltas.append(values.delta_value)
        bound_output_values.append(values.output.value)
        bound_output_deltas.append(values.output.delta_value)

    @hg.graph
    def app(values: hg.TSB[Nested]) -> hg.TSB[Nested]:
        observe(values)
        return values

    sample = {"keyed": {"item": 1}, "listed": (2, 3)}
    assert eval_node(app, [sample]) == [
        {"keyed": {"item": 1}, "listed": {0: 2, 1: 3}}
    ]
    expected_value = {"keyed": {"item": 1}, "listed": (2, 3)}
    expected_delta = {"keyed": {"item": 1}, "listed": {0: 2, 1: 3}}
    assert aggregate_values == [expected_value]
    assert aggregate_deltas == [expected_delta]
    assert bound_output_values == [expected_value]
    assert bound_output_deltas == [expected_delta]


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
