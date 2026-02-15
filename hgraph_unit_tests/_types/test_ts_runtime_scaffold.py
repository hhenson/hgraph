from datetime import timedelta

from hgraph import MIN_DT

import pytest

_hgraph = pytest.importorskip("hgraph._hgraph", reason="C++ extension not available")

if not hasattr(_hgraph, "_ts_runtime"):
    pytest.skip("TS runtime scaffolding bindings not exposed", allow_module_level=True)

runtime = _hgraph._ts_runtime
value = _hgraph.value


def _registry():
    return _hgraph.TSTypeRegistry.instance()


def _ts_int_meta():
    return _registry().ts(value.scalar_type_meta_int64())


def _tsb_meta(name: str, fields):
    return _registry().tsb(fields, name)


def _tsd_meta(key_type, value_ts):
    return _registry().tsd(key_type, value_ts)


def _int_value(v: int):
    return value.Value(value.scalar_type_meta_int64(), v)


def _str_value(v: str):
    return value.Value(value.scalar_type_meta_string(), v)


@pytest.mark.parametrize(
    "meta_factory",
    [
        lambda: _ts_int_meta(),
        lambda: _registry().tss(value.scalar_type_meta_int64()),
        lambda: _tsd_meta(value.scalar_type_meta_string(), _ts_int_meta()),
        lambda: _registry().tsl(_ts_int_meta(), 0),
        lambda: _registry().tsw(value.scalar_type_meta_double(), 8, 3),
        lambda: _registry().tsw_duration(
            value.scalar_type_meta_double(),
            timedelta(minutes=5),
            timedelta(minutes=1),
        ),
        lambda: _registry().ref(_ts_int_meta()),
        lambda: _registry().signal(),
        lambda: _tsb_meta("RootBindBundle", [("a", _ts_int_meta()), ("b", _ts_int_meta())]),
    ],
    ids=[
        "TS",
        "TSS",
        "TSD",
        "TSL_dynamic",
        "TSW_tick",
        "TSW_duration",
        "REF",
        "SIGNAL",
        "TSB",
    ],
)
def test_root_bind_unbind_roundtrip_for_root_link_kinds(meta_factory):
    meta = meta_factory()

    ts_input = runtime.TSInput(meta)
    ts_output = runtime.TSOutput(meta, 0)
    root = ts_input.input_view(MIN_DT)

    assert not root.is_bound()
    ts_input.bind(ts_output, MIN_DT)
    assert root.is_bound()
    ts_input.unbind(MIN_DT)
    assert not root.is_bound()


def test_input_tsb_switches_between_peered_and_unpeered_bind():
    ts_int = _ts_int_meta()
    pair_meta = _tsb_meta("PairBindMode", [("x", ts_int), ("y", ts_int)])

    ts_input = runtime.TSInput(pair_meta)
    ts_output = runtime.TSOutput(pair_meta, 0)

    in_root = ts_input.input_view(MIN_DT)
    in_x = in_root.child_by_name("x")
    out_root = ts_output.output_view(MIN_DT)
    out_x = out_root.child_by_name("x")

    ts_input.bind(ts_output, MIN_DT)
    assert in_root.is_bound()
    assert not in_x.is_bound()
    assert in_root.linked_target_indices() == []
    assert in_x.linked_target_indices() == []

    in_x.bind(out_x)
    assert not in_root.is_bound()
    assert in_x.is_bound()
    assert in_x.linked_target_indices() == [0]

    in_x.unbind()
    assert not in_x.is_bound()
    assert not in_root.is_bound()


def test_tsl_fixed_bind_unbind_binds_each_index():
    ts_int = _ts_int_meta()
    fixed_meta = _registry().tsl(ts_int, 3)

    ts_input = runtime.TSInput(fixed_meta)
    ts_output = runtime.TSOutput(fixed_meta, 0)
    root = ts_input.input_view(MIN_DT)

    ts_input.bind(ts_output, MIN_DT)
    assert not root.is_bound()
    assert root.child_at(0).is_bound()
    assert root.child_at(1).is_bound()
    assert root.child_at(2).is_bound()
    assert root.child_at(0).linked_target_indices() == [0]
    assert root.child_at(1).linked_target_indices() == [1]
    assert root.child_at(2).linked_target_indices() == [2]

    ts_input.unbind(MIN_DT)
    assert not root.is_bound()
    assert not root.child_at(0).is_bound()
    assert not root.child_at(1).is_bound()
    assert not root.child_at(2).is_bound()


@pytest.mark.parametrize(
    "meta_factory",
    [
        lambda: _registry().tsl(_ts_int_meta(), 0),
        lambda: _tsd_meta(value.scalar_type_meta_string(), _ts_int_meta()),
    ],
    ids=["TSL_dynamic", "TSD"],
)
def test_collection_level_links_for_dynamic_structures(meta_factory):
    meta = meta_factory()
    ts_input = runtime.TSInput(meta)
    ts_output = runtime.TSOutput(meta, 0)
    root = ts_input.input_view(MIN_DT)

    ts_input.bind(ts_output, MIN_DT)
    assert root.is_bound()
    assert root.child_at(0).is_bound()
    assert root.child_at(0).linked_target_indices() == []


def test_output_fq_path_uses_port_index_only_for_serialization():
    ts_int = _ts_int_meta()
    pair_meta = _tsb_meta("PairPath", [("x", ts_int), ("y", ts_int)])

    output = runtime.TSOutput(pair_meta, 3)
    root = output.output_view(MIN_DT)
    field_x = root.child_by_name("x")

    assert root.short_indices() == []
    assert field_x.short_indices() == [0]
    assert root.fq_path_str().endswith(":out/3")
    assert field_x.fq_path_str().endswith(":out/3/x")


def test_output_fq_path_nested_tsb_uses_field_names():
    ts_int = _ts_int_meta()
    inner_meta = _tsb_meta("InnerPath", [("u", ts_int), ("v", ts_int)])
    outer_meta = _tsb_meta("OuterPath", [("left", ts_int), ("inner", inner_meta)])

    output = runtime.TSOutput(outer_meta, 5)
    nested = output.output_view(MIN_DT).child_by_name("inner").child_by_name("v")
    assert nested.fq_path_str().endswith(":out/5/inner/v")


def test_output_fq_path_tsd_uses_key_when_value_present():
    ts_int = _ts_int_meta()
    key_type = value.scalar_type_meta_string()
    tsd_meta = _tsd_meta(key_type, ts_int)

    output = runtime.TSOutput(tsd_meta, 1)
    root = output.output_view(MIN_DT)

    map_value = value.Value(tsd_meta.value_type, {"alpha": 7})
    root.set_value(map_value.view())

    first_child = root.child_at(0)
    assert first_child.fq_path_str().endswith(":out/1/alpha")


def test_output_fq_path_tsd_falls_back_to_index_without_value():
    ts_int = _ts_int_meta()
    key_type = value.scalar_type_meta_string()
    tsd_meta = _tsd_meta(key_type, ts_int)

    output = runtime.TSOutput(tsd_meta, 2)
    root = output.output_view(MIN_DT)

    first_child = root.child_at(0)
    assert first_child.fq_path_str().endswith(":out/2/0")


def test_output_child_by_key_navigates_to_keyed_entry():
    ts_int = _ts_int_meta()
    tsd_meta = _tsd_meta(value.scalar_type_meta_string(), ts_int)

    output = runtime.TSOutput(tsd_meta, 4)
    root = output.output_view(MIN_DT)
    map_value = value.Value(tsd_meta.value_type, {"alpha": 11, "beta": 22})
    root.set_value(map_value.view())

    beta_key = next(k for k in map_value.as_map().keys() if k.as_string() == "beta")
    keyed_child = root.child_by_key(beta_key)
    assert keyed_child.fq_path_str().endswith(":out/4/beta")


def test_output_view_navigation_uses_at_field_and_at_key_aliases():
    ts_int = _ts_int_meta()
    pair_meta = _tsb_meta("PairAtAlias", [("x", ts_int), ("y", ts_int)])
    root = runtime.TSOutput(pair_meta, 6).output_view(MIN_DT)

    field_x = root.field("x")
    idx_0 = root.at(0)
    assert field_x.fq_path_str().endswith(":out/6/x")
    assert idx_0.fq_path_str().endswith(":out/6/x")
    assert root.count() == root.size() == 2

    tsd_meta = _tsd_meta(value.scalar_type_meta_string(), ts_int)
    dict_root = runtime.TSOutput(tsd_meta, 7).output_view(MIN_DT)
    map_value = value.Value(tsd_meta.value_type, {"alpha": 11, "beta": 22})
    dict_root.set_value(map_value.view())
    beta_key = next(k for k in map_value.as_map().keys() if k.as_string() == "beta")

    at_key_child = dict_root.at_key(beta_key)
    assert at_key_child.fq_path_str().endswith(":out/7/beta")
    assert dict_root.count() == dict_root.size() == 2


@pytest.mark.parametrize(
    "meta_factory",
    [
        lambda: _ts_int_meta(),
        lambda: _registry().tss(value.scalar_type_meta_int64()),
        lambda: _registry().tsw(value.scalar_type_meta_double(), 6, 2),
        lambda: _registry().ref(_ts_int_meta()),
        lambda: _registry().signal(),
    ],
    ids=["TS", "TSS", "TSW", "REF", "SIGNAL"],
)
def test_leaf_kinds_active_toggle(meta_factory):
    meta = meta_factory()
    ts_input = runtime.TSInput(meta)
    root = ts_input.input_view(MIN_DT)

    assert not root.active
    root.make_active()
    assert root.active
    root.make_passive()
    assert not root.active


def test_input_view_scoped_active_state_is_recursive():
    ts_int = _ts_int_meta()
    inner_meta = _tsb_meta("InnerActive", [("x", ts_int), ("y", ts_int)])
    outer_meta = _tsb_meta("OuterActive", [("left", ts_int), ("inner", inner_meta)])

    ts_input = runtime.TSInput(outer_meta)
    root = ts_input.input_view(MIN_DT)
    left = root.child_by_name("left")
    inner = root.child_by_name("inner")
    inner_x = inner.child_by_name("x")
    inner_y = inner.child_by_name("y")

    assert not ts_input.active()
    assert not root.active
    assert root.fq_path_str().endswith(":in")
    assert inner_x.fq_path_str().endswith(":in/inner/x")

    inner_x.make_active()
    assert ts_input.active()
    assert root.active
    assert inner_x.active
    assert not left.active

    inner_x.make_passive()
    assert not inner_x.active
    assert not ts_input.active()

    inner.make_active()
    assert ts_input.active()
    assert inner.active
    assert inner_x.active
    assert inner_y.active

    inner.make_passive()
    assert not inner.active
    assert not inner_x.active
    assert not inner_y.active
    assert not ts_input.active()


def test_input_active_root_tracks_any_active_branch():
    ts_int = _ts_int_meta()
    pair_meta = _tsb_meta("PairAnyActive", [("left", ts_int), ("right", ts_int)])

    ts_input = runtime.TSInput(pair_meta)
    root = ts_input.input_view(MIN_DT)
    left = root.child_by_name("left")
    right = root.child_by_name("right")

    left.make_active()
    right.make_active()
    assert ts_input.active()
    assert root.active

    left.make_passive()
    assert ts_input.active()
    assert right.active

    right.make_passive()
    assert not ts_input.active()
    assert not root.active


def test_tsl_fixed_child_scoped_active_updates_root_state():
    ts_int = _ts_int_meta()
    fixed_meta = _registry().tsl(ts_int, 3)

    ts_input = runtime.TSInput(fixed_meta)
    root = ts_input.input_view(MIN_DT)
    mid = root.child_at(1)

    mid.make_active()
    assert ts_input.active()
    assert root.active
    assert not root.child_at(0).active
    assert root.child_at(1).active
    assert not root.child_at(2).active

    mid.make_passive()
    assert not ts_input.active()
    assert not root.active


def test_alternative_binding_recurses_by_tsb_field_name():
    ts_int = _ts_int_meta()
    native_meta = _tsb_meta("NativeAB", [("a", ts_int), ("b", ts_int)])
    alt_meta = _tsb_meta("AltBA", [("b", ts_int), ("a", ts_int)])

    output = runtime.TSOutput(native_meta, 0)
    alt_view = output.output_view(MIN_DT, alt_meta)
    alt_b = alt_view.child_by_name("b")
    alt_a = alt_view.child_by_name("a")

    assert alt_view.is_bound()
    assert alt_b.is_bound()
    assert alt_a.is_bound()
    assert alt_b.linked_source_indices() == [1]
    assert alt_a.linked_source_indices() == [0]


def test_alternative_tsb_falls_back_to_index_when_names_do_not_match():
    ts_int = _ts_int_meta()
    native_meta = _tsb_meta("NativeIdx", [("a", ts_int), ("b", ts_int)])
    alt_meta = _tsb_meta("AltIdx", [("x", ts_int), ("y", ts_int)])

    output = runtime.TSOutput(native_meta, 0)
    alt_view = output.output_view(MIN_DT, alt_meta)

    assert alt_view.child_by_name("x").linked_source_indices() == [0]
    assert alt_view.child_by_name("y").linked_source_indices() == [1]


def test_alternative_tsl_fixed_recurses_into_nested_elements():
    ts_int = _ts_int_meta()
    native_elem = _tsb_meta("NativeElem", [("a", ts_int), ("b", ts_int)])
    alt_elem = _tsb_meta("AltElem", [("b", ts_int), ("a", ts_int)])

    native_meta = _registry().tsl(native_elem, 2)
    alt_meta = _registry().tsl(alt_elem, 2)

    output = runtime.TSOutput(native_meta, 0)
    alt_view = output.output_view(MIN_DT, alt_meta)

    assert not alt_view.is_bound()
    assert alt_view.child_at(0).is_bound()
    assert alt_view.child_at(1).is_bound()
    assert alt_view.child_at(0).child_by_name("b").linked_source_indices() == [0, 1]
    assert alt_view.child_at(1).child_by_name("a").linked_source_indices() == [1, 0]


def test_ref_alternative_binds_dereferenced_schema_at_root():
    ts_int = _ts_int_meta()
    ref_meta = _registry().ref(ts_int)
    deref_meta = _registry().dereference(ref_meta)

    output = runtime.TSOutput(ref_meta, 0)
    alt_view = output.output_view(MIN_DT, deref_meta)

    assert alt_view.is_bound()
    assert alt_view.linked_source_indices() == []


def test_input_unbind_clears_tsb_unpeered_child_links():
    ts_int = _ts_int_meta()
    pair_meta = _tsb_meta("PairUnbindCascade", [("x", ts_int), ("y", ts_int)])

    ts_input = runtime.TSInput(pair_meta)
    ts_output = runtime.TSOutput(pair_meta, 0)

    in_root = ts_input.input_view(MIN_DT)
    in_x = in_root.child_by_name("x")
    out_x = ts_output.output_view(MIN_DT).child_by_name("x")

    ts_input.bind(ts_output, MIN_DT)
    in_x.bind(out_x)
    assert not in_root.is_bound()
    assert in_x.is_bound()

    ts_input.unbind(MIN_DT)
    assert not in_root.is_bound()
    assert not in_x.is_bound()


def test_sampled_flag_propagates_to_child_views():
    ts_int = _ts_int_meta()
    pair_meta = _tsb_meta("PairSampledChild", [("x", ts_int), ("y", ts_int)])

    root = runtime.TSOutput(pair_meta, 0).output_view(MIN_DT)
    root.set_sampled(True)
    child = root.child_by_name("x")

    assert root.sampled()
    assert child.sampled()
    assert child.modified


def test_get_ts_ops_dispatch_for_tsw_kind_and_meta_variants():
    tsw_tick_meta = _registry().tsw(value.scalar_type_meta_double(), 6, 2)
    tsw_duration_meta = _registry().tsw_duration(
        value.scalar_type_meta_double(),
        timedelta(minutes=2),
        timedelta(minutes=1),
    )

    tick_ptr = runtime.ops_ptr_for_meta(tsw_tick_meta)
    duration_ptr = runtime.ops_ptr_for_meta(tsw_duration_meta)
    tsw_kind_ptr = runtime.ops_ptr_for_kind(_hgraph.TSKind.TSW)
    assert tick_ptr != 0
    assert duration_ptr != 0
    assert tick_ptr != duration_ptr
    assert tsw_kind_ptr in {tick_ptr, duration_ptr}


def test_get_ts_ops_by_kind_compaction_exposes_only_relevant_extensions():
    assert runtime.ops_kind_for_kind(_hgraph.TSKind.TSValue) == _hgraph.TSKind.TSValue
    assert not runtime.ops_has_window_for_kind(_hgraph.TSKind.TSValue)
    assert not runtime.ops_has_set_for_kind(_hgraph.TSKind.TSValue)
    assert not runtime.ops_has_dict_for_kind(_hgraph.TSKind.TSValue)
    assert not runtime.ops_has_list_for_kind(_hgraph.TSKind.TSValue)
    assert not runtime.ops_has_bundle_for_kind(_hgraph.TSKind.TSValue)

    assert runtime.ops_kind_for_kind(_hgraph.TSKind.TSW) == _hgraph.TSKind.TSW
    assert runtime.ops_has_window_for_kind(_hgraph.TSKind.TSW)
    assert not runtime.ops_has_set_for_kind(_hgraph.TSKind.TSW)
    assert not runtime.ops_has_dict_for_kind(_hgraph.TSKind.TSW)
    assert not runtime.ops_has_list_for_kind(_hgraph.TSKind.TSW)
    assert not runtime.ops_has_bundle_for_kind(_hgraph.TSKind.TSW)

    assert runtime.ops_kind_for_kind(_hgraph.TSKind.TSS) == _hgraph.TSKind.TSS
    assert not runtime.ops_has_window_for_kind(_hgraph.TSKind.TSS)
    assert runtime.ops_has_set_for_kind(_hgraph.TSKind.TSS)
    assert not runtime.ops_has_dict_for_kind(_hgraph.TSKind.TSS)
    assert not runtime.ops_has_list_for_kind(_hgraph.TSKind.TSS)
    assert not runtime.ops_has_bundle_for_kind(_hgraph.TSKind.TSS)

    assert runtime.ops_kind_for_kind(_hgraph.TSKind.TSD) == _hgraph.TSKind.TSD
    assert not runtime.ops_has_window_for_kind(_hgraph.TSKind.TSD)
    assert not runtime.ops_has_set_for_kind(_hgraph.TSKind.TSD)
    assert runtime.ops_has_dict_for_kind(_hgraph.TSKind.TSD)
    assert not runtime.ops_has_list_for_kind(_hgraph.TSKind.TSD)
    assert not runtime.ops_has_bundle_for_kind(_hgraph.TSKind.TSD)

    assert runtime.ops_kind_for_kind(_hgraph.TSKind.TSL) == _hgraph.TSKind.TSL
    assert not runtime.ops_has_window_for_kind(_hgraph.TSKind.TSL)
    assert not runtime.ops_has_set_for_kind(_hgraph.TSKind.TSL)
    assert not runtime.ops_has_dict_for_kind(_hgraph.TSKind.TSL)
    assert runtime.ops_has_list_for_kind(_hgraph.TSKind.TSL)
    assert not runtime.ops_has_bundle_for_kind(_hgraph.TSKind.TSL)

    assert runtime.ops_kind_for_kind(_hgraph.TSKind.TSB) == _hgraph.TSKind.TSB
    assert not runtime.ops_has_window_for_kind(_hgraph.TSKind.TSB)
    assert not runtime.ops_has_set_for_kind(_hgraph.TSKind.TSB)
    assert not runtime.ops_has_dict_for_kind(_hgraph.TSKind.TSB)
    assert not runtime.ops_has_list_for_kind(_hgraph.TSKind.TSB)
    assert runtime.ops_has_bundle_for_kind(_hgraph.TSKind.TSB)


def test_schema_cache_scalar_contract_surfaces_all_parallel_schemas():
    ts_meta = _ts_int_meta()

    value_meta = runtime.schema_value_meta(ts_meta)
    time_meta = runtime.schema_time_meta(ts_meta)
    observer_meta = runtime.schema_observer_meta(ts_meta)
    delta_meta = runtime.schema_delta_meta(ts_meta)
    link_meta = runtime.schema_link_meta(ts_meta)
    input_link_meta = runtime.schema_input_link_meta(ts_meta)
    active_meta = runtime.schema_active_meta(ts_meta)

    assert value_meta is ts_meta.value_type
    assert time_meta is value.scalar_type_meta_datetime()
    assert observer_meta is value.TypeMeta.get("object")
    assert delta_meta is None
    assert link_meta is value.TypeMeta.get("REFLink")
    assert input_link_meta is value.TypeMeta.get("LinkTarget")
    assert active_meta is value.scalar_type_meta_bool()


def test_schema_cache_tsb_link_and_active_shapes_include_container_slot():
    ts_int = _ts_int_meta()
    bundle_meta = _tsb_meta("SchemaBundle", [("x", ts_int), ("y", ts_int)])

    output_link = runtime.schema_link_meta(bundle_meta)
    input_link = runtime.schema_input_link_meta(bundle_meta)
    active = runtime.schema_active_meta(bundle_meta)

    ref_link_meta = value.TypeMeta.get("REFLink")
    link_target_meta = value.TypeMeta.get("LinkTarget")
    bool_meta = value.scalar_type_meta_bool()

    assert output_link.field_count == 3
    assert output_link.fields[0].type is ref_link_meta
    assert output_link.fields[1].type is ref_link_meta
    assert output_link.fields[2].type is ref_link_meta

    assert input_link.field_count == 3
    assert input_link.fields[0].type is link_target_meta
    assert input_link.fields[1].type is link_target_meta
    assert input_link.fields[2].type is link_target_meta

    assert active.field_count == 3
    assert active.fields[0].type is bool_meta
    assert active.fields[1].type is bool_meta
    assert active.fields[2].type is bool_meta


def test_schema_cache_tsl_link_shape_differs_for_fixed_and_dynamic_modes():
    ts_int = _ts_int_meta()
    fixed_meta = _registry().tsl(ts_int, 3)
    dynamic_meta = _registry().tsl(ts_int, 0)

    fixed_output_link = runtime.schema_link_meta(fixed_meta)
    fixed_input_link = runtime.schema_input_link_meta(fixed_meta)
    dynamic_output_link = runtime.schema_link_meta(dynamic_meta)
    dynamic_input_link = runtime.schema_input_link_meta(dynamic_meta)

    assert fixed_output_link.kind == value.TypeKind.List
    assert fixed_output_link.fixed_size == 3
    assert fixed_output_link.element_type is value.TypeMeta.get("REFLink")

    assert fixed_input_link.kind == value.TypeKind.List
    assert fixed_input_link.fixed_size == 3
    assert fixed_input_link.element_type is value.TypeMeta.get("LinkTarget")

    assert dynamic_output_link is value.TypeMeta.get("REFLink")
    assert dynamic_input_link is value.TypeMeta.get("LinkTarget")


def test_schema_cache_tsd_link_is_leaf_and_active_is_recursive():
    tsd_meta = _tsd_meta(value.scalar_type_meta_string(), _ts_int_meta())

    assert runtime.schema_link_meta(tsd_meta) is value.TypeMeta.get("REFLink")
    assert runtime.schema_input_link_meta(tsd_meta) is value.TypeMeta.get("LinkTarget")

    active = runtime.schema_active_meta(tsd_meta)
    assert active.field_count == 2
    assert active.fields[0].type is value.scalar_type_meta_bool()

    child_collection = active.fields[1].type
    assert child_collection.kind == value.TypeKind.List
    assert child_collection.fixed_size == 0
    assert child_collection.element_type is value.scalar_type_meta_bool()


def test_schema_cache_delta_contract_for_tss_tsd_and_tsb_nested_shapes():
    ts_int = _ts_int_meta()
    key_type = value.scalar_type_meta_string()

    tss_meta = _registry().tss(value.scalar_type_meta_int64())
    tss_delta = runtime.schema_delta_meta(tss_meta)
    assert tss_delta is not None
    assert tss_delta.field_count == 2
    assert tss_delta.fields[0].type is tss_meta.value_type
    assert tss_delta.fields[1].type is tss_meta.value_type

    tsd_scalar_meta = _tsd_meta(key_type, ts_int)
    tsd_scalar_delta = runtime.schema_delta_meta(tsd_scalar_meta)
    assert tsd_scalar_delta is not None
    assert tsd_scalar_delta.field_count == 3
    assert tsd_scalar_delta.fields[0].type.kind == value.TypeKind.Map
    assert tsd_scalar_delta.fields[0].type.key_type is key_type
    assert tsd_scalar_delta.fields[0].type.element_type is ts_int.value_type
    assert tsd_scalar_delta.fields[1].type.kind == value.TypeKind.Set
    assert tsd_scalar_delta.fields[1].type.element_type is key_type
    assert tsd_scalar_delta.fields[2].type.kind == value.TypeKind.Set
    assert tsd_scalar_delta.fields[2].type.element_type is key_type

    tsd_nested_meta = _tsd_meta(key_type, tss_meta)
    tsd_nested_delta = runtime.schema_delta_meta(tsd_nested_meta)
    assert tsd_nested_delta is not None
    assert tsd_nested_delta.field_count == 4
    nested_children = tsd_nested_delta.fields[3].type
    assert nested_children.kind == value.TypeKind.List
    nested_child_delta = nested_children.element_type
    assert nested_child_delta is not None
    assert nested_child_delta.kind == tss_delta.kind
    assert nested_child_delta.field_count == tss_delta.field_count
    assert nested_child_delta.fields[0].type is tss_delta.fields[0].type
    assert nested_child_delta.fields[1].type is tss_delta.fields[1].type

    bundle_meta = _tsb_meta("DeltaBundle", [("scalar", ts_int), ("set_value", tss_meta)])
    bundle_delta = runtime.schema_delta_meta(bundle_meta)
    assert bundle_delta is not None
    assert bundle_delta.field_count == 2
    assert bundle_delta.fields[0].type is value.TypeMeta.get("object")
    assert bundle_delta.fields[1].type.kind == tss_delta.kind
    assert bundle_delta.fields[1].type.field_count == tss_delta.field_count
    assert bundle_delta.fields[1].type.fields[0].type is tss_delta.fields[0].type
    assert bundle_delta.fields[1].type.fields[1].type is tss_delta.fields[1].type


@pytest.mark.parametrize(
    "meta_factory, kind",
    [
        (lambda: _ts_int_meta(), _hgraph.TSKind.TSValue),
        (lambda: _registry().tsw(value.scalar_type_meta_double(), 6, 2), _hgraph.TSKind.TSW),
        (lambda: _registry().tss(value.scalar_type_meta_int64()), _hgraph.TSKind.TSS),
        (lambda: _tsd_meta(value.scalar_type_meta_string(), _ts_int_meta()), _hgraph.TSKind.TSD),
        (lambda: _registry().tsl(_ts_int_meta(), 0), _hgraph.TSKind.TSL),
        (lambda: _tsb_meta("KindPredBundle", [("x", _ts_int_meta())]), _hgraph.TSKind.TSB),
    ],
)
def test_output_view_kind_predicates(meta_factory, kind):
    root = runtime.TSOutput(meta_factory(), 0).output_view(MIN_DT)

    assert root.kind() == kind
    assert root.is_window() == (kind == _hgraph.TSKind.TSW)
    assert root.is_set() == (kind == _hgraph.TSKind.TSS)
    assert root.is_dict() == (kind == _hgraph.TSKind.TSD)
    assert root.is_list() == (kind == _hgraph.TSKind.TSL)
    assert root.is_bundle() == (kind == _hgraph.TSKind.TSB)


def test_tsw_window_ops_surface_default_shape():
    tsw_meta = _registry().tsw(value.scalar_type_meta_double(), 6, 2)
    root = runtime.TSOutput(tsw_meta, 0).output_view(MIN_DT)

    assert root.has_window_ops()
    assert not root.has_set_ops()
    assert not root.has_dict_ops()
    assert root.window_size() == 6
    assert root.window_min_size() == 2
    assert root.window_length() == 0
    assert root.window_value_times_count() == 0
    assert root.window_value_times() == []
    assert root.window_first_modified_time() == MIN_DT
    assert not root.window_has_removed_value()
    assert root.window_removed_value_count() == 0


def test_tss_set_ops_roundtrip():
    tss_meta = _registry().tss(value.scalar_type_meta_int64())
    root = runtime.TSOutput(tss_meta, 0).output_view(MIN_DT)

    assert root.has_set_ops()
    assert not root.has_window_ops()
    assert not root.has_dict_ops()

    elem_1 = _int_value(1)
    elem_2 = _int_value(2)
    assert root.set_add(elem_1.view())
    assert root.set_add(elem_2.view())
    assert not root.set_add(elem_2.view())
    assert root.to_python() == {1, 2}

    assert root.set_remove(elem_1.view())
    assert not root.set_remove(elem_1.view())
    assert root.to_python() == {2}

    root.set_clear()
    assert root.to_python() == set()


def test_tsd_dict_ops_roundtrip():
    tsd_meta = _tsd_meta(value.scalar_type_meta_string(), _ts_int_meta())
    root = runtime.TSOutput(tsd_meta, 0).output_view(MIN_DT)

    assert root.has_dict_ops()
    assert not root.has_window_ops()
    assert not root.has_set_ops()

    alpha = _str_value("alpha")
    beta = _str_value("beta")
    val_7 = _int_value(7)

    set_child = root.dict_set(alpha.view(), val_7.view())
    assert set_child
    assert set_child.to_python() == 7
    assert root.to_python() == {"alpha": 7}

    created = root.dict_create(beta.view())
    assert created
    assert created.to_python() == 0
    created.from_python(3)
    assert root.to_python() == {"alpha": 7, "beta": 3}

    assert root.dict_remove(alpha.view())
    assert not root.dict_remove(alpha.view())
    assert root.to_python() == {"beta": 3}


def test_output_view_python_conversion_roundtrip_for_scalar_ts():
    meta = _ts_int_meta()
    output_view = runtime.TSOutput(meta, 0).output_view(MIN_DT)

    output_view.from_python(42)
    assert output_view.to_python() == 42
    assert output_view.delta_to_python() is None


@pytest.mark.parametrize(
    "meta_factory, expected_try, expected_cls",
    [
        (lambda: _registry().tsw(value.scalar_type_meta_double(), 6, 2), "try_as_window", "TSWOutputView"),
        (lambda: _registry().tss(value.scalar_type_meta_int64()), "try_as_set", "TSSOutputView"),
        (lambda: _tsd_meta(value.scalar_type_meta_string(), _ts_int_meta()), "try_as_dict", "TSDOutputView"),
        (lambda: _registry().tsl(_ts_int_meta(), 0), "try_as_list", "TSLOutputView"),
        (lambda: _tsb_meta("TypedBundle", [("x", _ts_int_meta())]), "try_as_bundle", "TSBOutputView"),
    ],
)
def test_output_view_try_as_and_as_expose_typed_views(meta_factory, expected_try, expected_cls):
    root = runtime.TSOutput(meta_factory(), 0).output_view(MIN_DT)

    typed_try = getattr(root, expected_try)()
    assert typed_try is not None
    assert isinstance(typed_try, getattr(runtime, expected_cls))
    assert isinstance(typed_try, runtime.TSOutputView)

    as_name = expected_try.replace("try_", "")
    typed_as = getattr(root, as_name)()
    assert isinstance(typed_as, getattr(runtime, expected_cls))
    assert isinstance(typed_as, runtime.TSOutputView)

    for other in ("try_as_window", "try_as_set", "try_as_dict", "try_as_list", "try_as_bundle"):
        if other != expected_try:
            assert getattr(root, other)() is None


def test_output_view_as_raises_for_wrong_kind():
    root = runtime.TSOutput(_ts_int_meta(), 0).output_view(MIN_DT)
    assert root.try_as_window() is None
    with pytest.raises(RuntimeError):
        root.as_window()


def test_typed_window_view_surface():
    root = runtime.TSOutput(_registry().tsw(value.scalar_type_meta_double(), 6, 2), 0).output_view(MIN_DT)
    window_view = root.as_window()

    assert window_view.value_times_count() == 0
    assert window_view.value_times() == []
    assert window_view.size() == 6
    assert window_view.min_size() == 2
    assert window_view.length() == 0
    assert not window_view.has_removed_value()


def test_typed_set_view_surface():
    root = runtime.TSOutput(_registry().tss(value.scalar_type_meta_int64()), 0).output_view(MIN_DT)
    set_view = root.as_set()

    assert set_view.add(_int_value(1).view())
    assert set_view.add(_int_value(2).view())
    assert not set_view.add(_int_value(2).view())
    assert set_view.to_python() == {1, 2}

    assert set_view.remove(_int_value(1).view())
    assert not set_view.remove(_int_value(1).view())
    assert set_view.to_python() == {2}

    set_view.clear()
    assert set_view.to_python() == set()


def test_typed_dict_view_surface():
    root = runtime.TSOutput(_tsd_meta(value.scalar_type_meta_string(), _ts_int_meta()), 0).output_view(MIN_DT)
    dict_view = root.as_dict()

    alpha = _str_value("alpha")
    beta = _str_value("beta")
    seven = _int_value(7)

    set_child = dict_view.set(alpha.view(), seven.view())
    assert set_child
    assert isinstance(set_child, runtime.TSOutputView)
    assert set_child.to_python() == 7
    assert dict_view.to_python() == {"alpha": 7}

    created = dict_view.create(beta.view())
    assert created
    created.from_python(3)
    assert dict_view.to_python() == {"alpha": 7, "beta": 3}

    beta_child = dict_view.at_key(beta.view())
    assert beta_child.to_python() == 3

    assert dict_view.remove(alpha.view())
    assert not dict_view.remove(alpha.view())
    assert dict_view.to_python() == {"beta": 3}


def test_typed_list_view_surface():
    list_meta = _registry().tsl(_ts_int_meta(), 2)
    root = runtime.TSOutput(list_meta, 0).output_view(MIN_DT)
    root.at(0).from_python(10)
    root.at(1).from_python(20)

    list_view = root.as_list()
    assert list_view.count() == 2
    assert list_view.size() == 2

    second = list_view.at(1)
    assert isinstance(second, runtime.TSOutputView)
    assert second.to_python() == 20


def test_typed_bundle_view_surface():
    pair_meta = _tsb_meta("TypedBundleOps", [("x", _ts_int_meta()), ("y", _ts_int_meta())])
    root = runtime.TSOutput(pair_meta, 0).output_view(MIN_DT)
    root.field("x").from_python(11)
    root.field("y").from_python(22)

    bundle_view = root.as_bundle()
    assert bundle_view.count() == 2
    assert bundle_view.size() == 2
    assert bundle_view.at(0).to_python() == 11
    assert bundle_view.field("y").to_python() == 22


@pytest.mark.parametrize(
    "meta_factory, expected_try, expected_cls",
    [
        (lambda: _registry().tsw(value.scalar_type_meta_double(), 6, 2), "try_as_window", "TSWInputView"),
        (lambda: _registry().tss(value.scalar_type_meta_int64()), "try_as_set", "TSSInputView"),
        (lambda: _tsd_meta(value.scalar_type_meta_string(), _ts_int_meta()), "try_as_dict", "TSDInputView"),
        (lambda: _registry().tsl(_ts_int_meta(), 0), "try_as_list", "TSLInputView"),
        (lambda: _tsb_meta("TypedBundleIn", [("x", _ts_int_meta())]), "try_as_bundle", "TSBInputView"),
    ],
)
def test_input_view_try_as_and_as_expose_typed_views(meta_factory, expected_try, expected_cls):
    root = runtime.TSInput(meta_factory()).input_view(MIN_DT)

    typed_try = getattr(root, expected_try)()
    assert typed_try is not None
    assert isinstance(typed_try, getattr(runtime, expected_cls))
    assert isinstance(typed_try, runtime.TSInputView)

    as_name = expected_try.replace("try_", "")
    typed_as = getattr(root, as_name)()
    assert isinstance(typed_as, getattr(runtime, expected_cls))
    assert isinstance(typed_as, runtime.TSInputView)

    for other in ("try_as_window", "try_as_set", "try_as_dict", "try_as_list", "try_as_bundle"):
        if other != expected_try:
            assert getattr(root, other)() is None


def test_input_view_as_raises_for_wrong_kind():
    root = runtime.TSInput(_ts_int_meta()).input_view(MIN_DT)
    assert root.try_as_window() is None
    with pytest.raises(RuntimeError):
        root.as_window()


def test_typed_input_window_view_surface():
    root = runtime.TSInput(_registry().tsw(value.scalar_type_meta_double(), 6, 2)).input_view(MIN_DT)
    window_view = root.as_window()

    assert window_view.value_times_count() == 0
    assert window_view.size() == 6
    assert window_view.min_size() == 2
    assert window_view.length() == 0
    assert not window_view.has_removed_value()


def test_typed_input_set_view_surface():
    tss_meta = _registry().tss(value.scalar_type_meta_int64())
    set_view = runtime.TSInput(tss_meta).input_view(MIN_DT).as_set()
    assert isinstance(set_view, runtime.TSSInputView)
    assert isinstance(set_view, runtime.TSInputView)


def test_typed_input_dict_view_surface():
    tsd_meta = _tsd_meta(value.scalar_type_meta_string(), _ts_int_meta())
    dict_view = runtime.TSInput(tsd_meta).input_view(MIN_DT).as_dict()
    assert dict_view.count() == 0
    assert dict_view.size() == 0

    beta_child = dict_view.at_key(_str_value("beta").view())
    assert isinstance(beta_child, runtime.TSInputView)


def test_typed_input_list_view_surface():
    list_meta = _registry().tsl(_ts_int_meta(), 2)
    list_view = runtime.TSInput(list_meta).input_view(MIN_DT).as_list()
    assert list_view.count() == 2
    assert list_view.size() == 2

    second = list_view.at(1)
    assert isinstance(second, runtime.TSInputView)


def test_typed_input_bundle_view_surface():
    pair_meta = _tsb_meta("TypedBundleInOps", [("x", _ts_int_meta()), ("y", _ts_int_meta())])
    bundle_view = runtime.TSInput(pair_meta).input_view(MIN_DT).as_bundle()
    assert bundle_view.count() == 2
    assert bundle_view.size() == 2
    assert isinstance(bundle_view.at(0), runtime.TSInputView)
    assert isinstance(bundle_view.field("y"), runtime.TSInputView)
