#pragma once

#include <hgraph/nodes/node_binding_utils.h>
#include <hgraph/types/feature_extension.h>
#include <hgraph/types/node.h>

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>

namespace hgraph {
    namespace ops {
        namespace access_ops_detail {
            inline TSBInputView input_bundle(Node& node) {
                return *node.input().try_as_bundle();
            }

            inline nb::object python_field(const TSBInputView& bundle, std::string_view field_name) {
                return bundle.field(field_name).to_python();
            }

            inline nb::object optional_python_field(
                const TSBInputView& bundle, std::string_view field_name, const nb::object& default_value) {
                auto field = bundle.field(field_name);
                if (!field || !field.valid()) {
                    return default_value;
                }
                return field.to_python();
            }

            inline void emit_python(Node& node, const nb::object& value) {
                node.output().from_python(value);
            }

            inline void emit_ref(Node& node, const TimeSeriesReference& value) {
                ViewData out_vd = node.output().as_ts_view().view_data();
                apply_ref_payload(out_vd, value, node_time(node));
            }

            inline void emit_empty_tsd_delta(Node& node) {
                TSOutputView out = node.output();
                const TSMeta* meta = out.ts_meta();
                if (meta == nullptr || meta->kind != TSKind::TSD || meta->value_type == nullptr) {
                    return;
                }

                value::Value empty_map(meta->value_type);
                empty_map.emplace();
                out.set_value(empty_map.view());
            }

            inline bool python_equal(const nb::object& lhs, const nb::object& rhs) {
                const int eq = PyObject_RichCompareBool(lhs.ptr(), rhs.ptr(), Py_EQ);
                if (eq < 0) {
                    nb::raise_python_error();
                }
                return eq == 1;
            }

            inline std::optional<ViewData> resolve_ref_tsd_target(const TSInputView& ts, const Node& node) {
                if (!ts || !ts.valid()) {
                    return std::nullopt;
                }
                auto target = resolve_non_ref_target_view_data(ts.as_ts_view(), RefBindOrder::RefValueThenBoundTarget);
                if (!target.has_value()) {
                    return std::nullopt;
                }

                TSView target_view(*target, node_time_ptr(node));
                const TSMeta* target_meta = target_view.ts_meta();
                if (target_meta == nullptr || target_meta->kind != TSKind::TSD) {
                    return std::nullopt;
                }
                return target;
            }

            inline TimeSeriesReference resolve_nested_ref_output(const TSOutputView& outer_ref_output, const Node& node) {
                if (!outer_ref_output) {
                    return TimeSeriesReference::make();
                }

                TimeSeriesReference result = resolve_ref_payload_from_view(outer_ref_output.as_ts_view().view_data());
                if (const ViewData* bound = result.bound_view(); bound != nullptr) {
                    TSView bound_view(*bound, node_time_ptr(node));
                    const TSMeta* bound_meta = bound_view.ts_meta();
                    if (bound_meta != nullptr && bound_meta->kind == TSKind::REF) {
                        result = resolve_ref_payload_from_view(*bound);
                    }
                }
                return result;
            }

            inline bool ref_target_modified(const TimeSeriesReference& ref, const Node& node) {
                const ViewData* bound = ref.bound_view();
                if (bound == nullptr) {
                    return false;
                }
                TSView bound_view(*bound, node_time_ptr(node));
                return bound_view.modified();
            }

            inline const nb::object& json_class() {
                static const nb::object json_cls = nb::cast<nb::object>(nb::module_::import_("hgraph").attr("JSON"));
                return json_cls;
            }
        }  // namespace access_ops_detail

        struct SetattrCsSpec {
            static constexpr const char* py_factory_name = "op_setattr_cs";

            struct state {
                std::string attr;
                nb::object copy_fn;
            };

            static state make_state(Node& node) {
                const nb::dict& scalars = node.scalars();
                return {
                    nb::cast<std::string>(nb::cast<nb::object>(scalars["attr"])),
                    nb::module_::import_("copy").attr("copy"),
                };
            }

            static void eval(Node& node, state& state) {
                auto bundle = access_ops_detail::input_bundle(node);
                const nb::object ts = access_ops_detail::python_field(bundle, "ts");
                const nb::object value = access_ops_detail::python_field(bundle, "value");
                nb::object copied = state.copy_fn(ts);
                if (PyObject_SetAttrString(copied.ptr(), state.attr.c_str(), value.ptr()) < 0) {
                    nb::raise_python_error();
                }
                access_ops_detail::emit_python(node, copied);
            }
        };

        struct GetattrCsSpec {
            static constexpr const char* py_factory_name = "op_getattr_cs";

            struct state {
                std::string attr;
            };

            static state make_state(Node& node) {
                const nb::dict& scalars = node.scalars();
                return {nb::cast<std::string>(nb::cast<nb::object>(scalars["attr"]))};
            }

            static void eval(Node& node, state& state) {
                auto bundle = access_ops_detail::input_bundle(node);
                const nb::object ts = access_ops_detail::python_field(bundle, "ts");
                const nb::object default_value =
                    access_ops_detail::optional_python_field(bundle, "default_value", nb::none());

                nb::object attr_value;
                if (PyObject* raw = PyObject_GetAttrString(ts.ptr(), state.attr.c_str())) {
                    attr_value = nb::steal<nb::object>(raw);
                } else {
                    if (!PyErr_ExceptionMatches(PyExc_AttributeError)) {
                        nb::raise_python_error();
                    }
                    PyErr_Clear();
                    attr_value = default_value;
                }

                access_ops_detail::emit_python(node, attr_value.is_none() ? default_value : attr_value);
            }
        };

        struct GetattrTypeNameSpec {
            static constexpr const char* py_factory_name = "op_getattr_type_name";

            struct state {
                std::string attr;
            };

            static state make_state(Node& node) {
                const nb::dict& scalars = node.scalars();
                return {nb::cast<std::string>(nb::cast<nb::object>(scalars["attr"]))};
            }

            static void eval(Node& node, state& state) {
                if (state.attr != "name" && state.attr != "__name__") {
                    const std::string msg = "Cannot get " + state.attr + " from TS[Type]";
                    PyErr_SetString(PyExc_AttributeError, msg.c_str());
                    nb::raise_python_error();
                }
                auto bundle = access_ops_detail::input_bundle(node);
                const nb::object ts_type = access_ops_detail::python_field(bundle, "ts");
                access_ops_detail::emit_python(node, nb::cast<nb::object>(ts_type.attr("__name__")));
            }
        };

        struct GetitemTupleFixedSpec {
            static constexpr const char* py_factory_name = "op_getitem_tuple_fixed";

            struct state {
                int64_t key;
            };

            static state make_state(Node& node) {
                const nb::dict& scalars = node.scalars();
                return {nb::cast<int64_t>(scalars["key"])};
            }

            static void eval(Node& node, state& state) {
                auto bundle = access_ops_detail::input_bundle(node);
                const nb::object ts = access_ops_detail::python_field(bundle, "ts");
                const nb::object key = nb::int_(state.key);
                nb::object value = nb::steal<nb::object>(PyObject_GetItem(ts.ptr(), key.ptr()));
                if (!value.is_valid()) {
                    nb::raise_python_error();
                }
                access_ops_detail::emit_python(node, value);
            }
        };

        struct GetitemTupleSpec {
            static constexpr const char* py_factory_name = "op_getitem_tuple";

            static void eval(Node& node) {
                auto bundle = access_ops_detail::input_bundle(node);
                const nb::object ts = access_ops_detail::python_field(bundle, "ts");
                const nb::object key = access_ops_detail::python_field(bundle, "key");
                nb::object value = nb::steal<nb::object>(PyObject_GetItem(ts.ptr(), key.ptr()));
                if (!value.is_valid()) {
                    nb::raise_python_error();
                }
                access_ops_detail::emit_python(node, value);
            }
        };

        struct GetitemFrozendictSpec {
            static constexpr const char* py_factory_name = "op_getitem_frozendict";

            static void eval(Node& node) {
                auto bundle = access_ops_detail::input_bundle(node);
                const nb::object ts = access_ops_detail::python_field(bundle, "ts");
                const nb::object key = access_ops_detail::python_field(bundle, "key");
                const nb::object default_value =
                    access_ops_detail::optional_python_field(bundle, "default_value", nb::none());
                access_ops_detail::emit_python(node, nb::cast<nb::object>(ts.attr("get")(key, default_value)));
            }
        };

        struct TsdGetItemDefaultSpec {
            static constexpr const char* py_factory_name = "op_tsd_get_item_default";

            struct state {
                nb::object requester;
                nb::object key_obj;
                std::optional<ViewData> source_target;
                bool bound{false};
            };

            static state make_state(Node&) {
                return {
                    nb::module_::import_("builtins").attr("object")(),
                    nb::none(),
                    std::nullopt,
                    false,
                };
            }

            static void release_binding(Node& node, state& state) {
                if (!state.bound || !state.source_target.has_value()) {
                    state.bound = false;
                    state.key_obj = nb::none();
                    state.source_target.reset();
                    return;
                }

                TSOutputView source_out(nullptr, TSView(*state.source_target, node_time_ptr(node)));
                if (auto source_dict = source_out.try_as_dict(); source_dict.has_value()) {
                    source_dict->release_ref(state.key_obj, state.requester);
                }

                state.bound = false;
                state.key_obj = nb::none();
                state.source_target.reset();
            }

            static TimeSeriesReference read_bound_ref(Node& node, const state& state) {
                if (!state.bound || !state.source_target.has_value()) {
                    return TimeSeriesReference::make();
                }

                TSOutputView source_out(nullptr, TSView(*state.source_target, node_time_ptr(node)));
                auto source_dict = source_out.try_as_dict();
                if (!source_dict.has_value()) {
                    return TimeSeriesReference::make();
                }

                TSOutputView outer_ref_output = source_dict->get_ref(state.key_obj, state.requester);
                if (!outer_ref_output) {
                    return TimeSeriesReference::make();
                }

                return access_ops_detail::resolve_nested_ref_output(outer_ref_output, node);
            }

            static void eval(Node& node, state& state) {
                auto bundle = access_ops_detail::input_bundle(node);
                auto ts = bundle.field("ts");
                auto key = bundle.field("key");

                const std::optional<ViewData> next_source = access_ops_detail::resolve_ref_tsd_target(ts, node);
                const bool can_bind = next_source.has_value() && key && key.valid();
                const nb::object next_key_obj = can_bind ? key.to_python() : nb::none();

                bool binding_changed = (state.bound != can_bind);
                if (!binding_changed && can_bind) {
                    binding_changed =
                        !state.source_target.has_value() ||
                        !same_view_identity(*state.source_target, *next_source) ||
                        !access_ops_detail::python_equal(state.key_obj, next_key_obj);
                }

                if (binding_changed) {
                    release_binding(node, state);
                    if (can_bind) {
                        state.source_target = next_source;
                        state.key_obj = next_key_obj;
                        state.bound = true;
                    }
                }

                TimeSeriesReference output_ref = read_bound_ref(node, state);
                access_ops_detail::emit_ref(node, output_ref);
            }

            static void stop(Node& node, state& state) {
                release_binding(node, state);
            }
        };

        struct TsdGetItemsSpec {
            static constexpr const char* py_factory_name = "op_tsd_get_items";

            struct key_state {
                nb::object key_obj;
                std::optional<TimeSeriesReference> last_ref;
                bool output_present{false};
            };

            struct state {
                nb::object requester;
                std::optional<ViewData> source_target;
                std::unordered_map<value::Value, key_state, ValueHash, ValueEqual> keys;
            };

            static state make_state(Node&) {
                return {
                    nb::module_::import_("builtins").attr("object")(),
                    std::nullopt,
                    {},
                };
            }

            static void release_all_refs(const state& state, const std::optional<ViewData>& source_target, const Node& node) {
                if (!source_target.has_value()) {
                    return;
                }
                TSOutputView source_out(nullptr, TSView(*source_target, node_time_ptr(node)));
                auto source_dict = source_out.try_as_dict();
                if (!source_dict.has_value()) {
                    return;
                }
                for (const auto& [_, key_state] : state.keys) {
                    source_dict->release_ref(key_state.key_obj, state.requester);
                }
            }

            static key_state make_key_state(value::View key_view) {
                return key_state{
                    key_view.to_python(),
                    std::nullopt,
                    false,
                };
            }

            static void add_key_if_missing(state& state, value::View key_view) {
                if (state.keys.find(key_view) != state.keys.end()) {
                    return;
                }
                state.keys.emplace(key_view.clone(), make_key_state(key_view));
            }

            static void remove_present_output_keys(TSDOutputView& output_dict, const state& state) {
                for (const auto& [key_value, key_state] : state.keys) {
                    if (key_state.output_present) {
                        output_dict.remove(key_value.view());
                    }
                }
            }

            static bool source_changed(const std::optional<ViewData>& lhs, const std::optional<ViewData>& rhs) {
                if (lhs.has_value() != rhs.has_value()) {
                    return true;
                }
                if (!lhs.has_value()) {
                    return false;
                }
                return !same_view_identity(*lhs, *rhs);
            }

            static void eval(Node& node, state& state) {
                auto bundle = access_ops_detail::input_bundle(node);
                auto ts = bundle.field("ts");
                auto key = bundle.field("key");

                const std::optional<ViewData> next_source = access_ops_detail::resolve_ref_tsd_target(ts, node);
                const bool source_was_changed = source_changed(state.source_target, next_source);
                const bool had_requested_keys_before_source_change = !state.keys.empty();
                TSDOutputView output_dict = node.output().as_dict();

                if (source_was_changed) {
                    release_all_refs(state, state.source_target, node);
                    remove_present_output_keys(output_dict, state);
                    state.keys.clear();
                    state.source_target = next_source;
                }

                if (!state.source_target.has_value() || !key) {
                    return;
                }

                if (!key.valid()) {
                    if (source_was_changed && key.modified()) {
                        // Python parity for empty/intersection key ticks represented as invalid-but-modified.
                        access_ops_detail::emit_empty_tsd_delta(node);
                    }
                    return;
                }

                TSOutputView source_out(nullptr, TSView(*state.source_target, node_time_ptr(node)));
                auto source_dict_opt = source_out.try_as_dict();
                if (!source_dict_opt.has_value()) {
                    return;
                }
                auto source_dict = *source_dict_opt;

                auto key_set = key.as_set();
                if (source_was_changed) {
                    // Source rebinding reinitializes requested keys from the current key-set.
                    for (value::View key_view : key_set.values()) {
                        add_key_if_missing(state, key_view);
                    }
                } else if (key.modified()) {
                    bool saw_delta = false;
                    for (value::View removed_key : key_set.removed()) {
                        saw_delta = true;
                        auto it = state.keys.find(removed_key);
                        if (it == state.keys.end()) {
                            continue;
                        }
                        source_dict.release_ref(it->second.key_obj, state.requester);
                        if (it->second.output_present) {
                            output_dict.remove(it->first.view());
                        }
                        state.keys.erase(it);
                    }
                    for (value::View added_key : key_set.added()) {
                        saw_delta = true;
                        add_key_if_missing(state, added_key);
                    }

                    if (!saw_delta) {
                        // Fallback when upstream key deltas are unavailable:
                        // rebuild requested-key tracking from current key-set.
                        for (const auto& [existing_key, existing_state] : state.keys) {
                            source_dict.release_ref(existing_state.key_obj, state.requester);
                            if (existing_state.output_present) {
                                output_dict.remove(existing_key.view());
                            }
                        }
                        state.keys.clear();
                        for (value::View key_view : key_set.values()) {
                            add_key_if_missing(state, key_view);
                        }
                    }
                } else if (state.keys.empty()) {
                    // Initial bind with stable key input: seed requested keys once.
                    for (value::View key_view : key_set.values()) {
                        add_key_if_missing(state, key_view);
                    }
                }

                if (source_was_changed &&
                    !had_requested_keys_before_source_change &&
                    state.keys.empty()) {
                    // Python parity: valid empty key-sets emit an explicit empty delta on source bind.
                    access_ops_detail::emit_empty_tsd_delta(node);
                    return;
                }

                for (auto& [key_value, key_state] : state.keys) {
                    TSOutputView outer_ref_output = source_dict.get_ref(key_state.key_obj, state.requester);
                    TimeSeriesReference ref_value = access_ops_detail::resolve_nested_ref_output(outer_ref_output, node);

                    const bool key_exists = source_dict.contains(key_value.view());
                    if (key_exists) {
                        TSOutputView source_child = source_dict.at_key(key_value.view());
                        const bool source_child_modified = source_child && source_child.modified();
                        const bool changed =
                            !key_state.output_present ||
                            source_child_modified ||
                            outer_ref_output.modified() ||
                            access_ops_detail::ref_target_modified(ref_value, node) ||
                            !key_state.last_ref.has_value() ||
                            !(*key_state.last_ref == ref_value);
                        if (changed) {
                            auto out_key = output_dict.get_or_create(key_value.view());
                            apply_ref_payload(out_key.as_ts_view().view_data(), ref_value, node_time(node));
                            key_state.last_ref = ref_value;
                            key_state.output_present = true;
                        }
                    } else if (key_state.output_present) {
                        output_dict.remove(key_value.view());
                        key_state.output_present = false;
                        key_state.last_ref.reset();
                    }
                }
            }

            static void stop(Node& node, state& state) {
                release_all_refs(state, state.source_target, node);
                state.keys.clear();
                state.source_target.reset();
            }
        };

        struct KeysTsdAsTssSpec {
            static constexpr const char* py_factory_name = "op_keys_tsd_as_tss";

            static void eval(Node& node) {
                auto bundle = access_ops_detail::input_bundle(node);
                auto tsd = bundle.field("tsd");

                TimeSeriesReference output_ref = TimeSeriesReference::make();
                auto source_target = access_ops_detail::resolve_ref_tsd_target(tsd, node);
                if (source_target.has_value()) {
                    TSView source_view(*source_target, node_time_ptr(node));
                    TSView key_set_view = source_view.as_dict().key_set();
                    output_ref = TimeSeriesReference::make(key_set_view.view_data());
                }

                access_ops_detail::emit_ref(node, output_ref);
            }
        };

        struct KeysTsdAsSetSpec {
            static constexpr const char* py_factory_name = "op_keys_tsd_as_set";

            static void eval(Node& node) {
                auto bundle = access_ops_detail::input_bundle(node);
                auto tsd = bundle.field("tsd").as_dict();

                nb::set out;
                for (value::View key : tsd.keys()) {
                    nb::object py_key = key.to_python();
                    if (PySet_Add(out.ptr(), py_key.ptr()) < 0) {
                        nb::raise_python_error();
                    }
                }
                node.output().from_python(out);
            }
        };

        struct LenTsdSpec {
            static constexpr const char* py_factory_name = "op_len_tsd";

            static void eval(Node& node) {
                auto bundle = access_ops_detail::input_bundle(node);
                auto tsd = bundle.field("ts").as_dict();
                node.output().set_value(static_cast<int64_t>(tsd.count()));
            }
        };

        struct GetItemSeriesSpec {
            static constexpr const char* py_factory_name = "op_get_item_series";

            struct state {
                int64_t key;
            };

            static state make_state(Node& node) {
                const nb::dict& scalars = node.scalars();
                return {nb::cast<int64_t>(scalars["key"])};
            }

            static void eval(Node& node, state& state) {
                auto bundle = access_ops_detail::input_bundle(node);
                const nb::object series = access_ops_detail::python_field(bundle, "series");
                const nb::object key = nb::int_(state.key);
                nb::object value = nb::steal<nb::object>(PyObject_GetItem(series.ptr(), key.ptr()));
                if (!value.is_valid()) {
                    nb::raise_python_error();
                }
                access_ops_detail::emit_python(node, value);
            }
        };

        struct GetItemSeriesTsSpec {
            static constexpr const char* py_factory_name = "op_get_item_series_ts";

            static void eval(Node& node) {
                auto bundle = access_ops_detail::input_bundle(node);
                const nb::object series = access_ops_detail::python_field(bundle, "series");
                const nb::object key = access_ops_detail::python_field(bundle, "key");
                nb::object value = nb::steal<nb::object>(PyObject_GetItem(series.ptr(), key.ptr()));
                if (!value.is_valid()) {
                    nb::raise_python_error();
                }
                access_ops_detail::emit_python(node, value);
            }
        };

        template<typename T>
        struct GetattrJsonSpec {
            static void eval(Node& node) {
                auto bundle = access_ops_detail::input_bundle(node);
                const nb::object ts = access_ops_detail::python_field(bundle, "ts");
                const nb::object json_value = nb::cast<nb::object>(ts.attr("json"));
                node.output().from_python(json_value);
            }
        };

        struct GetattrJsonStrSpec : GetattrJsonSpec<std::string> {
            static constexpr const char* py_factory_name = "op_getattr_json_str";
        };

        struct GetattrJsonBoolSpec : GetattrJsonSpec<bool> {
            static constexpr const char* py_factory_name = "op_getattr_json_bool";
        };

        struct GetattrJsonIntSpec : GetattrJsonSpec<int64_t> {
            static constexpr const char* py_factory_name = "op_getattr_json_int";
        };

        struct GetattrJsonFloatSpec : GetattrJsonSpec<double> {
            static constexpr const char* py_factory_name = "op_getattr_json_float";
        };

        struct GetattrJsonObjSpec : GetattrJsonSpec<nb::object> {
            static constexpr const char* py_factory_name = "op_getattr_json_obj";
        };

        struct GetitemJsonStrSpec {
            static constexpr const char* py_factory_name = "op_getitem_json_str";

            struct state {
                nb::object json_cls;
                nb::object key;
            };

            static state make_state(Node& node) {
                const nb::dict& scalars = node.scalars();
                return {
                    access_ops_detail::json_class(),
                    nb::cast<nb::object>(scalars["key"]),
                };
            }

            static void eval(Node& node, state& state) {
                auto bundle = access_ops_detail::input_bundle(node);
                const nb::object ts = access_ops_detail::python_field(bundle, "ts");
                nb::object value;
                if (PyObject* raw = PyObject_CallMethod(ts.attr("json").ptr(), "get", "O", state.key.ptr())) {
                    value = nb::steal<nb::object>(raw);
                } else {
                    PyErr_Clear();
                    return;
                }
                if (value.is_none()) {
                    return;
                }
                access_ops_detail::emit_python(node, state.json_cls(value));
            }
        };

        struct GetitemJsonIntSpec {
            static constexpr const char* py_factory_name = "op_getitem_json_int";

            struct state {
                nb::object json_cls;
                nb::object key;
            };

            static state make_state(Node& node) {
                const nb::dict& scalars = node.scalars();
                return {
                    access_ops_detail::json_class(),
                    nb::cast<nb::object>(scalars["key"]),
                };
            }

            static void eval(Node& node, state& state) {
                auto bundle = access_ops_detail::input_bundle(node);
                const nb::object ts = access_ops_detail::python_field(bundle, "ts");
                nb::object value = nb::steal<nb::object>(PyObject_GetItem(ts.attr("json").ptr(), state.key.ptr()));
                if (!value.is_valid()) {
                    PyErr_Clear();
                    return;
                }
                if (value.is_none()) {
                    return;
                }
                access_ops_detail::emit_python(node, state.json_cls(value));
            }
        };
    }  // namespace ops
}  // namespace hgraph
