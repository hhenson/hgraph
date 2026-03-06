#pragma once

#include <hgraph/nodes/node_binding_utils.h>
#include <hgraph/types/node.h>

#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

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

            inline bool python_equal(const nb::object& lhs, const nb::object& rhs) {
                const int eq = PyObject_RichCompareBool(lhs.ptr(), rhs.ptr(), Py_EQ);
                if (eq < 0) {
                    nb::raise_python_error();
                }
                return eq == 1;
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

            static std::optional<ViewData> resolve_dict_target(const TSInputView& ts, const Node& node) {
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

            static void eval(Node& node, state& state) {
                auto bundle = access_ops_detail::input_bundle(node);
                auto ts = bundle.field("ts");
                auto key = bundle.field("key");

                const std::optional<ViewData> next_source = resolve_dict_target(ts, node);
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
