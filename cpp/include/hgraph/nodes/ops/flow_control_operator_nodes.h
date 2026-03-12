#pragma once

#include <hgraph/types/node.h>
#include <hgraph/types/value/type_registry.h>

namespace hgraph {
    namespace ops {
        namespace flow_control_ops_detail {
            struct CmpResultConstants {
                nb::object lt;
                nb::object eq;
                nb::object gt;
            };

            inline const CmpResultConstants& cmp_result_constants() {
                static const CmpResultConstants cached = [] {
                    const nb::object cmp_result = nb::cast<nb::object>(nb::module_::import_("hgraph").attr("CmpResult"));
                    return CmpResultConstants{
                        nb::cast<nb::object>(cmp_result.attr("LT")),
                        nb::cast<nb::object>(cmp_result.attr("EQ")),
                        nb::cast<nb::object>(cmp_result.attr("GT")),
                    };
                }();
                return cached;
            }

            inline TSBInputView require_input_bundle(Node& node) {
                return *node.input().try_as_bundle();
            }

            inline bool input_value_as_bool(const TSInputView& input) {
                return input.valid() && input.value().template as<bool>();
            }

            inline void emit_bool(Node& node, bool value) {
                node.output().set_value(value::View(&value, value::scalar_type_meta<bool>()));
            }

            inline void emit_int(Node& node, int64_t value) {
                node.output().set_value(value::View(&value, value::scalar_type_meta<int64_t>()));
            }

            inline bool output_differs(const TSOutputView& output, const value::View& candidate) {
                return !output.valid() || output.value() != candidate;
            }

            inline bool python_not_equal(const nb::object& lhs, const nb::object& rhs) {
                const int eq = PyObject_RichCompareBool(lhs.ptr(), rhs.ptr(), Py_EQ);
                if (eq < 0) {
                    nb::raise_python_error();
                }
                return eq == 0;
            }

            inline bool output_ref_differs(Node& node, const TSInputView& candidate) {
                const nb::object current = node.output().to_python();
                const nb::object next = candidate.to_python();
                return python_not_equal(current, next);
            }

            inline void emit_ref(Node& node, const TSInputView& candidate) {
                node.output().copy_from_input(candidate);
            }
        }  // namespace flow_control_ops_detail

        struct AllDefaultSpec {
            static constexpr const char* py_factory_name = "op_all_default";

            static void eval(Node& node) {
                auto bundle = flow_control_ops_detail::require_input_bundle(node);
                auto args = bundle.field("args").as_list();

                bool out = true;
                for (size_t i = 0; i < args.count(); ++i) {
                    if (!flow_control_ops_detail::input_value_as_bool(args.at(i))) {
                        out = false;
                        break;
                    }
                }
                flow_control_ops_detail::emit_bool(node, out);
            }
        };

        struct AllTsdSpec {
            static constexpr const char* py_factory_name = "op_all_tsd";

            static void eval(Node& node) {
                auto bundle = flow_control_ops_detail::require_input_bundle(node);
                auto arg = bundle.field("arg").as_dict();

                bool has_false = false;
                for (TSInputView v : arg.modified_values()) {
                    if (!flow_control_ops_detail::input_value_as_bool(v)) {
                        has_false = true;
                        break;
                    }
                }
                if (has_false) {
                    flow_control_ops_detail::emit_bool(node, false);
                    return;
                }

                auto out = node.output();
                if (out.valid()) {
                    if (out.value().template as<bool>()) {
                        flow_control_ops_detail::emit_bool(node, true);
                        return;
                    }
                } else {
                    flow_control_ops_detail::emit_bool(node, true);
                    return;
                }

                bool all_true = true;
                for (TSInputView v : arg.values()) {
                    if (!flow_control_ops_detail::input_value_as_bool(v)) {
                        all_true = false;
                        break;
                    }
                }
                flow_control_ops_detail::emit_bool(node, all_true);
            }
        };

        struct AnyDefaultSpec {
            static constexpr const char* py_factory_name = "op_any_default";

            static void eval(Node& node) {
                auto bundle = flow_control_ops_detail::require_input_bundle(node);
                auto args = bundle.field("args").as_list();

                bool out = false;
                for (size_t i = 0; i < args.count(); ++i) {
                    if (flow_control_ops_detail::input_value_as_bool(args.at(i))) {
                        out = true;
                        break;
                    }
                }
                flow_control_ops_detail::emit_bool(node, out);
            }
        };

        struct AnyTsdSpec {
            static constexpr const char* py_factory_name = "op_any_tsd";

            static void eval(Node& node) {
                auto bundle = flow_control_ops_detail::require_input_bundle(node);
                auto arg = bundle.field("arg").as_dict();

                bool has_true = false;
                for (TSInputView v : arg.modified_values()) {
                    if (flow_control_ops_detail::input_value_as_bool(v)) {
                        has_true = true;
                        break;
                    }
                }
                if (has_true) {
                    flow_control_ops_detail::emit_bool(node, true);
                    return;
                }

                auto out = node.output();
                if (out.valid()) {
                    if (!out.value().template as<bool>()) {
                        flow_control_ops_detail::emit_bool(node, false);
                        return;
                    }
                } else {
                    flow_control_ops_detail::emit_bool(node, false);
                    return;
                }

                bool any_true = false;
                for (TSInputView v : arg.values()) {
                    if (flow_control_ops_detail::input_value_as_bool(v)) {
                        any_true = true;
                        break;
                    }
                }
                flow_control_ops_detail::emit_bool(node, any_true);
            }
        };

        struct MergeTsScalarSpec {
            static constexpr const char* py_factory_name = "op_merge_ts_scalar";

            static void eval(Node& node) {
                auto bundle = flow_control_ops_detail::require_input_bundle(node);
                auto tsl = bundle.field("tsl").as_list();

                if (tsl.modified()) {
                    for (TSInputView v : tsl.modified_values()) {
                        if (v.valid()) {
                            node.output().set_value(v.value());
                            return;
                        }
                    }
                    return;
                }

                bool has_candidate = false;
                engine_time_t last_modified = MIN_DT;
                value::View candidate;
                for (size_t i = 0; i < tsl.count(); ++i) {
                    TSInputView ts = tsl.at(i);
                    if (ts.valid() && ts.last_modified_time() > last_modified) {
                        last_modified = ts.last_modified_time();
                        candidate = ts.value();
                        has_candidate = true;
                    }
                }

                if (!has_candidate) {
                    return;
                }

                auto out = node.output();
                if (!out.valid() || out.value() != candidate) {
                    out.set_value(candidate);
                }
            }
        };

        struct IndexOfImplSpec {
            static constexpr const char* py_factory_name = "op_index_of_impl";

            static void eval(Node& node) {
                auto bundle = flow_control_ops_detail::require_input_bundle(node);
                auto ts = bundle.field("ts").as_list();
                TSInputView item = bundle.field("item");
                if (!item.valid()) {
                    flow_control_ops_detail::emit_int(node, -1);
                    return;
                }

                const value::View item_value = item.value();
                for (size_t i = 0; i < ts.count(); ++i) {
                    TSInputView v = ts.at(i);
                    if (v.valid() && v.value() == item_value) {
                        flow_control_ops_detail::emit_int(node, static_cast<int64_t>(i));
                        return;
                    }
                }

                flow_control_ops_detail::emit_int(node, -1);
            }
        };

        struct IfTrueImplSpec {
            static constexpr const char* py_factory_name = "op_if_true_impl";

            static void eval(Node& node) {
                auto bundle = flow_control_ops_detail::require_input_bundle(node);
                auto condition = bundle.field("condition");
                if (!condition.valid() || !condition.value().template as<bool>()) {
                    return;
                }

                const nb::dict& scalars = node.scalars();
                const bool tick_once_only = scalars.contains("tick_once_only")
                    ? nb::cast<bool>(scalars["tick_once_only"])
                    : false;
                if (tick_once_only) {
                    condition.make_passive();
                }

                flow_control_ops_detail::emit_bool(node, true);
            }
        };

        struct IfCmpImplSpec {
            static constexpr const char* py_factory_name = "op_if_cmp_impl";

            static void eval(Node& node) {
                auto bundle = flow_control_ops_detail::require_input_bundle(node);
                auto cmp = bundle.field("cmp");
                if (!cmp.valid()) {
                    return;
                }

                TSInputView selected;
                const nb::object cmp_value = cmp.value().template as<nb::object>();
                const auto& cmp_result = flow_control_ops_detail::cmp_result_constants();
                if (cmp_value.ptr() == cmp_result.lt.ptr()) {
                    selected = bundle.field("lt");
                } else if (cmp_value.ptr() == cmp_result.eq.ptr()) {
                    selected = bundle.field("eq");
                } else if (cmp_value.ptr() == cmp_result.gt.ptr()) {
                    selected = bundle.field("gt");
                } else {
                    return;
                }

                if (!selected.valid()) {
                    return;
                }

                if (flow_control_ops_detail::output_ref_differs(node, selected)) {
                    flow_control_ops_detail::emit_ref(node, selected);
                }
            }
        };

        struct IfThenElseImplSpec {
            static constexpr const char* py_factory_name = "op_if_then_else_impl";

            static void eval(Node& node) {
                auto bundle = flow_control_ops_detail::require_input_bundle(node);
                auto condition = bundle.field("condition");
                if (!condition.valid()) {
                    return;
                }

                auto true_value = bundle.field("true_value");
                auto false_value = bundle.field("false_value");
                const bool condition_value = condition.value().template as<bool>();

                if (condition.modified()) {
                    if (condition_value) {
                        if (true_value.valid()) {
                            if (flow_control_ops_detail::output_ref_differs(node, true_value)) {
                                flow_control_ops_detail::emit_ref(node, true_value);
                            }
                        }
                    } else {
                        if (false_value.valid()) {
                            if (flow_control_ops_detail::output_ref_differs(node, false_value)) {
                                flow_control_ops_detail::emit_ref(node, false_value);
                            }
                        }
                    }
                }

                if (condition_value && true_value.modified()) {
                    if (flow_control_ops_detail::output_ref_differs(node, true_value)) {
                        flow_control_ops_detail::emit_ref(node, true_value);
                    }
                    return;
                }

                if (!condition_value && false_value.modified()) {
                    if (flow_control_ops_detail::output_ref_differs(node, false_value)) {
                        flow_control_ops_detail::emit_ref(node, false_value);
                    }
                }
            }
        };
    }  // namespace ops
}  // namespace hgraph
