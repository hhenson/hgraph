#pragma once

#include <hgraph/types/node.h>
#include <hgraph/types/value/type_registry.h>

#include <chrono>

namespace hgraph {
    namespace ops {
        namespace ts_property_ops_detail {
            inline TSBInputView input_bundle(Node& node) {
                return *node.input().try_as_bundle();
            }

            inline TSInputView input_field(Node& node, std::string_view field_name) {
                return input_bundle(node).field(field_name);
            }

            inline void emit_bool(Node& node, bool value) {
                node.output().set_value(value::View(&value, value::scalar_type_meta<bool>()));
            }

            struct CmpResultConstants {
                nb::object eq;
                nb::object lt;
                nb::object gt;
            };

            inline const CmpResultConstants& cmp_result_constants() {
                static const CmpResultConstants cached = [] {
                    const nb::object cmp_result = nb::cast<nb::object>(nb::module_::import_("hgraph").attr("CmpResult"));
                    return CmpResultConstants{
                        nb::cast<nb::object>(cmp_result.attr("EQ")),
                        nb::cast<nb::object>(cmp_result.attr("LT")),
                        nb::cast<nb::object>(cmp_result.attr("GT")),
                    };
                }();
                return cached;
            }
        }  // namespace ts_property_ops_detail

        struct ModifiedImplSpec {
            static constexpr const char* py_factory_name = "op_modified_impl";

            static void start(Node& node) {
                ts_property_ops_detail::emit_bool(node, false);
            }

            static void eval(Node& node) {
                auto ts = ts_property_ops_detail::input_field(node, "ts");
                if (ts.modified()) {
                    node.scheduler()->schedule(MIN_TD, std::nullopt);
                    ts_property_ops_detail::emit_bool(node, true);
                    return;
                }
                if (node.has_scheduler() && node.scheduler()->is_scheduled_now()) {
                    ts_property_ops_detail::emit_bool(node, false);
                }
            }
        };

        struct LastModifiedTimeImplSpec {
            static constexpr const char* py_factory_name = "op_last_modified_time_impl";

            static void eval(Node& node) {
                const engine_time_t value = ts_property_ops_detail::input_field(node, "ts").last_modified_time();
                node.output().set_value(value::View(&value, value::scalar_type_meta<engine_time_t>()));
            }
        };

        struct LastModifiedDateImplSpec {
            static constexpr const char* py_factory_name = "op_last_modified_date_impl";

            static void eval(Node& node) {
                const engine_time_t value = ts_property_ops_detail::input_field(node, "ts").last_modified_time();
                const std::chrono::sys_days day = std::chrono::floor<std::chrono::days>(value);
                const engine_date_t date{day};
                node.output().set_value(value::View(&date, value::scalar_type_meta<engine_date_t>()));
            }
        };

        struct EvaluationTimeInRangeDateTimeSpec {
            static constexpr const char* py_factory_name = "op_evaluation_time_in_range_date_time";

            struct state {
                nb::object eq;
                nb::object lt;
                nb::object gt;
            };

            static state make_state(Node&) {
                const auto& constants = ts_property_ops_detail::cmp_result_constants();
                return {
                    constants.eq,
                    constants.lt,
                    constants.gt,
                };
            }

            static void eval(Node& node, state& state) {
                auto bundle = ts_property_ops_detail::input_bundle(node);
                const engine_time_t start = bundle.field("start_time").value().template as<engine_time_t>();
                const engine_time_t end = bundle.field("end_time").value().template as<engine_time_t>();

                if (start > end) {
                    PyErr_SetString(PyExc_ValueError, "Start time must be before end time");
                    nb::raise_python_error();
                }

                const engine_time_t et = node.graph()->evaluation_time();
                if (start <= et && et <= end) {
                    node.scheduler()->schedule(end + MIN_TD, std::string{"_next"});
                    node.output().from_python(state.eq);
                    return;
                }
                if (et < start) {
                    node.scheduler()->schedule(start, std::string{"_next"});
                    node.output().from_python(state.lt);
                    return;
                }
                node.scheduler()->un_schedule("_next");
                node.output().from_python(state.gt);
            }
        };
    }  // namespace ops
}  // namespace hgraph
