#pragma once

#include <hgraph/types/node.h>

#include <iostream>
#include <string>

namespace hgraph {
    namespace ops {
        namespace graph_ops_detail {
            inline TSBInputView require_input_bundle(Node& node) {
                return *node.input().try_as_bundle();
            }
        }  // namespace graph_ops_detail

        struct AssertDefaultSpec {
            static constexpr const char* py_factory_name = "op_assert_default";

            struct state {
                std::string error_msg;
            };

            static state make_state(Node& node) {
                const nb::dict& scalars = node.scalars();
                return {nb::cast<std::string>(nb::cast<nb::object>(scalars["error_msg"]))};
            }

            static void eval(Node& node, state& s) {
                auto bundle = graph_ops_detail::require_input_bundle(node);
                const bool condition = bundle.field("condition").value().template as<bool>();
                if (!condition) {
                    PyErr_SetString(PyExc_AssertionError, s.error_msg.c_str());
                    nb::raise_python_error();
                }
            }
        };

        struct AssertDefaultTsSpec {
            static constexpr const char* py_factory_name = "op_assert_default_ts";

            static void eval(Node& node) {
                auto bundle = graph_ops_detail::require_input_bundle(node);
                auto condition = bundle.field("condition");
                if (condition.modified() && !condition.value().template as<bool>()) {
                    auto error_msg = bundle.field("error_msg");
                    const std::string msg = nb::cast<std::string>(error_msg.to_python());
                    PyErr_SetString(PyExc_AssertionError, msg.c_str());
                    nb::raise_python_error();
                }
            }
        };

        struct PrintSpec {
            static constexpr const char* py_factory_name = "op__print";

            struct state {
                bool std_out;
                nb::object py_print;
                nb::object py_stderr;
            };

            static state make_state(Node& node) {
                const nb::dict& scalars = node.scalars();
                return {
                    scalars.contains("std_out") ? nb::cast<bool>(scalars["std_out"]) : true,
                    nb::module_::import_("builtins").attr("print"),
                    nb::module_::import_("sys").attr("stderr"),
                };
            }

            static void eval(Node& node, state& s) {
                auto bundle = graph_ops_detail::require_input_bundle(node);
                const nb::object value = bundle.field("ts").to_python();
                if (s.std_out) {
                    s.py_print(value);
                } else {
                    s.py_print(value, nb::arg("file") = s.py_stderr);
                }
            }
        };

        struct LogSpec {
            static constexpr const char* py_factory_name = "op__log";

            struct state {
                int64_t level;
                bool final_value;
                nb::object logger;
            };

            static state make_state(Node& node) {
                const nb::dict& scalars = node.scalars();
                const int64_t level = nb::cast<int64_t>(scalars["level"]);
                const bool final_value = scalars.contains("final_value")
                    ? nb::cast<bool>(scalars["final_value"])
                    : false;
                nb::object logger = nb::cast<nb::object>(scalars["logger"]);
                return {level, final_value, logger};
            }

            static void eval(Node& node, state& s) {
                auto bundle = graph_ops_detail::require_input_bundle(node);
                auto ts = bundle.field("ts");
                const nb::object last_mod = nb::cast(ts.last_modified_time());
                const nb::object val = ts.to_python();
                s.logger.attr("log")(s.level, "[%s] %s", last_mod, val);
            }

            static void stop(Node& node, state& s) {
                if (!s.final_value) {
                    return;
                }
                // raw_ts is a REF - need Python-level access for ref dereferencing
                auto bundle = graph_ops_detail::require_input_bundle(node);
                auto raw_ts = bundle.field("raw_ts");
                if (!raw_ts.valid()) {
                    return;
                }
                // Use Python-level ref protocol for the stop handler
                const nb::object raw_ts_py = raw_ts.to_python();
                // raw_ts is a TimeSeriesReference value; check has_output and get output
                if (!nb::cast<bool>(nb::getattr(raw_ts_py, "has_output"))) {
                    return;
                }
                const nb::object out = nb::getattr(raw_ts_py, "output");
                const nb::object last_mod = nb::getattr(out, "last_modified_time");
                const nb::object val = nb::getattr(out, "value");
                s.logger.attr("log")(s.level, "[%s] Final value: %s", last_mod, val);
            }
        };

        struct DebugPrintImplSpec {
            static constexpr const char* py_factory_name = "op_debug_print_impl";

            struct state {
                std::string label;
                bool print_delta;
                int64_t sample;
                int64_t count;
                nb::object py_print;
                nb::object py_format;
            };

            static state make_state(Node& node) {
                const nb::dict& scalars = node.scalars();
                nb::object builtins = nb::module_::import_("builtins");
                nb::object fmt = nb::str("[{}][{}]{} {}: {}");
                return {
                    nb::cast<std::string>(nb::cast<nb::object>(scalars["label"])),
                    scalars.contains("print_delta") ? nb::cast<bool>(scalars["print_delta"]) : true,
                    scalars.contains("sample") ? nb::cast<int64_t>(scalars["sample"]) : -1,
                    0,
                    builtins.attr("print"),
                    fmt,
                };
            }

            static void eval(Node& node, state& s) {
                s.count++;
                if (s.sample >= 2 && (s.count % s.sample) != 0) {
                    return;
                }

                auto bundle = graph_ops_detail::require_input_bundle(node);
                auto ts = bundle.field("ts");

                nb::object value;
                if (s.print_delta) {
                    value = ts.delta_to_python();
                } else {
                    value = ts.to_python();
                }

                // Get clock from graph
                auto g = node.graph();
                const engine_time_t eval_time = g->evaluation_time();
                // Use the evaluation clock for wall-clock "now"
                auto clock = g->evaluation_clock();

                nb::object py_now = nb::cast(clock->now());
                nb::object py_et = nb::cast(eval_time);

                nb::object count_str;
                if (s.sample > 1) {
                    count_str = nb::str("[{}]").attr("format")(s.count);
                } else {
                    count_str = nb::str("");
                }

                s.py_print(s.py_format.attr("format")(py_now, py_et, count_str, s.label, value));
            }
        };
    }  // namespace ops
}  // namespace hgraph
