#pragma once

#include <hgraph/types/node.h>
#include <hgraph/types/value/type_registry.h>

namespace hgraph {
    namespace ops {
        namespace flow_control_ops_detail {
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
    }  // namespace ops
}  // namespace hgraph
