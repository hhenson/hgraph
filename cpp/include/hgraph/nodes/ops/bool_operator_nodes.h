#pragma once

#include <hgraph/types/node.h>
#include <hgraph/types/value/type_registry.h>

#include <string_view>

namespace hgraph {
    namespace ops {
        namespace bool_ops_detail {
            inline TSBInputView require_input_bundle(Node& node) {
                return *node.input().try_as_bundle();
            }

            inline bool require_bool_field(const TSBInputView& bundle, std::string_view field_name) {
                return bundle.field(field_name).value().template as<bool>();
            }

            inline void emit_bool(Node& node, bool output_value) {
                node.output().set_value(value::View(&output_value, value::scalar_type_meta<bool>()));
            }
        }  // namespace bool_ops_detail

        struct AndBooleansSpec {
            static constexpr const char* py_factory_name = "op_and_booleans";

            static void eval(Node& node) {
                auto bundle = bool_ops_detail::require_input_bundle(node);
                const bool lhs = bool_ops_detail::require_bool_field(bundle, "lhs");
                const bool rhs = bool_ops_detail::require_bool_field(bundle, "rhs");
                bool_ops_detail::emit_bool(node, lhs && rhs);
            }
        };

        struct OrBooleansSpec {
            static constexpr const char* py_factory_name = "op_or_booleans";

            static void eval(Node& node) {
                auto bundle = bool_ops_detail::require_input_bundle(node);
                const bool lhs = bool_ops_detail::require_bool_field(bundle, "lhs");
                const bool rhs = bool_ops_detail::require_bool_field(bundle, "rhs");
                bool_ops_detail::emit_bool(node, lhs || rhs);
            }
        };

        struct NotBooleanSpec {
            static constexpr const char* py_factory_name = "op_not_boolean";

            static void eval(Node& node) {
                auto bundle = bool_ops_detail::require_input_bundle(node);
                const bool value = bool_ops_detail::require_bool_field(bundle, "ts");
                bool_ops_detail::emit_bool(node, !value);
            }
        };
    }  // namespace ops
}  // namespace hgraph
