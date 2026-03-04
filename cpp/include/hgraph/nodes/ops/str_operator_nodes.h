#pragma once

#include <hgraph/types/node.h>
#include <hgraph/types/value/type_registry.h>

#include <stdexcept>
#include <string>
#include <string_view>

namespace hgraph {
    namespace ops {
        namespace str_ops_detail {
            inline TSBInputView require_input_bundle(Node& node) {
                auto input = node.input();
                if (!input) {
                    throw std::runtime_error("string operator requires TS input");
                }
                auto bundle = input.try_as_bundle();
                if (!bundle.has_value()) {
                    throw std::runtime_error("string operator requires bundle input");
                }
                return *bundle;
            }

            inline nb::str require_string_field(const TSBInputView& bundle, std::string_view field_name) {
                auto field = bundle.field(field_name);
                if (!field || !field.valid()) {
                    throw std::runtime_error(std::string("string operator missing valid field '") + std::string(field_name) + "'");
                }
                const value::View view = field.value();
                if (!view.valid()) {
                    throw std::runtime_error(
                        std::string("string operator field has unexpected type: '") + std::string(field_name) + "'");
                }

                if (!view.template is_scalar_type<nb::object>()) {
                    throw std::runtime_error(
                        std::string("string operator field has unexpected type: '") + std::string(field_name) + "'");
                }

                const nb::object obj = view.template as<nb::object>();
                if (!obj.is_valid() || !nb::isinstance<nb::str>(obj)) {
                    throw std::runtime_error(
                        std::string("string operator field has unexpected type: '") + std::string(field_name) + "'");
                }
                return nb::str(obj);
            }

            inline int64_t require_int_field(const TSBInputView& bundle, std::string_view field_name) {
                auto field = bundle.field(field_name);
                if (!field || !field.valid()) {
                    throw std::runtime_error(std::string("string operator missing valid field '") + std::string(field_name) + "'");
                }
                const value::View view = field.value();
                if (!view.valid() || !view.template is_scalar_type<int64_t>()) {
                    throw std::runtime_error(
                        std::string("string operator field has unexpected type: '") + std::string(field_name) + "'");
                }
                return view.template as<int64_t>();
            }

            inline void emit_string(Node& node, const nb::object& output_value) {
                auto output = node.output();
                if (!output) {
                    throw std::runtime_error("string operator requires TS output");
                }
                output.from_python(output_value);
            }
        }  // namespace str_ops_detail

        struct AddStrSpec {
            static constexpr const char* py_factory_name = "op_add_str";

            static void eval(Node& node) {
                auto bundle = str_ops_detail::require_input_bundle(node);
                const nb::str lhs = str_ops_detail::require_string_field(bundle, "lhs");
                const nb::str rhs = str_ops_detail::require_string_field(bundle, "rhs");
                const nb::str output(lhs + rhs);
                str_ops_detail::emit_string(node, output);
            }
        };

        struct MulStrsSpec {
            static constexpr const char* py_factory_name = "op_mul_strs";

            static void eval(Node& node) {
                auto bundle = str_ops_detail::require_input_bundle(node);
                const nb::str lhs = str_ops_detail::require_string_field(bundle, "lhs");
                const int64_t rhs = str_ops_detail::require_int_field(bundle, "rhs");
                const nb::str output(lhs * nb::int_(rhs));
                str_ops_detail::emit_string(node, output);
            }
        };
    }  // namespace ops
}  // namespace hgraph
