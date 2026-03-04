#pragma once

#include <hgraph/types/node.h>
#include <hgraph/types/value/type_registry.h>

#include <optional>
#include <string_view>

namespace hgraph {
    namespace ops {
        namespace str_ops_detail {
            inline TSBInputView require_input_bundle(Node& node) {
                return *node.input().try_as_bundle();
            }

            inline nb::str require_string_field(const TSBInputView& bundle, std::string_view field_name) {
                const nb::object obj = bundle.field(field_name).value().template as<nb::object>();
                return nb::borrow<nb::str>(obj);
            }

            inline int64_t require_int_field(const TSBInputView& bundle, std::string_view field_name) {
                return bundle.field(field_name).value().template as<int64_t>();
            }

            inline std::optional<int64_t> optional_int_field(const TSBInputView& bundle, std::string_view field_name) {
                auto field = bundle.field(field_name);
                if (!field || !field.valid()) {
                    return std::nullopt;
                }
                return field.value().template as<int64_t>();
            }

            inline void emit_string(Node& node, const nb::object& output_value) {
                node.output().from_python(output_value);
            }

            inline void emit_bool(Node& node, bool output_value) {
                node.output().set_value(value::View(&output_value, value::scalar_type_meta<bool>()));
            }

            inline bool string_contains(const nb::str& value, const nb::str& needle) {
                const int contains = PySequence_Contains(value.ptr(), needle.ptr());
                if (contains < 0) {
                    nb::raise_python_error();
                }
                return contains != 0;
            }

            inline nb::str string_substring(const nb::str& value, int64_t start, std::optional<int64_t> end) {
                const nb::object start_obj = nb::int_(start);
                const nb::object end_obj = end.has_value() ? nb::object(nb::int_(*end)) : nb::none();

                nb::object slice = nb::steal<nb::object>(PySlice_New(start_obj.ptr(), end_obj.ptr(), nullptr));
                if (!slice.is_valid()) {
                    nb::raise_python_error();
                }
                nb::object sliced = nb::steal<nb::object>(PyObject_GetItem(value.ptr(), slice.ptr()));
                if (!sliced.is_valid()) {
                    nb::raise_python_error();
                }
                return nb::str(sliced);
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

        struct ContainsStrSpec {
            static constexpr const char* py_factory_name = "op_contains_str";

            static void eval(Node& node) {
                auto bundle = str_ops_detail::require_input_bundle(node);
                const nb::str ts = str_ops_detail::require_string_field(bundle, "ts");
                const nb::str key = str_ops_detail::require_string_field(bundle, "key");
                str_ops_detail::emit_bool(node, str_ops_detail::string_contains(ts, key));
            }
        };

        struct SubstrDefaultSpec {
            static constexpr const char* py_factory_name = "op_substr_default";

            static void eval(Node& node) {
                auto bundle = str_ops_detail::require_input_bundle(node);
                const nb::str s = str_ops_detail::require_string_field(bundle, "s");
                const int64_t start = str_ops_detail::require_int_field(bundle, "start");
                const std::optional<int64_t> end = str_ops_detail::optional_int_field(bundle, "end");
                str_ops_detail::emit_string(node, str_ops_detail::string_substring(s, start, end));
            }
        };
    }  // namespace ops
}  // namespace hgraph
