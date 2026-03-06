#pragma once

#include <hgraph/types/node.h>

#include <stdexcept>
#include <string>
#include <string_view>

namespace hgraph {
    namespace ops {
        namespace enum_ops_detail {
            inline TSBInputView require_input_bundle(Node& node) {
                return *node.input().try_as_bundle();
            }

            inline nb::object require_enum_field(const TSBInputView& bundle, std::string_view field_name) {
                return bundle.field(field_name).value().template as<nb::object>();
            }

            template<typename T>
            inline void emit_scalar(Node& node, const T& output_value) {
                node.output().set_value(value::View(&output_value, value::scalar_type_meta<T>()));
            }

            inline nb::object require_enum_value_attr(const nb::object& enum_obj) {
                return nb::cast<nb::object>(enum_obj.attr("value"));
            }

            enum class CompareOp : uint8_t {
                Lt,
                Le,
                Gt,
                Ge,
            };

            template<CompareOp Op>
            inline bool compare_enum_values(const nb::object& lhs_enum, const nb::object& rhs_enum) {
                const nb::object lhs_value = require_enum_value_attr(lhs_enum);
                const nb::object rhs_value = require_enum_value_attr(rhs_enum);

                if constexpr (Op == CompareOp::Lt) {
                    return nb::cast<bool>(lhs_value.attr("__lt__")(rhs_value));
                } else if constexpr (Op == CompareOp::Le) {
                    return nb::cast<bool>(lhs_value.attr("__le__")(rhs_value));
                } else if constexpr (Op == CompareOp::Gt) {
                    return nb::cast<bool>(lhs_value.attr("__gt__")(rhs_value));
                } else {
                    return nb::cast<bool>(lhs_value.attr("__ge__")(rhs_value));
                }
            }
        }  // namespace enum_ops_detail

        struct EqEnumSpec {
            static constexpr const char* py_factory_name = "op_eq_enum";

            static void eval(Node& node) {
                auto bundle = enum_ops_detail::require_input_bundle(node);
                const nb::object lhs = enum_ops_detail::require_enum_field(bundle, "lhs");
                const nb::object rhs = enum_ops_detail::require_enum_field(bundle, "rhs");
                enum_ops_detail::emit_scalar<bool>(node, lhs.ptr() == rhs.ptr());
            }
        };

        struct LtEnumSpec {
            static constexpr const char* py_factory_name = "op_lt_enum";

            static void eval(Node& node) {
                auto bundle = enum_ops_detail::require_input_bundle(node);
                const nb::object lhs = enum_ops_detail::require_enum_field(bundle, "lhs");
                const nb::object rhs = enum_ops_detail::require_enum_field(bundle, "rhs");
                enum_ops_detail::emit_scalar<bool>(
                    node,
                    enum_ops_detail::compare_enum_values<enum_ops_detail::CompareOp::Lt>(lhs, rhs));
            }
        };

        struct LeEnumSpec {
            static constexpr const char* py_factory_name = "op_le_enum";

            static void eval(Node& node) {
                auto bundle = enum_ops_detail::require_input_bundle(node);
                const nb::object lhs = enum_ops_detail::require_enum_field(bundle, "lhs");
                const nb::object rhs = enum_ops_detail::require_enum_field(bundle, "rhs");
                enum_ops_detail::emit_scalar<bool>(
                    node,
                    enum_ops_detail::compare_enum_values<enum_ops_detail::CompareOp::Le>(lhs, rhs));
            }
        };

        struct GtEnumSpec {
            static constexpr const char* py_factory_name = "op_gt_enum";

            static void eval(Node& node) {
                auto bundle = enum_ops_detail::require_input_bundle(node);
                const nb::object lhs = enum_ops_detail::require_enum_field(bundle, "lhs");
                const nb::object rhs = enum_ops_detail::require_enum_field(bundle, "rhs");
                enum_ops_detail::emit_scalar<bool>(
                    node,
                    enum_ops_detail::compare_enum_values<enum_ops_detail::CompareOp::Gt>(lhs, rhs));
            }
        };

        struct GeEnumSpec {
            static constexpr const char* py_factory_name = "op_ge_enum";

            static void eval(Node& node) {
                auto bundle = enum_ops_detail::require_input_bundle(node);
                const nb::object lhs = enum_ops_detail::require_enum_field(bundle, "lhs");
                const nb::object rhs = enum_ops_detail::require_enum_field(bundle, "rhs");
                enum_ops_detail::emit_scalar<bool>(
                    node,
                    enum_ops_detail::compare_enum_values<enum_ops_detail::CompareOp::Ge>(lhs, rhs));
            }
        };

        struct MinEnumUnarySpec {
            static constexpr const char* py_factory_name = "op_min_enum_unary";

            static void eval(Node& node) {
                auto bundle = enum_ops_detail::require_input_bundle(node);
                const nb::object ts_enum = bundle.field("ts").to_python();

                if (!node.output().valid()) {
                    node.output().from_python(ts_enum);
                    return;
                }
                const nb::object out_enum = node.output().to_python();
                const nb::object ts_val = enum_ops_detail::require_enum_value_attr(ts_enum);
                const nb::object out_val = enum_ops_detail::require_enum_value_attr(out_enum);
                if (PyObject_RichCompareBool(ts_val.ptr(), out_val.ptr(), Py_LT) > 0) {
                    node.output().from_python(ts_enum);
                }
            }
        };

        struct MinEnumBinarySpec {
            static constexpr const char* py_factory_name = "op_min_enum_binary";

            static void eval(Node& node) {
                auto bundle = enum_ops_detail::require_input_bundle(node);
                auto lhs_field = bundle.field("lhs");
                auto rhs_field = bundle.field("rhs");
                const nb::object lhs = lhs_field.to_python();
                const nb::object rhs = rhs_field.to_python();

                if (lhs.is_none()) {
                    node.output().from_python(rhs);
                } else if (rhs.is_none()) {
                    node.output().from_python(lhs);
                } else {
                    const nb::object l_val = enum_ops_detail::require_enum_value_attr(lhs);
                    const nb::object r_val = enum_ops_detail::require_enum_value_attr(rhs);
                    if (PyObject_RichCompareBool(l_val.ptr(), r_val.ptr(), Py_LE) > 0) {
                        node.output().from_python(lhs);
                    } else {
                        node.output().from_python(rhs);
                    }
                }
            }
        };

        struct MaxEnumUnarySpec {
            static constexpr const char* py_factory_name = "op_max_enum_unary";

            static void eval(Node& node) {
                auto bundle = enum_ops_detail::require_input_bundle(node);
                const nb::object ts_enum = bundle.field("ts").to_python();

                if (!node.output().valid()) {
                    node.output().from_python(ts_enum);
                    return;
                }
                const nb::object out_enum = node.output().to_python();
                const nb::object ts_val = enum_ops_detail::require_enum_value_attr(ts_enum);
                const nb::object out_val = enum_ops_detail::require_enum_value_attr(out_enum);
                if (PyObject_RichCompareBool(ts_val.ptr(), out_val.ptr(), Py_GT) > 0) {
                    node.output().from_python(ts_enum);
                }
            }
        };

        struct MaxEnumBinarySpec {
            static constexpr const char* py_factory_name = "op_max_enum_binary";

            static void eval(Node& node) {
                auto bundle = enum_ops_detail::require_input_bundle(node);
                auto lhs_field = bundle.field("lhs");
                auto rhs_field = bundle.field("rhs");
                const nb::object lhs = lhs_field.to_python();
                const nb::object rhs = rhs_field.to_python();

                if (lhs.is_none()) {
                    node.output().from_python(rhs);
                } else if (rhs.is_none()) {
                    node.output().from_python(lhs);
                } else {
                    const nb::object l_val = enum_ops_detail::require_enum_value_attr(lhs);
                    const nb::object r_val = enum_ops_detail::require_enum_value_attr(rhs);
                    if (PyObject_RichCompareBool(l_val.ptr(), r_val.ptr(), Py_GE) > 0) {
                        node.output().from_python(lhs);
                    } else {
                        node.output().from_python(rhs);
                    }
                }
            }
        };

        struct GetattrEnumNameSpec {
            static constexpr const char* py_factory_name = "op_getattr_enum_name";

            static void eval(Node& node) {
                auto bundle = enum_ops_detail::require_input_bundle(node);
                const nb::object enum_obj = enum_ops_detail::require_enum_field(bundle, "ts");
                const nb::dict& scalars = node.scalars();
                const std::string attribute = nb::cast<std::string>(nb::cast<nb::object>(scalars["attribute"]));
                if (attribute != "name") {
                    throw std::runtime_error("Cannot get " + attribute + " from TS[Enum]");
                }
                const nb::object name = nb::cast<nb::object>(enum_obj.attr("name"));
                node.output().from_python(name);
            }
        };
    }  // namespace ops
}  // namespace hgraph
