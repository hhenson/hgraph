#pragma once

#include <hgraph/types/node.h>
#include <hgraph/types/value/type_registry.h>

#include <cmath>
#include <stdexcept>
#include <string>
#include <string_view>

namespace hgraph {
    namespace ops {
        namespace number_ops_detail {
            inline TSBInputView require_input_bundle(Node& node) {
                auto input = node.input();
                if (!input) {
                    throw std::runtime_error("number operator requires TS input");
                }
                auto bundle = input.try_as_bundle();
                if (!bundle.has_value()) {
                    throw std::runtime_error("number operator requires bundle input");
                }
                return *bundle;
            }

            template<typename T>
            inline T require_scalar_field(const TSBInputView& bundle, std::string_view field_name) {
                auto field = bundle.field(field_name);
                if (!field || !field.valid()) {
                    throw std::runtime_error(std::string("number operator missing valid field '") + std::string(field_name) + "'");
                }

                const value::View view = field.value();
                if (!view.valid() || !view.template is_scalar_type<T>()) {
                    throw std::runtime_error(std::string("number operator field has unexpected type: '") + std::string(field_name) + "'");
                }
                return view.template as<T>();
            }

            template<typename T>
            inline void emit_scalar(Node& node, const T& output_value) {
                auto output = node.output();
                if (!output) {
                    throw std::runtime_error("number operator requires TS output");
                }
                output.set_value(value::View(&output_value, value::scalar_type_meta<T>()));
            }

            struct Add {
                template<typename L, typename R>
                static auto apply(L lhs, R rhs) {
                    return lhs + rhs;
                }
            };

            struct Sub {
                template<typename L, typename R>
                static auto apply(L lhs, R rhs) {
                    return lhs - rhs;
                }
            };

            struct Mul {
                template<typename L, typename R>
                static auto apply(L lhs, R rhs) {
                    return lhs * rhs;
                }
            };

            template<typename L, typename R, typename Out, typename Op>
            struct BinaryScalarSpec {
                static void eval(Node& node) {
                    auto bundle = require_input_bundle(node);
                    const L lhs_value = require_scalar_field<L>(bundle, "lhs");
                    const R rhs_value = require_scalar_field<R>(bundle, "rhs");
                    const Out output_value = static_cast<Out>(Op::apply(lhs_value, rhs_value));
                    emit_scalar<Out>(node, output_value);
                }
            };

            template<typename L, typename R>
            struct EqWithEpsilonSpec {
                static void eval(Node& node) {
                    auto bundle = require_input_bundle(node);
                    const L lhs_value = require_scalar_field<L>(bundle, "lhs");
                    const R rhs_value = require_scalar_field<R>(bundle, "rhs");
                    const double epsilon_value = require_scalar_field<double>(bundle, "epsilon");

                    const double diff = static_cast<double>(rhs_value) - static_cast<double>(lhs_value);
                    const bool output_value = (-epsilon_value <= diff) && (diff <= epsilon_value);
                    emit_scalar<bool>(node, output_value);
                }
            };
        }  // namespace number_ops_detail

        struct AddFloatToIntSpec
            : number_ops_detail::BinaryScalarSpec<int64_t, double, double, number_ops_detail::Add> {
            static constexpr const char* py_factory_name = "op_add_float_to_int";
        };

        struct AddIntToFloatSpec
            : number_ops_detail::BinaryScalarSpec<double, int64_t, double, number_ops_detail::Add> {
            static constexpr const char* py_factory_name = "op_add_int_to_float";
        };

        struct SubIntFromFloatSpec
            : number_ops_detail::BinaryScalarSpec<double, int64_t, double, number_ops_detail::Sub> {
            static constexpr const char* py_factory_name = "op_sub_int_from_float";
        };

        struct SubFloatFromIntSpec
            : number_ops_detail::BinaryScalarSpec<int64_t, double, double, number_ops_detail::Sub> {
            static constexpr const char* py_factory_name = "op_sub_float_from_int";
        };

        struct MulFloatAndIntSpec
            : number_ops_detail::BinaryScalarSpec<double, int64_t, double, number_ops_detail::Mul> {
            static constexpr const char* py_factory_name = "op_mul_float_and_int";
        };

        struct MulIntAndFloatSpec
            : number_ops_detail::BinaryScalarSpec<int64_t, double, double, number_ops_detail::Mul> {
            static constexpr const char* py_factory_name = "op_mul_int_and_float";
        };

        struct EqFloatIntSpec
            : number_ops_detail::EqWithEpsilonSpec<double, int64_t> {
            static constexpr const char* py_factory_name = "op_eq_float_int";
        };

        struct EqIntFloatSpec
            : number_ops_detail::EqWithEpsilonSpec<int64_t, double> {
            static constexpr const char* py_factory_name = "op_eq_int_float";
        };

        struct EqFloatFloatSpec
            : number_ops_detail::EqWithEpsilonSpec<double, double> {
            static constexpr const char* py_factory_name = "op_eq_float_float";
        };

        struct LnImplSpec {
            static constexpr const char* py_factory_name = "op_ln_impl";

            static void eval(Node& node) {
                auto bundle = number_ops_detail::require_input_bundle(node);
                const double ts_value = number_ops_detail::require_scalar_field<double>(bundle, "ts");
                const double output_value = std::log(ts_value);
                number_ops_detail::emit_scalar<double>(node, output_value);
            }
        };
    }  // namespace ops
}  // namespace hgraph
