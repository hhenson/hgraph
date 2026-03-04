#pragma once

#include <hgraph/types/node.h>
#include <hgraph/types/value/type_registry.h>

#include <cmath>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>

namespace hgraph {
    namespace ops {
        namespace number_ops_detail {
            enum class DivideByZeroMode : uint8_t {
                Error,
                Nan,
                Inf,
                None,
                Zero,
                One,
            };

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

            inline double require_numeric_field_as_double(const TSBInputView& bundle, std::string_view field_name) {
                auto field = bundle.field(field_name);
                if (!field || !field.valid()) {
                    throw std::runtime_error(std::string("number operator missing valid field '") + std::string(field_name) + "'");
                }

                const value::View view = field.value();
                if (!view.valid()) {
                    throw std::runtime_error(std::string("number operator missing valid field '") + std::string(field_name) + "'");
                }

                if (view.template is_scalar_type<double>()) {
                    return view.template as<double>();
                }
                if (view.template is_scalar_type<int64_t>()) {
                    return static_cast<double>(view.template as<int64_t>());
                }

                throw std::runtime_error(std::string("number operator field has unexpected type: '") + std::string(field_name) + "'");
            }

            template<typename T>
            inline void emit_scalar(Node& node, const T& output_value) {
                auto output = node.output();
                if (!output) {
                    throw std::runtime_error("number operator requires TS output");
                }
                output.set_value(value::View(&output_value, value::scalar_type_meta<T>()));
            }

            inline DivideByZeroMode parse_divide_by_zero_mode(const nb::object& mode_obj) {
                if (!mode_obj.is_valid() || mode_obj.is_none()) {
                    return DivideByZeroMode::Error;
                }

                try {
                    if (nb::hasattr(mode_obj, "name")) {
                        const std::string name = nb::cast<std::string>(nb::str(mode_obj.attr("name")));
                        if (name == "ERROR") {
                            return DivideByZeroMode::Error;
                        }
                        if (name == "NAN") {
                            return DivideByZeroMode::Nan;
                        }
                        if (name == "INF") {
                            return DivideByZeroMode::Inf;
                        }
                        if (name == "NONE") {
                            return DivideByZeroMode::None;
                        }
                        if (name == "ZERO") {
                            return DivideByZeroMode::Zero;
                        }
                        if (name == "ONE") {
                            return DivideByZeroMode::One;
                        }
                    }
                } catch (...) {
                }

                throw std::runtime_error("Unsupported divide_by_zero mode");
            }

            inline DivideByZeroMode divide_by_zero_mode_from_scalars(Node& node) {
                const nb::dict& scalars = node.scalars();
                if (!scalars.contains("divide_by_zero")) {
                    return DivideByZeroMode::Error;
                }
                return parse_divide_by_zero_mode(nb::cast<nb::object>(scalars["divide_by_zero"]));
            }

            template<typename T>
            inline bool emit_float_divide_by_zero_policy(Node& node, DivideByZeroMode mode) {
                switch (mode) {
                    case DivideByZeroMode::Nan:
                        emit_scalar<T>(node, std::numeric_limits<T>::quiet_NaN());
                        return true;
                    case DivideByZeroMode::Inf:
                        emit_scalar<T>(node, std::numeric_limits<T>::infinity());
                        return true;
                    case DivideByZeroMode::None:
                        return false;
                    case DivideByZeroMode::Zero:
                        emit_scalar<T>(node, static_cast<T>(0.0));
                        return true;
                    case DivideByZeroMode::One:
                        emit_scalar<T>(node, static_cast<T>(1.0));
                        return true;
                    case DivideByZeroMode::Error:
                    default:
                        throw std::runtime_error("division by zero");
                }
            }

            inline bool emit_int_divide_by_zero_policy(Node& node, DivideByZeroMode mode) {
                switch (mode) {
                    case DivideByZeroMode::None:
                        return false;
                    case DivideByZeroMode::Zero:
                        emit_scalar<int64_t>(node, 0);
                        return true;
                    case DivideByZeroMode::One:
                        emit_scalar<int64_t>(node, 1);
                        return true;
                    case DivideByZeroMode::Error:
                    case DivideByZeroMode::Nan:
                    case DivideByZeroMode::Inf:
                    default:
                        throw std::runtime_error("division by zero");
                }
            }

            inline double python_mod(double lhs, double rhs) {
                return lhs - std::floor(lhs / rhs) * rhs;
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

            template<typename L, typename R>
            struct DivSpec {
                struct state {
                    DivideByZeroMode divide_by_zero_mode{DivideByZeroMode::Error};
                };

                static state make_state(Node& node) {
                    return {divide_by_zero_mode_from_scalars(node)};
                }

                static void eval(Node& node, state& state) {
                    auto bundle = require_input_bundle(node);
                    const double lhs_value = require_numeric_field_as_double(bundle, "lhs");
                    const double rhs_value = require_numeric_field_as_double(bundle, "rhs");
                    if (rhs_value == 0.0) {
                        emit_float_divide_by_zero_policy<double>(node, state.divide_by_zero_mode);
                        return;
                    }
                    emit_scalar<double>(node, lhs_value / rhs_value);
                }
            };

            template<typename L, typename R>
            struct FloorDivFloatSpec {
                struct state {
                    DivideByZeroMode divide_by_zero_mode{DivideByZeroMode::Error};
                };

                static state make_state(Node& node) {
                    return {divide_by_zero_mode_from_scalars(node)};
                }

                static void eval(Node& node, state& state) {
                    auto bundle = require_input_bundle(node);
                    const double lhs_value = require_numeric_field_as_double(bundle, "lhs");
                    const double rhs_value = require_numeric_field_as_double(bundle, "rhs");
                    if (rhs_value == 0.0) {
                        emit_float_divide_by_zero_policy<double>(node, state.divide_by_zero_mode);
                        return;
                    }

                    const double quotient = std::floor(lhs_value / rhs_value);
                    emit_scalar<double>(node, quotient);
                }
            };

            struct FloorDivIntSpec {
                struct state {
                    DivideByZeroMode divide_by_zero_mode{DivideByZeroMode::Error};
                };

                static state make_state(Node& node) {
                    return {divide_by_zero_mode_from_scalars(node)};
                }

                static void eval(Node& node, state& state) {
                    auto bundle = require_input_bundle(node);
                    const int64_t lhs_value = require_scalar_field<int64_t>(bundle, "lhs");
                    const int64_t rhs_value = require_scalar_field<int64_t>(bundle, "rhs");
                    if (rhs_value == 0) {
                        emit_int_divide_by_zero_policy(node, state.divide_by_zero_mode);
                        return;
                    }

                    int64_t quotient = lhs_value / rhs_value;
                    const int64_t remainder = lhs_value % rhs_value;
                    if (remainder != 0 && ((remainder > 0) != (rhs_value > 0))) {
                        --quotient;
                    }
                    emit_scalar<int64_t>(node, quotient);
                }
            };

            template<typename L, typename R>
            struct ModFloatSpec {
                struct state {
                    DivideByZeroMode divide_by_zero_mode{DivideByZeroMode::Error};
                };

                static state make_state(Node& node) {
                    return {divide_by_zero_mode_from_scalars(node)};
                }

                static void eval(Node& node, state& state) {
                    auto bundle = require_input_bundle(node);
                    const double lhs_value = require_numeric_field_as_double(bundle, "lhs");
                    const double rhs_value = require_numeric_field_as_double(bundle, "rhs");
                    if (rhs_value == 0.0) {
                        switch (state.divide_by_zero_mode) {
                            case DivideByZeroMode::Nan:
                                emit_scalar<double>(node, std::numeric_limits<double>::quiet_NaN());
                                return;
                            case DivideByZeroMode::Inf:
                                emit_scalar<double>(node, std::numeric_limits<double>::infinity());
                                return;
                            case DivideByZeroMode::None:
                                return;
                            case DivideByZeroMode::Error:
                            case DivideByZeroMode::Zero:
                            case DivideByZeroMode::One:
                            default:
                                throw std::runtime_error("division by zero");
                        }
                    }

                    emit_scalar<double>(
                        node,
                        python_mod(lhs_value, rhs_value));
                }
            };

            struct ModIntSpec {
                struct state {
                    DivideByZeroMode divide_by_zero_mode{DivideByZeroMode::Error};
                };

                static state make_state(Node& node) {
                    return {divide_by_zero_mode_from_scalars(node)};
                }

                static void eval(Node& node, state& state) {
                    auto bundle = require_input_bundle(node);
                    const int64_t lhs_value = require_scalar_field<int64_t>(bundle, "lhs");
                    const int64_t rhs_value = require_scalar_field<int64_t>(bundle, "rhs");
                    if (rhs_value == 0) {
                        if (state.divide_by_zero_mode == DivideByZeroMode::None) {
                            return;
                        }
                        throw std::runtime_error("division by zero");
                    }

                    int64_t remainder = lhs_value % rhs_value;
                    if (remainder != 0 && ((remainder > 0) != (rhs_value > 0))) {
                        remainder += rhs_value;
                    }
                    emit_scalar<int64_t>(node, remainder);
                }
            };

            template<typename L, typename R>
            struct PowSpec {
                struct state {
                    DivideByZeroMode divide_by_zero_mode{DivideByZeroMode::Error};
                };

                static state make_state(Node& node) {
                    return {divide_by_zero_mode_from_scalars(node)};
                }

                static void eval(Node& node, state& state) {
                    auto bundle = require_input_bundle(node);
                    const L lhs_value = require_scalar_field<L>(bundle, "lhs");
                    const R rhs_value = require_scalar_field<R>(bundle, "rhs");

                    if (lhs_value == static_cast<L>(0) && rhs_value < static_cast<R>(0)) {
                        emit_float_divide_by_zero_policy<double>(node, state.divide_by_zero_mode);
                        return;
                    }

                    emit_scalar<double>(
                        node,
                        std::pow(static_cast<double>(lhs_value), static_cast<double>(rhs_value)));
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

        struct DivNumbersSpec
            : number_ops_detail::DivSpec<double, double> {
            static constexpr const char* py_factory_name = "op_div_numbers";
        };

        struct FloorDivNumbersSpec
            : number_ops_detail::FloorDivFloatSpec<double, double> {
            static constexpr const char* py_factory_name = "op_floordiv_numbers";
        };

        struct FloorDivIntsSpec
            : number_ops_detail::FloorDivIntSpec {
            static constexpr const char* py_factory_name = "op_floordiv_ints";
        };

        struct ModNumbersSpec
            : number_ops_detail::ModFloatSpec<double, double> {
            static constexpr const char* py_factory_name = "op_mod_numbers";
        };

        struct ModIntsSpec
            : number_ops_detail::ModIntSpec {
            static constexpr const char* py_factory_name = "op_mod_ints";
        };

        struct PowIntFloatSpec
            : number_ops_detail::PowSpec<int64_t, double> {
            static constexpr const char* py_factory_name = "op_pow_int_float";
        };

        struct PowFloatIntSpec
            : number_ops_detail::PowSpec<double, int64_t> {
            static constexpr const char* py_factory_name = "op_pow_float_int";
        };
    }  // namespace ops
}  // namespace hgraph
