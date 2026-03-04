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

            template<typename T>
            inline void emit_divmod_output(Node& node, T quotient, T remainder) {
                auto output = node.output();
                if (!output) {
                    throw std::runtime_error("number operator requires TSL output");
                }

                auto list = output.as_list();
                if (list.count() < 2) {
                    throw std::runtime_error("number operator divmod output requires size >= 2");
                }

                list.at(0).set_value(value::View(&quotient, value::scalar_type_meta<T>()));
                list.at(1).set_value(value::View(&remainder, value::scalar_type_meta<T>()));
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

            struct LShift {
                template<typename L, typename R>
                static auto apply(L lhs, R rhs) {
                    return lhs << rhs;
                }
            };

            struct RShift {
                template<typename L, typename R>
                static auto apply(L lhs, R rhs) {
                    return lhs >> rhs;
                }
            };

            struct BitAnd {
                template<typename L, typename R>
                static auto apply(L lhs, R rhs) {
                    return lhs & rhs;
                }
            };

            struct BitOr {
                template<typename L, typename R>
                static auto apply(L lhs, R rhs) {
                    return lhs | rhs;
                }
            };

            struct BitXor {
                template<typename L, typename R>
                static auto apply(L lhs, R rhs) {
                    return lhs ^ rhs;
                }
            };

            struct Eq {
                template<typename L, typename R>
                static bool apply(L lhs, R rhs) {
                    return lhs == rhs;
                }
            };

            struct Ne {
                template<typename L, typename R>
                static bool apply(L lhs, R rhs) {
                    return lhs != rhs;
                }
            };

            struct Lt {
                template<typename L, typename R>
                static bool apply(L lhs, R rhs) {
                    return lhs < rhs;
                }
            };

            struct Le {
                template<typename L, typename R>
                static bool apply(L lhs, R rhs) {
                    return lhs <= rhs;
                }
            };

            struct Gt {
                template<typename L, typename R>
                static bool apply(L lhs, R rhs) {
                    return lhs > rhs;
                }
            };

            struct Ge {
                template<typename L, typename R>
                static bool apply(L lhs, R rhs) {
                    return lhs >= rhs;
                }
            };

            struct Neg {
                template<typename T>
                static auto apply(T value) {
                    return -value;
                }
            };

            struct Pos {
                template<typename T>
                static auto apply(T value) {
                    return +value;
                }
            };

            struct Invert {
                template<typename T>
                static auto apply(T value) {
                    return ~value;
                }
            };

            struct Abs {
                template<typename T>
                static auto apply(T value) {
                    return std::abs(value);
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

            template<typename In, typename Out, typename Op>
            struct UnaryScalarSpec {
                static void eval(Node& node) {
                    auto bundle = require_input_bundle(node);
                    const In ts_value = require_scalar_field<In>(bundle, "ts");
                    const Out output_value = static_cast<Out>(Op::apply(ts_value));
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

            template<typename L, typename R, typename Compare>
            struct BinaryCompareSpec {
                static void eval(Node& node) {
                    auto bundle = require_input_bundle(node);
                    const L lhs_value = require_scalar_field<L>(bundle, "lhs");
                    const R rhs_value = require_scalar_field<R>(bundle, "rhs");
                    const bool output_value = Compare::apply(lhs_value, rhs_value);
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

            struct DivmodNumbersSpec {
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
                        if (state.divide_by_zero_mode == DivideByZeroMode::Error) {
                            throw std::runtime_error("division by zero");
                        }
                        return;
                    }

                    const double quotient = std::floor(lhs_value / rhs_value);
                    const double remainder = python_mod(lhs_value, rhs_value);
                    emit_divmod_output<double>(node, quotient, remainder);
                }
            };

            struct DivmodIntsSpec {
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

                    int64_t quotient = lhs_value / rhs_value;
                    int64_t remainder = lhs_value % rhs_value;
                    if (remainder != 0 && ((remainder > 0) != (rhs_value > 0))) {
                        quotient -= 1;
                        remainder += rhs_value;
                    }
                    emit_divmod_output<int64_t>(node, quotient, remainder);
                }
            };
        }  // namespace number_ops_detail

        struct AddFloatToIntSpec
            : number_ops_detail::BinaryScalarSpec<int64_t, double, double, number_ops_detail::Add> {
            static constexpr const char* py_factory_name = "op_add_float_to_int";
        };

        struct AddIntToIntSpec
            : number_ops_detail::BinaryScalarSpec<int64_t, int64_t, int64_t, number_ops_detail::Add> {
            static constexpr const char* py_factory_name = "op_add_int_to_int";
        };

        struct AddFloatToFloatSpec
            : number_ops_detail::BinaryScalarSpec<double, double, double, number_ops_detail::Add> {
            static constexpr const char* py_factory_name = "op_add_float_to_float";
        };

        struct AddIntToFloatSpec
            : number_ops_detail::BinaryScalarSpec<double, int64_t, double, number_ops_detail::Add> {
            static constexpr const char* py_factory_name = "op_add_int_to_float";
        };

        struct SubIntFromIntSpec
            : number_ops_detail::BinaryScalarSpec<int64_t, int64_t, int64_t, number_ops_detail::Sub> {
            static constexpr const char* py_factory_name = "op_sub_int_from_int";
        };

        struct SubFloatFromFloatSpec
            : number_ops_detail::BinaryScalarSpec<double, double, double, number_ops_detail::Sub> {
            static constexpr const char* py_factory_name = "op_sub_float_from_float";
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

        struct MulIntAndIntSpec
            : number_ops_detail::BinaryScalarSpec<int64_t, int64_t, int64_t, number_ops_detail::Mul> {
            static constexpr const char* py_factory_name = "op_mul_int_and_int";
        };

        struct MulFloatAndFloatSpec
            : number_ops_detail::BinaryScalarSpec<double, double, double, number_ops_detail::Mul> {
            static constexpr const char* py_factory_name = "op_mul_float_and_float";
        };

        struct MulIntAndFloatSpec
            : number_ops_detail::BinaryScalarSpec<int64_t, double, double, number_ops_detail::Mul> {
            static constexpr const char* py_factory_name = "op_mul_int_and_float";
        };

        struct LShiftIntsSpec
            : number_ops_detail::BinaryScalarSpec<int64_t, int64_t, int64_t, number_ops_detail::LShift> {
            static constexpr const char* py_factory_name = "op_lshift_ints";
        };

        struct RShiftIntsSpec
            : number_ops_detail::BinaryScalarSpec<int64_t, int64_t, int64_t, number_ops_detail::RShift> {
            static constexpr const char* py_factory_name = "op_rshift_ints";
        };

        struct BitAndIntsSpec
            : number_ops_detail::BinaryScalarSpec<int64_t, int64_t, int64_t, number_ops_detail::BitAnd> {
            static constexpr const char* py_factory_name = "op_bit_and_ints";
        };

        struct BitOrIntsSpec
            : number_ops_detail::BinaryScalarSpec<int64_t, int64_t, int64_t, number_ops_detail::BitOr> {
            static constexpr const char* py_factory_name = "op_bit_or_ints";
        };

        struct BitXorIntsSpec
            : number_ops_detail::BinaryScalarSpec<int64_t, int64_t, int64_t, number_ops_detail::BitXor> {
            static constexpr const char* py_factory_name = "op_bit_xor_ints";
        };

        struct EqFloatIntSpec
            : number_ops_detail::EqWithEpsilonSpec<double, int64_t> {
            static constexpr const char* py_factory_name = "op_eq_float_int";
        };

        struct EqIntIntSpec
            : number_ops_detail::BinaryCompareSpec<int64_t, int64_t, number_ops_detail::Eq> {
            static constexpr const char* py_factory_name = "op_eq_int_int";
        };

        struct EqIntFloatSpec
            : number_ops_detail::EqWithEpsilonSpec<int64_t, double> {
            static constexpr const char* py_factory_name = "op_eq_int_float";
        };

        struct EqFloatFloatSpec
            : number_ops_detail::EqWithEpsilonSpec<double, double> {
            static constexpr const char* py_factory_name = "op_eq_float_float";
        };

        struct NeIntIntSpec
            : number_ops_detail::BinaryCompareSpec<int64_t, int64_t, number_ops_detail::Ne> {
            static constexpr const char* py_factory_name = "op_ne_int_int";
        };

        struct NeIntFloatSpec
            : number_ops_detail::BinaryCompareSpec<int64_t, double, number_ops_detail::Ne> {
            static constexpr const char* py_factory_name = "op_ne_int_float";
        };

        struct NeFloatIntSpec
            : number_ops_detail::BinaryCompareSpec<double, int64_t, number_ops_detail::Ne> {
            static constexpr const char* py_factory_name = "op_ne_float_int";
        };

        struct NeFloatFloatSpec
            : number_ops_detail::BinaryCompareSpec<double, double, number_ops_detail::Ne> {
            static constexpr const char* py_factory_name = "op_ne_float_float";
        };

        struct LtIntIntSpec
            : number_ops_detail::BinaryCompareSpec<int64_t, int64_t, number_ops_detail::Lt> {
            static constexpr const char* py_factory_name = "op_lt_int_int";
        };

        struct LtIntFloatSpec
            : number_ops_detail::BinaryCompareSpec<int64_t, double, number_ops_detail::Lt> {
            static constexpr const char* py_factory_name = "op_lt_int_float";
        };

        struct LtFloatIntSpec
            : number_ops_detail::BinaryCompareSpec<double, int64_t, number_ops_detail::Lt> {
            static constexpr const char* py_factory_name = "op_lt_float_int";
        };

        struct LtFloatFloatSpec
            : number_ops_detail::BinaryCompareSpec<double, double, number_ops_detail::Lt> {
            static constexpr const char* py_factory_name = "op_lt_float_float";
        };

        struct LeIntIntSpec
            : number_ops_detail::BinaryCompareSpec<int64_t, int64_t, number_ops_detail::Le> {
            static constexpr const char* py_factory_name = "op_le_int_int";
        };

        struct LeIntFloatSpec
            : number_ops_detail::BinaryCompareSpec<int64_t, double, number_ops_detail::Le> {
            static constexpr const char* py_factory_name = "op_le_int_float";
        };

        struct LeFloatIntSpec
            : number_ops_detail::BinaryCompareSpec<double, int64_t, number_ops_detail::Le> {
            static constexpr const char* py_factory_name = "op_le_float_int";
        };

        struct LeFloatFloatSpec
            : number_ops_detail::BinaryCompareSpec<double, double, number_ops_detail::Le> {
            static constexpr const char* py_factory_name = "op_le_float_float";
        };

        struct GtIntIntSpec
            : number_ops_detail::BinaryCompareSpec<int64_t, int64_t, number_ops_detail::Gt> {
            static constexpr const char* py_factory_name = "op_gt_int_int";
        };

        struct GtIntFloatSpec
            : number_ops_detail::BinaryCompareSpec<int64_t, double, number_ops_detail::Gt> {
            static constexpr const char* py_factory_name = "op_gt_int_float";
        };

        struct GtFloatIntSpec
            : number_ops_detail::BinaryCompareSpec<double, int64_t, number_ops_detail::Gt> {
            static constexpr const char* py_factory_name = "op_gt_float_int";
        };

        struct GtFloatFloatSpec
            : number_ops_detail::BinaryCompareSpec<double, double, number_ops_detail::Gt> {
            static constexpr const char* py_factory_name = "op_gt_float_float";
        };

        struct GeIntIntSpec
            : number_ops_detail::BinaryCompareSpec<int64_t, int64_t, number_ops_detail::Ge> {
            static constexpr const char* py_factory_name = "op_ge_int_int";
        };

        struct GeIntFloatSpec
            : number_ops_detail::BinaryCompareSpec<int64_t, double, number_ops_detail::Ge> {
            static constexpr const char* py_factory_name = "op_ge_int_float";
        };

        struct GeFloatIntSpec
            : number_ops_detail::BinaryCompareSpec<double, int64_t, number_ops_detail::Ge> {
            static constexpr const char* py_factory_name = "op_ge_float_int";
        };

        struct GeFloatFloatSpec
            : number_ops_detail::BinaryCompareSpec<double, double, number_ops_detail::Ge> {
            static constexpr const char* py_factory_name = "op_ge_float_float";
        };

        struct NegIntSpec
            : number_ops_detail::UnaryScalarSpec<int64_t, int64_t, number_ops_detail::Neg> {
            static constexpr const char* py_factory_name = "op_neg_int";
        };

        struct NegFloatSpec
            : number_ops_detail::UnaryScalarSpec<double, double, number_ops_detail::Neg> {
            static constexpr const char* py_factory_name = "op_neg_float";
        };

        struct PosIntSpec
            : number_ops_detail::UnaryScalarSpec<int64_t, int64_t, number_ops_detail::Pos> {
            static constexpr const char* py_factory_name = "op_pos_int";
        };

        struct PosFloatSpec
            : number_ops_detail::UnaryScalarSpec<double, double, number_ops_detail::Pos> {
            static constexpr const char* py_factory_name = "op_pos_float";
        };

        struct InvertIntSpec
            : number_ops_detail::UnaryScalarSpec<int64_t, int64_t, number_ops_detail::Invert> {
            static constexpr const char* py_factory_name = "op_invert_int";
        };

        struct AbsIntSpec
            : number_ops_detail::UnaryScalarSpec<int64_t, int64_t, number_ops_detail::Abs> {
            static constexpr const char* py_factory_name = "op_abs_int";
        };

        struct AbsFloatSpec
            : number_ops_detail::UnaryScalarSpec<double, double, number_ops_detail::Abs> {
            static constexpr const char* py_factory_name = "op_abs_float";
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

        struct DivmodNumbersSpec
            : number_ops_detail::DivmodNumbersSpec {
            static constexpr const char* py_factory_name = "op_divmod_numbers";
        };

        struct DivmodIntsSpec
            : number_ops_detail::DivmodIntsSpec {
            static constexpr const char* py_factory_name = "op_divmod_ints";
        };
    }  // namespace ops
}  // namespace hgraph
