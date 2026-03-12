#pragma once

#include <hgraph/types/node.h>
#include <hgraph/types/value/type_registry.h>

#include <cmath>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <cstdint>

namespace hgraph {
    namespace ops {
        namespace scalar_ops_detail {
            enum class DivideByZeroMode : uint8_t {
                Error,
                Nan,
                Inf,
                None,
                Zero,
                One,
            };

            using binary_number_op = PyObject* (*)(PyObject*, PyObject*);
            using unary_number_op = PyObject* (*)(PyObject*);

            inline TSBInputView input_bundle(Node& node) {
                return *node.input().try_as_bundle();
            }

            inline nb::object python_field(const TSBInputView& bundle, std::string_view field_name) {
                return bundle.field(field_name).to_python();
            }

            inline void emit_python(Node& node, const nb::object& value) {
                node.output().from_python(value);
            }

            inline void emit_bool(Node& node, bool value) {
                node.output().set_value(value::View(&value, value::scalar_type_meta<bool>()));
            }

            inline bool python_truth(const nb::object& value) {
                const int result = PyObject_IsTrue(value.ptr());
                if (result < 0) {
                    nb::raise_python_error();
                }
                return result != 0;
            }

            inline bool rich_compare_bool(const nb::object& lhs, const nb::object& rhs, int op) {
                const int result = PyObject_RichCompareBool(lhs.ptr(), rhs.ptr(), op);
                if (result < 0) {
                    nb::raise_python_error();
                }
                return result != 0;
            }

            inline nb::object apply_binary_number(binary_number_op op, const nb::object& lhs, const nb::object& rhs) {
                nb::object result = nb::steal<nb::object>(op(lhs.ptr(), rhs.ptr()));
                if (!result.is_valid()) {
                    nb::raise_python_error();
                }
                return result;
            }

            inline nb::object apply_unary_number(unary_number_op op, const nb::object& value) {
                nb::object result = nb::steal<nb::object>(op(value.ptr()));
                if (!result.is_valid()) {
                    nb::raise_python_error();
                }
                return result;
            }

            inline DivideByZeroMode parse_divide_by_zero_mode(const nb::object& mode_obj) {
                if (!mode_obj.is_valid() || mode_obj.is_none()) {
                    return DivideByZeroMode::Error;
                }

                const std::string name = nb::cast<std::string>(nb::cast<nb::object>(mode_obj.attr("name")));
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

                throw std::runtime_error("Unsupported divide_by_zero mode");
            }

            inline DivideByZeroMode divide_by_zero_mode_from_scalars(Node& node) {
                const nb::dict& scalars = node.scalars();
                if (!scalars.contains("divide_by_zero")) {
                    return DivideByZeroMode::Error;
                }
                return parse_divide_by_zero_mode(nb::cast<nb::object>(scalars["divide_by_zero"]));
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

            template<binary_number_op Op>
            struct BinaryNumberSpec {
                static void eval(Node& node) {
                    auto bundle = input_bundle(node);
                    const nb::object lhs = python_field(bundle, "lhs");
                    const nb::object rhs = python_field(bundle, "rhs");
                    emit_python(node, apply_binary_number(Op, lhs, rhs));
                }
            };

            template<int Op>
            struct BinaryCompareSpec {
                static void eval(Node& node) {
                    auto bundle = input_bundle(node);
                    const nb::object lhs = python_field(bundle, "lhs");
                    const nb::object rhs = python_field(bundle, "rhs");
                    emit_bool(node, rich_compare_bool(lhs, rhs, Op));
                }
            };

            template<unary_number_op Op>
            struct UnaryNumberSpec {
                static void eval(Node& node) {
                    auto bundle = input_bundle(node);
                    const nb::object ts = python_field(bundle, "ts");
                    emit_python(node, apply_unary_number(Op, ts));
                }
            };
        }  // namespace scalar_ops_detail

        struct AddScalarsSpec : scalar_ops_detail::BinaryNumberSpec<&PyNumber_Add> {
            static constexpr const char* py_factory_name = "op_add_scalars";
        };

        struct SubScalarsSpec : scalar_ops_detail::BinaryNumberSpec<&PyNumber_Subtract> {
            static constexpr const char* py_factory_name = "op_sub_scalars";
        };

        struct MulScalarsSpec : scalar_ops_detail::BinaryNumberSpec<&PyNumber_Multiply> {
            static constexpr const char* py_factory_name = "op_mul_scalars";
        };

        struct PowScalarsSpec {
            static constexpr const char* py_factory_name = "op_pow_scalars";

            struct state {
                scalar_ops_detail::DivideByZeroMode divide_by_zero_mode{scalar_ops_detail::DivideByZeroMode::Error};
            };

            static state make_state(Node& node) {
                return {scalar_ops_detail::divide_by_zero_mode_from_scalars(node)};
            }

            static void eval(Node& node, state& state) {
                auto bundle = scalar_ops_detail::input_bundle(node);
                const nb::object lhs = scalar_ops_detail::python_field(bundle, "lhs");
                const nb::object rhs = scalar_ops_detail::python_field(bundle, "rhs");

                nb::object result = nb::steal<nb::object>(PyNumber_Power(lhs.ptr(), rhs.ptr(), Py_None));
                if (result.is_valid()) {
                    scalar_ops_detail::emit_python(node, result);
                    return;
                }

                if (!PyErr_ExceptionMatches(PyExc_ZeroDivisionError)) {
                    nb::raise_python_error();
                }

                switch (state.divide_by_zero_mode) {
                    case scalar_ops_detail::DivideByZeroMode::Error:
                        nb::raise_python_error();
                    case scalar_ops_detail::DivideByZeroMode::Nan:
                        PyErr_Clear();
                        scalar_ops_detail::emit_python(node, nb::float_(std::numeric_limits<double>::quiet_NaN()));
                        return;
                    case scalar_ops_detail::DivideByZeroMode::Inf:
                        PyErr_Clear();
                        scalar_ops_detail::emit_python(node, nb::float_(std::numeric_limits<double>::infinity()));
                        return;
                    case scalar_ops_detail::DivideByZeroMode::None:
                        PyErr_Clear();
                        return;
                    case scalar_ops_detail::DivideByZeroMode::Zero:
                        PyErr_Clear();
                        scalar_ops_detail::emit_python(node, nb::float_(0.0));
                        return;
                    case scalar_ops_detail::DivideByZeroMode::One:
                        PyErr_Clear();
                        scalar_ops_detail::emit_python(node, nb::float_(1.0));
                        return;
                    default:
                        nb::raise_python_error();
                }
            }
        };

        struct LShiftScalarsSpec : scalar_ops_detail::BinaryNumberSpec<&PyNumber_Lshift> {
            static constexpr const char* py_factory_name = "op_lshift_scalars";
        };

        struct RShiftScalarsSpec : scalar_ops_detail::BinaryNumberSpec<&PyNumber_Rshift> {
            static constexpr const char* py_factory_name = "op_rshift_scalars";
        };

        struct BitAndScalarsSpec : scalar_ops_detail::BinaryNumberSpec<&PyNumber_And> {
            static constexpr const char* py_factory_name = "op_bit_and_scalars";
        };

        struct BitOrScalarsSpec : scalar_ops_detail::BinaryNumberSpec<&PyNumber_Or> {
            static constexpr const char* py_factory_name = "op_bit_or_scalars";
        };

        struct BitXorScalarsSpec : scalar_ops_detail::BinaryNumberSpec<&PyNumber_Xor> {
            static constexpr const char* py_factory_name = "op_bit_xor_scalars";
        };

        struct EqScalarsSpec : scalar_ops_detail::BinaryCompareSpec<Py_EQ> {
            static constexpr const char* py_factory_name = "op_eq_scalars";
        };

        struct NeScalarsSpec : scalar_ops_detail::BinaryCompareSpec<Py_NE> {
            static constexpr const char* py_factory_name = "op_ne_scalars";
        };

        struct LtScalarsSpec : scalar_ops_detail::BinaryCompareSpec<Py_LT> {
            static constexpr const char* py_factory_name = "op_lt_scalars";
        };

        struct LeScalarsSpec : scalar_ops_detail::BinaryCompareSpec<Py_LE> {
            static constexpr const char* py_factory_name = "op_le_scalars";
        };

        struct GtScalarsSpec : scalar_ops_detail::BinaryCompareSpec<Py_GT> {
            static constexpr const char* py_factory_name = "op_gt_scalars";
        };

        struct GeScalarsSpec : scalar_ops_detail::BinaryCompareSpec<Py_GE> {
            static constexpr const char* py_factory_name = "op_ge_scalars";
        };

        struct NegScalarSpec : scalar_ops_detail::UnaryNumberSpec<&PyNumber_Negative> {
            static constexpr const char* py_factory_name = "op_neg_scalar";
        };

        struct PosScalarSpec : scalar_ops_detail::UnaryNumberSpec<&PyNumber_Positive> {
            static constexpr const char* py_factory_name = "op_pos_scalar";
        };

        struct InvertScalarSpec : scalar_ops_detail::UnaryNumberSpec<&PyNumber_Invert> {
            static constexpr const char* py_factory_name = "op_invert_scalar";
        };

        struct AbsScalarSpec : scalar_ops_detail::UnaryNumberSpec<&PyNumber_Absolute> {
            static constexpr const char* py_factory_name = "op_abs_scalar";
        };

        struct NotScalarSpec {
            static constexpr const char* py_factory_name = "op_not_scalar";

            static void eval(Node& node) {
                auto bundle = scalar_ops_detail::input_bundle(node);
                const nb::object ts = scalar_ops_detail::python_field(bundle, "ts");
                scalar_ops_detail::emit_bool(node, !scalar_ops_detail::python_truth(ts));
            }
        };

        struct AndScalarsSpec {
            static constexpr const char* py_factory_name = "op_and_scalars";

            static void eval(Node& node) {
                auto bundle = scalar_ops_detail::input_bundle(node);
                const nb::object lhs = scalar_ops_detail::python_field(bundle, "lhs");
                if (!scalar_ops_detail::python_truth(lhs)) {
                    scalar_ops_detail::emit_bool(node, false);
                    return;
                }
                const nb::object rhs = scalar_ops_detail::python_field(bundle, "rhs");
                scalar_ops_detail::emit_bool(node, scalar_ops_detail::python_truth(rhs));
            }
        };

        struct OrScalarsSpec {
            static constexpr const char* py_factory_name = "op_or_scalars";

            static void eval(Node& node) {
                auto bundle = scalar_ops_detail::input_bundle(node);
                const nb::object lhs = scalar_ops_detail::python_field(bundle, "lhs");
                if (scalar_ops_detail::python_truth(lhs)) {
                    scalar_ops_detail::emit_bool(node, true);
                    return;
                }
                const nb::object rhs = scalar_ops_detail::python_field(bundle, "rhs");
                scalar_ops_detail::emit_bool(node, scalar_ops_detail::python_truth(rhs));
            }
        };

        struct ContainsScalarSpec {
            static constexpr const char* py_factory_name = "op_contains_scalar";

            static void eval(Node& node) {
                auto bundle = scalar_ops_detail::input_bundle(node);
                const nb::object ts = scalar_ops_detail::python_field(bundle, "ts");
                const nb::object key = scalar_ops_detail::python_field(bundle, "key");
                const nb::object contains = nb::cast<nb::object>(ts.attr("__contains__")(key));
                scalar_ops_detail::emit_bool(node, nb::cast<bool>(contains));
            }
        };

        struct LenScalarSpec {
            static constexpr const char* py_factory_name = "op_len_scalar";

            static void eval(Node& node) {
                auto bundle = scalar_ops_detail::input_bundle(node);
                const nb::object ts = scalar_ops_detail::python_field(bundle, "ts");
                const Py_ssize_t size = PyObject_Size(ts.ptr());
                if (size < 0) {
                    nb::raise_python_error();
                }
                const int64_t out = static_cast<int64_t>(size);
                node.output().set_value(value::View(&out, value::scalar_type_meta<int64_t>()));
            }
        };

        struct CmpScalarsSpec {
            static constexpr const char* py_factory_name = "op_cmp_scalars";


            static void eval(Node& node) {
                auto bundle = scalar_ops_detail::input_bundle(node);
                const nb::object lhs = scalar_ops_detail::python_field(bundle, "lhs");
                const nb::object rhs = scalar_ops_detail::python_field(bundle, "rhs");
                const auto& constants = scalar_ops_detail::cmp_result_constants();
                if (scalar_ops_detail::rich_compare_bool(lhs, rhs, Py_EQ)) {
                    scalar_ops_detail::emit_python(node, constants.eq);
                    return;
                }
                if (scalar_ops_detail::rich_compare_bool(lhs, rhs, Py_LT)) {
                    scalar_ops_detail::emit_python(node, constants.lt);
                    return;
                }
                scalar_ops_detail::emit_python(node, constants.gt);
            }
        };
    }  // namespace ops
}  // namespace hgraph
