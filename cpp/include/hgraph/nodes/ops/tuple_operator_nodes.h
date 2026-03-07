#pragma once

#include <hgraph/types/node.h>
#include <hgraph/types/value/type_registry.h>

#include <cstdint>
#include <string_view>

namespace hgraph {
    namespace ops {
        namespace tuple_ops_detail {
            inline TSBInputView input_bundle(Node& node) {
                return *node.input().try_as_bundle();
            }

            inline nb::object python_field(const TSBInputView& bundle, std::string_view field_name) {
                return bundle.field(field_name).to_python();
            }

            inline nb::object optional_python_field(const TSBInputView& bundle, std::string_view field_name) {
                auto field = bundle.field(field_name);
                if (!field || !field.valid()) {
                    return nb::none();
                }
                return field.to_python();
            }

            inline void emit_python(Node& node, const nb::object& value) {
                node.output().from_python(value);
            }

            inline void emit_bool(Node& node, bool value) {
                node.output().set_value(value::View(&value, value::scalar_type_meta<bool>()));
            }

            inline void emit_int(Node& node, int64_t value) {
                node.output().set_value(value::View(&value, value::scalar_type_meta<int64_t>()));
            }

            inline void emit_float(Node& node, double value) {
                node.output().set_value(value::View(&value, value::scalar_type_meta<double>()));
            }
        }  // namespace tuple_ops_detail

        struct MulTupleIntSpec {
            static constexpr const char* py_factory_name = "op_mul_tuple_int";

            static void eval(Node& node) {
                auto bundle = tuple_ops_detail::input_bundle(node);
                const nb::object lhs = tuple_ops_detail::python_field(bundle, "lhs");
                const nb::object rhs = tuple_ops_detail::python_field(bundle, "rhs");
                nb::object result = nb::steal<nb::object>(PyNumber_Multiply(lhs.ptr(), rhs.ptr()));
                if (!result.is_valid()) {
                    nb::raise_python_error();
                }
                tuple_ops_detail::emit_python(node, result);
            }
        };

        struct AndTuplesSpec {
            static constexpr const char* py_factory_name = "op_and_tuples";

            static void eval(Node& node) {
                auto bundle = tuple_ops_detail::input_bundle(node);
                const nb::object lhs = tuple_ops_detail::python_field(bundle, "lhs");
                const int lhs_truth = PyObject_IsTrue(lhs.ptr());
                if (lhs_truth < 0) {
                    nb::raise_python_error();
                }
                if (!lhs_truth) {
                    tuple_ops_detail::emit_bool(node, false);
                    return;
                }
                const nb::object rhs = tuple_ops_detail::python_field(bundle, "rhs");
                const int rhs_truth = PyObject_IsTrue(rhs.ptr());
                if (rhs_truth < 0) {
                    nb::raise_python_error();
                }
                tuple_ops_detail::emit_bool(node, rhs_truth != 0);
            }
        };

        struct OrTuplesSpec {
            static constexpr const char* py_factory_name = "op_or_tuples";

            static void eval(Node& node) {
                auto bundle = tuple_ops_detail::input_bundle(node);
                const nb::object lhs = tuple_ops_detail::python_field(bundle, "lhs");
                const int lhs_truth = PyObject_IsTrue(lhs.ptr());
                if (lhs_truth < 0) {
                    nb::raise_python_error();
                }
                if (lhs_truth) {
                    tuple_ops_detail::emit_bool(node, true);
                    return;
                }
                const nb::object rhs = tuple_ops_detail::python_field(bundle, "rhs");
                const int rhs_truth = PyObject_IsTrue(rhs.ptr());
                if (rhs_truth < 0) {
                    nb::raise_python_error();
                }
                tuple_ops_detail::emit_bool(node, rhs_truth != 0);
            }
        };

        struct MinTupleUnarySpec {
            static constexpr const char* py_factory_name = "op_min_tuple_unary";

            static void eval(Node& node) {
                auto bundle = tuple_ops_detail::input_bundle(node);
                const nb::object ts = tuple_ops_detail::python_field(bundle, "ts");
                const nb::object default_value = tuple_ops_detail::optional_python_field(bundle, "default_value");
                nb::object builtins_min = nb::module_::import_("builtins").attr("min");
                nb::object result = builtins_min(ts, nb::arg("default") = default_value);
                tuple_ops_detail::emit_python(node, result);
            }
        };

        struct MaxTupleUnarySpec {
            static constexpr const char* py_factory_name = "op_max_tuple_unary";

            static void eval(Node& node) {
                auto bundle = tuple_ops_detail::input_bundle(node);
                const nb::object ts = tuple_ops_detail::python_field(bundle, "ts");
                const nb::object default_value = tuple_ops_detail::optional_python_field(bundle, "default_value");
                nb::object builtins_max = nb::module_::import_("builtins").attr("max");
                nb::object result = builtins_max(ts, nb::arg("default") = default_value);
                tuple_ops_detail::emit_python(node, result);
            }
        };

        struct SumTupleUnarySpec {
            static constexpr const char* py_factory_name = "op__sum_tuple_unary";

            static void eval(Node& node) {
                auto bundle = tuple_ops_detail::input_bundle(node);
                const nb::object ts = tuple_ops_detail::python_field(bundle, "ts");
                const nb::object zero_ts = tuple_ops_detail::python_field(bundle, "zero_ts");
                nb::object builtins_sum = nb::module_::import_("builtins").attr("sum");
                nb::object result = builtins_sum(ts, zero_ts);
                tuple_ops_detail::emit_python(node, result);
            }
        };

        struct MeanTupleUnarySpec {
            static constexpr const char* py_factory_name = "op_mean_tuple_unary";

            static void eval(Node& node) {
                auto bundle = tuple_ops_detail::input_bundle(node);
                const nb::object ts = tuple_ops_detail::python_field(bundle, "ts");
                const Py_ssize_t length = PyObject_Size(ts.ptr());
                if (length < 0) {
                    nb::raise_python_error();
                }
                if (length == 0) {
                    tuple_ops_detail::emit_float(node, std::numeric_limits<double>::quiet_NaN());
                    return;
                }
                nb::object builtins_sum = nb::module_::import_("builtins").attr("sum");
                nb::object sum_result = builtins_sum(ts);
                const double sum_val = nb::cast<double>(sum_result);
                tuple_ops_detail::emit_float(node, sum_val / static_cast<double>(length));
            }
        };

        struct StdTupleUnarySpec {
            static constexpr const char* py_factory_name = "op_std_tuple_unary";

            struct state {
                nb::object stdev_fn;
            };

            static state make_state(Node&) {
                return {nb::module_::import_("statistics").attr("stdev")};
            }

            static void eval(Node& node, state& s) {
                auto bundle = tuple_ops_detail::input_bundle(node);
                const nb::object ts = tuple_ops_detail::python_field(bundle, "ts");
                const Py_ssize_t length = PyObject_Size(ts.ptr());
                if (length < 0) {
                    nb::raise_python_error();
                }
                if (length <= 1) {
                    tuple_ops_detail::emit_float(node, 0.0);
                    return;
                }
                tuple_ops_detail::emit_float(node, nb::cast<double>(s.stdev_fn(ts)));
            }
        };

        struct VarTupleUnarySpec {
            static constexpr const char* py_factory_name = "op_var_tuple_unary";

            struct state {
                nb::object variance_fn;
            };

            static state make_state(Node&) {
                return {nb::module_::import_("statistics").attr("variance")};
            }

            static void eval(Node& node, state& s) {
                auto bundle = tuple_ops_detail::input_bundle(node);
                const nb::object ts = tuple_ops_detail::python_field(bundle, "ts");
                const Py_ssize_t length = PyObject_Size(ts.ptr());
                if (length < 0) {
                    nb::raise_python_error();
                }
                if (length <= 1) {
                    tuple_ops_detail::emit_float(node, 0.0);
                    return;
                }
                tuple_ops_detail::emit_float(node, nb::cast<double>(s.variance_fn(ts)));
            }
        };

        struct IndexOfTupleSpec {
            static constexpr const char* py_factory_name = "op_index_of_tuple";

            static void eval(Node& node) {
                auto bundle = tuple_ops_detail::input_bundle(node);
                const nb::object ts = tuple_ops_detail::python_field(bundle, "ts");
                const nb::object item = tuple_ops_detail::python_field(bundle, "item");
                nb::object result = nb::steal<nb::object>(
                    PyObject_CallMethod(ts.ptr(), "index", "O", item.ptr()));
                if (!result.is_valid()) {
                    if (PyErr_ExceptionMatches(PyExc_ValueError)) {
                        PyErr_Clear();
                        tuple_ops_detail::emit_int(node, -1);
                        return;
                    }
                    nb::raise_python_error();
                }
                tuple_ops_detail::emit_int(node, nb::cast<int64_t>(result));
            }
        };

        struct AddTupleScalarSpec {
            static constexpr const char* py_factory_name = "op_add_tuple_scalar";

            static void eval(Node& node) {
                auto bundle = tuple_ops_detail::input_bundle(node);
                auto lhs_field = bundle.field("lhs");
                auto rhs_field = bundle.field("rhs");
                const bool lhs_valid = lhs_field.valid();
                const bool rhs_valid = rhs_field.valid();

                if (lhs_valid) {
                    const nb::object lhs = lhs_field.to_python();
                    if (rhs_valid) {
                        const nb::object rhs = rhs_field.to_python();
                        nb::object rhs_tuple = nb::steal<nb::object>(PyTuple_Pack(1, rhs.ptr()));
                        nb::object result = nb::steal<nb::object>(
                            PyNumber_Add(lhs.ptr(), rhs_tuple.ptr()));
                        if (!result.is_valid()) {
                            nb::raise_python_error();
                        }
                        tuple_ops_detail::emit_python(node, result);
                    } else {
                        tuple_ops_detail::emit_python(node, lhs);
                    }
                } else {
                    const nb::object rhs = rhs_field.to_python();
                    nb::object result = nb::steal<nb::object>(PyTuple_Pack(1, rhs.ptr()));
                    tuple_ops_detail::emit_python(node, result);
                }
            }
        };

        struct SubTupleScalarSpec {
            static constexpr const char* py_factory_name = "op_sub_tuple_scalar";

            static void eval(Node& node) {
                auto bundle = tuple_ops_detail::input_bundle(node);
                const nb::object lhs = tuple_ops_detail::python_field(bundle, "lhs");
                const nb::object rhs = tuple_ops_detail::python_field(bundle, "rhs");
                nb::list result_list;
                const Py_ssize_t len = PyTuple_Size(lhs.ptr());
                for (Py_ssize_t i = 0; i < len; ++i) {
                    PyObject* item = PyTuple_GetItem(lhs.ptr(), i);
                    const int eq = PyObject_RichCompareBool(item, rhs.ptr(), Py_NE);
                    if (eq < 0) {
                        nb::raise_python_error();
                    }
                    if (eq) {
                        result_list.append(nb::borrow<nb::object>(item));
                    }
                }
                tuple_ops_detail::emit_python(node, nb::tuple(result_list));
            }
        };
    }  // namespace ops
}  // namespace hgraph
