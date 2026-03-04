#pragma once

#include <hgraph/types/node.h>

#include <string>
#include <string_view>

namespace hgraph {
    namespace ops {
        namespace type_ops_detail {
            inline TSBInputView input_bundle(Node& node) {
                return *node.input().try_as_bundle();
            }

            inline nb::object python_field(const TSBInputView& bundle, std::string_view field_name) {
                return bundle.field(field_name).to_python();
            }

            inline nb::object python_type(const nb::object& obj) {
                return nb::borrow<nb::object>(reinterpret_cast<PyObject*>(Py_TYPE(obj.ptr())));
            }

            inline void emit_python(Node& node, const nb::object& value) {
                node.output().from_python(value);
            }

            inline nb::object call_one_arg(const nb::object& fn, const nb::object& arg) {
                return nb::cast<nb::object>(fn(arg));
            }
        }  // namespace type_ops_detail

        struct TypeCsSchemaSpec {
            static constexpr const char* py_factory_name = "op_type_cs_schema";

            static void eval(Node& node) {
                auto bundle = type_ops_detail::input_bundle(node);
                const nb::object value = type_ops_detail::python_field(bundle, "ts");
                type_ops_detail::emit_python(node, type_ops_detail::python_type(value));
            }
        };

        struct TypeCsTypevarSpec {
            static constexpr const char* py_factory_name = "op_type_cs_typevar";

            static void eval(Node& node) {
                auto bundle = type_ops_detail::input_bundle(node);
                const nb::object value = type_ops_detail::python_field(bundle, "ts");
                type_ops_detail::emit_python(node, type_ops_detail::python_type(value));
            }
        };

        struct TypeScalarSpec {
            static constexpr const char* py_factory_name = "op_type_scalar";

            static void eval(Node& node) {
                auto bundle = type_ops_detail::input_bundle(node);
                const nb::object value = type_ops_detail::python_field(bundle, "ts");
                type_ops_detail::emit_python(node, type_ops_detail::python_type(value));
            }
        };

        struct CastImplSpec {
            static constexpr const char* py_factory_name = "op_cast_impl";

            struct state {
                nb::object tp;
            };

            static state make_state(Node& node) {
                const nb::dict& scalars = node.scalars();
                return {nb::cast<nb::object>(scalars["tp"])};
            }

            static void eval(Node& node, state& state) {
                auto bundle = type_ops_detail::input_bundle(node);
                const nb::object value = type_ops_detail::python_field(bundle, "ts");
                type_ops_detail::emit_python(node, type_ops_detail::call_one_arg(state.tp, value));
            }
        };

        struct DowncastImplSpec {
            static constexpr const char* py_factory_name = "op_downcast_impl";

            struct state {
                nb::object tp;
                nb::object target_type;
            };

            static state make_state(Node& node) {
                const nb::dict& scalars = node.scalars();
                const nb::object tp = nb::cast<nb::object>(scalars["tp"]);
                return {tp, nb::getattr(tp, "__origin__", tp)};
            }

            static void eval(Node& node, state& state) {
                auto bundle = type_ops_detail::input_bundle(node);
                const nb::object value = type_ops_detail::python_field(bundle, "ts");
                const int is_instance = PyObject_IsInstance(value.ptr(), state.target_type.ptr());
                if (is_instance < 0) {
                    nb::raise_python_error();
                }
                if (is_instance == 0) {
                    const std::string error = nb::cast<std::string>(
                        nb::str("During downcast, expected an instance of {}, got {} ({})")
                            .format(state.tp, type_ops_detail::python_type(value), value));
                    PyErr_SetString(PyExc_AssertionError, error.c_str());
                    nb::raise_python_error();
                }
                type_ops_detail::emit_python(node, value);
            }
        };

        struct DowncastRefImplSpec {
            static constexpr const char* py_factory_name = "op_downcast_ref_impl";

            static void eval(Node& node) {
                auto bundle = type_ops_detail::input_bundle(node);
                type_ops_detail::emit_python(node, type_ops_detail::python_field(bundle, "ts"));
            }
        };
    }  // namespace ops
}  // namespace hgraph
