#pragma once

#include <hgraph/types/node.h>

#include <stdexcept>
#include <string>

namespace hgraph {
    namespace ops {
        namespace access_ops_detail {
            inline TSBInputView input_bundle(Node& node) {
                return *node.input().try_as_bundle();
            }

            inline nb::object python_field(const TSBInputView& bundle, std::string_view field_name) {
                return bundle.field(field_name).to_python();
            }

            inline nb::object optional_python_field(
                const TSBInputView& bundle, std::string_view field_name, const nb::object& default_value) {
                auto field = bundle.field(field_name);
                if (!field || !field.valid()) {
                    return default_value;
                }
                return field.to_python();
            }

            inline void emit_python(Node& node, const nb::object& value) {
                node.output().from_python(value);
            }
        }  // namespace access_ops_detail

        struct GetattrCsSpec {
            static constexpr const char* py_factory_name = "op_getattr_cs";

            struct state {
                std::string attr;
            };

            static state make_state(Node& node) {
                const nb::dict& scalars = node.scalars();
                return {nb::cast<std::string>(nb::str(scalars["attr"]))};
            }

            static void eval(Node& node, state& state) {
                auto bundle = access_ops_detail::input_bundle(node);
                const nb::object ts = access_ops_detail::python_field(bundle, "ts");
                const nb::object default_value =
                    access_ops_detail::optional_python_field(bundle, "default_value", nb::none());

                nb::object attr_value;
                if (PyObject* raw = PyObject_GetAttrString(ts.ptr(), state.attr.c_str())) {
                    attr_value = nb::steal<nb::object>(raw);
                } else {
                    if (!PyErr_ExceptionMatches(PyExc_AttributeError)) {
                        nb::raise_python_error();
                    }
                    PyErr_Clear();
                    attr_value = default_value;
                }

                access_ops_detail::emit_python(node, attr_value.is_none() ? default_value : attr_value);
            }
        };

        struct GetattrTypeNameSpec {
            static constexpr const char* py_factory_name = "op_getattr_type_name";

            struct state {
                std::string attr;
            };

            static state make_state(Node& node) {
                const nb::dict& scalars = node.scalars();
                return {nb::cast<std::string>(nb::str(scalars["attr"]))};
            }

            static void eval(Node& node, state& state) {
                if (state.attr != "name" && state.attr != "__name__") {
                    const std::string msg = "Cannot get " + state.attr + " from TS[Type]";
                    PyErr_SetString(PyExc_AttributeError, msg.c_str());
                    nb::raise_python_error();
                }
                auto bundle = access_ops_detail::input_bundle(node);
                const nb::object ts_type = access_ops_detail::python_field(bundle, "ts");
                access_ops_detail::emit_python(node, nb::cast<nb::object>(ts_type.attr("__name__")));
            }
        };

        struct GetitemTupleFixedSpec {
            static constexpr const char* py_factory_name = "op_getitem_tuple_fixed";

            struct state {
                int64_t key;
            };

            static state make_state(Node& node) {
                const nb::dict& scalars = node.scalars();
                return {nb::cast<int64_t>(scalars["key"])};
            }

            static void eval(Node& node, state& state) {
                auto bundle = access_ops_detail::input_bundle(node);
                const nb::object ts = access_ops_detail::python_field(bundle, "ts");
                const nb::object key = nb::int_(state.key);
                nb::object value = nb::steal<nb::object>(PyObject_GetItem(ts.ptr(), key.ptr()));
                if (!value.is_valid()) {
                    nb::raise_python_error();
                }
                access_ops_detail::emit_python(node, value);
            }
        };

        struct GetitemTupleSpec {
            static constexpr const char* py_factory_name = "op_getitem_tuple";

            static void eval(Node& node) {
                auto bundle = access_ops_detail::input_bundle(node);
                const nb::object ts = access_ops_detail::python_field(bundle, "ts");
                const nb::object key = access_ops_detail::python_field(bundle, "key");
                nb::object value = nb::steal<nb::object>(PyObject_GetItem(ts.ptr(), key.ptr()));
                if (!value.is_valid()) {
                    nb::raise_python_error();
                }
                access_ops_detail::emit_python(node, value);
            }
        };

        struct GetitemFrozendictSpec {
            static constexpr const char* py_factory_name = "op_getitem_frozendict";

            static void eval(Node& node) {
                auto bundle = access_ops_detail::input_bundle(node);
                const nb::object ts = access_ops_detail::python_field(bundle, "ts");
                const nb::object key = access_ops_detail::python_field(bundle, "key");
                const nb::object default_value =
                    access_ops_detail::optional_python_field(bundle, "default_value", nb::none());
                access_ops_detail::emit_python(node, nb::cast<nb::object>(ts.attr("get")(key, default_value)));
            }
        };
    }  // namespace ops
}  // namespace hgraph
