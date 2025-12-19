//
// Created by Claude on 19/12/2024.
//
// Python wrapper for the hgraph value type system.
// Exposes Value as HgValue to Python for testing and interop.
//

#ifndef HGRAPH_VALUE_PY_VALUE_H
#define HGRAPH_VALUE_PY_VALUE_H

#include <nanobind/nanobind.h>
#include <hgraph/types/value/value.h>
#include <hgraph/types/value/python_conversion.h>

namespace nb = nanobind;

namespace hgraph::value {

    /**
     * PyHgValue - Python wrapper for the Value class
     *
     * Provides a Python-accessible wrapper around the type-erased Value class,
     * allowing Python code to create, manipulate, and test values of any schema.
     *
     * Example Python usage:
     *   schema = _hgraph.get_scalar_type_meta(int)
     *   value = _hgraph.HgValue(schema)
     *   value.py_value = 42
     *   assert value.py_value == 42
     */
    class PyHgValue {
    public:
        // Default constructor - invalid value
        PyHgValue() = default;

        // Construct from schema - allocates and default-constructs value
        explicit PyHgValue(const TypeMeta* schema)
            : _value(schema) {}

        // Move constructor
        PyHgValue(PyHgValue&&) = default;
        PyHgValue& operator=(PyHgValue&&) = default;

        // No copy - Value is move-only
        PyHgValue(const PyHgValue&) = delete;
        PyHgValue& operator=(const PyHgValue&) = delete;

        // Validity check
        [[nodiscard]] bool valid() const { return _value.valid(); }

        // Get the schema
        [[nodiscard]] const TypeMeta* schema() const { return _value.schema(); }

        // Get the type kind
        [[nodiscard]] TypeKind kind() const { return _value.kind(); }

        // Get value as Python object
        [[nodiscard]] nb::object py_value() const {
            if (!valid()) return nb::none();
            return value_to_python(_value.data(), _value.schema());
        }

        // Set value from Python object
        void set_py_value(nb::object py_obj) {
            if (!valid()) {
                throw std::runtime_error("Cannot set value on invalid HgValue");
            }
            if (py_obj.is_none()) {
                // For None, we could re-construct to default, but that may not be
                // what's expected. For now, just ignore.
                return;
            }
            value_from_python(_value.data(), py_obj, _value.schema());
        }

        // String representation
        [[nodiscard]] std::string to_string() const {
            return _value.to_string();
        }

        // Type name
        [[nodiscard]] std::string type_name() const {
            if (!valid()) return "<invalid>";
            return _value.schema()->type_name_str();
        }

        // Equality comparison
        [[nodiscard]] bool equals(const PyHgValue& other) const {
            return _value.equals(other._value);
        }

        // Hash
        [[nodiscard]] size_t hash() const {
            return _value.hash();
        }

        // Create a copy
        [[nodiscard]] PyHgValue copy() const {
            PyHgValue result;
            result._value = Value::copy(_value);
            return result;
        }

        // Access to underlying Value (for advanced use)
        [[nodiscard]] Value& value() { return _value; }
        [[nodiscard]] const Value& value() const { return _value; }

        // Static factory: create from Python value with explicit schema
        static PyHgValue from_python(const TypeMeta* schema, nb::object py_obj) {
            PyHgValue result(schema);
            result.set_py_value(std::move(py_obj));
            return result;
        }

    private:
        Value _value;
    };

    /**
     * Register PyHgValue with nanobind
     */
    void register_py_value_with_nanobind(nb::module_& m);

} // namespace hgraph::value

#endif // HGRAPH_VALUE_PY_VALUE_H
