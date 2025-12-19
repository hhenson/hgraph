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
     * All operators are implemented at the C++ TypeOps level:
     * - Comparison: ==, !=, <, <=, >, >=
     * - Arithmetic: +, -, *, /, //, %, **, unary -, abs()
     * - Boolean: bool()
     * - Container: len(), in, [], iteration
     *
     * If a type does not support an operation, calling it will throw.
     *
     * Example Python usage:
     *   schema = _hgraph.get_scalar_type_meta(int)
     *   value = _hgraph.HgValue(schema)
     *   value.py_value = 42
     *   assert value.py_value == 42
     *   assert (value + value).py_value == 84
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

        // =========================================================================
        // Comparison operators
        // =========================================================================

        // Equality comparison (uses C++ equals for efficiency)
        [[nodiscard]] bool equals(const PyHgValue& other) const {
            return _value.equals(other._value);
        }

        // Less than - delegate to Value's C++ implementation
        [[nodiscard]] bool less_than(const PyHgValue& other) const {
            return _value.less_than(other._value);
        }

        [[nodiscard]] bool less_equal(const PyHgValue& other) const {
            return less_than(other) || equals(other);
        }

        [[nodiscard]] bool greater_than(const PyHgValue& other) const {
            return other.less_than(*this);
        }

        [[nodiscard]] bool greater_equal(const PyHgValue& other) const {
            return !less_than(other);
        }

        // =========================================================================
        // Arithmetic operators (binary) - delegate to Value's C++ implementation
        // =========================================================================

        [[nodiscard]] PyHgValue add(const PyHgValue& other) const {
            PyHgValue result;
            result._value = _value.add(other._value);
            return result;
        }

        [[nodiscard]] PyHgValue subtract(const PyHgValue& other) const {
            PyHgValue result;
            result._value = _value.subtract(other._value);
            return result;
        }

        [[nodiscard]] PyHgValue multiply(const PyHgValue& other) const {
            PyHgValue result;
            result._value = _value.multiply(other._value);
            return result;
        }

        [[nodiscard]] PyHgValue divide(const PyHgValue& other) const {
            PyHgValue result;
            result._value = _value.divide(other._value);
            return result;
        }

        [[nodiscard]] PyHgValue floor_divide(const PyHgValue& other) const {
            PyHgValue result;
            result._value = _value.floor_divide(other._value);
            return result;
        }

        [[nodiscard]] PyHgValue modulo(const PyHgValue& other) const {
            PyHgValue result;
            result._value = _value.modulo(other._value);
            return result;
        }

        [[nodiscard]] PyHgValue power(const PyHgValue& other) const {
            PyHgValue result;
            result._value = _value.power(other._value);
            return result;
        }

        // =========================================================================
        // Unary operators - delegate to Value's C++ implementation
        // =========================================================================

        [[nodiscard]] PyHgValue negate() const {
            PyHgValue result;
            result._value = _value.negate();
            return result;
        }

        [[nodiscard]] PyHgValue positive() const {
            // Positive returns a copy of the value
            return copy();
        }

        [[nodiscard]] PyHgValue absolute() const {
            PyHgValue result;
            result._value = _value.absolute();
            return result;
        }

        [[nodiscard]] PyHgValue invert() const {
            PyHgValue result;
            result._value = _value.invert();
            return result;
        }

        // =========================================================================
        // Boolean conversion - delegate to Value's C++ implementation
        // =========================================================================

        [[nodiscard]] bool to_bool() const {
            if (!valid()) return false;
            return _value.to_bool();
        }

        // =========================================================================
        // Container operations - delegate to Value's C++ implementation
        // =========================================================================

        [[nodiscard]] size_t length() const {
            return _value.length();
        }

        [[nodiscard]] bool contains(nb::object item) const {
            if (!valid()) return false;
            // Convert item to a temporary Value and check contains
            // For now, convert via Python since item could be any type
            nb::object v = py_value();
            return nb::cast<bool>(v.attr("__contains__")(item));
        }

        [[nodiscard]] nb::object getitem(nb::object key) const {
            if (!valid()) return nb::none();
            nb::object v = py_value();
            return v[key];
        }

        void setitem(nb::object key, nb::object value) {
            if (!valid()) {
                throw std::runtime_error("Cannot set item on invalid HgValue");
            }
            nb::object v = py_value();
            v[key] = value;
            // Write back the modified container
            set_py_value(v);
        }

        [[nodiscard]] nb::iterator iter() const {
            if (!valid()) {
                throw std::runtime_error("Cannot iterate invalid HgValue");
            }
            nb::object v = py_value();
            return nb::iter(v);
        }

        // =========================================================================
        // Utility
        // =========================================================================

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
