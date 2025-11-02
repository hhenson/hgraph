//
// Created by Howard Henson on 24/10/2025.
//

#ifndef HGRAPH_CPP_ENGINE_SCALAR_TYPES_H
#define HGRAPH_CPP_ENGINE_SCALAR_TYPES_H

#include <hgraph/types/schema_type.h>

namespace hgraph {
    /**
     * CompoundScalar - Abstract base class for scalar values with complex structure
     *
     * In Python, CompoundScalar derives from AbstractSchema and uses dataclass
     * decorators to define properties. In C++, we provide an abstract base class
     * that can have different concrete implementations:
     * - PythonCompoundScalar: Wraps Python dataclass instances
     * - (Future) Pure C++ implementations for better performance
     *
     * This is primarily used for:
     * - Error handling (NodeError, BacktraceSignature)
     * - Recordable state schemas
     * - Complex scalar types passed between nodes
     */
    struct CompoundScalar : AbstractSchema {
        using ptr = nb::ref<CompoundScalar>;

        CompoundScalar() = default;

        // Convert to dictionary representation (pure virtual)
        [[nodiscard]] virtual nb::dict to_dict() const = 0;

        // String representation (like dataclass __str__)
        [[nodiscard]] virtual std::string to_string() const;

        // Equality comparison (like dataclass __eq__)
        virtual bool operator==(const CompoundScalar &other) const;

        // Hash function (like dataclass __hash__)
        [[nodiscard]] virtual size_t hash() const;

        static void register_with_nanobind(nb::module_ &m);

        virtual ~CompoundScalar() = default;
    };

    /**
     * PythonCompoundScalar - Python-backed CompoundScalar implementation
     *
     * This wraps a Python object (typically a dataclass instance) and provides
     * access to its fields through the CompoundScalar interface.
     *
     * Usage pattern:
     * 1. Python creates dataclass instances
     * 2. C++ receives them and wraps in PythonCompoundScalar
     * 3. C++ can extract values via to_dict() or get_value()
     */
    struct PythonCompoundScalar : CompoundScalar {
        using ptr = nb::ref<PythonCompoundScalar>;

        // Construct from keys and a Python object representing the values
        explicit PythonCompoundScalar(std::vector<std::string> keys, nb::object values);

        // Override from AbstractSchema
        [[nodiscard]] const std::vector<std::string> &keys() const override;

        [[nodiscard]] nb::object get_value(const std::string &key) const override;

        // Override from CompoundScalar
        [[nodiscard]] nb::dict to_dict() const override;

        // Override string representation
        [[nodiscard]] std::string to_string() const override;

        // Override equality comparison
        bool operator==(const CompoundScalar &other) const override;

        // Override hash function
        [[nodiscard]] size_t hash() const override;

        // Create from dictionary
        static ptr from_dict(const std::vector<std::string> &keys, const nb::dict &d);

        static void register_with_nanobind(nb::module_ &m);

    protected:
        std::vector<std::string> _keys; // Property names
        nb::object _values; // Python object holding the actual values
    };
} // namespace hgraph

#endif  // HGRAPH_CPP_ENGINE_SCALAR_TYPES_H