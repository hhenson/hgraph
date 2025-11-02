//
// Created by Howard Henson on 24/10/2025.
//

#ifndef HGRAPH_CPP_ENGINE_SCHEMA_TYPE_H
#define HGRAPH_CPP_ENGINE_SCHEMA_TYPE_H

#include <nanobind/nanobind.h>
#include <nanobind/intrusive/ref.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>
#include <string>
#include <vector>

namespace nb = nanobind;
using namespace nb::literals;

namespace hgraph {
    /**
     * AbstractSchema - Abstract base class for schema-based types
     *
     * In Python, this provides meta-data extraction from class annotations.
     * In C++, we provide a pure interface that concrete implementations fulfill.
     *
     * Unlike Python, we don't need type discovery machinery - types are
     * statically known at compile time in C++.
     *
     * This is an abstract base class - concrete implementations include:
     * - CompoundScalar (and its subclasses like PythonCompoundScalar)
     * - TimeSeriesSchema
     */
    struct AbstractSchema : nb::intrusive_base {
        using ptr = nb::ref<AbstractSchema>;

        AbstractSchema() = default;

        // Get the keys (property names) of this schema (pure virtual)
        [[nodiscard]] virtual const std::vector<std::string> &keys() const = 0;

        // Get the value for a specific key (pure virtual - must be implemented)
        [[nodiscard]] virtual nb::object get_value(const std::string &key) const = 0;

        static void register_with_nanobind(nb::module_ &m);

        virtual ~AbstractSchema() = default;
    };
} // namespace hgraph

#endif  // HGRAPH_CPP_ENGINE_SCHEMA_TYPE_H