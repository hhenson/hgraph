#ifndef HGRAPH_PYTHON_NATIVE_SCALAR_REGISTRATION_H
#define HGRAPH_PYTHON_NATIVE_SCALAR_REGISTRATION_H

#include <hgraph/config.h>

#if HGRAPH_ENABLE_PYTHON_USER_NODES

#include <hgraph/hgraph_export.h>
#include <hgraph/types/static_schema.h>

#include <nanobind/nanobind.h>

#include <string_view>

namespace hgraph::python_bridge
{
    /**
     * Associate a Python class with a native atomic scalar schema.
     *
     * The association is process-wide and bidirectional. Registering the same
     * pair more than once is harmless; associating either member with a
     * different counterpart throws ``std::invalid_argument``.
     */
    HGRAPH_EXPORT void register_native_scalar_type(
        nanobind::handle python_type,
        const ValueTypeMetaData *native_value_type);

    /** Return the registered native scalar schema for an exact Python class. */
    [[nodiscard]] HGRAPH_EXPORT const ValueTypeMetaData *
    native_scalar_type_for_python(nanobind::handle python_type);

    /** Return the registered Python class for a native scalar schema. */
    [[nodiscard]] HGRAPH_EXPORT nanobind::object
    python_type_for_native_scalar(const ValueTypeMetaData *native_value_type);

    /**
     * Return the registered native scalar schema for a Python value.
     *
     * Exact class matches are preferred; registered base classes are accepted
     * through Python's normal ``isinstance`` semantics.
     */
    [[nodiscard]] HGRAPH_EXPORT const ValueTypeMetaData *
    native_scalar_type_for_value(nanobind::handle value);

    /** Test-only lifecycle hook used when the complete hgraph registry resets. */
    HGRAPH_EXPORT void clear_native_scalar_types() noexcept;

    /**
     * Register ``T`` through its ``scalar_descriptor`` and associate it with
     * the supplied Python class.
     */
    template <typename T>
    const ValueTypeMetaData *register_native_scalar_type(nanobind::handle python_type)
    {
        const auto *native_value_type = scalar_descriptor<T>::value_meta();
        register_native_scalar_type(python_type, native_value_type);
        return native_value_type;
    }

    /**
     * Register ``T`` under an explicit native schema name and associate it
     * with the supplied Python class.
     */
    template <typename T>
    const ValueTypeMetaData *register_native_scalar_type(
        nanobind::handle python_type,
        std::string_view native_name)
    {
        const auto *native_value_type =
            TypeRegistry::instance().register_scalar<T>(native_name);
        register_native_scalar_type(python_type, native_value_type);
        return native_value_type;
    }
}  // namespace hgraph::python_bridge

#endif  // HGRAPH_ENABLE_PYTHON_USER_NODES
#endif  // HGRAPH_PYTHON_NATIVE_SCALAR_REGISTRATION_H
