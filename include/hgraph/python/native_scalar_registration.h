#ifndef HGRAPH_PYTHON_NATIVE_SCALAR_REGISTRATION_H
#define HGRAPH_PYTHON_NATIVE_SCALAR_REGISTRATION_H

#include <hgraph/config.h>

#if HGRAPH_ENABLE_PYTHON_USER_NODES

#include <hgraph/hgraph_export.h>
#include <hgraph/python/chrono.h>
#include <hgraph/python/conversion.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/python_object.h>
#include <hgraph/types/python_ops.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/util/date_time.h>

#include <nanobind/nanobind.h>
#include <nanobind/ndarray.h>
#include <nanobind/stl/string.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <typeindex>
#include <typeinfo>

namespace hgraph
{
    /**
     * Python-conversion customization point for scalar types the generic
     * nanobind cast cannot handle (RFC 0003, RFC 0035). Specialise it for
     * ``T`` and register the type with ``register_python_scalar_conversion<T>``
     * (which ``register_native_scalar_type<T>`` does for you); the scalar's
     * ops table resolves to the registration on the type's first conversion,
     * so the specialisation only has to be visible where it is registered.
     * Provide:
     *   static nb::object to_python(const T &);
     *   static T          from_python(nb::handle);
     */
    template <typename T>
    struct python_conversion_traits;
}  // namespace hgraph

namespace hgraph::python_bridge
{
    namespace nb = nanobind;

    namespace scalar_detail
    {
        template <typename T>
        constexpr bool python_scalar_castable =
            std::is_arithmetic_v<T> || std::is_same_v<T, std::string> || std::is_same_v<T, Date> ||
            std::is_same_v<T, DateTime> || std::is_same_v<T, TimeDelta>;

        template <typename T>
        concept has_python_conversion_traits = requires(const T &value, nb::handle source) {
            { python_conversion_traits<T>::to_python(value) } -> std::same_as<nb::object>;
            { python_conversion_traits<T>::from_python(source) } -> std::same_as<T>;
        };

        template <typename T>
        PyNewRef to_python_thunk(const void *, const void *memory)
        {
            if constexpr (python_scalar_castable<T>)
            {
                return give(nb::cast(*static_cast<const T *>(memory)));
            }
            else if constexpr (has_python_conversion_traits<T>)
            {
                return give(python_conversion_traits<T>::to_python(*static_cast<const T *>(memory)));
            }
            else
            {
                throw std::logic_error("ValueOps::to_python is not available for this scalar type");
            }
        }

        template <typename T>
        void from_python_thunk(const void *, const ValueTypeRef &, void *memory, PyRef source_ref)
        {
            const nb::handle source = handle_of(source_ref);
            if constexpr (python_scalar_castable<T>)
            {
                if constexpr (std::is_arithmetic_v<T>)
                {
                    // Pythonic strictness: numeric scalars never convert from
                    // strings (PyNumber coercion would accept "1"); numeric
                    // cross-conversions stay permitted.
                    if (nb::isinstance<nb::str>(source) || nb::isinstance<nb::bytes>(source))
                    {
                        throw nb::type_error("cannot convert a python string to a numeric scalar");
                    }
                }
                *static_cast<T *>(memory) = nb::cast<T>(source);
            }
            else if constexpr (has_python_conversion_traits<T>)
            {
                *static_cast<T *>(memory) = python_conversion_traits<T>::from_python(source);
            }
            else
            {
                throw std::logic_error("ValueOps::from_python is not available for this scalar type");
            }
        }

        template <typename T>
        struct python_buffer_traits
        {
            using storage_type = T;

            static storage_type convert(const void *memory) { return *static_cast<const T *>(memory); }

            [[nodiscard]] static constexpr const char *numpy_view_dtype() noexcept { return nullptr; }
        };

        template <>
        struct python_buffer_traits<DateTime>
        {
            using storage_type = std::int64_t;

            static storage_type convert(const void *memory)
            {
                return static_cast<storage_type>(static_cast<const DateTime *>(memory)->time_since_epoch().count());
            }

            [[nodiscard]] static constexpr const char *numpy_view_dtype() noexcept { return "datetime64[us]"; }
        };

        template <>
        struct python_buffer_traits<TimeDelta>
        {
            using storage_type = std::int64_t;

            static storage_type convert(const void *memory)
            {
                return static_cast<storage_type>(static_cast<const TimeDelta *>(memory)->count());
            }

            [[nodiscard]] static constexpr const char *numpy_view_dtype() noexcept { return "timedelta64[us]"; }
        };

        template <>
        struct python_buffer_traits<Date>
        {
            using storage_type = std::int64_t;

            static storage_type convert(const void *memory)
            {
                const auto days =
                    std::chrono::sys_days{*static_cast<const Date *>(memory)}.time_since_epoch().count();
                return static_cast<storage_type>(days);
            }

            [[nodiscard]] static constexpr const char *numpy_view_dtype() noexcept { return "datetime64[D]"; }
        };

        template <typename Storage>
        void copy_value_array_span(Storage *&dst, ValueArraySpan span)
        {
            if (span.size == 0) { return; }
            if (span.data == nullptr) { throw std::logic_error("ValueOps::to_python_buffer span has null data"); }

            if (span.stride == sizeof(Storage))
            {
                std::memcpy(dst, span.data, span.size * sizeof(Storage));
                dst += span.size;
                return;
            }

            const auto *src = static_cast<const std::byte *>(span.data);
            for (std::size_t index = 0; index < span.size; ++index)
            {
                std::memcpy(dst + index, src + index * span.stride, sizeof(Storage));
            }
            dst += span.size;
        }

        template <typename Storage>
        void delete_python_buffer(void *memory) noexcept
        {
            delete[] static_cast<Storage *>(memory);
        }

        template <typename T>
        PyNewRef to_python_buffer_thunk(const void *, const ValueTypeRef &, const ValueArraySource &source)
        {
            if constexpr (detail::buffer_compatible_type<T>)
            {
                using traits       = python_buffer_traits<T>;
                using storage_type = typename traits::storage_type;

                auto          owner = std::make_unique<storage_type[]>(std::max<std::size_t>(source.size, 1));
                storage_type *data  = owner.get();

                constexpr bool direct_copy =
                    std::is_same_v<std::remove_cv_t<T>, storage_type> && std::is_trivially_copyable_v<storage_type>;
                if constexpr (direct_copy)
                {
                    if (source.first.size + source.second.size == source.size)
                    {
                        storage_type *dst = data;
                        copy_value_array_span<storage_type>(dst, source.first);
                        copy_value_array_span<storage_type>(dst, source.second);
                    }
                    else
                    {
                        if (source.element_at == nullptr)
                        {
                            throw std::logic_error("ValueOps::to_python_buffer requires an element accessor");
                        }
                        for (std::size_t index = 0; index < source.size; ++index)
                        {
                            data[index] = traits::convert(source.element_at(source.owner, index));
                        }
                    }
                }
                else
                {
                    if (source.element_at == nullptr)
                    {
                        throw std::logic_error("ValueOps::to_python_buffer requires an element accessor");
                    }
                    for (std::size_t index = 0; index < source.size; ++index)
                    {
                        data[index] = traits::convert(source.element_at(source.owner, index));
                    }
                }

                storage_type *owned = owner.release();
                nb::capsule   owner_capsule{owned, &delete_python_buffer<storage_type>};
                nb::ndarray<nb::numpy, const storage_type, nb::ndim<1>> array{owned, {source.size}, owner_capsule};
                nb::object result = array.cast();
                if constexpr (traits::numpy_view_dtype() != nullptr)
                {
                    return give(result.attr("view")(nb::str{traits::numpy_view_dtype()}));
                }
                return give(std::move(result));
            }
            else
            {
                throw std::logic_error("ValueOps::to_python_buffer is not available for this scalar type");
            }
        }
    }  // namespace scalar_detail

    /** The registered slot table for ``T``: one function-local static per type. */
    template <typename T>
    [[nodiscard]] const PythonScalarSlots &python_scalar_slots_for() noexcept
    {
        static const PythonScalarSlots slots{
            .to_python        = &scalar_detail::to_python_thunk<T>,
            .from_python      = &scalar_detail::from_python_thunk<T>,
            .to_python_buffer = &scalar_detail::to_python_buffer_thunk<T>,
        };
        return slots;
    }

    /**
     * Register the Python conversion for the C++ scalar type ``type``. The
     * scalar's ops table (``ops_for<T>``) resolves to it on the type's first
     * conversion (RFC 0035, "Scalars"); registering the same table again is
     * a no-op and registering a different table for a type replaces it.
     */
    HGRAPH_EXPORT void register_python_scalar_conversion(const std::type_info &type, const PythonScalarSlots *slots);

    template <typename T>
    void register_python_scalar_conversion()
    {
        register_python_scalar_conversion(typeid(T), &python_scalar_slots_for<T>());
    }

    /** The provider table the bridge registers (idempotent; also at unit load). */
    HGRAPH_EXPORT void register_python_ops() noexcept;

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

    /** Associate an arbitrary Python annotation with a nominal Any-kind schema. */
    HGRAPH_EXPORT void register_python_opaque_type(
        nanobind::handle python_type,
        const ValueTypeMetaData *value_type);

    /** Return the nominal opaque schema registered for this exact annotation. */
    [[nodiscard]] HGRAPH_EXPORT const ValueTypeMetaData *
    opaque_type_for_python(nanobind::handle python_type);

    /** Return the Python annotation associated with a nominal opaque schema. */
    [[nodiscard]] HGRAPH_EXPORT nanobind::object
    python_type_for_opaque(const ValueTypeMetaData *value_type);

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
    HGRAPH_EXPORT void clear_python_opaque_types() noexcept;

    /**
     * Register ``T`` through its ``scalar_descriptor`` and associate it with
     * the supplied Python class. Registers ``T``'s Python conversion first.
     */
    template <typename T>
    const ValueTypeMetaData *register_native_scalar_type(nanobind::handle python_type)
    {
        register_python_scalar_conversion<T>();
        const auto *native_value_type = scalar_descriptor<T>::value_meta();
        register_native_scalar_type(python_type, native_value_type);
        return native_value_type;
    }

    /**
     * Register ``T`` under an explicit native schema name and associate it
     * with the supplied Python class. Registers ``T``'s Python conversion first.
     */
    template <typename T>
    const ValueTypeMetaData *register_native_scalar_type(
        nanobind::handle python_type,
        std::string_view native_name)
    {
        register_python_scalar_conversion<T>();
        const auto *native_value_type =
            TypeRegistry::instance().register_scalar<T>(native_name);
        register_native_scalar_type(python_type, native_value_type);
        return native_value_type;
    }
}  // namespace hgraph::python_bridge

#endif  // HGRAPH_ENABLE_PYTHON_USER_NODES
#endif  // HGRAPH_PYTHON_NATIVE_SCALAR_REGISTRATION_H
