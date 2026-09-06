#ifndef HGRAPH_TYPES_PYTHON_OPS_H
#define HGRAPH_TYPES_PYTHON_OPS_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/python_object.h>

#include <atomic>
#include <typeinfo>

/**
 * The provider table through which the type layer reaches every Python
 * conversion (RFC 0035, "PythonOps -- the provider table").
 *
 * The type layer owns the table's shape and the forwarders its ops-table
 * slots hold; the bridge (``src/hgraph/python/impl/python_ops.cpp``) fills
 * one table and registers it at load, and again, idempotently, from the
 * ``_hgraph`` module initializer. A forwarder reads the registered table
 * when its slot is called and passes the slot's arguments to the matching
 * entry unchanged; with no table, or a null entry, it throws the
 * *no Python conversion is registered* error at the moment the old
 * compiled-in thunks threw *not available*. Nothing reads the table at
 * table-construction time, so registration has no ordering rule beyond
 * "before the first conversion".
 */
namespace hgraph
{
    class ValueTypeRef;
    struct ValueArraySource;

    /** The three value-ops slots of one C++ scalar type, as the bridge
        registers them (``register_python_scalar_conversion<T>``). */
    struct PythonScalarSlots
    {
        PyNewRef (*to_python)(const void *context, const void *memory){nullptr};
        void (*from_python)(const void *context, const ValueTypeRef &binding, void *memory, PyRef source){nullptr};
        PyNewRef (*to_python_buffer)(const void *context, const ValueTypeRef &binding,
                                     const ValueArraySource &source){nullptr};
    };

    struct PythonOps
    {
        struct Scalars
        {
            /** The slots registered for a C++ scalar type, or nullptr when
                the bridge has none for it. Called by ``T``'s scalar forwarders
                on their first conversion; a non-null result is cached. */
            const PythonScalarSlots *(*conversion_for)(const std::type_info &type){nullptr};
        } scalars;

        /** Enum ops: ``context`` is the enum's ``ValueTypeMetaData``. */
        struct Enums
        {
            static constexpr const char *name = "enum";
            PyNewRef (*to_python)(const void *context, const void *memory){nullptr};
            void (*from_python)(const void *context, const ValueTypeRef &binding, void *memory, PyRef source){nullptr};
        } enums;

        /** The compact (immutable-API) containers of ``compact_container_ops.h``:
            ``memory`` is the storage object; from_python rebuilds it through
            the value builders. */
        struct Compact
        {
            static constexpr const char *name = "compact container";
            PyNewRef (*list_to_python)(const void *context, const void *memory){nullptr};
            PyNewRef (*list_to_python_tuple)(const void *context, const void *memory){nullptr};
            PyNewRef (*list_to_python_array)(const void *context, const void *memory){nullptr};
            void (*list_from_python)(const void *context, const ValueTypeRef &binding, void *memory, PyRef source){nullptr};
            PyNewRef (*cyclic_buffer_to_python)(const void *context, const void *memory){nullptr};
            void (*cyclic_buffer_from_python)(const void *context, const ValueTypeRef &binding, void *memory, PyRef source){nullptr};
            PyNewRef (*queue_to_python)(const void *context, const void *memory){nullptr};
            void (*queue_from_python)(const void *context, const ValueTypeRef &binding, void *memory, PyRef source){nullptr};
            PyNewRef (*set_to_python)(const void *context, const void *memory){nullptr};
            void (*set_from_python)(const void *context, const ValueTypeRef &binding, void *memory, PyRef source){nullptr};
            PyNewRef (*map_to_python)(const void *context, const void *memory){nullptr};
            void (*map_from_python)(const void *context, const ValueTypeRef &binding, void *memory, PyRef source){nullptr};
            PyNewRef (*map_key_adapter_to_python)(const void *context, const void *memory){nullptr};
        } compact;

        /** The mutable containers of ``mutable_container_ops.h`` (read-back only:
            mutation goes through the mutation protocol, never from_python). */
        struct Mutable
        {
            static constexpr const char *name = "mutable container";
            PyNewRef (*list_to_python)(const void *context, const void *memory){nullptr};
            PyNewRef (*map_to_python)(const void *context, const void *memory){nullptr};
            PyNewRef (*set_to_python)(const void *context, const void *memory){nullptr};
        } mutable_containers;

        /** The realized structural values of the plan factory, the type
            realization and the pooled polymorphic entry: composite (Tuple /
            Bundle), fixed and bounded arrays, owned and shared entries, the
            closed Bundle and its pooled form. ``context`` is the family's
            private context; the bridge reaches it through the seams of
            ``src/hgraph/types/metadata/detail/realized_value_seams.h``. */
        struct Realized
        {
            static constexpr const char *name = "realized value";
            PyNewRef (*composite_to_python)(const void *context, const void *memory){nullptr};
            void (*composite_from_python)(const void *context, const ValueTypeRef &binding, void *memory, PyRef source){nullptr};
            PyNewRef (*array_to_python)(const void *context, const void *memory){nullptr};
            PyNewRef (*array_to_numpy)(const void *context, const void *memory){nullptr};
            void (*array_from_python)(const void *context, const ValueTypeRef &binding, void *memory, PyRef source){nullptr};
            PyNewRef (*owned_to_python)(const void *context, const void *memory){nullptr};
            void (*owned_from_python)(const void *context, const ValueTypeRef &binding, void *memory, PyRef source){nullptr};
            PyNewRef (*shared_to_python)(const void *context, const void *memory){nullptr};
            void (*shared_from_python)(const void *context, const ValueTypeRef &binding, void *memory, PyRef source){nullptr};
            PyNewRef (*closed_bundle_to_python)(const void *context, const void *memory){nullptr};
            void (*closed_bundle_from_python)(const void *context, const ValueTypeRef &binding, void *memory, PyRef source){nullptr};
            PyNewRef (*pooled_to_python)(const void *context, const void *memory){nullptr};
            void (*pooled_from_python)(const void *context, const ValueTypeRef &binding, void *memory, PyRef source){nullptr};
            /** Which realized alternative a Python source belongs to
                (``context`` is a ``realized_detail::PolymorphicAlternatives``). */
            ValueTypeRef (*polymorphic_source_type)(const void *context, PyRef source){nullptr};
        } realized;

        /** ``Any`` and the nominal JSON ``Any``: ``memory`` is the boxed ``Value``. */
        struct Any
        {
            static constexpr const char *name = "Any";
            PyNewRef (*to_python)(const void *context, const void *memory){nullptr};
            void (*from_python)(const void *context, const ValueTypeRef &binding, void *memory, PyRef source){nullptr};
            PyNewRef (*json_to_python)(const void *context, const void *memory){nullptr};
            void (*json_from_python)(const void *context, const ValueTypeRef &binding, void *memory, PyRef source){nullptr};
        } any;
    };

    /** Register (or replace) the provider. May be called at any time before
        the first conversion; the bridge unit calls it at load. */
    HGRAPH_EXPORT void set_python_ops(const PythonOps *ops) noexcept;
    [[nodiscard]] HGRAPH_EXPORT const PythonOps *python_ops() noexcept;

    namespace python_ops_detail
    {
        /** The registered table, or throw the *no Python conversion is
            registered* error naming ``what``. */
        [[nodiscard]] HGRAPH_EXPORT const PythonOps &require_python_ops(const char *what);
        [[noreturn]] HGRAPH_EXPORT void throw_unregistered(const char *what);
        [[noreturn]] HGRAPH_EXPORT void throw_unregistered_scalar(const std::type_info &type);
        [[nodiscard]] HGRAPH_EXPORT const PythonScalarSlots *scalar_slots(const std::type_info &type) noexcept;

        /**
         * The forwarder a slot holds for a provider entry: 
         * ``forwarder<&PythonOps::Any::to_python>::call`` reads the table,
         * takes ``python_ops()->any.to_python`` and calls it with the slot's
         * arguments unchanged. The section is deduced from the member
         * pointer and located in the table by ``section_of``; its ``name``
         * is what the error names.
         */
        template <typename Section>
        [[nodiscard]] const Section &section_of(const PythonOps &ops) noexcept;

        template <>
        [[nodiscard]] inline const PythonOps::Enums &section_of<PythonOps::Enums>(const PythonOps &ops) noexcept
        {
            return ops.enums;
        }
        template <>
        [[nodiscard]] inline const PythonOps::Any &section_of<PythonOps::Any>(const PythonOps &ops) noexcept
        {
            return ops.any;
        }
        template <>
        [[nodiscard]] inline const PythonOps::Compact &section_of<PythonOps::Compact>(const PythonOps &ops) noexcept
        {
            return ops.compact;
        }
        template <>
        [[nodiscard]] inline const PythonOps::Mutable &section_of<PythonOps::Mutable>(const PythonOps &ops) noexcept
        {
            return ops.mutable_containers;
        }
        template <>
        [[nodiscard]] inline const PythonOps::Realized &section_of<PythonOps::Realized>(const PythonOps &ops) noexcept
        {
            return ops.realized;
        }

        template <auto Member>
        struct forwarder;

        template <typename Section, typename Result, typename... Args, Result (*Section::*Member)(Args...)>
        struct forwarder<Member>
        {
            static Result call(Args... args)
            {
                const auto entry = section_of<Section>(require_python_ops(Section::name)).*Member;
                if (entry == nullptr) { throw_unregistered(Section::name); }
                return entry(args...);
            }
        };

        /** Scalar forwarders: resolved by ``typeid(T)`` on first use and
            cached once found (a null result is retried by the next call, so
            a conversion registered later is picked up). */
        template <typename T>
        [[nodiscard]] const PythonScalarSlots &scalar_slots_for()
        {
            static const PythonScalarSlots *slots = nullptr;
            if (slots == nullptr) { slots = scalar_slots(typeid(T)); }
            if (slots == nullptr) { throw_unregistered_scalar(typeid(T)); }
            return *slots;
        }

        template <typename T>
        PyNewRef scalar_to_python(const void *context, const void *memory)
        {
            const auto &slots = scalar_slots_for<T>();
            if (slots.to_python == nullptr) { throw_unregistered_scalar(typeid(T)); }
            return slots.to_python(context, memory);
        }

        template <typename T>
        void scalar_from_python(const void *context, const ValueTypeRef &binding, void *memory, PyRef source)
        {
            const auto &slots = scalar_slots_for<T>();
            if (slots.from_python == nullptr) { throw_unregistered_scalar(typeid(T)); }
            slots.from_python(context, binding, memory, source);
        }

        template <typename T>
        PyNewRef scalar_to_python_buffer(const void *context, const ValueTypeRef &binding,
                                         const ValueArraySource &source)
        {
            const auto &slots = scalar_slots_for<T>();
            if (slots.to_python_buffer == nullptr) { throw_unregistered_scalar(typeid(T)); }
            return slots.to_python_buffer(context, binding, source);
        }
    }  // namespace python_ops_detail
}  // namespace hgraph

#endif  // HGRAPH_TYPES_PYTHON_OPS_H
