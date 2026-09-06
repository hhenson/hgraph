#include <hgraph/python/scalar_conversions.h>

#include <hgraph/python/bridge_state.h>
#include <hgraph/python/conversion.h>
#include <hgraph/python/native_scalar_registration.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/python_ops.h>
#include <hgraph/types/value/value.h>

#include <ankerl/unordered_dense.h>

#include <cstdint>
#include <mutex>
#include <stdexcept>
#include <string>

/**
 * The bridge's ``PythonOps`` table (RFC 0035): the scalar registry the
 * scalar forwarders resolve through, the enum and ``Any`` entries, and the
 * registration that makes the table active. This unit is compiled in
 * exactly when Python user nodes are enabled, with or without the
 * ``_hgraph`` module, so it registers at load; the module initializer
 * calls ``register_python_ops`` again, idempotently.
 */
namespace hgraph::python_bridge
{
    namespace
    {
        // -- scalars ---------------------------------------------------------
        // Registration runs at import (extension module initializers) and
        // the lookup once per scalar type on its first conversion; both hold
        // the GIL, the mutex keeps the table safe regardless. It is a plain
        // mutex, not the counted type-system mutex: the one lookup per type
        // is not a registry access the evaluation snapshot should count.
        //
        // Keyed by the mangled type NAME, not std::type_index: the forwarder
        // that looks a type up is instantiated in whichever library first
        // used the type (hgraph_stdlib for its enums, an extension's native
        // library for its scalars) while the registration comes from the
        // Python module, and on macOS two libraries' type_info objects for
        // one type compare unequal unless the RTTI is marked non-unique.
        // The name is identical everywhere.
        std::mutex &scalar_registry_mutex()
        {
            static std::mutex mutex;
            return mutex;
        }

        ankerl::unordered_dense::map<std::string, const PythonScalarSlots *> &scalar_registry()
        {
            static auto *registry = new ankerl::unordered_dense::map<std::string, const PythonScalarSlots *>{};
            return *registry;
        }

        const PythonScalarSlots *scalar_conversion_for(const std::type_info &type)
        {
            const std::lock_guard lock{scalar_registry_mutex()};
            const auto           &registry = scalar_registry();
            const auto            found    = registry.find(std::string{type.name()});
            return found == registry.end() ? nullptr : found->second;
        }

        // -- enums -----------------------------------------------------------
        PyNewRef enum_to_python(const void *context, const void *memory)
        {
            const auto *meta = static_cast<const ValueTypeMetaData *>(context);
            if (enum_to_python_slot() == nullptr)
            {
                throw std::logic_error("enum python conversion requires the python module's enum registry");
            }
            return give(enum_to_python_slot()(meta, *static_cast<const Int *>(memory)));
        }

        void enum_from_python(const void *context, const ValueTypeRef &, void *memory, PyRef source)
        {
            const auto *meta = static_cast<const ValueTypeMetaData *>(context);
            if (enum_from_python_slot() == nullptr)
            {
                throw std::logic_error("enum python conversion requires the python module's enum registry");
            }
            *static_cast<Int *>(memory) = enum_from_python_slot()(meta, handle_of(source));
        }

        // -- Any -------------------------------------------------------------
        nb::object any_to_python_object(const Value &value)
        {
            if (!value.has_value()) { return nb::none(); }
            // Type-erased delegation: the BOXED value's own binding converts.
            return to_python(value.view());
        }

        PyNewRef any_to_python(const void *, const void *memory)
        {
            return give(any_to_python_object(*static_cast<const Value *>(memory)));
        }

        void any_from_python(const void *, const ValueTypeRef &, void *memory, PyRef source_ref)
        {
            const nb::handle source = handle_of(source_ref);
            Value           &boxed  = *static_cast<Value *>(memory);
            if (source.is_none())
            {
                boxed = Value{};
                return;
            }
            // Schema-free INFERENCE lives in the module (a dispatch on
            // python types); it installs this hook at import.
            const auto slot = py_infer_value_slot();
            if (slot == nullptr)
            {
                throw std::logic_error("Any::from_python requires the python module's inference hook");
            }
            Value inferred = reinterpret_cast<Value (*)(nb::handle)>(slot)(source);
            // Schema-free Python inference may itself choose the public Any
            // type (opaque objects and heterogeneous containers). The storage
            // here is already the EMBEDDED value of an Any box, so retain its
            // concrete content rather than creating Any<Any<T>>.
            if (inferred.view().is_any())
            {
                const ValueView contained = inferred.as_any().get();
                boxed                     = contained.valid() ? Value{contained} : Value{};
            }
            else { boxed = std::move(inferred); }
        }

        PyNewRef json_any_to_python(const void *context, const void *memory)
        {
            const Value &inner = *static_cast<const Value *>(memory);
            if (const auto slot = py_json_to_python_slot()) { return give(slot(inner)); }
            return any_to_python(context, memory);
        }

        void json_any_from_python(const void *context, const ValueTypeRef &binding, void *memory, PyRef source_ref)
        {
            const auto slot = py_json_from_python_slot();
            if (slot == nullptr)
            {
                any_from_python(context, binding, memory, source_ref);
                return;
            }

            Value outer = slot(handle_of(source_ref));
            if (outer.schema() != TypeRegistry::instance().json())
            {
                throw nb::type_error("native JSON conversion returned the wrong schema");
            }
            const ValueView inner       = outer.as_any().get();
            *static_cast<Value *>(memory) = inner.valid() ? Value{inner} : Value{};
        }

        // -- the table -------------------------------------------------------
        const PythonOps &provider_table()
        {
            static const PythonOps table = [] {
                PythonOps ops;
                ops.scalars.conversion_for = &scalar_conversion_for;
                ops.enums.to_python        = &enum_to_python;
                ops.enums.from_python      = &enum_from_python;
                ops.any.to_python          = &any_to_python;
                ops.any.from_python        = &any_from_python;
                ops.any.json_to_python     = &json_any_to_python;
                ops.any.json_from_python   = &json_any_from_python;
                return ops;
            }();
            return table;
        }

        /** The scalars the runtime library itself knows how to convert. The
            stdlib enums and the bridge's own object scalar are registered by
            the module (they are declared above this library). */
        void register_core_scalar_conversions()
        {
            register_python_scalar_conversion<bool>();
            register_python_scalar_conversion<std::int8_t>();
            register_python_scalar_conversion<std::int16_t>();
            register_python_scalar_conversion<std::int32_t>();
            register_python_scalar_conversion<std::int64_t>();
            register_python_scalar_conversion<std::uint8_t>();
            register_python_scalar_conversion<std::uint16_t>();
            register_python_scalar_conversion<std::uint32_t>();
            register_python_scalar_conversion<std::uint64_t>();
            register_python_scalar_conversion<float>();
            register_python_scalar_conversion<double>();
            register_python_scalar_conversion<std::string>();
            register_python_scalar_conversion<Date>();
            register_python_scalar_conversion<DateTime>();
            register_python_scalar_conversion<TimeDelta>();
            register_python_scalar_conversion<Time>();
            register_python_scalar_conversion<Bytes>();
            register_python_scalar_conversion<Period>();
            register_python_scalar_conversion<CivilDateTime>();
            register_python_scalar_conversion<ZoneId>();
            register_python_scalar_conversion<ZonedDateTime>();
            register_python_scalar_conversion<InstantRange>();
            register_python_scalar_conversion<CivilDateRange>();
            register_python_scalar_conversion<InstantRangeSet>();
            register_python_scalar_conversion<CivilDateRangeSet>();
            register_python_scalar_conversion<MonthEndPolicy>();
            register_python_scalar_conversion<AmbiguousTimePolicy>();
            register_python_scalar_conversion<NonexistentTimePolicy>();
            register_python_scalar_conversion<Boundary>();
            register_python_scalar_conversion<Frame>();
            register_python_scalar_conversion<Series>();
            register_python_scalar_conversion<TimeSeriesReference>();
            register_python_scalar_conversion<ValueCallable>();
            register_python_scalar_conversion<WiredFn>();
        }
    }  // namespace

    void register_python_scalar_conversion(const std::type_info &type, const PythonScalarSlots *slots)
    {
        const std::lock_guard lock{scalar_registry_mutex()};
        scalar_registry()[std::string{type.name()}] = slots;
    }

    void register_python_ops() noexcept
    {
        register_core_scalar_conversions();
        set_python_ops(&provider_table());
    }

    EnumToPythonFn &enum_to_python_slot() noexcept
    {
        static EnumToPythonFn slot = nullptr;
        return slot;
    }

    EnumFromPythonFn &enum_from_python_slot() noexcept
    {
        static EnumFromPythonFn slot = nullptr;
        return slot;
    }

    namespace
    {
        // Linking this unit is what makes the conversions active (the
        // ``python-user-nodes`` preset embeds the bridge without the module's
        // initializer), so it registers itself at load; the module's explicit
        // registration is idempotent.
        [[maybe_unused]] const bool python_ops_registered_at_load = [] {
            register_python_ops();
            return true;
        }();
    }  // namespace
}  // namespace hgraph::python_bridge
