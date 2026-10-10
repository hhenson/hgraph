#ifndef HGL_NATIVE_VALUES_H
#define HGL_NATIVE_VALUES_H

#include <hgraph/types/metadata/type_record_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/value/binary_codec.h>
#include <hgraph/types/value/value.h>

#include <stdexcept>
#include <string_view>

namespace hgl::native_values
{
    inline constexpr std::string_view binding_label            = "hgl.native.atomic";
    inline constexpr std::string_view serialized_binding_label = "hgl.native.atomic.serialized";

    // The canonical physical binding and the exposed source contract remain
    // distinct. This preparation record is shared by the existing core registry;
    // copies use its exact plan/ops and hooks never look up the contract again.
    [[nodiscard]] inline hgraph::TypeRecordKey key(hgraph::ValueTypeRef binding, bool serialization) {
        return {.schema               = &binding.schema()->header,
                .role                 = hgraph::TypeRole::Instance,
                .plan                 = binding.plan(),
                .ops                  = binding.ops(),
                .debug                = binding.record()->debug,
                .implementation_label = serialization ? serialized_binding_label : binding_label};
    }

    [[nodiscard]] inline const hgraph::TypeRecord *contract(const hgraph::ValueTypeMetaData *schema) {
        if (!schema || schema->try_value_kind() != hgraph::ValueTypeKind::Atomic) { return nullptr; }
        const auto physical = hgraph::ValuePlanFactory::instance().type_for(schema);
        const auto owner    = hgraph::value_owning_type(physical);
        const auto name     = owner.record()->implementation_name();
        return name == binding_label || name == serialized_binding_label ? owner.record() : nullptr;
    }

    [[nodiscard]] inline hgraph::ValueTypeRef bind(hgraph::ValueTypeRef physical, std::string_view canonical_identity,
                                                   bool equality, bool hash, bool order, bool serialization) {
        using namespace hgraph;
        if (!physical || physical.schema()->try_value_kind() != ValueTypeKind::Atomic ||
            physical.schema()->name() != canonical_identity) {
            throw std::invalid_argument("native atomic provider does not bind its exact canonical scalar identity");
        }
        const auto canonical = ValuePlanFactory::instance().type_for(physical.schema());
        if (physical.plan() != canonical.plan() || physical.ops() != canonical.ops() ||
            physical.record()->ops_abi_version != canonical.record()->ops_abi_version) {
            throw std::invalid_argument("native atomic provider has an incompatible layout or lifecycle binding");
        }
        if (!physical.checked_plan().lifecycle.can_copy_construct() || !physical.ops_ref().to_string_impl) {
            throw std::invalid_argument("native atomic provider requires owning copy and text operations");
        }
        // This is the existing raw-scalar storage ABI, not an exposed HGL
        // capability. Incompatible providers must fail before the owning
        // contract is published or any TS/aggregate storage is constructed.
        if (!physical.checked_plan().lifecycle.can_default_construct() || !physical.checked_plan().lifecycle.can_copy_assign()) {
            throw std::invalid_argument("native atomic provider is incompatible with the scalar storage lifecycle");
        }
        constexpr auto exposure     = TypeCapabilities::Equatable | TypeCapabilities::Hashable | TypeCapabilities::Comparable;
        auto           capabilities = static_cast<TypeCapabilities>(static_cast<std::uint32_t>(physical.capabilities()) &
                                                                    ~static_cast<std::uint32_t>(exposure));
        if (equality) { capabilities |= TypeCapabilities::Equatable; }
        if (hash) { capabilities |= TypeCapabilities::Hashable; }
        if (order) { capabilities |= TypeCapabilities::Comparable; }
        if ((capabilities & physical.capabilities()) != capabilities) {
            throw std::invalid_argument("native atomic provider lacks a declared capability");
        }
        if (serialization) { static_cast<void>(bind_binary_converter(physical.schema())); }
        // checked() rejects grants or changes to layout/lifecycle capabilities.
        if (const auto *existing = contract(physical.schema())) {
            const auto serialized = existing->implementation_name() == serialized_binding_label;
            if (existing->capabilities != capabilities || serialized != serialization) {
                throw std::invalid_argument("canonical native scalar has a mismatched exposed provider contract");
            }
        }
        const auto &record  = TypeRecordRegistry::instance().intern({.key             = key(physical, serialization),
                                                                     .ops_abi_version = physical.record()->ops_abi_version,
                                                                     .capabilities    = capabilities});
        const auto  binding = ValueTypeRef::checked(AnyPtr::typed_null(record));
        register_value_owning_type(canonical, binding);
        return binding;
    }

    template <typename T>
    [[nodiscard]] inline hgraph::ValueTypeRef bind(std::string_view canonical_identity, bool equality, bool hash, bool order,
                                                   bool serialization) {
        const auto *schema   = hgraph::scalar_descriptor<T>::value_meta();
        const auto  physical = hgraph::TypeRegistry::instance().scalar_type<T>();
        if (!physical || physical.schema() != schema || physical.checked_plan().layout.size != sizeof(T) ||
            physical.checked_plan().layout.alignment != alignof(T)) {
            throw std::invalid_argument("native atomic C++ mapping has an incompatible provider identity or layout");
        }
        return bind(physical, canonical_identity, equality, hash, order, serialization);
    }
}  // namespace hgl::native_values

#endif
