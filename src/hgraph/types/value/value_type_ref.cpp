#include <hgraph/types/value/value_type_ref.h>

#include <hgraph/types/metadata/debug_descriptor.h>
#include <hgraph/types/metadata/type_record_registry.h>
#include <hgraph/types/value/value_ops.h>

#include <stdexcept>
#include <string>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] const ValueOps &checked_record_ops(const TypeRecord &record)
        {
            if (record.ops_abi_version != VALUE_OPS_ABI_VERSION)
            {
                throw std::invalid_argument(
                    "ValueTypeRef requires value ops ABI version " +
                    std::to_string(VALUE_OPS_ABI_VERSION));
            }
            const auto *ops = static_cast<const ValueOps *>(record.ops);
            if (ops == nullptr || ops->kind <= ValueOpsKind::Invalid || ops->kind > ValueOpsKind::MutableMap)
            {
                throw std::invalid_argument("ValueTypeRef requires a valid ValueOps table");
            }
            return *ops;
        }

        void validate_record(const TypeRecord &record)
        {
            if (!record.valid() || record.schema->family != TypeFamily::Value || record.role != TypeRole::Instance)
            {
                throw std::invalid_argument("ValueTypeRef requires a Value/Instance TypeRecord");
            }
            const auto &schema = *reinterpret_cast<const ValueTypeMetaData *>(record.schema);
            const auto &ops = checked_record_ops(record);
            if (record.capabilities != value_type_capabilities(schema, *record.plan, ops))
            {
                throw std::invalid_argument("ValueTypeRef capabilities do not match the value schema, plan, and ops");
            }
        }
    } // namespace

    TypeCapabilities value_type_capabilities(const ValueTypeMetaData &schema,
                                             const MemoryUtils::StoragePlan &plan,
                                             const ValueOps &ops)
    {
        TypeCapabilities result = TypeCapabilities::Viewable;
        if (plan.can_default_construct()) result |= TypeCapabilities::Constructible;
        if (plan.trivially_destructible || plan.lifecycle.can_destroy())
            result |= TypeCapabilities::Destructible;
        if (plan.can_copy_construct()) result |= TypeCapabilities::Copyable;
        if (plan.can_move_construct()) result |= TypeCapabilities::Movable;
        if (ops.can_begin_mutation()) result |= TypeCapabilities::Mutable;

        const auto require_hook = [](bool advertised, auto hook, const char *name) {
            if (advertised && hook == nullptr)
            {
                throw std::invalid_argument(std::string{"value schema advertises "} + name + " without an ops hook");
            }
            return advertised && hook != nullptr;
        };
        if (require_hook(schema.is_equatable(), ops.equals_impl, "equality"))
            result |= TypeCapabilities::Equatable;
        if (require_hook(schema.is_comparable(), ops.compare_impl, "comparison"))
            result |= TypeCapabilities::Comparable;
        if (require_hook(schema.is_hashable(), ops.hash_impl, "hashing"))
            result |= TypeCapabilities::Hashable;
        if (schema.field_count != 0 || schema.element_type != nullptr || schema.key_type != nullptr)
            result |= TypeCapabilities::HasChildren;
        return result;
    }

    ValueTypeRef intern_value_type(const ValueTypeMetaData &schema,
                                   const MemoryUtils::StoragePlan &plan,
                                   const ValueOps &ops,
                                   const DebugDescriptor *debug)
    {
        if (ops.kind <= ValueOpsKind::Invalid || ops.kind > ValueOpsKind::MutableMap)
        {
            throw std::invalid_argument("intern_value_type requires a valid ValueOps kind");
        }
        if (debug == nullptr) { debug = find_value_debug_descriptor(schema, plan); }
        const TypeRecordDefinition definition{
            .key = TypeRecordKey{.schema = &schema.header,
                                 .role = TypeRole::Instance,
                                 .plan = &plan,
                                 .ops = &ops,
                                 .debug = debug},
            .ops_abi_version = VALUE_OPS_ABI_VERSION,
            .capabilities = value_type_capabilities(schema, plan, ops),
            .implementation_label = {},
        };
        return ValueTypeRef{&TypeRecordRegistry::instance().intern(definition)};
    }

    ValueTypeRef ValueTypeRef::checked(AnyPtr pointer)
    {
        if (pointer.is_unbound()) return {};
        if (!pointer.well_formed() || pointer.record() == nullptr)
            throw std::invalid_argument("ValueTypeRef requires a well-formed pointer");
        validate_record(*pointer.record());
        return ValueTypeRef{pointer.record()};
    }

    const MemoryUtils::StoragePlan &ValueTypeRef::checked_plan() const
    {
        if (plan() == nullptr) throw std::logic_error("ValueTypeRef is unbound");
        return *plan();
    }

    ValuePtr ValueTypeRef::typed_null() const noexcept
    {
        return ValuePtr{AnyPtr{record_, nullptr, AccessMode::ReadOnly}, ValuePtr::UncheckedTag{}};
    }

    ValuePtr ValueTypeRef::read_only(const void *data) const noexcept
    {
        return ValuePtr{AnyPtr{record_, data, AccessMode::ReadOnly}, ValuePtr::UncheckedTag{}};
    }

    ValuePtr ValueTypeRef::writable(void *data) const noexcept
    {
        return ValuePtr{AnyPtr{record_, data, AccessMode::Writable}, ValuePtr::UncheckedTag{}};
    }
} // namespace hgraph
