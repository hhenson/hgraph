#include <hgraph/runtime/evaluation_clock.h>

#include <hgraph/types/metadata/type_record_registry.h>
#include <hgraph/util/scope.h>

#include <stdexcept>

namespace hgraph
{
    namespace
    {
        void validate_clock_record(const TypeRecord &record)
        {
            if (!record.valid() || record.schema->family != TypeFamily::Clock ||
                record.role != TypeRole::Runtime || record.schema->kind != TYPE_KIND_NONE)
            {
                throw std::invalid_argument("ClockTypeRef requires a Clock/Runtime TypeRecord");
            }
            if (record.ops_abi_version != CLOCK_OPS_ABI_VERSION || record.ops == nullptr)
            {
                throw std::invalid_argument("ClockTypeRef requires clock ops ABI version 1");
            }
            if (record.capabilities != clock_type_capabilities(*record.plan))
            {
                throw std::invalid_argument("ClockTypeRef capabilities do not match its storage plan");
            }
        }
    }  // namespace

    TypeCapabilities clock_type_capabilities(const MemoryUtils::StoragePlan &)
    {
        return TypeCapabilities::Viewable;
    }

    ClockTypeRef intern_clock_type(const EvaluationClockTypeMetaData &schema,
                                   const MemoryUtils::StoragePlan &plan,
                                   const EvaluationClockOps &ops,
                                   std::string_view implementation_label)
    {
        if (!schema.header.valid() || schema.header.family != TypeFamily::Clock ||
            schema.header.kind != TYPE_KIND_NONE)
        {
            throw std::invalid_argument("intern_clock_type requires a valid clock schema header");
        }
        const TypeRecordDefinition definition{
            .key = TypeRecordKey{.schema = &schema.header,
                                 .role = TypeRole::Runtime,
                                 .plan = &plan,
                                 .ops = &ops,
                                 .debug = nullptr,
                                 .implementation_label = implementation_label},
            .ops_abi_version = CLOCK_OPS_ABI_VERSION,
            .capabilities = clock_type_capabilities(plan),
        };
        return ClockTypeRef{&TypeRecordRegistry::instance().intern(definition)};
    }

    ClockTypeRef ClockTypeRef::checked(AnyPtr pointer)
    {
        if (pointer.is_unbound()) return {};
        if (!pointer.well_formed() || pointer.record() == nullptr)
            throw std::invalid_argument("ClockTypeRef requires a well-formed pointer");
        validate_clock_record(*pointer.record());
        return ClockTypeRef{pointer.record()};
    }

    bool ClockTypeRef::valid() const noexcept
    {
        if (record_ == nullptr) return false;
        return fallback_on_exception(false, [&] {
            validate_clock_record(*record_);
            return true;
        });
    }

    const EvaluationClockTypeMetaData *ClockTypeRef::schema() const noexcept
    {
        return record_ != nullptr ? reinterpret_cast<const EvaluationClockTypeMetaData *>(record_->schema) : nullptr;
    }

    const MemoryUtils::StoragePlan &ClockTypeRef::checked_plan() const
    {
        if (plan() == nullptr) throw std::logic_error("ClockTypeRef is unbound");
        return *plan();
    }

    ClockPtr ClockTypeRef::typed_null() const noexcept
    {
        return ClockPtr{AnyPtr{record_, nullptr, AccessMode::ReadOnly}, ClockPtr::UncheckedTag{}};
    }

    ClockPtr ClockTypeRef::read_only(const void *data) const noexcept
    {
        return ClockPtr{AnyPtr{record_, data, AccessMode::ReadOnly}, ClockPtr::UncheckedTag{}};
    }

    namespace detail
    {
        const EvaluationClockTypeMetaData &evaluation_clock_schema() noexcept
        {
            static const EvaluationClockTypeMetaData schema{
                .header = SchemaHeader{TypeFamily::Clock, TYPE_KIND_NONE, "evaluation_clock"},
                .display_name = "evaluation_clock",
            };
            return schema;
        }

    }  // namespace detail
}  // namespace hgraph
