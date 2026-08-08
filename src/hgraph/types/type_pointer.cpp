#include <hgraph/types/type_pointer.h>

#include <stdexcept>

namespace hgraph
{
    namespace
    {
        [[nodiscard]] constexpr bool known_access(AccessMode access) noexcept
        {
            return access == AccessMode::ReadOnly || access == AccessMode::Writable || access == AccessMode::Mutation;
        }

        void validate_factory_record(const TypeRecord &record)
        {
            const auto address = reinterpret_cast<std::uintptr_t>(&record);
            if ((address & TaggedTypeRecordPtr::tag_mask) != 0 || !record.valid())
            {
                throw std::invalid_argument("AnyPtr requires an aligned, valid TypeRecord");
            }
        }
    } // namespace

    AnyPtr AnyPtr::typed_null(const TypeRecord &record)
    {
        validate_factory_record(record);
        return AnyPtr(&record, nullptr, AccessMode::ReadOnly);
    }

    AnyPtr AnyPtr::read_only(const TypeRecord &record, const void *data)
    {
        validate_factory_record(record);
        if (data == nullptr)
        {
            throw std::invalid_argument("A live AnyPtr requires non-null data");
        }
        return AnyPtr(&record, data, AccessMode::ReadOnly);
    }

    AnyPtr AnyPtr::writable(const TypeRecord &record, void *data)
    {
        validate_factory_record(record);
        if (data == nullptr)
        {
            throw std::invalid_argument("A live AnyPtr requires non-null data");
        }
        return AnyPtr(&record, data, AccessMode::Writable);
    }

    bool AnyPtr::well_formed() const noexcept
    {
        if (is_unbound())
        {
            return true;
        }
        if (record() == nullptr || !known_access(access_mode()) || !record()->valid())
        {
            return false;
        }
        if (data_ == nullptr)
        {
            return access_mode() == AccessMode::ReadOnly;
        }
        if (access_mode() == AccessMode::Mutation && !has_capability(record()->capabilities, TypeCapabilities::Mutable))
        {
            return false;
        }
        return true;
    }

    bool AnyPtr::bound() const noexcept { return record() != nullptr && well_formed(); }

    bool AnyPtr::is_typed_null() const noexcept
    {
        return bound() && data_ == nullptr && access_mode() == AccessMode::ReadOnly;
    }

    bool AnyPtr::has_value() const noexcept { return data_ != nullptr && well_formed(); }

    bool AnyPtr::valid() const noexcept { return has_value(); }

    const SchemaHeader *AnyPtr::schema() const noexcept
    {
        return well_formed() && record() != nullptr ? record()->schema : nullptr;
    }

    const MemoryUtils::StoragePlan *AnyPtr::plan() const noexcept
    {
        return well_formed() && record() != nullptr ? record()->plan : nullptr;
    }

    const void *AnyPtr::ops() const noexcept { return well_formed() && record() != nullptr ? record()->ops : nullptr; }

    const DebugDescriptor *AnyPtr::debug() const noexcept
    {
        return well_formed() && record() != nullptr ? record()->debug : nullptr;
    }

    TypeClassification AnyPtr::classification() const noexcept
    {
        return well_formed() && record() != nullptr ? record()->classification() : TypeClassification{};
    }

    TypeFamily AnyPtr::family() const noexcept { return classification().family; }

    TypeRole AnyPtr::role() const noexcept { return classification().role; }

    TypeKind AnyPtr::kind() const noexcept { return classification().kind; }

    TypeCapabilities AnyPtr::capabilities() const noexcept
    {
        return well_formed() && record() != nullptr ? record()->capabilities : TypeCapabilities::None;
    }

    std::string_view AnyPtr::semantic_name() const noexcept
    {
        return well_formed() && record() != nullptr ? record()->semantic_name() : std::string_view{};
    }

    std::string_view AnyPtr::implementation_name() const noexcept
    {
        return well_formed() && record() != nullptr ? record()->implementation_name() : std::string_view{};
    }

    std::string_view AnyPtr::effective_name() const noexcept
    {
        return well_formed() && record() != nullptr ? record()->effective_name() : std::string_view{};
    }

    AnyPtr AnyPtr::as_read_only() const
    {
        if (!well_formed())
        {
            throw std::logic_error("Cannot downgrade a malformed AnyPtr");
        }
        if (is_unbound())
        {
            return {};
        }
        return AnyPtr(record(), data_, AccessMode::ReadOnly);
    }

    AnyPtr AnyPtr::begin_mutation() const
    {
        if (!valid() || (access_mode() != AccessMode::Writable && access_mode() != AccessMode::Mutation) ||
            !has_capability(capabilities(), TypeCapabilities::Mutable))
        {
            throw std::logic_error("Mutation requires live writable mutable data");
        }
        return AnyPtr(record(), data_, AccessMode::Mutation);
    }

    AnyPtr AnyPtr::end_mutation() const
    {
        if (!valid() || access_mode() != AccessMode::Mutation)
        {
            throw std::logic_error("Only a live mutation pointer can end mutation");
        }
        return AnyPtr(record(), data_, AccessMode::Writable);
    }

    void *AnyPtr::mutable_data() const
    {
        if (!valid() || access_mode() != AccessMode::Mutation ||
            !has_capability(record()->capabilities, TypeCapabilities::Mutable))
        {
            throw std::logic_error("Mutable data requires a live mutation pointer");
        }
        return const_cast<void *>(data_);
    }
} // namespace hgraph
