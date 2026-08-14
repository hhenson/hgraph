#ifndef HGRAPH_TYPES_TIME_SERIES_TS_TYPE_REF_H
#define HGRAPH_TYPES_TIME_SERIES_TS_TYPE_REF_H

#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/type_pointer.h>

#include <cstddef>
#include <stdexcept>
#include <string_view>
#include <type_traits>

namespace hgraph
{
    struct TSDataOps;
    struct TSParentLink;

    inline constexpr std::uint16_t TS_DATA_OPS_ABI_VERSION = 6;

    class TSRoleTypeRef
    {
      public:
        constexpr TSRoleTypeRef() noexcept = default;
        constexpr TSRoleTypeRef(std::nullptr_t) noexcept {}

        [[nodiscard]] static TSRoleTypeRef checked(AnyPtr pointer);

        [[nodiscard]] constexpr const TypeRecord *record() const noexcept { return record_; }
        [[nodiscard]] constexpr bool bound() const noexcept { return record_ != nullptr; }
        [[nodiscard]] bool valid() const noexcept;
        [[nodiscard]] constexpr explicit operator bool() const noexcept { return bound(); }
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept
        {
            return record_ != nullptr
                       ? reinterpret_cast<const TSValueTypeMetaData *>(record_->schema)
                       : nullptr;
        }
        [[nodiscard]] constexpr const MemoryUtils::StoragePlan *plan() const noexcept
        {
            return record_ != nullptr ? record_->plan : nullptr;
        }
        [[nodiscard]] const MemoryUtils::StoragePlan &checked_plan() const;
        [[nodiscard]] const TSDataOps *ops() const noexcept
        {
            return record_ != nullptr ? static_cast<const TSDataOps *>(record_->ops) : nullptr;
        }
        [[nodiscard]] const TSDataOps &ops_ref() const
        {
            const auto *table = ops();
            if (table == nullptr) throw std::logic_error("TSRoleTypeRef is unbound");
            return *table;
        }
        [[nodiscard]] constexpr TypeRole role() const noexcept
        {
            return record_ != nullptr ? record_->role : TypeRole::Invalid;
        }
        [[nodiscard]] constexpr TypeCapabilities capabilities() const noexcept
        {
            return record_ != nullptr ? record_->capabilities : TypeCapabilities::None;
        }

        [[nodiscard]] friend constexpr bool operator==(TSRoleTypeRef, TSRoleTypeRef) noexcept = default;

      private:
        template <TypeRole> friend class BasicTSTypeRef;
        friend struct TSParentLink;
        friend TSRoleTypeRef intern_ts_type(const TSValueTypeMetaData &, TypeRole,
                                            const MemoryUtils::StoragePlan &, const TSDataOps &,
                                            std::string_view);
        explicit constexpr TSRoleTypeRef(const TypeRecord *record) noexcept : record_(record) {}

        const TypeRecord *record_{nullptr};
    };

    template <TypeRole Role> class BasicTSTypeRef
    {
        static_assert(Role == TypeRole::Data || Role == TypeRole::Input || Role == TypeRole::Output);

      public:
        constexpr BasicTSTypeRef() noexcept = default;
        constexpr BasicTSTypeRef(std::nullptr_t) noexcept {}

        [[nodiscard]] static BasicTSTypeRef checked(AnyPtr pointer)
        {
            const auto generic = TSRoleTypeRef::checked(pointer);
            if (generic && generic.role() != Role) { throw std::invalid_argument("time-series role mismatch"); }
            if (pointer.writable_access() && generic &&
                !has_capability(generic.capabilities(), TypeCapabilities::Mutable))
                throw std::invalid_argument("time-series role does not allow writable access");
            return BasicTSTypeRef{generic.record(), TrustedTag{}};
        }

        [[nodiscard]] static BasicTSTypeRef checked(TSRoleTypeRef type)
        {
            if (type && !type.valid()) { throw std::invalid_argument("invalid time-series type record"); }
            if (type && type.role() != Role) { throw std::invalid_argument("time-series role mismatch"); }
            return BasicTSTypeRef{type.record(), TrustedTag{}};
        }

        [[nodiscard]] constexpr const TypeRecord *record() const noexcept { return record_; }
        [[nodiscard]] constexpr bool bound() const noexcept { return record_ != nullptr; }
        [[nodiscard]] bool valid() const noexcept { return as_role().valid() && as_role().role() == Role; }
        [[nodiscard]] constexpr explicit operator bool() const noexcept { return bound(); }
        [[nodiscard]] const TSValueTypeMetaData *schema() const noexcept { return as_role().schema(); }
        [[nodiscard]] constexpr const MemoryUtils::StoragePlan *plan() const noexcept
        {
            return record_ != nullptr ? record_->plan : nullptr;
        }
        [[nodiscard]] const MemoryUtils::StoragePlan &checked_plan() const
        {
            if (record_ == nullptr || record_->plan == nullptr)
                throw std::logic_error("time-series type reference is unbound");
            return *record_->plan;
        }
        [[nodiscard]] const TSDataOps *ops() const noexcept
        {
            return record_ != nullptr ? static_cast<const TSDataOps *>(record_->ops) : nullptr;
        }
        [[nodiscard]] const TSDataOps &ops_ref() const
        {
            const auto *table = ops();
            if (table == nullptr) throw std::logic_error("time-series type reference is unbound");
            return *table;
        }
        [[nodiscard]] constexpr TypeCapabilities capabilities() const noexcept
        {
            return record_ != nullptr ? record_->capabilities : TypeCapabilities::None;
        }
        [[nodiscard]] TSRoleTypeRef as_role() const noexcept
        {
            return TSRoleTypeRef{record_};
        }

        [[nodiscard]] TypedPtr<TypeFamily::TimeSeries, Role> typed_null() const
        {
            using Pointer = TypedPtr<TypeFamily::TimeSeries, Role>;
            return Pointer{AnyPtr{record_, nullptr, AccessMode::ReadOnly}, typename Pointer::UncheckedTag{}};
        }
        [[nodiscard]] TypedPtr<TypeFamily::TimeSeries, Role> read_only(const void *data) const
        {
            using Pointer = TypedPtr<TypeFamily::TimeSeries, Role>;
            return Pointer{AnyPtr{record_, data, AccessMode::ReadOnly}, typename Pointer::UncheckedTag{}};
        }
        [[nodiscard]] TypedPtr<TypeFamily::TimeSeries, Role> writable(void *data) const
        {
            if (record_ != nullptr && !has_capability(capabilities(), TypeCapabilities::Mutable))
                throw std::logic_error("time-series role is not mutable");
            using Pointer = TypedPtr<TypeFamily::TimeSeries, Role>;
            return Pointer{AnyPtr{record_, data, AccessMode::Writable}, typename Pointer::UncheckedTag{}};
        }

        [[nodiscard]] friend constexpr bool operator==(BasicTSTypeRef, BasicTSTypeRef) noexcept = default;

      private:
        struct TrustedTag {};
        constexpr BasicTSTypeRef(const TypeRecord *record, TrustedTag) noexcept : record_(record) {}

        const TypeRecord *record_{nullptr};
    };

    using TSDataTypeRef = BasicTSTypeRef<TypeRole::Data>;
    using TSInputTypeRef = BasicTSTypeRef<TypeRole::Input>;
    using TSOutputTypeRef = BasicTSTypeRef<TypeRole::Output>;

    [[nodiscard]] TSRoleTypeRef intern_ts_type(const TSValueTypeMetaData &schema,
                                               TypeRole role,
                                               const MemoryUtils::StoragePlan &plan,
                                               const TSDataOps &ops,
                                               std::string_view implementation_label = {});
    [[nodiscard]] bool is_migrated_ts_root_schema(const TSValueTypeMetaData *schema) noexcept;
    [[nodiscard]] TypeCapabilities ts_type_capabilities(TypeRole role,
                                                        const MemoryUtils::StoragePlan &plan,
                                                        const TSDataOps &ops);

    template <TypeRole Role>
    [[nodiscard]] BasicTSTypeRef<Role> checked_ts_role_type(TSRoleTypeRef type,
                                                            std::integral_constant<TypeRole, Role>)
    {
        return BasicTSTypeRef<Role>::checked(type);
    }

    static_assert(sizeof(TSRoleTypeRef) == sizeof(void *));
    static_assert(sizeof(TSDataTypeRef) == sizeof(void *));
    static_assert(sizeof(TSInputTypeRef) == sizeof(void *));
    static_assert(sizeof(TSOutputTypeRef) == sizeof(void *));
    static_assert(std::is_trivially_copyable_v<TSRoleTypeRef>);
    static_assert(std::is_trivially_copyable_v<TSDataTypeRef>);
} // namespace hgraph

#endif
