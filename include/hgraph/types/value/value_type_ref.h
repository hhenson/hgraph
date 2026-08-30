#ifndef HGRAPH_TYPES_VALUE_VALUE_TYPE_REF_H
#define HGRAPH_TYPES_VALUE_VALUE_TYPE_REF_H

#include <hgraph/types/metadata/type_record.h>
#include <hgraph/types/metadata/value_type_meta_data.h>
#include <hgraph/types/type_pointer.h>

#include <cstddef>
#include <functional>
#include <type_traits>

namespace hgraph
{
    struct ValueOps;
    class Value;
    class ValueView;

    class HGRAPH_CLASS_EXPORT ValueTypeRef
    {
      public:
        constexpr ValueTypeRef() noexcept = default;
        constexpr ValueTypeRef(std::nullptr_t) noexcept {}

        [[nodiscard]] static ValueTypeRef checked(AnyPtr pointer);

        [[nodiscard]] constexpr const TypeRecord *record() const noexcept { return record_; }
        [[nodiscard]] constexpr bool bound() const noexcept { return record_ != nullptr; }
        [[nodiscard]] bool valid() const noexcept { return record_ != nullptr && record_->valid(); }
        [[nodiscard]] constexpr explicit operator bool() const noexcept { return bound(); }

        [[nodiscard]] const ValueTypeMetaData *schema() const noexcept
        {
            return record_ != nullptr ? reinterpret_cast<const ValueTypeMetaData *>(record_->schema) : nullptr;
        }
        [[nodiscard]] constexpr const MemoryUtils::StoragePlan *plan() const noexcept
        {
            return record_ != nullptr ? record_->plan : nullptr;
        }
        [[nodiscard]] const MemoryUtils::StoragePlan &checked_plan() const;
        // Header-inline like the other trivial record accessors: these run
        // on hot evaluation paths and the out-of-line definitions were not
        // inlined even under LTO (profiled as bare call overhead).
        [[nodiscard]] const ValueOps *ops() const noexcept
        {
            return record_ != nullptr ? static_cast<const ValueOps *>(record_->ops) : nullptr;
        }
        [[nodiscard]] const ValueOps &ops_ref() const
        {
            const auto *table = ops();
            if (table == nullptr) { throw std::logic_error("ValueTypeRef is unbound"); }
            return *table;
        }
        [[nodiscard]] const MemoryUtils::LifecycleOps *lifecycle() const noexcept
        {
            return plan() != nullptr ? &plan()->lifecycle : nullptr;
        }
        [[nodiscard]] const void *lifecycle_context() const noexcept
        {
            return plan() != nullptr ? plan()->lifecycle_context : nullptr;
        }
        void default_construct_at(void *memory) const { checked_plan().default_construct(memory); }
        void destroy_at(void *memory) const noexcept { if (plan() != nullptr) plan()->destroy(memory); }
        void copy_construct_at(void *dst, const void *src) const { checked_plan().copy_construct(dst, src); }
        void move_construct_at(void *dst, void *src) const { checked_plan().move_construct(dst, src); }
        void copy_assign_at(void *dst, const void *src) const { checked_plan().copy_assign(dst, src); }
        void move_assign_at(void *dst, void *src) const { checked_plan().move_assign(dst, src); }

        [[nodiscard]] constexpr TypeCapabilities capabilities() const noexcept
        {
            return record_ != nullptr ? record_->capabilities : TypeCapabilities::None;
        }

        [[nodiscard]] ValuePtr typed_null() const noexcept;
        [[nodiscard]] ValuePtr read_only(const void *data) const noexcept;
        [[nodiscard]] ValuePtr writable(void *data) const noexcept;

        [[nodiscard]] friend constexpr bool operator==(ValueTypeRef, ValueTypeRef) noexcept = default;

      private:
        friend ValueTypeRef intern_value_type(const ValueTypeMetaData &, const MemoryUtils::StoragePlan &,
                                              const ValueOps &, const DebugDescriptor *);
        friend HGRAPH_EXPORT ValueTypeRef value_owning_type(ValueTypeRef);
        friend class Value;
        friend class ValueView;

        explicit constexpr ValueTypeRef(const TypeRecord *record) noexcept : record_(record) {}

        const TypeRecord *record_{nullptr};
    };

    [[nodiscard]] TypeCapabilities value_type_capabilities(const ValueTypeMetaData &schema,
                                                           const MemoryUtils::StoragePlan &plan,
                                                           const ValueOps &ops);
    [[nodiscard]] ValueTypeRef intern_value_type(const ValueTypeMetaData &schema,
                                                 const MemoryUtils::StoragePlan &plan,
                                                 const ValueOps &ops,
                                                 const DebugDescriptor *debug = nullptr);

    /** Register and resolve the ordinary owning representation for a
        graph-local binding.  The association is published once on the type
        record, so ordinary construction requires no registry lookup. */
    HGRAPH_EXPORT void register_value_owning_type(ValueTypeRef binding,
                                                  ValueTypeRef owning_type);
    [[nodiscard]] HGRAPH_EXPORT ValueTypeRef
    value_owning_type(ValueTypeRef binding);

    static_assert(sizeof(ValueTypeRef) == sizeof(void *));
    static_assert(alignof(ValueTypeRef) == alignof(void *));
    static_assert(std::is_standard_layout_v<ValueTypeRef>);
    static_assert(std::is_trivially_copyable_v<ValueTypeRef>);
} // namespace hgraph

template <> struct std::hash<hgraph::ValueTypeRef>
{
    [[nodiscard]] std::size_t operator()(hgraph::ValueTypeRef type) const noexcept
    {
        return std::hash<const hgraph::TypeRecord *>{}(type.record());
    }
};

#endif // HGRAPH_TYPES_VALUE_VALUE_TYPE_REF_H
