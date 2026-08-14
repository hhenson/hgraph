#ifndef HGRAPH_RUNTIME_EXECUTOR_TYPE_REF_H
#define HGRAPH_RUNTIME_EXECUTOR_TYPE_REF_H

#include <hgraph/types/metadata/type_record.h>
#include <hgraph/types/type_pointer.h>

#include <cstddef>
#include <functional>
#include <stdexcept>
#include <string_view>
#include <type_traits>

namespace hgraph
{
    struct GraphExecutorOps;
    struct GraphExecutorTypeMetaData;

    inline constexpr std::uint16_t EXECUTOR_OPS_ABI_VERSION = 5;

    class ExecutorTypeRef
    {
      public:
        constexpr ExecutorTypeRef() noexcept = default;
        constexpr ExecutorTypeRef(std::nullptr_t) noexcept {}

        [[nodiscard]] static ExecutorTypeRef checked(AnyPtr pointer);

        [[nodiscard]] constexpr const TypeRecord *record() const noexcept { return record_; }
        [[nodiscard]] constexpr bool bound() const noexcept { return record_ != nullptr; }
        [[nodiscard]] bool valid() const noexcept;
        [[nodiscard]] constexpr explicit operator bool() const noexcept { return bound(); }
        [[nodiscard]] const GraphExecutorTypeMetaData *schema() const noexcept;
        [[nodiscard]] constexpr const MemoryUtils::StoragePlan *plan() const noexcept
        {
            return record_ != nullptr ? record_->plan : nullptr;
        }
        [[nodiscard]] const MemoryUtils::StoragePlan &checked_plan() const;
        // Header-inline like the other trivial record accessors: these run
        // on hot evaluation paths and the out-of-line definitions were not
        // inlined even under LTO (profiled as bare call overhead).
        [[nodiscard]] const GraphExecutorOps *ops() const noexcept
        {
            return record_ != nullptr ? static_cast<const GraphExecutorOps *>(record_->ops) : nullptr;
        }
        [[nodiscard]] const GraphExecutorOps &ops_ref() const
        {
            const auto *table = ops();
            if (table == nullptr) { throw std::logic_error("ExecutorTypeRef is unbound"); }
            return *table;
        }
        [[nodiscard]] constexpr TypeCapabilities capabilities() const noexcept
        {
            return record_ != nullptr ? record_->capabilities : TypeCapabilities::None;
        }

        [[nodiscard]] ExecutorPtr typed_null() const noexcept;
        [[nodiscard]] ExecutorPtr read_only(const void *data) const noexcept;
        [[nodiscard]] ExecutorPtr writable(void *data) const noexcept;

        [[nodiscard]] friend constexpr bool operator==(ExecutorTypeRef, ExecutorTypeRef) noexcept = default;

      private:
        friend ExecutorTypeRef intern_executor_type(const GraphExecutorTypeMetaData &,
                                                    const MemoryUtils::StoragePlan &,
                                                    const GraphExecutorOps &, std::string_view);
        friend class GraphExecutorView;
        friend class GraphExecutorValue;
        friend class PushQueueEngineView;

        explicit constexpr ExecutorTypeRef(const TypeRecord *record) noexcept : record_(record) {}

        const TypeRecord *record_{nullptr};
    };

    [[nodiscard]] TypeCapabilities executor_type_capabilities(const MemoryUtils::StoragePlan &plan);
    [[nodiscard]] ExecutorTypeRef intern_executor_type(const GraphExecutorTypeMetaData &schema,
                                                       const MemoryUtils::StoragePlan &plan,
                                                       const GraphExecutorOps &ops,
                                                       std::string_view implementation_label = {});

    static_assert(sizeof(ExecutorTypeRef) == sizeof(void *));
    static_assert(alignof(ExecutorTypeRef) == alignof(void *));
    static_assert(std::is_standard_layout_v<ExecutorTypeRef>);
    static_assert(std::is_trivially_copyable_v<ExecutorTypeRef>);
}  // namespace hgraph

template <> struct std::hash<hgraph::ExecutorTypeRef>
{
    [[nodiscard]] std::size_t operator()(hgraph::ExecutorTypeRef type) const noexcept
    {
        return std::hash<const hgraph::TypeRecord *>{}(type.record());
    }
};

#endif  // HGRAPH_RUNTIME_EXECUTOR_TYPE_REF_H
