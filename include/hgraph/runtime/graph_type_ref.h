#ifndef HGRAPH_RUNTIME_GRAPH_TYPE_REF_H
#define HGRAPH_RUNTIME_GRAPH_TYPE_REF_H

#include <hgraph/types/metadata/type_record.h>
#include <hgraph/types/type_pointer.h>

#include <cstddef>
#include <functional>
#include <stdexcept>
#include <string_view>
#include <type_traits>

namespace hgraph
{
    struct GraphOps;
    struct GraphTypeMetaData;

    inline constexpr std::uint16_t GRAPH_OPS_ABI_VERSION = 5;

    class GraphTypeRef
    {
      public:
        constexpr GraphTypeRef() noexcept = default;
        constexpr GraphTypeRef(std::nullptr_t) noexcept {}

        [[nodiscard]] static GraphTypeRef checked(AnyPtr pointer);

        [[nodiscard]] constexpr const TypeRecord *record() const noexcept { return record_; }
        [[nodiscard]] constexpr bool bound() const noexcept { return record_ != nullptr; }
        [[nodiscard]] bool valid() const noexcept;
        [[nodiscard]] constexpr explicit operator bool() const noexcept { return bound(); }
        [[nodiscard]] const GraphTypeMetaData *schema() const noexcept;
        [[nodiscard]] constexpr const MemoryUtils::StoragePlan *plan() const noexcept
        {
            return record_ != nullptr ? record_->plan : nullptr;
        }
        [[nodiscard]] const MemoryUtils::StoragePlan &checked_plan() const;
        void destroy_at(void *memory) const noexcept
        {
            if (plan() != nullptr) plan()->destroy(memory);
        }
        // Header-inline like the other trivial record accessors: these run
        // on hot evaluation paths and the out-of-line definitions were not
        // inlined even under LTO (profiled as bare call overhead).
        [[nodiscard]] const GraphOps *ops() const noexcept
        {
            return record_ != nullptr ? static_cast<const GraphOps *>(record_->ops) : nullptr;
        }
        [[nodiscard]] const GraphOps &ops_ref() const
        {
            const auto *table = ops();
            if (table == nullptr) { throw std::logic_error("GraphTypeRef is unbound"); }
            return *table;
        }
        [[nodiscard]] constexpr TypeCapabilities capabilities() const noexcept
        {
            return record_ != nullptr ? record_->capabilities : TypeCapabilities::None;
        }

        [[nodiscard]] GraphPtr typed_null() const noexcept;
        [[nodiscard]] GraphPtr read_only(const void *data) const noexcept;
        [[nodiscard]] GraphPtr writable(void *data) const noexcept;

        [[nodiscard]] friend constexpr bool operator==(GraphTypeRef, GraphTypeRef) noexcept = default;

      private:
        friend GraphTypeRef intern_graph_type(const GraphTypeMetaData &, const MemoryUtils::StoragePlan &,
                                              const GraphOps &, std::string_view);
        friend class GraphView;
        friend class GraphValue;

        explicit constexpr GraphTypeRef(const TypeRecord *record) noexcept : record_(record) {}

        const TypeRecord *record_{nullptr};
    };

    [[nodiscard]] TypeCapabilities graph_type_capabilities(const MemoryUtils::StoragePlan &plan);
    [[nodiscard]] GraphTypeRef intern_graph_type(const GraphTypeMetaData &schema,
                                                 const MemoryUtils::StoragePlan &plan,
                                                 const GraphOps &ops,
                                                 std::string_view implementation_label = {});

    static_assert(sizeof(GraphTypeRef) == sizeof(void *));
    static_assert(alignof(GraphTypeRef) == alignof(void *));
    static_assert(std::is_standard_layout_v<GraphTypeRef>);
    static_assert(std::is_trivially_copyable_v<GraphTypeRef>);
}  // namespace hgraph

template <> struct std::hash<hgraph::GraphTypeRef>
{
    [[nodiscard]] std::size_t operator()(hgraph::GraphTypeRef type) const noexcept
    {
        return std::hash<const hgraph::TypeRecord *>{}(type.record());
    }
};

#endif  // HGRAPH_RUNTIME_GRAPH_TYPE_REF_H
