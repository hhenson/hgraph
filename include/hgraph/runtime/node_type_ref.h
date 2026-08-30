#ifndef HGRAPH_RUNTIME_NODE_TYPE_REF_H
#define HGRAPH_RUNTIME_NODE_TYPE_REF_H

#include <hgraph/types/metadata/type_record.h>
#include <hgraph/types/type_pointer.h>

#include <cstddef>
#include <functional>
#include <span>
#include <string_view>
#include <type_traits>

namespace hgraph
{
    struct DebugField;
    struct DebugDynamicLayout;
    struct NodeOps;
    struct NodeTypeMetaData;

    inline constexpr std::uint16_t NODE_OPS_ABI_VERSION = 5;

    /** One-word canonical identity for a runtime node implementation. */
    class HGRAPH_CLASS_EXPORT NodeTypeRef
    {
      public:
        constexpr NodeTypeRef() noexcept = default;
        constexpr NodeTypeRef(std::nullptr_t) noexcept {}

        [[nodiscard]] static NodeTypeRef checked(AnyPtr pointer);

        [[nodiscard]] constexpr const TypeRecord *record() const noexcept { return record_; }
        [[nodiscard]] constexpr bool bound() const noexcept { return record_ != nullptr; }
        [[nodiscard]] bool valid() const noexcept;
        [[nodiscard]] constexpr explicit operator bool() const noexcept { return bound(); }
        [[nodiscard]] const NodeTypeMetaData *schema() const noexcept;
        [[nodiscard]] constexpr const MemoryUtils::StoragePlan *plan() const noexcept
        {
            return record_ != nullptr ? record_->plan : nullptr;
        }
        [[nodiscard]] const MemoryUtils::StoragePlan &checked_plan() const;
        void destroy_at(void *memory) const noexcept
        {
            if (plan() != nullptr) plan()->destroy(memory);
        }
        // ops()/ops_ref() are header-inline like plan()/capabilities():
        // they run on every node evaluation and profiling showed the
        // out-of-line definitions were not inlined even under LTO
        // (~2-3% self time as bare call overhead).
        [[nodiscard]] const NodeOps *ops() const noexcept
        {
            return record_ != nullptr ? static_cast<const NodeOps *>(record_->ops) : nullptr;
        }
        [[nodiscard]] const NodeOps &ops_ref() const
        {
            const auto *table = ops();
            if (table == nullptr) { throw std::logic_error("NodeTypeRef is unbound"); }
            return *table;
        }
        [[nodiscard]] constexpr TypeCapabilities capabilities() const noexcept
        {
            return record_ != nullptr ? record_->capabilities : TypeCapabilities::None;
        }

        [[nodiscard]] NodePtr typed_null() const noexcept;
        [[nodiscard]] NodePtr read_only(const void *data) const noexcept;
        [[nodiscard]] NodePtr writable(void *data) const noexcept;

        [[nodiscard]] friend constexpr bool operator==(NodeTypeRef, NodeTypeRef) noexcept = default;

      private:
        friend NodeTypeRef intern_node_type(const NodeTypeMetaData &, const MemoryUtils::StoragePlan &,
                                            const NodeOps &, std::string_view, std::span<const DebugField>,
                                            const TypeRecord *, const TypeRecord *, const DebugDynamicLayout *);
        friend class NodeView;
        friend class NodeValue;
        friend struct TSParentLink;

        explicit constexpr NodeTypeRef(const TypeRecord *record) noexcept : record_(record) {}

        const TypeRecord *record_{nullptr};
    };

    [[nodiscard]] TypeCapabilities node_type_capabilities(const MemoryUtils::StoragePlan &plan);
    [[nodiscard]] NodeTypeRef intern_node_type(const NodeTypeMetaData &schema,
                                               const MemoryUtils::StoragePlan &plan,
                                               const NodeOps &ops,
                                               std::string_view implementation_label = {},
                                               std::span<const DebugField> debug_fields = {},
                                               const TypeRecord *debug_key_type = nullptr,
                                               const TypeRecord *debug_element_type = nullptr,
                                               const DebugDynamicLayout *debug_dynamic_layout = nullptr);

    static_assert(sizeof(NodeTypeRef) == sizeof(void *));
    static_assert(alignof(NodeTypeRef) == alignof(void *));
    static_assert(std::is_standard_layout_v<NodeTypeRef>);
    static_assert(std::is_trivially_copyable_v<NodeTypeRef>);
}  // namespace hgraph

template <> struct std::hash<hgraph::NodeTypeRef>
{
    [[nodiscard]] std::size_t operator()(hgraph::NodeTypeRef type) const noexcept
    {
        return std::hash<const hgraph::TypeRecord *>{}(type.record());
    }
};

#endif  // HGRAPH_RUNTIME_NODE_TYPE_REF_H
