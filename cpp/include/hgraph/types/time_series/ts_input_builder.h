#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/ts_value_builder.h>
#include <hgraph/types/time_series/value/builder.h>
#include <hgraph/util/tagged_ptr.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <variant>
#include <vector>

namespace hgraph
{
    struct TSInput;

    // No detail namespace needed — TSInputBuilder delegates directly to TSValueBuilder.

    struct HGRAPH_EXPORT TSInputBindingRef
    {
        int64_t              src_node{-1};
        std::vector<int64_t> output_path;

        [[nodiscard]] bool operator==(const TSInputBindingRef &other) const noexcept
        {
            return src_node == other.src_node && output_path == other.output_path;
        }
    };

    enum class TSInputSlotKind : uint8_t
    {
        Empty,
        NonPeeredCollection,
        LinkTerminal,
    };

    struct HGRAPH_EXPORT TSInputConstructionSlot
    {
        TSInputConstructionSlot() = default;
        TSInputConstructionSlot(const TSInputConstructionSlot &) = default;
        TSInputConstructionSlot(TSInputConstructionSlot &&) noexcept = default;
        TSInputConstructionSlot &operator=(const TSInputConstructionSlot &) = default;
        TSInputConstructionSlot &operator=(TSInputConstructionSlot &&) noexcept = default;
        ~TSInputConstructionSlot() = default;

        [[nodiscard]] static TSInputConstructionSlot create_non_peered_collection(const TSMeta *schema,
                                                                                  const TSMeta *bound_schema = nullptr);
        [[nodiscard]] static TSInputConstructionSlot create_link_terminal(const TSMeta *schema,
                                                                          TSInputBindingRef binding = {}) noexcept;

        [[nodiscard]] TSInputSlotKind kind() const noexcept
        {
            return static_cast<TSInputSlotKind>(m_schema.tag());
        }

        [[nodiscard]] const TSMeta *schema() const noexcept
        {
            return m_schema.ptr();
        }

        [[nodiscard]] const TSMeta *bound_schema() const noexcept
        {
            return m_bound_schema;
        }

        void set_bound_schema(const TSMeta *schema) noexcept
        {
            m_bound_schema = schema;
        }

        [[nodiscard]] bool is_non_peered_collection() const noexcept
        {
            return kind() == TSInputSlotKind::NonPeeredCollection;
        }

        [[nodiscard]] bool is_link_terminal() const noexcept
        {
            return kind() == TSInputSlotKind::LinkTerminal;
        }

        [[nodiscard]] const std::vector<TSInputConstructionSlot> &children() const;
        [[nodiscard]] std::vector<TSInputConstructionSlot> &children();
        [[nodiscard]] const TSInputBindingRef &binding() const;
        [[nodiscard]] TSInputBindingRef &binding();

      private:
        using Payload = std::variant<std::monostate, std::vector<TSInputConstructionSlot>, TSInputBindingRef>;
        using SchemaPtr = tagged_ptr<TSMeta, 2>;

        constexpr void set_schema_and_kind(const TSMeta *schema, TSInputSlotKind kind) noexcept
        {
            m_schema.set(const_cast<TSMeta *>(schema), static_cast<typename SchemaPtr::storage_type>(kind));
        }

        SchemaPtr m_schema;
        const TSMeta *m_bound_schema{nullptr};
        Payload m_payload;

        friend struct TSInputConstructionPlan;
        friend struct TSInputConstructionPlanCompiler;
        friend struct TSInputBuilderFactory;
    };

    struct HGRAPH_EXPORT TSInputConstructionEdge
    {
        std::vector<int64_t> input_path;
        TSInputBindingRef    binding;
        const TSMeta        *source_schema{nullptr};
    };

    struct HGRAPH_EXPORT TSInputConstructionPlan
    {
        explicit TSInputConstructionPlan(const TSMeta *root_schema);

        [[nodiscard]] const TSMeta &schema() const noexcept { return *m_root.schema(); }
        [[nodiscard]] const TSInputConstructionSlot &root() const noexcept { return m_root; }
        [[nodiscard]] TSInputConstructionSlot &root() noexcept { return m_root; }

      private:
        TSInputConstructionSlot m_root;
    };

    struct HGRAPH_EXPORT TSInputConstructionPlanCompiler
    {
        [[nodiscard]] static TSInputConstructionPlan compile(const TSMeta &root_schema,
                                                             const std::vector<TSInputConstructionEdge> &edges);
    };

    /**
     * Cached plan-specific builder for `TSInput`.
     *
     * The input storage is a single `TSValue` region for the published TS
     * payload and runtime state. Active state is tracked separately via the
     * `ActiveTrie` member on `TSInput` (not managed by this builder).
     */
    struct HGRAPH_EXPORT TSInputBuilder
    {
        enum class MemoryOwnership : uint8_t
        {
            External,
            Owned = 1,
        };

        [[nodiscard]] const TSMeta &schema() const noexcept { return *m_schema; }
        [[nodiscard]] const TSValueBuilder &ts_value_builder() const noexcept { return m_ts_value_builder; }
        [[nodiscard]] size_t size() const noexcept { return m_ts_value_builder.size(); }
        [[nodiscard]] size_t alignment() const noexcept { return m_ts_value_builder.alignment(); }

        [[nodiscard]] void *allocate() const;
        void construct(void *memory) const;
        void destruct(void *memory) const noexcept;
        void deallocate(void *memory) const noexcept;
        void copy_construct(void *dst, const void *src, const TSInputBuilder &src_builder) const;
        void move_construct(void *dst, void *src, const TSInputBuilder &src_builder) const;

        [[nodiscard]] void *ts_value_memory(void *memory) const noexcept;
        [[nodiscard]] const void *ts_value_memory(const void *memory) const noexcept;

        [[nodiscard]] bool compatible_with(const TSInputBuilder &other) const noexcept;
        [[nodiscard]] TSInput make_input() const;

        void construct_input(TSInput &input, void *memory, MemoryOwnership ownership = MemoryOwnership::External) const;
        void construct_input(TSInput &input) const;
        void copy_construct_input(TSInput &input,
                                  const TSInput &other,
                                  void *memory,
                                  MemoryOwnership ownership = MemoryOwnership::External) const;
        void copy_construct_input(TSInput &input, const TSInput &other) const;
        void move_construct_input(TSInput &input, TSInput &other) const;
        void destruct_input(TSInput &input) const noexcept;

      private:
        friend struct TSInputBuilderFactory;

        TSInputBuilder(const TSMeta &schema,
                       std::shared_ptr<const detail::TSBuilderOps> ts_state_builder_ops) noexcept;

        const TSMeta *m_schema{nullptr};
        std::shared_ptr<const detail::TSBuilderOps> m_ts_state_builder_ops;
        TSValueBuilder m_ts_value_builder;
    };

    struct HGRAPH_EXPORT TSInputBuilderFactory
    {
        [[nodiscard]] static const TSInputBuilder *builder_for(const TSInputConstructionPlan &construction_plan);
        [[nodiscard]] static const TSInputBuilder &checked_builder_for(const TSInputConstructionPlan &construction_plan);
    };
}  // namespace hgraph
