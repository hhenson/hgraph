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

    namespace detail
    {
        struct TSInputBuilderLayout
        {
            size_t ts_value_offset{0};
            size_t active_offset{0};
            size_t size{0};
            size_t alignment{alignof(std::max_align_t)};
        };

        /**
         * Composite lifecycle dispatcher for `TSInput` storage.
         *
         * `TSInput` is built from two schema-bound sub-builders:
         * - a `TSValueBuilder` for the published TS payload and runtime state
         * - a `ValueBuilder` for the parallel active-state payload
         *
         * `TSInputBuilderOps` combines those sub-builders into one owning
         * storage layout without taking ownership of the memory itself.
         */
        struct HGRAPH_EXPORT TSInputBuilderOps
        {
            [[nodiscard]] virtual TSInputBuilderLayout layout(const TSValueBuilder &ts_value_builder,
                                                              const ValueBuilder &active_builder) const noexcept = 0;
            virtual void construct(void *ts_value_memory,
                                   void *active_memory,
                                   const TSValueBuilder &ts_value_builder,
                                   const ValueBuilder &active_builder) const = 0;
            virtual void destruct(void *ts_value_memory,
                                  void *active_memory,
                                  const TSValueBuilder &ts_value_builder,
                                  const ValueBuilder &active_builder) const noexcept = 0;
            virtual void copy_construct(void *dst_ts_value_memory,
                                        void *dst_active_memory,
                                        const void *src_ts_value_memory,
                                        const void *src_active_memory,
                                        const TSValueBuilder &ts_value_builder,
                                        const ValueBuilder &active_builder) const = 0;
            virtual void move_construct(void *dst_ts_value_memory,
                                        void *dst_active_memory,
                                        void *src_ts_value_memory,
                                        void *src_active_memory,
                                        const TSValueBuilder &ts_value_builder,
                                        const ValueBuilder &active_builder) const = 0;

          protected:
            ~TSInputBuilderOps() = default;
        };
    }  // namespace detail

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

        [[nodiscard]] static TSInputConstructionSlot create_non_peered_collection(const TSMeta *schema);
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
        Payload m_payload;

        friend struct TSInputConstructionPlan;
        friend struct TSInputConstructionPlanCompiler;
        friend struct TSInputBuilderFactory;
    };

    struct HGRAPH_EXPORT TSInputConstructionEdge
    {
        std::vector<int64_t> input_path;
        TSInputBindingRef    binding;
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
     * `TSInputBuilder` follows the same role split as the value-layer
     * builders:
     * - the construction plan is data
     * - builder ops provide construction and copy/move behavior
     * - the builder caches the resulting combined layout
     *
     * The combined input storage contains:
     * - a `TSValue` region for the published TS payload/state
     * - an active-state `Value` region for input activation tracking
     */
    struct HGRAPH_EXPORT TSInputBuilder
    {
        enum class MemoryOwnership : uint8_t
        {
            External,
            Owned = 1,
        };

        [[nodiscard]] const TSMeta &schema() const noexcept { return *m_schema; }
        [[nodiscard]] const value::TypeMeta &active_schema() const noexcept { return *m_active_schema; }
        [[nodiscard]] const TSValueBuilder &ts_value_builder() const noexcept { return m_ts_value_builder; }
        [[nodiscard]] const ValueBuilder &active_builder() const noexcept { return *m_active_builder; }
        [[nodiscard]] size_t size() const noexcept { return m_size; }
        [[nodiscard]] size_t alignment() const noexcept { return m_alignment; }
        [[nodiscard]] size_t ts_value_offset() const noexcept { return m_ts_value_offset; }
        [[nodiscard]] size_t active_offset() const noexcept { return m_active_offset; }

        [[nodiscard]] void *allocate() const;
        void construct(void *memory) const;
        void destruct(void *memory) const noexcept;
        void deallocate(void *memory) const noexcept;
        void copy_construct(void *dst, const void *src, const TSInputBuilder &src_builder) const;
        void move_construct(void *dst, void *src, const TSInputBuilder &src_builder) const;

        [[nodiscard]] void *ts_value_memory(void *memory) const noexcept;
        [[nodiscard]] const void *ts_value_memory(const void *memory) const noexcept;
        [[nodiscard]] void *active_memory(void *memory) const noexcept;
        [[nodiscard]] const void *active_memory(const void *memory) const noexcept;

        [[nodiscard]] bool compatible_with(const TSInputBuilder &other) const noexcept;
        [[nodiscard]] TSInput make_input() const;
        /**
         * Construct an input into caller-supplied storage.
         *
         * This assumes `input` is uninitialized and `memory` points at a raw
         * storage block that satisfies this builder's `size()` and
         * `alignment()` requirements. If construction throws, any partially
         * constructed subobjects are cleaned up, `input` is reset to the
         * unbound state, and ownership of `memory` remains with the caller.
         */
        void construct_input(TSInput &input, void *memory, MemoryOwnership ownership = MemoryOwnership::External) const;
        /**
         * Allocate storage and construct an input into it.
         *
         * This is the convenience heap-owning wrapper over
         * `construct_input(input, memory)`.
         */
        void construct_input(TSInput &input) const;
        /**
         * Copy-construct an input into caller-supplied storage.
         *
         * This assumes `input` is uninitialized. The destination storage is
         * supplied by the caller unless `ownership` is set to `Owned`.
         */
        void copy_construct_input(TSInput &input,
                                  const TSInput &other,
                                  void *memory,
                                  MemoryOwnership ownership = MemoryOwnership::External) const;
        /**
         * Allocate storage and copy-construct an input into it.
         *
         * This is the convenience heap-owning wrapper over
         * `copy_construct_input(input, other, memory)`.
         */
        void copy_construct_input(TSInput &input, const TSInput &other) const;
        void                move_construct_input(TSInput &input, TSInput &other) const;
        void                destruct_input(TSInput &input) const noexcept;

      private:
        friend struct TSInputBuilderFactory;

        TSInputBuilder(const TSMeta &schema,
                       const value::TypeMeta &active_schema,
                       std::shared_ptr<const detail::TSBuilderOps> ts_state_builder_ops,
                       const detail::TSInputBuilderOps &builder_ops) noexcept;

        const TSMeta *m_schema{nullptr};
        const value::TypeMeta *m_active_schema{nullptr};
        std::shared_ptr<const detail::TSBuilderOps> m_ts_state_builder_ops;
        TSValueBuilder m_ts_value_builder;
        const ValueBuilder *m_active_builder{nullptr};
        std::reference_wrapper<const detail::TSInputBuilderOps> m_builder_ops;
        size_t m_ts_value_offset{0};
        size_t m_active_offset{0};
        size_t m_size{0};
        size_t m_alignment{alignof(std::max_align_t)};
    };

    struct HGRAPH_EXPORT TSInputBuilderFactory
    {
        [[nodiscard]] static const TSInputBuilder *builder_for(const TSInputConstructionPlan &construction_plan);
        [[nodiscard]] static const TSInputBuilder &checked_builder_for(const TSInputConstructionPlan &construction_plan);
    };
}  // namespace hgraph
