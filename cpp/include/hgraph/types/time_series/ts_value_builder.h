#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/time_series_state.h>
#include <hgraph/types/time_series/ts_meta.h>
#include <hgraph/types/time_series/value/builder.h>

#include <functional>
#include <new>

namespace hgraph
{
    struct TSValueBuilder;

    namespace detail
    {
        /**
         * Lifecycle dispatcher for the time-series extension region.
         *
         * The value region and the time-series region are owned by the same
         * `TSValue`, but the two regions serve different purposes. The value
         * region remains data-first and schema-shaped, while the TS region
         * holds the time-series runtime state associated with that data.
         *
         * `TSStateOps` is the TS analogue of the value-layer `ValueBuilderOps`: it
         * knows how to size, construct, destroy, copy, and move the TS region
         * for a schema, but it does not own the memory itself.
         */
        struct HGRAPH_EXPORT TSStateOps
        {
            virtual ~TSStateOps() = default;

            virtual void expand_builder(TSValueBuilder &builder, const TSMeta &schema) const noexcept = 0;
            virtual void construct(void *memory, const TSMeta &schema) const = 0;
            virtual void destroy(void *memory) const noexcept = 0;
            virtual void copy_construct(void *dst, const void *src, const TSMeta &schema) const = 0;
            virtual void move_construct(void *dst, void *src, const TSMeta &schema) const = 0;
        };
    }  // namespace detail

    /**
     * Cached schema-bound builder for combined time-series storage.
     *
     * `TSValueBuilder` mirrors the value-layer builder design, but it plans a
     * single allocation containing two regions:
     * - a data-first value region described by `ValueBuilder`
     * - a TS extension region described by `TSStateOps`
     *
     * The value region is laid out first so fixed shapes retain their clean
     * memory representation. The TS region is placed after that at the
     * required alignment boundary. `TSView` can later cache both expanded
     * pointers for efficient navigation, but the owning `TSValue` only needs
     * one storage pointer.
     */
    struct HGRAPH_EXPORT TSValueBuilder
    {
        TSValueBuilder(const TSMeta &schema, const ValueBuilder &value_builder, const detail::TSStateOps &state_ops) noexcept;

        [[nodiscard]] const TSMeta &schema() const noexcept { return m_schema.get(); }
        [[nodiscard]] const ValueBuilder &value_builder() const noexcept { return m_value_builder.get(); }
        [[nodiscard]] size_t value_offset() const noexcept { return m_value_offset; }
        [[nodiscard]] size_t ts_offset() const noexcept { return m_ts_offset; }
        [[nodiscard]] size_t size() const noexcept { return m_size; }
        [[nodiscard]] size_t alignment() const noexcept { return m_alignment; }

        void cache_layout(size_t value_offset, size_t ts_offset, size_t size, size_t alignment) noexcept;

        [[nodiscard]] void *allocate() const;
        void deallocate(void *memory) const noexcept;

        void construct(void *memory) const;
        void destroy(void *memory) const noexcept;
        void copy_construct(void *dst, const void *src, const TSValueBuilder &src_builder) const;
        void move_construct(void *dst, void *src, const TSValueBuilder &src_builder) const;

        [[nodiscard]] void *value_memory(void *memory) const noexcept;
        [[nodiscard]] const void *value_memory(const void *memory) const noexcept;
        [[nodiscard]] void *ts_memory(void *memory) const noexcept;
        [[nodiscard]] const void *ts_memory(const void *memory) const noexcept;

      private:
        std::reference_wrapper<const TSMeta> m_schema;
        std::reference_wrapper<const ValueBuilder> m_value_builder;
        std::reference_wrapper<const detail::TSStateOps> m_state_ops;
        size_t m_value_offset{0};
        size_t m_ts_offset{0};
        size_t m_size{0};
        size_t m_alignment{alignof(std::max_align_t)};
    };

    /**
     * Schema-to-builder lookup for `TSValue`.
     *
     * Builders are cached singletons per time-series schema pointer. The
     * builder captures both the value-layer builder for `TSMeta::value_type`
     * and the time-series extension layout for the same schema.
     */
    struct HGRAPH_EXPORT TSValueBuilderFactory
    {
        [[nodiscard]] static const TSValueBuilder *builder_for(const TSMeta &schema);
        [[nodiscard]] static const TSValueBuilder *builder_for(const TSMeta *schema);
        [[nodiscard]] static const TSValueBuilder &checked_builder_for(const TSMeta &schema);
        [[nodiscard]] static const TSValueBuilder &checked_builder_for(const TSMeta *schema);
    };
}  // namespace hgraph
