#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/tracking.h>
#include <hgraph/types/time_series/value/associative.h>
#include <hgraph/types/time_series/value/atomic.h>
#include <hgraph/types/time_series/value/list.h>
#include <hgraph/types/time_series/value/record.h>
#include <hgraph/types/time_series/value/sequence.h>

#include <functional>
#include <new>

namespace hgraph
{
    namespace detail
    {

        /**
         * Shared small-buffer policy for dynamically sized containers.
         *
         * Dynamic containers still need heap growth when they exceed their
         * inline budget, but the root value allocation can reserve enough
         * schema-shaped storage for a small number of elements up front. This
         * keeps small collections in one allocation and defers heap growth
         * until the inline budget is exhausted.
         *
         * The policy is bounded by both a byte budget and an element-count
         * budget so wide element schemas do not force an excessively large root
         * allocation, while narrow schemas still get a useful inline capacity.
         */
        struct SmallBufferPolicy
        {
            static constexpr size_t target_bytes = 256;
            static constexpr size_t max_elements = 20;

            template <typename TFits>
            [[nodiscard]] static size_t capacity_for(TFits &&fits) noexcept
            {
                for (size_t elements = max_elements; elements > 0; --elements) {
                    if (fits(elements)) { return elements; }
                }
                return 0;
            }
        };

        /**
         * Lifecycle dispatcher for raw value storage.
         *
         * `StateOps` is responsible only for constructing, destroying, copying,
         * and moving plain storage. It does not own memory and it does not own
         * behavior dispatch.
         */
        struct HGRAPH_EXPORT StateOps
        {
            virtual ~StateOps() = default;

            virtual void expand_builder(struct ValueBuilder &builder, const value::TypeMeta &schema) const noexcept = 0;
            [[nodiscard]] virtual const ViewDispatch &view_dispatch(const value::TypeMeta &schema) const noexcept = 0;
            [[nodiscard]] virtual bool requires_destroy(const value::TypeMeta &schema) const noexcept = 0;
            [[nodiscard]] virtual bool requires_deallocate(const value::TypeMeta &schema) const noexcept = 0;
            [[nodiscard]] virtual bool stores_inline_in_value_handle(const value::TypeMeta &schema) const noexcept = 0;
            virtual void construct(void *memory) const = 0;
            virtual void destroy(void *memory) const noexcept = 0;
            virtual void copy_construct(void *dst, const void *src) const = 0;
            virtual void move_construct(void *dst, void *src) const = 0;
        };

    }  // namespace detail

    /**
     * Cached schema-bound storage builder.
     *
     * The builder owns the layout and lifecycle facts for a schema. It is the
     * one place that decides whether a value is stored inline in the `Value`
     * handle or in a separately allocated block.
     */
    struct HGRAPH_EXPORT ValueBuilder
    {
        ValueBuilder(const value::TypeMeta &schema, MutationTracking tracking, const detail::StateOps &state_ops) noexcept;

        [[nodiscard]] const value::TypeMeta &schema() const noexcept { return m_schema.get(); }
        [[nodiscard]] MutationTracking tracking() const noexcept { return m_tracking; }
        [[nodiscard]] size_t size() const noexcept { return m_size; }
        [[nodiscard]] size_t alignment() const noexcept { return m_alignment; }
        [[nodiscard]] bool requires_destroy() const noexcept { return m_requires_destroy; }
        [[nodiscard]] bool requires_deallocate() const noexcept { return m_requires_deallocate; }
        [[nodiscard]] bool stores_inline_in_value_handle() const noexcept { return m_stores_inline_in_value_handle; }
        [[nodiscard]] const detail::ViewDispatch &dispatch() const noexcept { return m_view_dispatch.get(); }

        void cache_layout(size_t size, size_t alignment) noexcept
        {
            m_size      = size;
            m_alignment = alignment;
        }

        void cache_lifecycle(bool requires_destroy, bool requires_deallocate, bool stores_inline_in_value_handle) noexcept
        {
            m_requires_destroy               = requires_destroy;
            m_requires_deallocate            = requires_deallocate;
            m_stores_inline_in_value_handle  = stores_inline_in_value_handle;
        }

        [[nodiscard]] void *allocate() const;
        void destroy(void *memory) const noexcept;
        void deallocate(void *memory) const noexcept;
        void construct(void *memory) const;
        void copy_construct(void *dst, const void *src, const ValueBuilder &src_builder) const;
        void move_construct(void *dst, void *src, const ValueBuilder &src_builder) const;

      private:
        std::reference_wrapper<const value::TypeMeta>  m_schema;
        MutationTracking                               m_tracking{MutationTracking::Plain};
        size_t                                         m_size{0};
        size_t                                         m_alignment{alignof(std::max_align_t)};
        bool                                           m_requires_destroy{true};
        bool                                           m_requires_deallocate{true};
        bool                                           m_stores_inline_in_value_handle{false};
        std::reference_wrapper<const detail::StateOps> m_state_ops;
        std::reference_wrapper<const detail::ViewDispatch> m_view_dispatch;
    };

    /**
     * Schema-to-builder lookup.
     *
     * Builders are cached singletons per `(schema pointer, tracking mode)`.
     * Builder identity is the compatibility contract for copy and move in this
     * value layer. Plain tracking is the default lookup mode so ordinary value
     * storage does not retain delta state unless the caller requests it.
     */
    struct HGRAPH_EXPORT ValueBuilderFactory
    {
        [[nodiscard]] static const ValueBuilder *builder_for(
            const value::TypeMeta *schema, MutationTracking tracking = MutationTracking::Plain);
        [[nodiscard]] static const ValueBuilder &checked_builder_for(
            const value::TypeMeta *schema, MutationTracking tracking = MutationTracking::Plain);
    };

}  // namespace hgraph
