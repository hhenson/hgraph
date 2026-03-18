#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/associative.h>
#include <hgraph/types/time_series/value/atomic.h>
#include <hgraph/types/time_series/value/list.h>
#include <hgraph/types/time_series/value/record.h>
#include <hgraph/types/time_series/value/sequence.h>
#include <hgraph/types/time_series/value/tracking.h>

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

            template <typename TFits> [[nodiscard]] static size_t capacity_for(TFits &&fits) noexcept {
                for (size_t elements = max_elements; elements > 0; --elements) {
                    if (fits(elements)) { return elements; }
                }
                return 0;
            }
        };

        /**
         * Cached layout facts for schema-bound value storage.
         */
        struct BuilderLayout
        {
            size_t size{0};
            size_t alignment{alignof(std::max_align_t)};
        };

        /**
         * Lifecycle dispatcher for raw value storage.
         *
         * `ValueBuilderOps` computes schema-bound layout/lifecycle facts and
         * performs plain storage operations. It does not own memory and it
         * does not own behavior dispatch.
         */
        struct HGRAPH_EXPORT ValueBuilderOps
        {
            virtual ~ValueBuilderOps() = default;

            // META DATA
            [[nodiscard]] virtual BuilderLayout       layout(const value::TypeMeta &schema) const noexcept              = 0;
            [[nodiscard]] virtual const ViewDispatch &view_dispatch(const value::TypeMeta &schema) const noexcept       = 0;
            [[nodiscard]] virtual bool                requires_destruct(const value::TypeMeta &schema) const noexcept   = 0;
            [[nodiscard]] virtual bool                requires_deallocate(const value::TypeMeta &schema) const noexcept = 0;
            [[nodiscard]] virtual bool stores_inline_in_value_handle(const value::TypeMeta &schema) const noexcept      = 0;

            // BEHAVIOUR
            virtual void construct(void *memory) const                    = 0;
            virtual void destruct(void *memory) const noexcept            = 0;
            virtual void copy_construct(void *dst, const void *src) const = 0;
            virtual void move_construct(void *dst, void *src) const       = 0;
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
        ValueBuilder(const value::TypeMeta &schema, MutationTracking tracking, const detail::ValueBuilderOps &state_ops) noexcept;

        [[nodiscard]] const value::TypeMeta &schema() const noexcept;
        [[nodiscard]] size_t                 size() const noexcept;
        [[nodiscard]] size_t                 alignment() const noexcept;

        [[nodiscard]] MutationTracking            tracking() const noexcept;
        [[nodiscard]] bool                        requires_destruct() const noexcept;
        [[nodiscard]] bool                        requires_deallocate() const noexcept;
        [[nodiscard]] bool                        stores_inline_in_value_handle() const noexcept;
        [[nodiscard]] const detail::ViewDispatch &dispatch() const noexcept;

        // Provide a default allocator to allocate memory.
        // This step can be skipped if allocation is performed externally
        [[nodiscard]] void *allocate() const;

        /**
         * Construct or initialise the memory, calls the constructor logic.
         * @param memory
         */
        void construct(void *memory) const;

        /**
         * Calls the destructor on the memory.
         * @param memory
         */
        void destruct(void *memory) const noexcept;

        /**
         * Release the allocated memory, this should only be called if the memory used to allocate this
         * was created using this builders allocate.
         * @param memory
         */
        void deallocate(void *memory) const noexcept;

        void copy_construct(void *dst, const void *src, const ValueBuilder &src_builder) const;
        void move_construct(void *dst, void *src, const ValueBuilder &src_builder) const;

      private:
        std::reference_wrapper<const value::TypeMeta>         m_schema;
        MutationTracking                                      m_tracking{MutationTracking::Plain};
        size_t                                                m_size{0};
        size_t                                                m_alignment{alignof(std::max_align_t)};
        bool                                                  m_requires_destruct{true};
        bool                                                  m_requires_deallocate{true};
        bool                                                  m_stores_inline_in_value_handle{false};
        std::reference_wrapper<const detail::ValueBuilderOps> m_builder_ops;
        std::reference_wrapper<const detail::ViewDispatch>    m_view_dispatch;
    };

    /**
     * Schema-to-builder lookup.
     *
     * Builders are cached singletons per `(schema pointer, tracking mode)`.
     * Builder identity is the compatibility contract for copy and move in this
     * value layer. Plain tracking is the default lookup mode so ordinary value
     * storage does not retain delta-state unless the caller requests it.
     */
    struct HGRAPH_EXPORT ValueBuilderFactory
    {
        [[nodiscard]] static const ValueBuilder *builder_for(const value::TypeMeta *schema,
                                                             MutationTracking       tracking = MutationTracking::Plain);
        [[nodiscard]] static const ValueBuilder &checked_builder_for(const value::TypeMeta *schema,
                                                                     MutationTracking       tracking = MutationTracking::Plain);
    };

}  // namespace hgraph
