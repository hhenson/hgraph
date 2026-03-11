#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/atomic.h>
#include <hgraph/types/time_series/value/value.h>

#include <new>

namespace hgraph {

class ValueBuilder;

namespace detail
{

    /**
     * Abstract lifecycle and view dispatcher for schema-resolved state storage.
     *
     * This mirrors the role of `ViewDispatch`, but for storage lifecycle rather
     * than value behavior. Concrete implementations know the resolved state type
     * and are responsible for constructing, destroying, copying, moving, and
     * operating on that state once memory has already been allocated for it.
     *
     * Allocation is intentionally excluded from this interface. That
     * responsibility belongs to `ValueBuilder`, which asks the resolved state
     * ops to expand the builder with the layout required by the selected
     * schema and then caches that result.
     */
    struct HGRAPH_EXPORT StateOps
    {
        virtual ~StateOps() = default;

        virtual void expand_builder(ValueBuilder &builder, const value::TypeMeta &schema) const noexcept = 0;
        virtual void construct(void *memory) const                                 = 0;
        virtual void destroy(void *memory) const noexcept                          = 0;
        virtual void copy_construct(void *dst, const void *src) const              = 0;
        virtual void move_construct(void *dst, void *src) const                    = 0;
    };

}  // namespace detail

/**
 * Cached schema-bound builder for value storage.
 *
 * `ValueBuilder` owns the layout facts derived from a schema and a reference to
 * the corresponding state-ops dispatcher. The builder is responsible for
 * allocating appropriately aligned memory for the schema subtree and then
 * delegating lifecycle operations into the resolved state ops.
 *
 * Builders are intended to be cached singletons per schema. Builder identity
 * is therefore the runtime compatibility check for copying and moving storage
 * between values in this layer.
 *
 * This split keeps the memory-layout decision at the schema level while the
 * state ops remain focused on operating on already-allocated memory.
 */
class HGRAPH_EXPORT ValueBuilder
{
  public:
    ValueBuilder(const value::TypeMeta &schema, const detail::StateOps &state_ops) noexcept;

    [[nodiscard]] const value::TypeMeta &schema() const noexcept { return m_schema.get(); }
    [[nodiscard]] size_t                 size() const noexcept { return m_size; }
    [[nodiscard]] size_t                 alignment() const noexcept { return m_alignment; }

    /**
     * Cache the concrete layout chosen during state-op driven builder
     * expansion.
     *
     * This is intended only for use by `StateOps::expand_builder(...)` while a
     * builder is being assembled.
     */
    void cache_layout(size_t size, size_t alignment) noexcept
    {
        m_size      = size;
        m_alignment = alignment;
    }

    /**
     * Allocate raw memory suitable for the schema-selected state.
     */
    [[nodiscard]] void *allocate() const;

    /**
     * Release raw memory previously allocated by this builder.
     */
    void deallocate(void *memory) const noexcept;

    /**
     * Default-construct the schema-selected state into the supplied memory.
     */
    void construct(void *memory) const;

    /**
     * Destroy the schema-selected state from the supplied memory.
     */
    void destroy(void *memory) const noexcept;

    /**
     * Copy-construct the schema-selected state into the supplied destination
     * memory from an existing source memory block.
     */
    void copy_construct(void *dst, const void *src, const ValueBuilder &src_builder) const;

    /**
     * Move-construct the schema-selected state into the supplied destination
     * memory from an existing source memory block.
     */
    void move_construct(void *dst, void *src, const ValueBuilder &src_builder) const;

  private:
    std::reference_wrapper<const value::TypeMeta> m_schema;
    size_t                                        m_size;
    size_t                                        m_alignment;
    std::reference_wrapper<const detail::StateOps> m_state_ops;
};

/**
 * Factory for schema-bound value builders.
 *
 * The current implementation resolves atomic schemas to singleton
 * `ValueBuilder` instances backed by typed atomic state ops. This gives the
 * first pass of the intended builder model without yet introducing the dynamic
 * schema cache needed for composite nested builders. Callers are expected to
 * treat the returned builder address as the identity of the schema-selected
 * storage plan.
 */
class HGRAPH_EXPORT ValueBuilderFactory
{
  public:
    /**
     * Return the cached builder for the supplied schema, or `nullptr` when the
     * schema is not currently supported by this experimental value layer.
     */
    [[nodiscard]] static const ValueBuilder *builder_for(const value::TypeMeta *schema) noexcept;

    /**
     * Return the cached builder for the supplied schema, throwing when the
     * schema is not currently supported.
     */
    [[nodiscard]] static const ValueBuilder &checked_builder_for(const value::TypeMeta *schema);

  private:
    template <typename T> struct AtomicStateOps final : detail::StateOps
    {
        void expand_builder(ValueBuilder &builder, const value::TypeMeta &schema) const noexcept override
        {
            static_cast<void>(schema);
            builder.cache_layout(sizeof(AtomicState<T>), alignof(AtomicState<T>));
        }

        void construct(void *memory) const override
        {
            std::construct_at(state(memory));
        }

        void destroy(void *memory) const noexcept override
        {
            std::destroy_at(state(memory));
        }

        void copy_construct(void *dst, const void *src) const override
        {
            std::construct_at(state(dst), *state(src));
        }

        void move_construct(void *dst, void *src) const override
        {
            std::construct_at(state(dst), std::move(*state(src)));
        }

      private:
        [[nodiscard]] static AtomicState<T> *state(void *memory) noexcept
        {
            return std::launder(reinterpret_cast<AtomicState<T> *>(memory));
        }

        [[nodiscard]] static const AtomicState<T> *state(const void *memory) noexcept
        {
            return std::launder(reinterpret_cast<const AtomicState<T> *>(memory));
        }
    };

    template <typename T>
    [[nodiscard]] static const detail::StateOps &atomic_state_ops() noexcept
    {
        static const AtomicStateOps<T> ops{};
        return ops;
    }

    template <typename T>
    [[nodiscard]] static const ValueBuilder &atomic_builder() noexcept
    {
        static const ValueBuilder builder{
            *value::scalar_type_meta<T>(),
            atomic_state_ops<T>(),
        };
        return builder;
    }
};

}  // namespace hgraph
