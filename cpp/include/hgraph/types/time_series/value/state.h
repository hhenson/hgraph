#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/atomic.h>
#include <hgraph/types/time_series/value/value.h>

#include <new>

namespace hgraph {

struct ValueBuilder;

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
        [[nodiscard]] virtual bool requires_destroy(const value::TypeMeta &schema) const noexcept        = 0;
        [[nodiscard]] virtual bool requires_deallocate(const value::TypeMeta &schema) const noexcept     = 0;
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
 * The builder also caches lifecycle traits such as whether destruction and
 * separate deallocation are required. These traits are schema-resolved, so
 * caching them here lets the owning `Value` skip unnecessary lifecycle work on
 * hot paths when the state representation permits it.
 *
 * In the current erased-state design, even atomic payloads still live inside a
 * full dispatch-bearing state object, so destruction is still required. The
 * cached traits are here so a later lighter-weight state representation can
 * avoid that cost without changing the owning `Value` contract.
 *
 * Builders are intended to be cached singletons per schema. Builder identity
 * is therefore the runtime compatibility check for copying and moving storage
 * between values in this layer.
 *
 * This split keeps the memory-layout decision at the schema level while the
 * state ops remain focused on operating on already-allocated memory.
 */
struct HGRAPH_EXPORT ValueBuilder
{
    ValueBuilder(const value::TypeMeta &schema, const detail::StateOps &state_ops) noexcept;

    [[nodiscard]] const value::TypeMeta &schema() const noexcept { return m_schema.get(); }
    [[nodiscard]] size_t                 size() const noexcept { return m_size; }
    [[nodiscard]] size_t                 alignment() const noexcept { return m_alignment; }
    [[nodiscard]] bool                   requires_destroy() const noexcept { return m_requires_destroy; }
    [[nodiscard]] bool                   requires_deallocate() const noexcept { return m_requires_deallocate; }

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
     * Cache lifecycle traits derived from the resolved state ops.
     *
     * This allows the owning `Value` to skip unnecessary lifecycle steps for
     * states that do not require destruction or separate deallocation.
     */
    void cache_lifecycle(bool requires_destroy, bool requires_deallocate) noexcept
    {
        m_requires_destroy    = requires_destroy;
        m_requires_deallocate = requires_deallocate;
    }

    /**
     * Allocate raw memory suitable for the schema-selected state.
     *
     * This is only used when the builder requires separate storage. Builders
     * that eventually store their payload directly in the owning handle will
     * report `requires_deallocate() == false` and can bypass this path.
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
    bool                                          m_requires_destroy;
    bool                                          m_requires_deallocate;
    std::reference_wrapper<const detail::StateOps> m_state_ops;
};

/**
 * Factory for schema-bound value builders.
 *
 * The current implementation resolves atomic schemas to process-wide
 * singleton builders and caches composite builders by schema pointer.
 * Callers are expected to treat the returned builder address as the
 * identity of the schema-selected storage plan.
 */
struct HGRAPH_EXPORT ValueBuilderFactory
{
    /**
     * Return the cached builder for the supplied schema, or `nullptr` when the
     * schema is not currently supported by this experimental value layer.
     */
    [[nodiscard]] static const ValueBuilder *builder_for(const value::TypeMeta *schema);

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
            builder.cache_lifecycle(!std::is_trivially_destructible_v<AtomicState<T>>, true);
        }

        [[nodiscard]] bool requires_destroy(const value::TypeMeta &schema) const noexcept override
        {
            static_cast<void>(schema);
            return !std::is_trivially_destructible_v<AtomicState<T>>;
        }

        [[nodiscard]] bool requires_deallocate(const value::TypeMeta &schema) const noexcept override
        {
            static_cast<void>(schema);
            return true;
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
