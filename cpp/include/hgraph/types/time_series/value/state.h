#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/storage.h>
#include <hgraph/types/time_series/value/value.h>

#include <new>
#include <variant>

namespace hgraph {

class ValueBuilder;

/**
 * Value-state for linked data positions.
 *
 * The linked node does not own a local payload. It delegates value access to
 * another value-state branch when linked, and may also be observed while
 * currently unlinked.
 *
 * This is the only link concept on the value side. Reference behavior remains
 * a time-series state concern rather than a separate value-storage category.
 *
 * Ownership:
 * Owns no payload of its own.
 *
 * Access:
 * Reads delegate to `target` when bound. Inspection must also handle the
 * unlinked state explicitly.
 *
 * Exportability:
 * This is not directly exportable. It must first be resolved to owned data or
 * preserved as a link in some higher-level transport representation.
 */
struct HGRAPH_EXPORT LinkedValueState
{
    TimeSeriesValueStatePtr target;

    /**
     * Return `true` when this linked value currently resolves to a concrete
     * target branch.
     */
    [[nodiscard]] bool is_bound() const noexcept
    {
        return std::visit([](const auto *ptr) { return ptr != nullptr; }, target);
    }
};

/**
 * Value-state for signal positions.
 *
 * Signals delegate through linked data rather than owning an independent
 * payload. The signal view is expected to derive its presence semantics from
 * the modified state of the linked time-series branch.
 *
 * This reflects the intended model that signal value-state is effectively a
 * linked value whose public behavior is driven by time-series modification
 * state rather than by a separate stored boolean payload.
 *
 * Ownership:
 * Owns no payload of its own.
 *
 * Access:
 * Presence is derived from the linked branch rather than from a stored scalar
 * value.
 *
 * Exportability:
 * This is not directly exportable as owned data; it first needs a materialized
 * signal representation or a resolved linked source.
 */
struct HGRAPH_EXPORT SignalValueState : LinkedValueState
{};

/**
 * Value variant covering the concrete time-series value-state nodes.
 *
 * The schema determines which storage primitive a node represents. Lists and
 * bundles intentionally share `IndexedTimeSeriesValueStorage`; their semantic
 * difference is carried by the time-series schema rather than by distinct
 * wrapper structs.
 */
using TimeSeriesValueStateV =
    std::variant<Value, IndexedTimeSeriesValueStorage, TimeSeriesMapStorage, value::SetStorage, TimeSeriesWindowStorage,
                 LinkedValueState, SignalValueState>;

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
