#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/storage.h>
#include <hgraph/types/time_series/value/value.h>

namespace hgraph {

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

/**
 * Factory for schema-resolved value-state operations.
 *
 * The current implementation resolves atomic schemas to the typed atomic state
 * needed to construct, destroy, copy, move, and view the stored payload. The
 * factory lives with the value-state definitions because it is responsible for
 * materializing the concrete state objects held by `Value`.
 *
 * This is a state factory rather than an atomic-specific factory because the
 * responsibility here is construction of the concrete stored state selected by
 * the schema.
 */
class ValueStateFactory
{
  public:
    /**
     * Schema-resolved operations for the concrete state object selected by a
     * `value::TypeMeta`.
     */
    struct StateOps
    {
        size_t size;
        size_t alignment;
        bool   stores_inline;
        void (*construct)(ValueStateUnion &storage);
        void (*destroy)(ValueStateUnion &storage) noexcept;
        void (*copy_construct)(ValueStateUnion &dst, const ValueStateUnion &src);
        void (*move_construct)(ValueStateUnion &dst, ValueStateUnion &src) noexcept;
        View (*view_of)(ValueStateUnion &storage) noexcept;
        View (*view_of_const)(const ValueStateUnion &storage) noexcept;
    };

    /**
     * Return the operations table for the supplied schema, or `nullptr` when the
     * schema is not currently supported by this experimental value layer.
     */
    [[nodiscard]] static const StateOps *ops_for(const value::TypeMeta *schema) noexcept
    {
        if (schema == nullptr || schema->kind != value::TypeKind::Atomic) {
            return nullptr;
        }

#define HGRAPH_VALUE_STATE_FACTORY_CASE(type_)                                                                                   \
    if (schema == value::scalar_type_meta<type_>()) {                                                                           \
        return &state_ops<type_>();                                                                                             \
    }

        HGRAPH_VALUE_STATE_FACTORY_CASE(bool)
        HGRAPH_VALUE_STATE_FACTORY_CASE(int8_t)
        HGRAPH_VALUE_STATE_FACTORY_CASE(int16_t)
        HGRAPH_VALUE_STATE_FACTORY_CASE(int32_t)
        HGRAPH_VALUE_STATE_FACTORY_CASE(int64_t)
        HGRAPH_VALUE_STATE_FACTORY_CASE(uint8_t)
        HGRAPH_VALUE_STATE_FACTORY_CASE(uint16_t)
        HGRAPH_VALUE_STATE_FACTORY_CASE(uint32_t)
        HGRAPH_VALUE_STATE_FACTORY_CASE(uint64_t)
        HGRAPH_VALUE_STATE_FACTORY_CASE(size_t)
        HGRAPH_VALUE_STATE_FACTORY_CASE(float)
        HGRAPH_VALUE_STATE_FACTORY_CASE(double)
        HGRAPH_VALUE_STATE_FACTORY_CASE(std::string)
        HGRAPH_VALUE_STATE_FACTORY_CASE(engine_date_t)
        HGRAPH_VALUE_STATE_FACTORY_CASE(engine_time_t)
        HGRAPH_VALUE_STATE_FACTORY_CASE(engine_time_delta_t)

#undef HGRAPH_VALUE_STATE_FACTORY_CASE

        return nullptr;
    }

    /**
     * Construct the concrete state selected by the supplied schema into the
     * provided erased storage slot.
     */
    static void construct(ValueStateUnion &storage, const value::TypeMeta *schema)
    {
        checked_ops(schema)->construct(storage);
    }

    /**
     * Destroy the concrete state currently stored for the supplied schema.
     */
    static void destroy(ValueStateUnion &storage, const value::TypeMeta *schema) noexcept
    {
        if (const StateOps *ops = ops_for(schema); ops != nullptr) {
            ops->destroy(storage);
        }
    }

    /**
     * Copy-construct the concrete state selected by the supplied schema.
     */
    static void copy_construct(ValueStateUnion &dst, const ValueStateUnion &src, const value::TypeMeta *schema)
    {
        checked_ops(schema)->copy_construct(dst, src);
    }

    /**
     * Move-construct the concrete state selected by the supplied schema.
     */
    static void move_construct(ValueStateUnion &dst, ValueStateUnion &src, const value::TypeMeta *schema) noexcept
    {
        checked_ops(schema)->move_construct(dst, src);
    }

    /**
     * Return a mutable erased view over the concrete state selected by the
     * supplied schema.
     */
    [[nodiscard]] static View view_of(ValueStateUnion &storage, const value::TypeMeta *schema) noexcept
    {
        if (const StateOps *ops = ops_for(schema); ops != nullptr) {
            return ops->view_of(storage);
        }
        return View{};
    }

    /**
     * Return a const erased view over the concrete state selected by the
     * supplied schema.
     */
    [[nodiscard]] static View view_of(const ValueStateUnion &storage, const value::TypeMeta *schema) noexcept
    {
        if (const StateOps *ops = ops_for(schema); ops != nullptr) {
            return ops->view_of_const(storage);
        }
        return View{};
    }

  private:
    template <typename T>
    static constexpr bool stores_state_inline =
        sizeof(AtomicState<T>) <= sizeof(void *) && alignof(AtomicState<T>) <= alignof(void *);

    template <typename T>
    /**
     * Reinterpret the inline erased storage slot as the resolved atomic state.
     */
    [[nodiscard]] static AtomicState<T> *inline_state(ValueStateUnion &storage) noexcept
    {
        return std::launder(reinterpret_cast<AtomicState<T> *>(storage.inline_state));
    }

    template <typename T>
    /**
     * Reinterpret the inline erased storage slot as the resolved atomic state.
     */
    [[nodiscard]] static const AtomicState<T> *inline_state(const ValueStateUnion &storage) noexcept
    {
        return std::launder(reinterpret_cast<const AtomicState<T> *>(storage.inline_state));
    }

    template <typename T>
    /**
     * Default-construct the resolved atomic state into inline or heap storage.
     */
    static void construct_state(ValueStateUnion &storage)
    {
        if constexpr (stores_state_inline<T>) {
            std::construct_at(inline_state<T>(storage));
        } else {
            storage.pointer = new AtomicState<T>{};
        }
    }

    template <typename T>
    /**
     * Destroy the resolved atomic state from inline or heap storage.
     */
    static void destroy_state(ValueStateUnion &storage) noexcept
    {
        if constexpr (stores_state_inline<T>) {
            std::destroy_at(inline_state<T>(storage));
        } else {
            delete static_cast<AtomicState<T> *>(storage.pointer);
            storage.pointer = nullptr;
        }
    }

    template <typename T>
    /**
     * Copy-construct the resolved atomic state into inline or heap storage.
     */
    static void copy_construct_state(ValueStateUnion &dst, const ValueStateUnion &src)
    {
        if constexpr (stores_state_inline<T>) {
            std::construct_at(inline_state<T>(dst), *inline_state<T>(src));
        } else {
            dst.pointer = new AtomicState<T>(*static_cast<const AtomicState<T> *>(src.pointer));
        }
    }

    template <typename T>
    /**
     * Move-construct the resolved atomic state into inline or heap storage.
     */
    static void move_construct_state(ValueStateUnion &dst, ValueStateUnion &src) noexcept
    {
        if constexpr (stores_state_inline<T>) {
            std::construct_at(inline_state<T>(dst), std::move(*inline_state<T>(src)));
            std::destroy_at(inline_state<T>(src));
        } else {
            dst.pointer = src.pointer;
            src.pointer = nullptr;
        }
    }

    template <typename T>
    /**
     * Return a mutable erased view for the resolved atomic state.
     */
    [[nodiscard]] static View view_of_state(ValueStateUnion &storage) noexcept
    {
        if constexpr (stores_state_inline<T>) {
            return inline_state<T>(storage)->view();
        } else {
            return static_cast<AtomicState<T> *>(storage.pointer)->view();
        }
    }

    template <typename T>
    /**
     * Return a const erased view for the resolved atomic state.
     */
    [[nodiscard]] static View view_of_state_const(const ValueStateUnion &storage) noexcept
    {
        if constexpr (stores_state_inline<T>) {
            return inline_state<T>(storage)->view();
        } else {
            return static_cast<const AtomicState<T> *>(storage.pointer)->view();
        }
    }

    template <typename T>
    /**
     * Return the operations table for the resolved atomic state `T`.
     */
    [[nodiscard]] static const StateOps &state_ops() noexcept
    {
        static const StateOps ops{
            .size           = sizeof(AtomicState<T>),
            .alignment      = alignof(AtomicState<T>),
            .stores_inline  = stores_state_inline<T>,
            .construct      = &construct_state<T>,
            .destroy        = &destroy_state<T>,
            .copy_construct = &copy_construct_state<T>,
            .move_construct = &move_construct_state<T>,
            .view_of        = &view_of_state<T>,
            .view_of_const  = &view_of_state_const<T>,
        };
        return ops;
    }

    /**
     * Return the operations table for the supplied schema, throwing when the
     * schema is not supported by this experimental value layer.
     */
    [[nodiscard]] static const StateOps *checked_ops(const value::TypeMeta *schema)
    {
        if (const StateOps *ops = ops_for(schema); ops != nullptr) {
            return ops;
        }
        throw std::runtime_error("ValueStateFactory: unsupported atomic schema");
    }
};

}  // namespace hgraph
