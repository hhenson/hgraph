#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/atomic.h>
#include <hgraph/types/time_series/value/list.h>

#include <functional>
#include <type_traits>
#include <utility>

namespace hgraph {

struct ValueBuilder;

/**
 * Owning schema-bound value shell.
 *
 * `Value` owns a memory block selected by a schema-bound `ValueBuilder`. The
 * builder captures the resolved storage requirements for the schema, including
 * total size, alignment, and the state-ops dispatcher used to construct,
 * destroy, copy, move, and view the stored state.
 *
 * Keeping allocation on the builder side makes the ownership model line up
 * with the intended future design for nested schemas: the builder decides the
 * cumulative memory requirement for the represented schema subtree, while the
 * concrete state ops are only responsible for constructing into the supplied
 * memory.
 */
struct Value
{
    /**
     * Construct storage for the supplied schema.
     *
     * `Value` is schema-bound at construction time and keeps that schema for its
     * lifetime.
     */
    explicit Value(const value::TypeMeta &schema);

    /**
     * Copy the stored value while preserving the bound schema.
     */
    Value(const Value &other);

    /**
     * Move the stored value while preserving the bound schema.
     *
     * The moved-from value retains its schema binding but releases ownership of
     * its stored payload. It remains assignable and destructible, but no longer
     * reports a live payload until storage is assigned again.
     */
    Value(Value &&other);

    /**
     * Assignment is allowed only when both values already describe the same
     * schema.
     *
     * This preserves the invariant that a `Value` does not rebind its schema
     * after construction. Rebinding would require a different semantic choice
     * for destruction and reconstruction, so it is intentionally rejected.
     */
    Value &operator=(const Value &other);

    /**
     * Move assignment follows the same matching-schema rule as copy
     * assignment.
     *
     * The moved-from value retains its schema binding but releases ownership of
     * its stored payload.
     */
    Value &operator=(Value &&other);

    /**
     * Destroy the currently stored payload and release its allocated memory.
     */
    ~Value();

    /**
     * Return `true` when this value currently owns live storage for its bound
     * schema.
     *
     * Moved-from values retain their schema binding but report `false` here
     * until storage is assigned again.
     */
    [[nodiscard]] bool valid() const noexcept;

    /**
     * Return `true` when this value currently owns live storage for its bound
     * schema.
     */
    explicit operator bool() const noexcept;

    /**
     * Return the schema bound to this value.
     */
    [[nodiscard]] const value::TypeMeta *schema() const noexcept;

    /**
     * Return a mutable erased view over the stored payload.
     */
    [[nodiscard]] View view() noexcept;

    /**
     * Return a const erased view over the stored payload.
     */
    [[nodiscard]] View view() const noexcept;

  private:
    /**
     * Allocate memory and default-construct the schema-selected state into it.
     */
    void allocate_and_construct();

    /**
     * Destroy the current payload and release its allocated memory.
     */
    void reset() noexcept;

    std::reference_wrapper<const ValueBuilder> m_builder;
    detail::ViewDispatch                      *m_dispatch{nullptr};
};

template <typename T> inline void View::set(T &&value)
{
    if (!valid()) { throw std::runtime_error("View::set(T) on invalid view"); }

    using TValue = std::remove_cvref_t<T>;
    if constexpr (std::is_lvalue_reference_v<T &&>) {
        dispatch()->set_from_cpp(std::addressof(value), value::scalar_type_meta<TValue>());
    } else {
        dispatch()->move_from_cpp(std::addressof(value), value::scalar_type_meta<TValue>());
    }
}

/**
 * Construct a transient schema-bound value from a raw atomic payload.
 *
 * This is intended for short-lived values that need to participate in the new
 * erased value layer without manually constructing a default `Value` and then
 * mutating it through `view().as_atomic()`.
 */
template <typename T>
[[nodiscard]] Value value_for(T &&value)
{
    using TValue = std::remove_cvref_t<T>;

    Value out{*value::scalar_type_meta<TValue>()};
    out.view().as_atomic().set(std::forward<T>(value));
    return out;
}

/**
 * Construct a transient schema-bound value from a raw C++ object using an
 * explicit target schema.
 *
 * This is intended for short-lived values where the source C++ type is simple
 * but the target schema may later describe richer structures. The actual
 * construction is delegated to `View::set(T)`, which currently supports atomic
 * schemas and is expected to grow native collection construction surfaces over
 * time.
 */
template <typename T>
[[nodiscard]] Value value_for(const value::TypeMeta &schema, T &&value)
{
    Value out{schema};
    out.view().set(std::forward<T>(value));
    return out;
}

}  // namespace hgraph
