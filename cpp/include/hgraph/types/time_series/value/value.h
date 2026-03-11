#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/time_series/value/atomic.h>

#include <functional>

namespace hgraph {

class ValueStateFactory;

/**
 * Owning schema-bound value shell.
 *
 * For the atomic-first implementation, `Value` owns a schema-bound erased
 * atomic state storage and produces a non-owning raw `View` over that state.
 *
 * The schema is stored by reference rather than pointer so every `Value`
 * instance is guaranteed to be schema-bound. This removes the null-schema path
 * and treats "has a schema" as an invariant rather than a runtime condition to
 * check repeatedly.
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
     */
    Value(Value &&other) noexcept;

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
     */
    Value &operator=(Value &&other);

    /**
     * Destroy the currently stored payload.
     */
    ~Value();

    /**
     * Return `true` when this value is bound to a schema and can produce a view.
     *
     * The schema-binding invariant means this is currently always `true`.
     */
    [[nodiscard]] bool valid() const noexcept;

    /**
     * Return `true` when this value is bound to a schema and can produce a view.
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
     * Destroy the current payload using the state factory selected by the bound
     * schema.
     */
    void reset() noexcept;

    ValueStateUnion                             m_state;
    std::reference_wrapper<const value::TypeMeta> m_schema;
};

}  // namespace hgraph
