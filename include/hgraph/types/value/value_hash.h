#ifndef HGRAPH_TYPES_VALUE_VALUE_HASH_H
#define HGRAPH_TYPES_VALUE_VALUE_HASH_H

#include <hgraph/types/value/value.h>

#include <ankerl/unordered_dense.h>

#include <cstddef>

namespace hgraph
{
    /**
     * Transparent hash for owning ``Value`` keys and borrowed ``ValueView``
     * lookups.
     *
     * Values without a payload hash to zero. Live values dispatch through
     * their erased ``ValueOps`` hash implementation, so an unhashable live
     * value retains the normal ``Value::hash`` failure behavior.
     */
    struct ValueHash
    {
        using is_transparent = void;

        [[nodiscard]] std::size_t operator()(const Value &value) const
        {
            return value.has_value() ? value.hash() : std::size_t{0};
        }

        [[nodiscard]] std::size_t operator()(const ValueView &value) const
        {
            return value.has_value() ? value.hash() : std::size_t{0};
        }
    };

    /**
     * Transparent semantic equality for every owning and borrowed value-key
     * combination.
     *
     * Equality delegates to ``ValueView::equals``. This preserves schema
     * identity for typed-null values while allowing semantically equivalent
     * live values with compatible erased representations to compare equal.
     */
    struct ValueEqual
    {
        using is_transparent = void;

        [[nodiscard]] bool operator()(const Value &lhs, const Value &rhs) const
        {
            return lhs.equals(rhs);
        }

        [[nodiscard]] bool operator()(const Value &lhs, const ValueView &rhs) const
        {
            return lhs.equals(rhs);
        }

        [[nodiscard]] bool operator()(const ValueView &lhs, const Value &rhs) const
        {
            return lhs.equals(rhs.view());
        }

        [[nodiscard]] bool operator()(const ValueView &lhs, const ValueView &rhs) const
        {
            return lhs.equals(rhs);
        }
    };
    /**
     * A set of keys the caller already holds, by address, transparent to
     * ``Value`` and ``ValueView`` lookups.
     *
     * For "is this key one of those?" asked once per element of another
     * collection: build the set once and ask it, instead of searching the list
     * per element, which is quadratic. Nothing is copied, so the keys must
     * stay where they are -- do not grow the container they live in -- for as
     * long as the set is used.
     */
    struct BorrowedValueHash
    {
        using is_transparent = void;
        [[nodiscard]] std::size_t operator()(const Value *value) const { return ValueHash{}(*value); }
        [[nodiscard]] std::size_t operator()(const Value &value) const { return ValueHash{}(value); }
        [[nodiscard]] std::size_t operator()(const ValueView &value) const { return ValueHash{}(value); }
    };

    struct BorrowedValueEqual
    {
        using is_transparent = void;
        [[nodiscard]] bool operator()(const Value *lhs, const Value *rhs) const { return lhs->equals(*rhs); }
        [[nodiscard]] bool operator()(const Value *lhs, const Value &rhs) const { return lhs->equals(rhs); }
        [[nodiscard]] bool operator()(const Value &lhs, const Value *rhs) const { return lhs.equals(*rhs); }
        [[nodiscard]] bool operator()(const Value *lhs, const ValueView &rhs) const { return lhs->equals(rhs); }
        [[nodiscard]] bool operator()(const ValueView &lhs, const Value *rhs) const { return rhs->equals(lhs); }
    };

    using BorrowedValueSet = ankerl::unordered_dense::set<const Value *, BorrowedValueHash, BorrowedValueEqual>;
}  // namespace hgraph

#endif  // HGRAPH_TYPES_VALUE_VALUE_HASH_H
