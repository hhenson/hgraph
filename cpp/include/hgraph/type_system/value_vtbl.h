#ifndef HGRAPH_VALUE_VTBL_H
#define HGRAPH_VALUE_VTBL_H

#include <hgraph/type_system/value_placeholder.h>

namespace hgraph {
    /**
     * ValueVTable - Virtual table for value operations
     *
     * This struct defines a set of function pointers for operations
     * that can be performed on values in the type system. It allows
     * for polymorphic behavior when dealing with different value types.
     */
    struct ValueVTable {
        Type* type;
        void (*copy)(const ValuePlaceholder* src, ValuePlaceholder* dest);
        void (*destroy)(ValuePlaceholder* value);
        bool (*equal)(const ValuePlaceholder* lhs, const ValuePlaceholder* rhs);
        bool (*less)(const ValuePlaceholder* lhs, const ValuePlaceholder* rhs);
        size_t (*hash)(const ValuePlaceholder* value);
    };
} // namespace hgraph

#endif // HGRAPH_VALUE_VTBL_H