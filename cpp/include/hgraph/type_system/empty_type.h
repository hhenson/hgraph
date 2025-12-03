#ifndef HGRAPH_EMPTY_TYPE_H
#define HGRAPH_EMPTY_TYPE_H

#include <hgraph/type_system/type.h>

namespace hgraph {
    /**
     * EmptyType - Represents an empty type in the type system
     *
     * This class extends the base Type class to specifically represent
     * an empty type, which can be used as a placeholder or default type
     * in various scenarios within the type system.
     */
    struct EmptyType : public Type {
        EmptyType();        
        ~EmptyType() override = default;
        
        bool is_scalar() const override;
        bool is_time_series() const override;
    };
} // namespace hgraph

#endif // HGRAPH_EMPTY_TYPE_H