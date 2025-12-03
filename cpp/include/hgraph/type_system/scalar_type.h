#ifndef HGRAPH_SCALAR_TYPE_H
#define HGRAPH_SCALAR_TYPE_H

#include <hgraph/type_system/type.h>

namespace hgraph {
    /**
     * ScalarType - Represents scalar types in the type system
     *
     * This class extends the base Type class to specifically represent
     * scalar types such as integers, floats, and strings.
     */
    struct ScalarType : public Type {
        explicit ScalarType(const std::string &name);

        ~ScalarType() override = default;

        bool is_scalar() const override;
        bool is_time_series() const override;
    };
} // namespace hgraph

#endif // HGRAPH_SCALAR_TYPE_H