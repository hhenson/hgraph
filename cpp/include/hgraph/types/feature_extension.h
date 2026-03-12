#ifndef FEATURE_EXTENSION_H
#define FEATURE_EXTENSION_H

#include <hgraph/hgraph_base.h>
#include <hgraph/types/value/value.h>

namespace hgraph {

    /**
     * @brief Hash functor for Value with transparent lookup support.
     * Enables heterogeneous lookup with View keys.
     */
    struct ValueHash {
        using is_transparent = void;  // Enable heterogeneous lookup

        size_t operator()(const value::Value& v) const {
            return v.has_value() ? v.hash() : 0u;
        }

        size_t operator()(const value::View& v) const {
            return v.hash();
        }
    };

    /**
     * @brief Equality functor for Value with transparent lookup support.
     * Enables heterogeneous comparison with View keys.
     */
    struct ValueEqual {
        using is_transparent = void;  // Enable heterogeneous lookup

        bool operator()(const value::Value& a, const value::Value& b) const {
            return a.equals(b);
        }

        bool operator()(const value::Value& a, const value::View& b) const {
            return a.equals(b);
        }

        bool operator()(const value::View& a, const value::Value& b) const {
            return b.equals(a);
        }

        bool operator()(const value::View& a, const value::View& b) const {
            if (!a.valid() || !b.valid()) return false;
            return a.hash() == b.hash() && a.schema() == b.schema();
        }
    };

} // namespace hgraph

#endif  // FEATURE_EXTENSION_H
