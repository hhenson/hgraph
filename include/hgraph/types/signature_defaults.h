#ifndef HGRAPH_TYPES_SIGNATURE_DEFAULTS_H
#define HGRAPH_TYPES_SIGNATURE_DEFAULTS_H

#include <tuple>

namespace hgraph
{
    template <typename T>
    struct signature_defaults
    {
        using no_defaults = void;
        [[nodiscard]] static auto values() { return std::tuple{}; }
    };
}  // namespace hgraph

#endif  // HGRAPH_TYPES_SIGNATURE_DEFAULTS_H
