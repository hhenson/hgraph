#pragma once
#include <native-provider.h>

namespace checks::scalar_provider
{
    struct Implementation
    {
        static hgraph::Int bit_and(hgraph::Int lhs, hgraph::Int rhs) noexcept { return lhs & rhs; }
    };
    inline constexpr auto native = checks::native_provider::native_interface::bind<Implementation>();
}  // namespace checks::scalar_provider
