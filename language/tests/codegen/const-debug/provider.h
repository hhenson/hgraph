#pragma once
#include <const-debug.h>
#include <iostream>

namespace bootstrap
{
    struct Implementation
    {
        static void print_i64(hgraph::Int value) noexcept { std::cout << value << '\n'; }
    };
    inline constexpr auto native = examples::const_debug::native_interface::bind<Implementation>();
}
