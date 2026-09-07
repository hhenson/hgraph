#include <partial-materialization.h>

#include "wiring/backend.h"

#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/types/graph_wiring.h>

#include <catch2/catch_test_macros.hpp>

using namespace hgraph;
namespace partial = checks::partial_materialization;

namespace
{
    template <std::size_t Size> GraphBuilder compose_preserved_list() {
        static_assert(Size == 2 || Size == 3);
        hgl::wiring::ensure_session();
        partial::register_operators();
        Wiring w;
        auto   first  = wire<stdlib::const_, TS<Int>>(w, Int{1});
        auto   second = wire<stdlib::const_, TS<Int>>(w, Int{2});
        if constexpr (Size == 2) {
            Port<TSL<TS<Int>, 2>> values{stdlib::to_tsl<TSL<TS<Int>, 2>>(w, first, second).erased()};
            static_cast<void>(wire<partial::operators::preserve>(w, values));
        } else {
            auto                  third = wire<stdlib::const_, TS<Int>>(w, Int{3});
            Port<TSL<TS<Int>, 3>> values{stdlib::to_tsl<TSL<TS<Int>, 3>>(w, first, second, third).erased()};
            static_cast<void>(wire<partial::operators::preserve>(w, values));
        }
        return std::move(w).finish();
    }
}  // namespace

TEST_CASE("a retained size materialization resolves for different fixed list sizes",
          "[codegen][generated][generic][materialization]") {
    CHECK_NOTHROW(compose_preserved_list<2>());
    CHECK_NOTHROW(compose_preserved_list<3>());
}
