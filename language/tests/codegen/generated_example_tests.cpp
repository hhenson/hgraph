#include <collection-views.h>
#include <stateful-node.h>

#include "wiring/backend.h"

#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>

#include <catch2/catch_test_macros.hpp>

#include <string>

using namespace hgraph;
using namespace hgraph::testing;
using namespace std::string_literals;

namespace collections = examples::collection_views;
namespace stateful    = examples::stateful_node;

namespace
{
    using FirstModifiedIndex =
        Operator<"examples.collection_views.first_modified_index", In<"samples", TSL<TS<Float>>>, Out<TS<Int>>>;

    using CountRecent = Operator<"examples.collection_views.count_recent", In<"book", TSD<Str, TS<Float>>>,
                                 Scalar<"some_time", DateTime>, Out<TS<Int>>>;

    using LatestByKey =
        Operator<"examples.stateful_node.latest_by_key", In<"key", TS<Str>>, In<"value", TS<Float>>, Out<TSD<Str, TS<Float>>>>;

    void session() {
        hgl::wiring::ensure_session();
        collections::register_operators();
        stateful::register_operators();
    }
}  // namespace

TEST_CASE("generated list iteration observes the modified children", "[codegen][generated][examples]") {
    session();

    CHECK_OUTPUT((eval_node<FirstModifiedIndex, TSL<TS<Float>>>(values<Value>(dynamic_list_delta<TS<Float>>({{0, 1.0}}),
                                                                              dynamic_list_delta<TS<Float>>({{1, 2.0}}),
                                                                              dynamic_list_delta<TS<Float>>({}, {1})))),
                 values<Int>(0, 1, none));
}

TEST_CASE("generated collection predicates receive keys and values", "[codegen][generated][examples]") {
    session();

    CHECK_OUTPUT((eval_node<CountRecent, TSD<Str, TS<Float>>>(values<Value>(dict_delta<Str, TS<Float>>({{"A"s, 1.0}, {"B"s, 2.0}}),
                                                                            dict_delta<Str, TS<Float>>({{"A"s, 3.0}})),
                                                              arg<"some_time">(MIN_ST - MIN_TD))),
                 values<Int>(2, 2));
}

TEST_CASE("generated keyed output assignment accumulates a TSD delta", "[codegen][generated][examples]") {
    session();

    CHECK_OUTPUT((eval_node<LatestByKey>(values<Str>("A"s, "B"s), values<Float>(1.0, 2.0))),
                 values<Value>(dict_delta<Str, TS<Float>>({{"A"s, 1.0}}), dict_delta<Str, TS<Float>>({{"B"s, 2.0}})));
}
