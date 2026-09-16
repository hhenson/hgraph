// Capturing a TSD delta for one group of keys (RFC 0037).
//
// A distributed map_ sends each worker the same delta an ordinary capture
// would produce, minus the keys another worker owns. Two properties make that
// safe, and both are asserted here:
//
//   PARTITION   every key lands in exactly one group -- none duplicated, none
//               dropped, which is what stops a key's history splitting across
//               two workers or vanishing;
//   EQUIVALENCE the union of the groups is the unfiltered capture.

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <set>
#include <string>
#include <vector>

namespace
{
    using namespace hgraph;

    /** Even keys to group 0, odd to group 1 -- deterministic and checkable. */
    bool even_keys(const void *context, const ValueView &key)
    {
        const auto wanted = *static_cast<const Int *>(context);
        return (key.checked_as<Int>() % 2) == wanted;
    }

    std::set<Int> modified_keys(const Value &delta)
    {
        std::set<Int> keys;
        const auto    bundle = delta.view().as_indexed_view();
        for (const auto entry : bundle.at(1).as_map())
        {
            keys.insert(entry.first.checked_as<Int>());
        }
        return keys;
    }

    std::set<Int> removed_keys(const Value &delta)
    {
        std::set<Int> keys;
        const auto    bundle = delta.view().as_indexed_view();
        for (const auto element : bundle.at(0).as_set())
        {
            keys.insert(element.checked_as<Int>());
        }
        return keys;
    }

    /** Captures the whole delta and each half, on every tick. */
    struct SplitProbe
    {
        static constexpr auto name = "split_probe";

        static void eval(In<"ts", TSD<Int, TS<Int>>> ts, GlobalStateView gs)
        {
            static const Int even = 0;
            static const Int odd  = 1;

            Value whole = capture_delta(ts.base());
            Value left  = capture_dict_delta_where(ts.base(), &even_keys, &even);
            Value right = capture_dict_delta_where(ts.base(), &even_keys, &odd);

            gs.set("whole", whole);
            gs.set("left", left);
            gs.set("right", right);
        }
    };

    /** Ticks the keys named by the replayed bitmask, so a test chooses them. */
    struct Spread
    {
        static constexpr auto name = "split_spread";
        static void           eval(In<"in", TS<Int>> in, Out<TSD<Int, TS<Int>>> out)
        {
            const Int mask = in.value();
            for (Int key = 1; key <= 5; ++key)
            {
                if ((mask & (Int{1} << (key - 1))) != 0) { out[key].set(key * 10); }
            }
        }
    };

    struct SplitGraph
    {
        static constexpr auto name = "split_graph";
        static void           compose(Wiring &w)
        {
            auto src  = wire<stdlib::replay_impl, TS<Int>>(w, Str{"in"});
            auto dict = wire<Spread>(w, src);
            wire<SplitProbe>(w, dict);
        }
    };
}  // namespace

TEST_CASE("partitioned delta: the groups partition the keys and lose nothing")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    GraphBuilder gb = build_graph<SplitGraph>();
    // Keys 1..5 tick together, so one capture carries five modified entries.
    testing::set_replay_values<Int>(gb.global_state(), "in", {Int{0b11111}});

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{100});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    const auto gs    = ex.view().graph().global_state();
    const Value whole{gs.get("whole")};
    const Value left{gs.get("left")};
    const Value right{gs.get("right")};

    const auto all   = modified_keys(whole);
    const auto evens = modified_keys(left);
    const auto odds  = modified_keys(right);

    REQUIRE(all == std::set<Int>{1, 2, 3, 4, 5});
    CHECK(evens == std::set<Int>{2, 4});
    CHECK(odds == std::set<Int>{1, 3, 5});

    // The two properties, stated as such.
    std::set<Int> both;
    std::set_intersection(evens.begin(), evens.end(), odds.begin(), odds.end(),
                          std::inserter(both, both.begin()));
    CHECK(both.empty());   // no key in two groups

    std::set<Int> union_of;
    union_of.insert(evens.begin(), evens.end());
    union_of.insert(odds.begin(), odds.end());
    CHECK(union_of == all);   // and none lost

    CHECK(removed_keys(whole).empty());
}

TEST_CASE("partitioned delta: a group that owns nothing captures an empty delta")
{
    (void)TypeRegistry::instance().register_scalar<Int>("int");

    GraphBuilder gb = build_graph<SplitGraph>();
    // Only odd keys tick, so group 0 must produce a delta with no entries --
    // not a malformed one, and not the whole thing.
    testing::set_replay_values<Int>(gb.global_state(), "in", {Int{0b00101}});

    GraphExecutorBuilder eb;
    eb.graph_builder(std::move(gb)).start_time(MIN_ST).end_time(MIN_ST + TimeDelta{100});
    GraphExecutorValue ex = eb.make_executor();
    ex.view().run();

    const auto  gs = ex.view().graph().global_state();
    const Value left{gs.get("left")};
    const Value right{gs.get("right")};

    CHECK(modified_keys(left).empty());
    CHECK(modified_keys(right) == std::set<Int>{1, 3});
}
