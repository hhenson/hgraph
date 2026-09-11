#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/time_series/output_mutation.h>

#include <catch2/catch_test_macros.hpp>

#include <concepts>
#include <stdexcept>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    template <typename TOut>
    concept accepts_set_upsert = requires(const TOut &out) { hgraph::upsert(out, Int{1}); };

    template <typename TOut>
    concept accepts_dict_update = requires(const TOut &out) { hgraph::update(out, Str{"key"}, Int{1}); };

    static_assert(accepts_set_upsert<Out<TSS<Int>>>);
    static_assert(!accepts_set_upsert<Out<TS<Int>>>);
    static_assert(accepts_dict_update<Out<TSD<Str, TS<Int>>>>);
    static_assert(!accepts_dict_update<Out<TSS<Int>>>);
    static_assert(!accepts_dict_update<Out<TSD<Str, TSS<Int>>>>);

    struct FunctionalSetMutation
    {
        static constexpr auto name = "functional_set_mutation";

        static void eval(In<"step", TS<Int>> step, Out<TSS<Int>> out) {
            if (step.value() == 1) {
                insert(out, Int{1});
                upsert(out, Int{1});
                upsert(out, Int{2});
                REQUIRE_THROWS_AS(insert(out, Int{2}), std::invalid_argument);
                discard(out, Int{99});
            } else {
                remove(out, Int{1});
                REQUIRE_THROWS_AS(remove(out, Int{1}), std::out_of_range);
                upsert(out, Int{3});
                discard(out, Int{99});
            }
        }
    };

    struct FunctionalDictMutation
    {
        static constexpr auto name = "functional_dict_mutation";

        static void eval(In<"step", TS<Int>> step, Out<TSD<Str, TS<Int>>> out) {
            if (step.value() == 1) {
                insert(out, Str{"a"}, Int{1});
                upsert(out, Str{"b"}, Int{2});
                REQUIRE_THROWS_AS(insert(out, Str{"a"}, Int{9}), std::invalid_argument);
                REQUIRE_THROWS_AS(update(out, Str{"missing"}, Int{9}), std::out_of_range);
                discard(out, Str{"missing"});
            } else if (step.value() == 2) {
                update(out, Str{"a"}, Int{3});
                upsert(out, Str{"b"}, Int{4});
                upsert(out, Str{"c"}, Int{5});
                remove(out, Str{"a"});
                REQUIRE_THROWS_AS(remove(out, Str{"a"}), std::out_of_range);
            } else {
                invalidate(out, Str{"b"});
                REQUIRE(out.contains(Str{"b"}));
                REQUIRE_FALSE(out.at_slot(out.find_slot(Str{"b"})).valid());
                REQUIRE_THROWS_AS(invalidate(out, Str{"missing"}), std::out_of_range);
                clear(out);
            }
        }
    };

    struct FunctionalSetNoOpMutation
    {
        static constexpr auto name = "functional_set_no_op_mutation";

        static void eval(In<"step", TS<Int>> step, Out<TSS<Int>> out) {
            if (step.value() == 1) {
                upsert(out, Int{1});
            } else if (step.value() == 2) {
                upsert(out, Int{1});
                discard(out, Int{99});
            } else if (step.value() == 3) {
                remove(out, Int{1});
            } else {
                discard(out, Int{99});
                clear(out);
            }
        }
    };

    struct FunctionalDictNoOpMutation
    {
        static constexpr auto name = "functional_dict_no_op_mutation";

        static void eval(In<"step", TS<Int>> step, Out<TSD<Str, TS<Int>>> out) {
            if (step.value() == 1) {
                upsert(out, Str{"a"}, Int{1});
            } else if (step.value() == 2) {
                discard(out, Str{"missing"});
            } else if (step.value() == 3) {
                remove(out, Str{"a"});
            } else {
                discard(out, Str{"missing"});
                clear(out);
            }
        }
    };
}  // namespace

TEST_CASE("functional TSS mutations enforce strict and tolerant forms") {
    CHECK_OUTPUT(eval_node<FunctionalSetMutation>(values<Int>(1, 2)),
                 values<Value>(set_delta<Int>({1, 2}, {}), set_delta<Int>({3}, {1})));
}

TEST_CASE("functional TSD mutations preserve structural and invalidation semantics") {
    CHECK_OUTPUT(eval_node<FunctionalDictMutation>(values<Int>(1, 2, 3)),
                 values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}, {"b", 2}}),
                               dict_delta<Str, TS<Int>>({{"b", 4}, {"c", 5}}, {"a"}), dict_delta<Str, TS<Int>>({}, {"b", "c"})));
}

TEST_CASE("tolerant functional mutations do not produce empty ticks") {
    CHECK_OUTPUT(eval_node<FunctionalSetNoOpMutation>(values<Int>(1, 2, 3, 4)),
                 values<Value>(set_delta<Int>({1}, {}), none, set_delta<Int>({}, {1}), none));
    CHECK_OUTPUT(eval_node<FunctionalDictNoOpMutation>(values<Int>(1, 2, 3, 4)),
                 values<Value>(dict_delta<Str, TS<Int>>({{"a", 1}}), none, dict_delta<Str, TS<Int>>({}, {"a"}), none));
}
