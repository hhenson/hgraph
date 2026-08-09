#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/subgraph_wiring.h>
#include <hgraph/types/value/value_builder.h>

#include <catch2/catch_test_macros.hpp>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    using DelayedPair = TSB<"DelayedPair",
                            Field<"first", TS<Int>>,
                            Field<"second", TS<Int>>>;

    Value delayed_pair_delta(Int first, Int second)
    {
        const auto binding = ValuePlanFactory::instance().type_for(
            schema_descriptor<DelayedPair>::ts_meta()->delta_value_schema);
        BundleBuilder builder{binding};
        Value first_value{first};
        Value second_value{second};
        builder.set("first", first_value.view());
        builder.set("second", second_value.view());
        return builder.build();
    }

    struct IdentityInt
    {
        static constexpr auto name = "delayed_binding_identity_int";

        static void eval(In<"value", TS<Int>> value, Out<TS<Int>> out)
        {
            out.set(value.value());
        }
    };

    struct AddInts
    {
        static constexpr auto name = "delayed_binding_add_ints";

        static void eval(In<"lhs", TS<Int>> lhs, In<"rhs", TS<Int>> rhs, Out<TS<Int>> out)
        {
            out.set(lhs.value() + rhs.value());
        }
    };

    struct DelayedIdentityGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value)
        {
            auto delayed = delayed_binding<TS<Int>>(w);
            auto out     = wire<IdentityInt>(w, delayed());
            delayed(value);
            return out;
        }
    };

    struct DelayedFixedListGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TSL<TS<Int>, 2>> values)
        {
            auto delayed = delayed_binding<TSL<TS<Int>, 2>>(w);
            auto out = wire<AddInts>(w, tsl_element(delayed(), 0), tsl_element(delayed(), 1));
            delayed(values);
            return out;
        }
    };

    struct DelayedStructuralListGraph
    {
        static Port<TSL<TS<Int>, 2>> compose(
            Wiring &w,
            Port<TS<Int>> first,
            Port<TS<Int>> second)
        {
            auto delayed = delayed_binding<TSL<TS<Int>, 2>>(w);
            auto out = wire<stdlib::pass_through_node>(w, delayed()).as<TSL<TS<Int>, 2>>();
            delayed(stdlib::to_tsl<TSL<TS<Int>, 2>>(w, first, second));
            return out;
        }
    };

    struct DelayedStructuralBundleGraph
    {
        static Port<DelayedPair> compose(
            Wiring &w,
            Port<TS<Int>> first,
            Port<TS<Int>> second)
        {
            auto delayed = delayed_binding<DelayedPair>(w);
            auto out = wire<stdlib::pass_through_node>(w, delayed()).as<DelayedPair>();
            delayed(stdlib::to_tsb<DelayedPair>(w, first, second));
            return out;
        }
    };

    struct DelayedDictionaryKeysGraph
    {
        static Port<TSS<Int>> compose(Wiring &w, Port<TSD<Int, TS<Int>>> values)
        {
            auto delayed = delayed_binding<TSD<Int, TS<Int>>>(w);
            auto out     = wire<stdlib::keys_>(w, delayed()).as<TSS<Int>>();
            delayed(values);
            return out;
        }
    };

    struct DelayedChainGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value)
        {
            auto first  = delayed_binding<TS<Int>>(w);
            auto second = delayed_binding<TS<Int>>(w);
            auto out    = wire<IdentityInt>(w, first());
            first(second());
            second(value);
            return out;
        }
    };

    struct MapDelayedIdentityGraph
    {
        static Port<TSD<Int, TS<Int>>> compose(Wiring &w, Port<TSD<Int, TS<Int>>> values)
        {
            return wire<stdlib::map_>(w, fn<DelayedIdentityGraph>(), values)
                .as<TSD<Int, TS<Int>>>();
        }
    };

    struct UnboundDelayedGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>>)
        {
            auto delayed = delayed_binding<TS<Int>>(w);
            return wire<IdentityInt>(w, delayed());
        }
    };

    struct MismatchedDelayedGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Float>> value)
        {
            auto delayed = delayed_binding<TS<Int>>(w);
            auto out     = wire<IdentityInt>(w, delayed());
            delayed(value);
            return out;
        }
    };

    struct DoubleBoundDelayedGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> first, Port<TS<Int>> second)
        {
            auto delayed = delayed_binding<TS<Int>>(w);
            auto out     = wire<IdentityInt>(w, delayed());
            delayed(first);
            delayed(second);
            return out;
        }
    };

    struct CyclicDelayedGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value)
        {
            auto delayed = delayed_binding<TS<Int>>(w);
            auto observed = wire<IdentityInt>(w, delayed());
            auto sum      = wire<AddInts>(w, observed, value);
            delayed(sum);
            return observed;
        }
    };
}  // namespace

TEST_CASE("delayed_binding resolves an ordinary producer after its consumer is wired")
{
    CHECK_OUTPUT(eval_node<DelayedIdentityGraph>(values<Int>(1, 2, 3)), values<Int>(1, 2, 3));
}

TEST_CASE("delayed_binding retains fixed-list projections until the producer is known")
{
    CHECK_OUTPUT(
        eval_node<DelayedFixedListGraph>(
            values<Value>(list_delta<TS<Int>>({1, 2}), list_delta<TS<Int>>({{1, 5}}))),
        values<Int>(3, 6));
}

TEST_CASE("delayed_binding expands fixed structural sources into delayed leaves")
{
    CHECK_OUTPUT(
        eval_node<DelayedStructuralListGraph>(values<Int>(1, 2), values<Int>(10, 20)),
        values<Value>(list_delta<TS<Int>>({1, 10}), list_delta<TS<Int>>({{0, 2}, {1, 20}})));

    CHECK_OUTPUT(
        eval_node<DelayedStructuralBundleGraph>(values<Int>(1, 2), values<Int>(10, 20)),
        values<Value>(delayed_pair_delta(1, 10), delayed_pair_delta(2, 20)));
}

TEST_CASE("delayed_binding retains dictionary key-set projections")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(
        eval_node<DelayedDictionaryKeysGraph>(
            values<Value>(dict_delta<Int, TS<Int>>({{1, 10}, {2, 20}}),
                          dict_delta<Int, TS<Int>>({}, {1}))),
        values<Value>(set_delta<Int>({1, 2}, {}), set_delta<Int>({}, {1})));
}

TEST_CASE("delayed_binding resolves chains and compiled child boundary sources")
{
    CHECK_OUTPUT(eval_node<DelayedChainGraph>(values<Int>(1, 2)), values<Int>(1, 2));

    stdlib::register_standard_operators();
    CHECK_OUTPUT(
        eval_node<MapDelayedIdentityGraph>(
            values<Value>(dict_delta<Int, TS<Int>>({{1, 10}, {2, 20}}),
                          dict_delta<Int, TS<Int>>({{1, 11}}, {2}))),
        values<Value>(dict_delta<Int, TS<Int>>({{1, 10}, {2, 20}}),
                      dict_delta<Int, TS<Int>>({{1, 11}}, {2})));
}

TEST_CASE("delayed_binding rejects unbound, mismatched, and duplicate bindings")
{
    CHECK_THROWS_AS((void)eval_node<UnboundDelayedGraph>(values<Int>(1)), std::logic_error);
    CHECK_THROWS_AS((void)eval_node<MismatchedDelayedGraph>(values<Float>(1.0)), std::invalid_argument);
    CHECK_THROWS_AS(
        (void)eval_node<DoubleBoundDelayedGraph>(values<Int>(1), values<Int>(2)),
        std::logic_error);
}

TEST_CASE("delayed_binding does not turn a dependency cycle into feedback")
{
    CHECK_THROWS_AS((void)eval_node<CyclicDelayedGraph>(values<Int>(1)), std::runtime_error);
}
