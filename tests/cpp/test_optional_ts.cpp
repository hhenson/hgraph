// Tests for ``TS<std::optional<T>>`` (RFC 0050): a time series whose tick may
// be a null. A native node publishes a value that holds a payload, a value
// that is unset, or nothing, and ``eval_node`` tells the three apart.

#include <hgraph/lib/std/operators/impl/json_impl.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <optional>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;  // `none`

    /** An ``Optional[int]`` value that is unset: a tick whose value is null. */
    [[nodiscard]] Value null_int()
    {
        return Value{ValuePlanFactory::instance().type_for(scalar_descriptor<std::optional<Int>>::value_meta())};
    }

    /** An ``Optional[int]`` value that holds ``payload``. */
    [[nodiscard]] Value some_int(Int payload)
    {
        Value value = null_int();
        value.view().as_optional().begin_mutation().emplace(Value{payload}.view());
        return value;
    }

    // 0 publishes a value that is unset, a negative number publishes
    // nothing, and any other publishes a value that holds it.
    struct Cleared
    {
        static constexpr auto name = "optional_cleared";
        static void           eval(In<"command", TS<Int>> command, Out<TS<std::optional<Int>>> out)
        {
            const Int value = command.value();
            if (value == 0)
            {
                out.set(std::nullopt);
                return;
            }
            if (value < 0) { return; }
            out.set(value);
        }
    };

    // Reads each tick: -1 for a value that is unset, the payload otherwise.
    struct PayloadOrMinusOne
    {
        static constexpr auto name = "optional_payload_or_minus_one";
        static void           eval(In<"value", TS<std::optional<Int>>> value, Out<TS<Int>> out)
        {
            const std::optional<Int> held = value.value();
            out.set(held.has_value() ? *held : Int{-1});
        }
    };

    // Passes each tick on, through the erased value.
    struct PassOn
    {
        static constexpr auto name = "optional_pass_on";
        static void eval(In<"value", TS<std::optional<Int>>> value, Out<TS<std::optional<Int>>> out)
        {
            out.apply(value.base().value());
        }
    };

    // Passes each tick on, as the ``std::optional`` it is.
    struct PassOnTyped
    {
        static constexpr auto name = "optional_pass_on_typed";
        static void eval(In<"value", TS<std::optional<Int>>> value, Out<TS<std::optional<Int>>> out)
        {
            const std::optional<Int> held = value.value();
            out.set(held);
        }
    };

    // Says what an input sees of a tick: whether it is valid and modified,
    // and whether its value holds a payload.
    struct Seen
    {
        static constexpr auto name = "optional_seen";
        static void           eval(In<"value", TS<std::optional<Int>>> value, Out<TS<Int>> out)
        {
            const Int valid = value.valid() ? 100 : 0;
            const Int modified = value.modified() ? 10 : 0;
            const Int held = value.value().has_value() ? 1 : 0;
            out.set(valid + modified + held);
        }
    };

    // Holds a payload of the payload's own type through the erased view.
    struct Doubled
    {
        static constexpr auto name = "optional_doubled";
        static void eval(In<"value", TS<std::optional<Int>>> value, Out<TS<std::optional<Int>>> out)
        {
            const auto payload = value.view().value();
            if (!payload.valid())
            {
                out.set(std::nullopt);
                return;
            }
            out.set(Value{Int{payload.checked_as<Int>() * 2}}.view());
        }
    };

    // Gives an Optional output a value that is no Optional.
    struct WrongApply
    {
        static constexpr auto name = "optional_wrong_apply";
        static void           eval(In<"command", TS<Int>> command, Out<TS<std::optional<Int>>> out)
        {
            out.apply(Value{command.value()}.view());
        }
    };

    struct OptionalLenGraph
    {
        [[maybe_unused]] static constexpr auto name = "optional_len_graph";
        static Port<TS<Int>> compose(Wiring &w, Port<TS<std::optional<Int>>> value)
        {
            return wire<stdlib::len_>(w, value).as<TS<Int>>();
        }
    };

    struct OptionalToTableGraph
    {
        [[maybe_unused]] static constexpr auto name = "optional_to_table_graph";
        static auto compose(Wiring &w, Port<TS<std::optional<Int>>> value) { return wire<stdlib::to_table>(w, value); }
    };
}  // namespace

TEST_CASE("TS<std::optional<T>>: a node publishes a held payload, a null, or nothing")
{
    // A tick whose value is unset is a tick: it is recorded, and is no
    // `none`. Publishing nothing is `none`.
    CHECK_OUTPUT(testing::eval_node<Cleared>({5, 0, -1, 7, 0, 0}),
                 values<Value>(some_int(5), null_int(), none, some_int(7), null_int(), null_int()));
}

TEST_CASE("TS<std::optional<T>>: an input reads the payload, or that there is none")
{
    CHECK_OUTPUT(testing::eval_node<PayloadOrMinusOne>(values<Value>(some_int(5), null_int(), none, some_int(0))),
                 {5, -1, none, 0});
}

TEST_CASE("TS<std::optional<T>>: a tick whose value is unset is valid and modified")
{
    // 111: valid, modified, a payload. 110: valid, modified, no payload.
    CHECK_OUTPUT(testing::eval_node<Seen>(values<Value>(some_int(5), null_int(), none, null_int())),
                 {111, 110, none, 110});
}

TEST_CASE("TS<std::optional<T>>: a value is passed on in either state")
{
    CHECK_OUTPUT(testing::eval_node<PassOn>(values<Value>(some_int(5), null_int(), none, some_int(7))),
                 values<Value>(some_int(5), null_int(), none, some_int(7)));
    // And as the ``std::optional`` a typed node reads and sets.
    CHECK_OUTPUT(testing::eval_node<PassOnTyped>(values<Value>(some_int(5), null_int(), none, some_int(7))),
                 values<Value>(some_int(5), null_int(), none, some_int(7)));
    CHECK_OUTPUT(testing::eval_node<Doubled>(values<Value>(some_int(5), null_int(), some_int(-2))),
                 values<Value>(some_int(10), null_int(), some_int(-4)));
}

TEST_CASE("TS<std::optional<T>>: an Optional output takes an Optional value and no other")
{
    CHECK_THROWS(testing::eval_node<WrongApply>({1}));
}

TEST_CASE("TS<std::optional<T>>: an Optional is no tuple to what takes one")
{
    stdlib::register_standard_operators();

    // By kind it is a one-element Tuple, and the operators of a container
    // do not apply to it: `len_` finds no overload. (A typed tuple input,
    // `In<"x", TS<HomogeneousTuple<Int>>>`, refuses the port where the
    // graph is compiled.)
    CHECK_THROWS(testing::eval_node<OptionalLenGraph>(values<Value>(some_int(5))));
}

TEST_CASE("TS<std::optional<T>>: a table refuses an Optional leaf by name")
{
    stdlib::register_standard_operators();

    // An Arrow null cell reads back as no tick, so a column cannot hold a
    // tick whose value is unset. Until a presence column carries it, the
    // leaf is refused where the table is wired.
    CHECK_THROWS_WITH(testing::eval_node<OptionalToTableGraph>(values<Value>(some_int(5))),
                      Catch::Matchers::ContainsSubstring("an Optional leaf is not recorded yet"));
}

TEST_CASE("TS<std::optional<T>>: the JSON tree holds its payload's node, or the null node")
{
    const Value held = stdlib::json_tree::to_node(some_int(5).view());
    CHECK(stdlib::json_tree::unbox(held.view()).checked_as<Int>() == 5);

    const Value nothing = stdlib::json_tree::to_node(null_int().view());
    CHECK_FALSE(stdlib::json_tree::unbox(nothing.view()).valid());
}
