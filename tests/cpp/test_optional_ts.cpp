// Tests for ``TS<Optional<T>>`` (RFC 0050): a time series whose tick may
// be a null. A native node publishes a value that holds a payload, a value
// that is unset, or nothing, and ``eval_node`` tells the three apart.

#include <hgraph/lib/std/operators/impl/json_impl.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/lift.h>
#include <hgraph/types/operator_dispatch.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value/specialized_views.h>
#include <hgraph/types/value/value.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <optional>
#include <array>
#include <vector>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;  // `none`

    /** An ``Optional[int]`` value that is unset: a tick whose value is null. */
    [[nodiscard]] Value null_int()
    {
        return Value{ValuePlanFactory::instance().type_for(scalar_descriptor<Optional<Int>>::value_meta())};
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
        static void           eval(In<"command", TS<Int>> command, Out<TS<Optional<Int>>> out)
        {
            const Int value = command.value();
            if (value == 0)
            {
                out.set(Null);
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
        static void           eval(In<"value", TS<Optional<Int>>> value, Out<TS<Int>> out)
        {
            const auto held = value.value();
            out.set(held.has_value() ? held.value().checked_as<Int>() : Int{-1});
        }
    };

    // Passes each tick on, through the erased value.
    struct PassOn
    {
        static constexpr auto name = "optional_pass_on";
        static void eval(In<"value", TS<Optional<Int>>> value, Out<TS<Optional<Int>>> out)
        {
            out.apply(value.base().value());
        }
    };

    // Passes each tick on through its borrowed nullable view.
    struct PassOnTyped
    {
        static constexpr auto name = "optional_pass_on_typed";
        static void eval(In<"value", TS<Optional<Int>>> value, Out<TS<Optional<Int>>> out)
        {
            const auto held = value.value();
            out.set(held);
        }
    };

    // Says what an input sees of a tick: whether it is valid and modified,
    // and whether its value holds a payload.
    struct Seen
    {
        static constexpr auto name = "optional_seen";
        static void           eval(In<"value", TS<Optional<Int>>> value, Out<TS<Int>> out)
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
        static void eval(In<"value", TS<Optional<Int>>> value, Out<TS<Optional<Int>>> out)
        {
            const auto payload = value.view().value();
            if (!payload.valid())
            {
                out.set(Null);
                return;
            }
            out.set(Value{Int{payload.checked_as<Int>() * 2}}.view());
        }
    };

    // Gives an Optional output a value that is no Optional.
    struct WrongApply
    {
        static constexpr auto name = "optional_wrong_apply";
        static void           eval(In<"command", TS<Int>> command, Out<TS<Optional<Int>>> out)
        {
            out.apply(Value{command.value()}.view());
        }
    };

    // A plain series wired where an Optional one is declared.
    struct IntIntoOptionalGraph
    {
        [[maybe_unused]] static constexpr auto name = "int_into_optional_graph";
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value) { return wire<PayloadOrMinusOne>(w, value); }
    };

    struct IntSeenAsOptionalGraph
    {
        [[maybe_unused]] static constexpr auto name = "int_seen_as_optional_graph";
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value) { return wire<Seen>(w, value); }
    };

    struct OptionalLenGraph
    {
        [[maybe_unused]] static constexpr auto name = "optional_len_graph";
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Optional<Int>>> value)
        {
            return wire<stdlib::len_>(w, value).as<TS<Int>>();
        }
    };

    struct OptionalToTableGraph
    {
        [[maybe_unused]] static constexpr auto name = "optional_to_table_graph";
        static auto compose(Wiring &w, Port<TS<Optional<Int>>> value) { return wire<stdlib::to_table>(w, value); }
    };
}  // namespace

TEST_CASE("TS<Optional<T>>: a node publishes a held payload, a null, or nothing")
{
    // A tick whose value is unset is a tick: it is recorded, and is no
    // `none`. Publishing nothing is `none`.
    CHECK_OUTPUT(testing::eval_node<Cleared>({5, 0, -1, 7, 0, 0}),
                 values<Value>(some_int(5), null_int(), none, some_int(7), null_int(), null_int()));
}

namespace
{
    // Commands: positive = publish; 0 = null; -1 = silence; -2 = invalidate.
    struct NullableLifecycle
    {
        static void eval(In<"command", TS<Int>> command, Out<TS<Optional<Int>>> out)
        {
            const auto c = command.value();
            if (c == -2) { static_cast<void>(out.begin_mutation(out.evaluation_time()).invalidate()); }
            else if (c == 0) { out.set(Null); }
            else if (c > 0) { out.set(c); }
        }
    };

    struct PlainLifecycle
    {
        static void eval(In<"command", TS<Int>> command, Out<TS<Int>> out)
        {
            const auto c = command.value();
            if (c == -2) { static_cast<void>(out.begin_mutation(out.evaluation_time()).invalidate()); }
            else if (c >= 0) { out.set(c); }
        }
    };

    struct SampleNullable
    {
        static void eval(In<"value", TS<Optional<Int>>, InputValidity::Unchecked> value,
                         In<"clock", TS<Int>>, Out<TS<Str>> out)
        {
            const auto payload = value.try_value();
            out.set(!value.valid() ? Str{"invalid"} :
                (payload ? std::to_string(payload.checked_as<Int>()) : Str{"null"}) +
                (value.modified() ? ":tick" : ":held"));
        }
    };

    template <typename Producer>
    struct LifecycleGraph
    {
        static Port<TS<Str>> compose(Wiring &w, Port<TS<Int>> command)
        {
            return wire<SampleNullable>(w, wire<Producer>(w, command), command);
        }
    };

    struct BorrowedRecordingGraph
    {
        static Port<TS<Optional<Int>>> compose(Wiring &, Port<TS<Int>> value)
        {
            return value.as<TS<Optional<Int>>>();
        }
    };

    struct nullable_only_ : Operator<"test_nullable_only", In<"value", TsVar<"S">>, Out<TS<Int>>> {};
    struct nullable_rank_ : Operator<"test_nullable_rank", In<"value", TsVar<"S">>, Out<TS<Int>>> {};
    struct ExactInt
    {
        static void eval(In<"value", TS<Int>> value, Out<TS<Int>> out) { out.set(value.value() + 100); }
    };
    template <typename Op>
    struct OverloadGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value)
        {
            return wire<Op>(w, value).template as<TS<Int>>();
        }
    };

    struct BorrowedSource
    {
        static constexpr bool schedule_on_start = true;
        static void eval(Out<TS<Int>> out) { out.set(Int{5}); }
    };
    struct IntSink { static void eval(In<"value", TS<Int>>) {} };
    struct BorrowedShapeGraph
    {
        static void compose(Wiring &w)
        {
            wire<IntSink>(w, wire<PayloadOrMinusOne>(w, wire<BorrowedSource>(w)));
        }
    };

    struct RefWideningGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Bool>> select,
                                    Port<TS<Int>> a, Port<TS<Int>> b)
        {
            return wire<PayloadOrMinusOne>(w, wire<stdlib::if_then_else>(w, select, a, b));
        }
    };

    using Pair = Tuple<Int, Str>;
    struct OptionalPair
    {
        static void eval(In<"value", TS<Optional<Pair>>> value, Out<TS<Optional<Pair>>> out)
        {
            out.set(value.value());
        }
    };
    struct BorrowedPairGraph
    {
        static Port<TS<Optional<Pair>>> compose(Wiring &w, Port<TS<Pair>> value)
        {
            return wire<OptionalPair>(w, value);
        }
    };

    struct TripleCallable
    {
        static Int apply(Int value) { return value * 3; }
    };
    struct NullableCallableSource
    {
        static void eval(In<"command", TS<Int>> command, Out<TS<Optional<ValueCallable>>> out)
        {
            if (command.value() == 0) { out.set(Null); }
            else if (command.value() > 0) { out.set(value_fn<TripleCallable>()); }
        }
    };
    struct PlainCallableSource
    {
        static void eval(In<"command", TS<Int>> command, Out<TS<ValueCallable>> out)
        {
            if (command.value() > 0) { out.set(value_fn<TripleCallable>()); }
        }
    };
    struct CallNullable
    {
        static void eval(In<"fn", TS<Optional<ValueCallable>>> fn, Out<TS<Int>> out)
        {
            const auto held = fn.value();
            const auto *callable = held.try_value<ValueCallable>();
            if (callable == nullptr) { out.set(Int{-1}); return; }
            const Value argument{Int{10}};
            const std::array args{ValueCallArg{argument.view()}};
            out.set(callable->invoke(args).view().checked_as<Int>());
        }
    };
    template <typename Producer>
    struct NullableCallableGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> command)
        {
            return wire<CallNullable>(w, wire<Producer>(w, command));
        }
    };
}

TEST_CASE("nullable binding: runtime callables can be published and borrowed")
{
    CHECK_OUTPUT(testing::eval_node<NullableCallableGraph<NullableCallableSource>>({1, 0, -1, 2}),
                 {30, -1, none, 30});
    CHECK_OUTPUT(testing::eval_node<NullableCallableGraph<PlainCallableSource>>({1, none, 2}),
                 {30, none, 30});
}

TEST_CASE("nullable binding: null ticks, silence and invalidation are distinct")
{
    CHECK_OUTPUT(testing::eval_node<LifecycleGraph<NullableLifecycle>>({7, 0, -1, -2, -1, 9, 0, 0}),
        values<Str>("7:tick", "null:tick", "null:held", "invalid", "invalid", "9:tick", "null:tick", "null:tick"));
    CHECK_OUTPUT(testing::eval_node<LifecycleGraph<PlainLifecycle>>({7, -1, -2, -1, 9}),
        values<Str>("7:tick", "7:held", "invalid", "invalid", "9:tick"));
}

TEST_CASE("nullable binding: captures retain earlier values after writes and graph destruction")
{
    CHECK(testing::eval_node<BorrowedRecordingGraph>(std::vector<std::optional<Int>>{}).empty());
    const auto captured = testing::eval_node<BorrowedRecordingGraph>({5, none, 7, 11});
    CHECK_OUTPUT(captured, values<Value>(some_int(5), none, some_int(7), some_int(11)));
    const auto nullable = testing::eval_node<NullableLifecycle>({5, 0, -1, -2, 7});
    CHECK_OUTPUT(nullable, values<Value>(some_int(5), null_int(), none, none, some_int(7)));
    REQUIRE(captured[0].has_value());
    CHECK(captured[0]->binding() == some_int(5).binding());
}

TEST_CASE("nullable binding: overload widening selects a candidate and prefers an exact match")
{
    register_overload<nullable_only_, PayloadOrMinusOne>();
    register_overload<nullable_rank_, PayloadOrMinusOne>();
    register_overload<nullable_rank_, ExactInt>();
    CHECK_OUTPUT(testing::eval_node<OverloadGraph<nullable_only_>>({5, none, 7}), {5, none, 7});
    CHECK_OUTPUT(testing::eval_node<OverloadGraph<nullable_rank_>>({5, none, 7}), {105, none, 107});
}

TEST_CASE("nullable binding: the graph contains no conversion node")
{
    auto graph = build_graph<BorrowedShapeGraph>();
    CHECK(graph.node_count() == 3); // source -> nullable consumer -> sink
}

TEST_CASE("nullable binding: erased reads borrow the payload and cannot mutate it")
{
    const auto *plain = schema_descriptor<TS<Int>>::ts_meta();
    const auto *nullable = schema_descriptor<TS<Optional<Int>>>::ts_meta();
    TSOutput source{*plain};
    auto widened = source.view(MIN_ST).binding_for(*nullable);
    CHECK_FALSE(widened.view(MIN_ST).valid());
    CHECK_FALSE(widened.view(MIN_ST).value().valid());
    Out<TS<Int>>{source.view(MIN_ST), MIN_ST}.set(Int{42});
    const auto view = widened.view(MIN_ST).value().as_optional();
    REQUIRE(view.has_value());
    CHECK(view.try_value<Int>() == source.view(MIN_ST).value().try_as<Int>());
    CHECK(view.try_value<Str>() == nullptr);
    CHECK(null_int().view().as_optional().try_value<Int>() == nullptr);
    CHECK_FALSE(view.can_begin_mutation());
    CHECK_FALSE(view.value().writable_payload());
    CHECK_THROWS(view.begin_mutation());
    CHECK_THROWS(widened.view(MIN_ST).begin_mutation(MIN_ST));
    CHECK(view.hash() == some_int(42).hash());
    CHECK(view.equals(some_int(42).view()));
    const Value captured{view};
    Out<TS<Int>>{source.view(MIN_ST + TimeDelta{1}), MIN_ST + TimeDelta{1}}.set(Int{99});
    CHECK(captured.view().equals(some_int(42).view()));
    const auto descriptor = source.checkpoint_alternative(widened);
    REQUIRE(descriptor.has_value());
    CHECK(descriptor->source.same_as(source.view(MIN_ST).handle()));
    CHECK(descriptor->requested_schema == nullable);
}

TEST_CASE("nullable binding: widening does not admit whole collections or nullable references")
{
    const auto check = []<typename Expected, typename Actual>() {
        const auto *expected = schema_descriptor<Expected>::ts_meta();
        const auto *actual = schema_descriptor<Actual>::ts_meta();
        ResolutionMap map;
        CHECK_FALSE(graph_wiring_detail::input_accepts_output_schema(expected, actual));
        CHECK_FALSE(input_ts_pattern_match(to_pattern<Expected>(), actual, map));
    };
    check.operator()<TSD<Str, TS<Optional<Int>>>, TSD<Str, TS<Int>>>();
    check.operator()<TSL<TS<Optional<Int>>, 2>, TSL<TS<Int>, 2>>();
    check.operator()<REF<TS<Optional<Int>>>, REF<TS<Int>>>();
}

TEST_CASE("nullable binding: references retarget and subsequent ticks remain visible")
{
    stdlib::register_standard_operators();
    CHECK_OUTPUT(testing::eval_node<RefWideningGraph>(values<Bool>(true, none, false, none),
        values<Int>(5, none, none, none), values<Int>(7, none, none, 9)), {5, none, 7, 9});
}

TEST_CASE("nullable binding: composite payloads borrow and recordings own their values")
{
    const auto pair_type = ValuePlanFactory::instance().type_for(scalar_descriptor<Pair>::value_meta());
    const auto make_pair = [pair_type](Int n, Str text) {
        BundleBuilder b{pair_type};
        b.set(0, Value{n});
        b.set(1, Value{std::move(text)});
        return b.build();
    };
    const Value first = make_pair(5, "five"), last = make_pair(7, "seven");
    const auto records = testing::eval_node<BorrowedPairGraph>(values<Value>(first, none, last));
    REQUIRE(records.size() == 3);
    REQUIRE(records[0].has_value());
    REQUIRE(records[2].has_value());
    CHECK(records[0]->view().as_optional().value().equals(first.view()));
    CHECK_FALSE(records[1].has_value());
    CHECK(records[2]->view().as_optional().value().equals(last.view()));

    TSOutput source{*schema_descriptor<TS<Pair>>::ts_meta()};
    auto mutation = source.view(MIN_ST).begin_mutation(MIN_ST);
    static_cast<void>(mutation.copy_value_from(first.view()));
    const auto widened = source.view(MIN_ST).binding_for(*schema_descriptor<TS<Optional<Pair>>>::ts_meta());
    CHECK(widened.view(MIN_ST).value().as_optional().value().data() == source.view(MIN_ST).value().data());
}

TEST_CASE("TS<Optional<T>>: an input reads the payload, or that there is none")
{
    CHECK_OUTPUT(testing::eval_node<PayloadOrMinusOne>(values<Value>(some_int(5), null_int(), none, some_int(0))),
                 {5, -1, none, 0});
}

TEST_CASE("TS<Optional<T>>: a tick whose value is unset is valid and modified")
{
    // 111: valid, modified, a payload. 110: valid, modified, no payload.
    CHECK_OUTPUT(testing::eval_node<Seen>(values<Value>(some_int(5), null_int(), none, null_int())),
                 {111, 110, none, 110});
}

TEST_CASE("TS<Optional<T>>: a value is passed on in either state")
{
    CHECK_OUTPUT(testing::eval_node<PassOn>(values<Value>(some_int(5), null_int(), none, some_int(7))),
                 values<Value>(some_int(5), null_int(), none, some_int(7)));
    // And through the borrowed view a typed node reads and sets.
    CHECK_OUTPUT(testing::eval_node<PassOnTyped>(values<Value>(some_int(5), null_int(), none, some_int(7))),
                 values<Value>(some_int(5), null_int(), none, some_int(7)));
    CHECK_OUTPUT(testing::eval_node<Doubled>(values<Value>(some_int(5), null_int(), some_int(-2))),
                 values<Value>(some_int(10), null_int(), some_int(-4)));
}

TEST_CASE("TS<Optional<T>>: an Optional output takes an Optional value and no other")
{
    CHECK_THROWS(testing::eval_node<WrongApply>({1}));
}

TEST_CASE("TS<Optional<T>>: an Optional is no tuple to what takes one")
{
    stdlib::register_standard_operators();

    // An Optional is its own kind, so the operators of a container
    // do not apply to it: `len_` finds no overload. (A typed tuple input,
    // `In<"x", TS<HomogeneousTuple<Int>>>`, refuses the port where the
    // graph is compiled.)
    CHECK_THROWS(testing::eval_node<OptionalLenGraph>(values<Value>(some_int(5))));
}

TEST_CASE("TS<Optional<T>>: a table refuses an Optional leaf by name")
{
    stdlib::register_standard_operators();

    // An Arrow null cell reads back as no tick, so a column cannot hold a
    // tick whose value is unset. Until a presence column carries it, the
    // leaf is refused where the table is wired.
    CHECK_THROWS_WITH(testing::eval_node<OptionalToTableGraph>(values<Value>(some_int(5))),
                      Catch::Matchers::ContainsSubstring("an Optional leaf is not recorded yet"));
}

TEST_CASE("TS<Optional<T>>: the JSON tree holds its payload's node, or the null node")
{
    const Value held = stdlib::json_tree::to_node(some_int(5).view());
    CHECK(stdlib::json_tree::unbox(held.view()).checked_as<Int>() == 5);

    const Value nothing = stdlib::json_tree::to_node(null_int().view());
    CHECK_FALSE(stdlib::json_tree::unbox(nothing.view()).valid());
}

TEST_CASE("TS<Optional<T>>: a TS<T> source binds to an Optional input, each tick held")
{
    stdlib::register_standard_operators();

    // Binding borrows the producer: the node sees a value that holds each
    // tick, and never one that is unset.
    CHECK_OUTPUT(testing::eval_node<IntIntoOptionalGraph>({5, none, 0, 7}), {5, none, 0, 7});
    // Valid, modified, and holding a payload.
    CHECK_OUTPUT(testing::eval_node<IntSeenAsOptionalGraph>({5, none, 7}), {111, none, 111});

    // The rule both ways: a T is taken for an Optional[T] where the source
    // is then adapted, an Optional[T] is not taken for a T, and only its
    // own payload is.
    const auto *plain = schema_descriptor<TS<Int>>::ts_meta();
    const auto *optional = schema_descriptor<TS<Optional<Int>>>::ts_meta();
    const auto *other = schema_descriptor<TS<Str>>::ts_meta();
    CHECK(graph_wiring_detail::input_binds_output_schema(optional, plain));
    CHECK_FALSE(graph_wiring_detail::input_binds_output_schema(plain, optional));
    CHECK_FALSE(graph_wiring_detail::input_binds_output_schema(optional, other));
    CHECK(graph_wiring_detail::input_binds_output_schema(optional, optional));

    // The runtime binding now supplies a read-only nullable alternative.
    CHECK(graph_wiring_detail::input_accepts_output_schema(optional, plain));
    CHECK_FALSE(graph_wiring_detail::input_accepts_output_schema(plain, optional));
    CHECK(graph_wiring_detail::input_accepts_output_schema(optional, optional));
}
