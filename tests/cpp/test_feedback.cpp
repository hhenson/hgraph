#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/value/compact_container_ops.h>
#include <hgraph/types/value/value_builder.h>

#include <catch2/catch_test_macros.hpp>

namespace polymorphic_feedback_repro
{
    struct Event
    {
    };
}

namespace hgraph
{
    template <>
    struct scalar_descriptor<polymorphic_feedback_repro::Event>
    {
        [[nodiscard]] static constexpr bool is_concrete() noexcept { return true; }
        [[nodiscard]] static const ValueTypeMetaData *value_meta()
        {
            auto &registry = TypeRegistry::instance();
            return registry.bundle(
                "tests.feedback", "Event", {{"event_id", registry.value_type("str")}}, {}, true);
        }
    };
}

namespace hgraph::testing
{
    template <>
    struct ts_harness<TS<polymorphic_feedback_repro::Event>>
        : bundle_ts_harness<TS<polymorphic_feedback_repro::Event>>
    {
    };
}

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    using PolymorphicEvent = polymorphic_feedback_repro::Event;

    struct PolymorphicFeedbackSchemas
    {
        const ValueTypeMetaData *event{nullptr};
        const ValueTypeMetaData *heartbeat_event{nullptr};
        const ValueTypeMetaData *create_event{nullptr};
    };

    [[nodiscard]] PolymorphicFeedbackSchemas polymorphic_feedback_schemas()
    {
        auto       &registry = TypeRegistry::instance();
        const auto *text = registry.value_type("str");
        const auto *event = scalar_descriptor<PolymorphicEvent>::value_meta();
        const auto *heartbeat_event = registry.bundle(
            "tests.feedback", "HeartbeatEvent", {{"event_id", text}}, {event});
        const auto *order_event = registry.bundle(
            "tests.feedback", "OrderEvent",
            {{"event_id", text}, {"order_id", text}}, {event}, true);
        const auto *create_event = registry.bundle(
            "tests.feedback", "CreateEvent",
            {{"event_id", text}, {"order_id", text}, {"payload", text},
             {"details_a", text}, {"details_b", text},
             {"details_c", text}, {"details_d", text}},
            {order_event});
        return PolymorphicFeedbackSchemas{
            .event = event,
            .heartbeat_event = heartbeat_event,
            .create_event = create_event,
        };
    }

    [[nodiscard]] Value realize_polymorphic_event(
        const TypeRealizationSnapshot &realization,
        const ValueTypeMetaData        *event,
        const Value                    &concrete)
    {
        const auto event_binding = realization.type_for(event);
        Value      event_value{event_binding};
        event_binding.ops_ref().copy_assign_from(
            event_binding, event_value.begin_mutation().mutable_data(),
            concrete.binding(), concrete.view().data());
        return event_value;
    }

    [[nodiscard]] Value heartbeat_event_value(
        const TypeRealizationSnapshot    &realization,
        const PolymorphicFeedbackSchemas &schemas)
    {
        BundleBuilder heartbeat{
            ValuePlanFactory::instance().type_for(schemas.heartbeat_event)};
        heartbeat.set("event_id", Value{Str{"heartbeat"}});
        const Value concrete = heartbeat.build();
        return realize_polymorphic_event(realization, schemas.event, concrete);
    }

    [[nodiscard]] Value create_event_value(
        const TypeRealizationSnapshot    &realization,
        const PolymorphicFeedbackSchemas &schemas)
    {
        BundleBuilder create{
            ValuePlanFactory::instance().type_for(schemas.create_event)};
        create.set("event_id", Value{Str{"event"}});
        create.set("order_id", Value{Str{"order"}});
        create.set("payload", Value{Str{"created"}});
        create.set("details_a", Value{Str{"a"}});
        create.set("details_b", Value{Str{"b"}});
        create.set("details_c", Value{Str{"c"}});
        create.set("details_d", Value{Str{"d"}});
        const Value concrete = create.build();
        return realize_polymorphic_event(realization, schemas.event, concrete);
    }

    void check_polymorphic_event(
        const Value &actual, const ValueTypeMetaData *expected_schema,
        std::initializer_list<std::pair<std::string_view, std::string_view>> expected_fields)
    {
        const auto concrete = actual.view().concrete();
        CHECK(concrete.schema() == expected_schema);
        const auto bundle = concrete.as_bundle();
        for (const auto &[name, expected] : expected_fields)
        {
            CAPTURE(name);
            CHECK(bundle.field(name).checked_as<Str>() == Str{expected});
        }
    }

    [[nodiscard]] Value immutable_int_tuple()
    {
        const auto *meta = scalar_descriptor<HomogeneousTuple<Int>>::value_meta();
        const auto binding =
            ValuePlanFactory::instance().type_for(scalar_descriptor<Int>::value_meta());
        ListBuilder builder{binding};
        builder.push_back(Int{1});
        builder.push_back(Int{2});
        ListStorage storage = builder.build_storage();
        return Value{compact_list_type(binding, *meta), &storage};
    }

    struct AddInts
    {
        static constexpr auto name = "feedback_add_ints";

        static void eval(In<"lhs", TS<Int>> lhs,
                         In<"rhs", TS<Int>> rhs,
                         Out<TS<Int>> out)
        {
            out.set(lhs.value() + rhs.value());
        }
    };

    struct IndependentFeedbackGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> left, Port<TS<Int>> right)
        {
            auto left_feedback  = stdlib::feedback<TS<Int>>(w, Int{0});
            auto right_feedback = stdlib::feedback<TS<Int>>(w, Int{0});

            left_feedback(left);
            right_feedback(right);

            return wire<AddInts>(w, left_feedback(), right_feedback());
        }
    };

    struct NoDefaultFeedbackGraph
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value)
        {
            auto feedback = stdlib::feedback<TS<Int>>(w);
            feedback(value);
            return feedback();
        }
    };

    struct PolymorphicFeedbackGraph
    {
        static Port<TS<PolymorphicEvent>> compose(
            Wiring &w, Port<TS<PolymorphicEvent>> value)
        {
            auto feedback = stdlib::feedback<TS<PolymorphicEvent>>(w);
            feedback(value);
            return feedback();
        }
    };

    struct PolymorphicDefaultFeedbackGraph
    {
        static Port<TS<PolymorphicEvent>> compose(
            Wiring &w, Port<TS<PolymorphicEvent>> value,
            Scalar<"initial_delta", Value> initial_delta)
        {
            auto feedback = stdlib::feedback<TS<PolymorphicEvent>>(
                w, initial_delta.value().clone());
            feedback(value);
            return feedback();
        }
    };

    struct ImmutableDefaultFeedbackGraph
    {
        static Port<TS<HomogeneousTuple<Int>>> compose(Wiring &w)
        {
            auto feedback = stdlib::feedback<TS<HomogeneousTuple<Int>>>(
                w, immutable_int_tuple());
            return feedback();
        }
    };

    struct FibStep
    {
        static constexpr auto name = "feedback_fib_step";

        static void start(State<Int> remaining) { remaining.set(Int{0}); }

        static void eval(In<"count", TS<Int>> count,
                         In<"a", TS<Int>> a,
                         In<"b", TS<Int>> b,
                         State<Int> remaining,
                         Out<TS<Int>> out)
        {
            Int steps_left = remaining.get();
            if (count.modified()) { steps_left = count.value(); }

            out.set(a.value() + b.value());

            if (steps_left > Int{0}) { --steps_left; }
            remaining.set(steps_left);
            if (steps_left == Int{0})
            {
                a.make_passive();
                b.make_passive();
            }
        }
    };

    struct FeedbackFibonacciGraph
    {
        static Port<TS<Int>> compose(Wiring &w)
        {
            auto a = stdlib::feedback<TS<Int>>(w, Int{0});
            auto b = stdlib::feedback<TS<Int>>(w, Int{1});

            auto count = wire<stdlib::const_, TS<Int>>(w, Int{5});
            auto c = wire<FibStep>(w, count, a(), b());
            a(b());
            b(c);
            return c;
        }
    };
}  // namespace

TEST_CASE("stdlib feedback creates independent source nodes for identical schemas")
{
    CHECK_OUTPUT(hgraph::testing::eval_node<IndependentFeedbackGraph>(
                     hgraph::testing::values<hgraph::Int>(7),
                     hgraph::testing::values<hgraph::Int>(11)),
                 hgraph::testing::values<hgraph::Int>(0, 18));
}

TEST_CASE("stdlib feedback without a default is initially silent")
{
    CHECK_OUTPUT(hgraph::testing::eval_node<NoDefaultFeedbackGraph>(
                     hgraph::testing::values<hgraph::Int>(7, 11)),
                 hgraph::testing::values<hgraph::Int>(hgraph::testing::none, 7, 11));
}

TEST_CASE("stdlib feedback preserves concrete Bundle alternatives")
{
    const auto schemas = polymorphic_feedback_schemas();
    const auto realization = hgraph::TypeRealizationSnapshot::capture(
        hgraph::TypeRegistry::instance());
    hgraph::TypeRealizationScope realization_scope{realization.get()};
    const hgraph::Value heartbeat = heartbeat_event_value(*realization, schemas);
    const hgraph::Value created = create_event_value(*realization, schemas);

    const auto actual = hgraph::testing::eval_node<PolymorphicFeedbackGraph>(
        hgraph::testing::values<hgraph::Value>(heartbeat, created));

    REQUIRE(actual.size() == 3);
    CHECK_FALSE(actual.front().has_value());
    REQUIRE(actual[1].has_value());
    check_polymorphic_event(
        *actual[1], schemas.heartbeat_event, {{"event_id", "heartbeat"}});
    REQUIRE(actual[2].has_value());
    check_polymorphic_event(
        *actual[2], schemas.create_event,
        {{"event_id", "event"}, {"order_id", "order"},
         {"payload", "created"}, {"details_a", "a"},
         {"details_b", "b"}, {"details_c", "c"}, {"details_d", "d"}});
}

TEST_CASE("stdlib feedback preserves concrete Bundle alternatives after its default")
{
    const auto schemas = polymorphic_feedback_schemas();
    const auto realization = hgraph::TypeRealizationSnapshot::capture(
        hgraph::TypeRegistry::instance());
    hgraph::TypeRealizationScope realization_scope{realization.get()};
    const hgraph::Value heartbeat = heartbeat_event_value(*realization, schemas);
    const hgraph::Value created = create_event_value(*realization, schemas);

    const auto actual = hgraph::testing::eval_node<PolymorphicDefaultFeedbackGraph>(
        hgraph::testing::values<hgraph::Value>(heartbeat, created), created);

    REQUIRE(actual.size() == 3);
    REQUIRE(actual[0].has_value());
    check_polymorphic_event(
        *actual[0], schemas.create_event,
        {{"event_id", "event"}, {"order_id", "order"},
         {"payload", "created"}, {"details_a", "a"},
         {"details_b", "b"}, {"details_c", "c"}, {"details_d", "d"}});
    REQUIRE(actual[1].has_value());
    check_polymorphic_event(
        *actual[1], schemas.heartbeat_event, {{"event_id", "heartbeat"}});
    REQUIRE(actual[2].has_value());
    check_polymorphic_event(
        *actual[2], schemas.create_event,
        {{"event_id", "event"}, {"order_id", "order"},
         {"payload", "created"}, {"details_a", "a"},
         {"details_b", "b"}, {"details_c", "c"}, {"details_d", "d"}});
}

TEST_CASE("stdlib feedback preserves concrete Bundle alternatives in pooled graph storage")
{
    auto &registry = hgraph::TypeRegistry::instance();
    const auto schemas = polymorphic_feedback_schemas();

    const hgraph::TypeRealizationOptions options{
        .polymorphic_compound_storage =
            hgraph::PolymorphicCompoundStoragePolicy::Pooled,
    };
    const auto realization =
        hgraph::TypeRealizationSnapshot::capture(registry, options);
    static_cast<void>(realization->graph_type_for(schemas.event));
    const auto inspection = realization->inspect(schemas.event);
    INFO("feedback pooled realization size range " << inspection.minimum_leaf_size
                                                     << ".." << inspection.maximum_leaf_size);
    REQUIRE(inspection.representation ==
            hgraph::GraphValueRepresentation::PooledUnion);

    hgraph::GlobalState graph_state;
    hgraph::set_pooled_compound_scalar_storage(graph_state.view());
    hgraph::GlobalContext graph_context{graph_state};
    hgraph::TypeRealizationScope realization_scope{realization.get()};
    const hgraph::Value heartbeat = heartbeat_event_value(*realization, schemas);
    const hgraph::Value created = create_event_value(*realization, schemas);

    const auto delayed = hgraph::testing::eval_node<PolymorphicFeedbackGraph>(
        hgraph::testing::values<hgraph::Value>(heartbeat, created));
    REQUIRE(delayed.size() == 3);
    CHECK_FALSE(delayed.front().has_value());
    REQUIRE(delayed[1].has_value());
    check_polymorphic_event(
        *delayed[1], schemas.heartbeat_event, {{"event_id", "heartbeat"}});
    REQUIRE(delayed[2].has_value());
    check_polymorphic_event(
        *delayed[2], schemas.create_event,
        {{"event_id", "event"}, {"order_id", "order"},
         {"payload", "created"}, {"details_a", "a"},
         {"details_b", "b"}, {"details_c", "c"}, {"details_d", "d"}});

    const auto from_default =
        hgraph::testing::eval_node<PolymorphicDefaultFeedbackGraph>(
            hgraph::testing::values<hgraph::Value>(heartbeat, created), created);
    REQUIRE(from_default.size() == 3);
    REQUIRE(from_default[0].has_value());
    check_polymorphic_event(
        *from_default[0], schemas.create_event,
        {{"event_id", "event"}, {"order_id", "order"},
         {"payload", "created"}, {"details_a", "a"},
         {"details_b", "b"}, {"details_c", "c"}, {"details_d", "d"}});
    REQUIRE(from_default[1].has_value());
    check_polymorphic_event(
        *from_default[1], schemas.heartbeat_event, {{"event_id", "heartbeat"}});
    REQUIRE(from_default[2].has_value());
    check_polymorphic_event(
        *from_default[2], schemas.create_event,
        {{"event_id", "event"}, {"order_id", "order"},
         {"payload", "created"}, {"details_a", "a"},
         {"details_b", "b"}, {"details_c", "c"}, {"details_d", "d"}});
}

TEST_CASE("stdlib feedback accepts an immutable compact initial value")
{
    CHECK_OUTPUT(hgraph::testing::eval_node<ImmutableDefaultFeedbackGraph>(),
                 hgraph::testing::values<hgraph::Value>(
                     immutable_int_tuple()));
}

TEST_CASE("stdlib feedback supports canonical Fibonacci-style wiring")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(hgraph::testing::eval_node<FeedbackFibonacciGraph>(),
                 hgraph::testing::values<hgraph::Int>(1, 2, 3, 5, 8));
}

namespace
{
    struct PassiveFeedbackAccumulator
    {
        [[maybe_unused]] static constexpr auto name = "passive_feedback_accumulator";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            auto fb    = stdlib::feedback<TS<Int>>(w, Int{0});
            auto total = wire<stdlib::add_>(w, ts, passive(fb())).as<TS<Int>>();
            fb(total);
            return total;
        }
    };
}  // namespace

TEST_CASE("feedback: passive consumption lets the loop quiesce naturally")
{
    stdlib::register_standard_operators();
    // No end-time bound: the adder only fires on live ticks because the
    // feedback read is passive, so the simulation ends when the inputs do.
    CHECK_OUTPUT(hgraph::testing::eval_node<PassiveFeedbackAccumulator>(
                     hgraph::testing::values<Int>(1, 2, 3)),
                 hgraph::testing::values<Int>(1, 3, 6));
}
