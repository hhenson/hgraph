#include <hgl/capture.h>
#include <hgl/execution_error.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/subgraph_wiring.h>
#include <catch2/catch_test_macros.hpp>

namespace {
using namespace hgraph;
using namespace hgraph::testing;

struct Required {
    static NodeResult<void> eval(In<"value", TS<Int>> value, Out<TS<Int>> out) {
        if (value.value() < 0) { return std::unexpected{CanonicalError{"value.missing_key", "missing price", {}}}; }
        out.set(value.value());
        return {};
    }
};
struct Message {
    static void eval(In<"error", TS<CanonicalError>> error, Out<TS<Str>> out) { out.set(error.value().message); }
};
struct CapturedValue {
    static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value) {
        return hgl::capture_node(wire<Required>(w, value)).value;
    }
};
struct CapturedMessage {
    static Port<TS<Str>> compose(Wiring &w, Port<TS<Int>> value) {
        return wire<Message>(w, hgl::capture_node(wire<Required>(w, value)).error);
    }
};
struct KeyedValues {
    static Port<TSD<Int, TS<Int>>> compose(Wiring &w, Port<TSD<Int, TS<Int>>> values) {
        return wire<stdlib::map_>(w, fn<CapturedValue>(), values).as<TSD<Int, TS<Int>>>();
    }
};
struct KeyedErrors {
    static Port<TSD<Int, TS<Str>>> compose(Wiring &w, Port<TSD<Int, TS<Int>>> values) {
        return wire<stdlib::map_>(w, fn<CapturedMessage>(), values).as<TSD<Int, TS<Str>>>();
    }
};
struct Future {
    static void start(NodeScheduler scheduler) {
        scheduler.schedule(MIN_ST, "current");
        scheduler.schedule(MIN_ST + MIN_TD * 2, "future");
    }
    static void eval(In<"required", TS<Int>, InputValidity::Unchecked>, Out<TS<Int>> out) { out.set(Int{9}); }
};
struct ScheduledGraph {
    static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value) {
        return wire<Future>(w, wire<Required>(w, value));
    }
};
struct CapturedSchedule {
    static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value) {
        return hgl::capture_graph<TS<Int>, ScheduledGraph>(w, value).value;
    }
};
struct StopFailure {
    static void eval(In<"value", TS<Int>> value, Out<TS<Int>> out) { out.set(value.value()); }
    static void stop() { throw ExecutionFailure{CanonicalError{"value.missing_key", "stop failure", {}}}; }
};
struct CapturedStop {
    static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value) {
        return hgl::capture_node(wire<StopFailure>(w, value)).value;
    }
};
struct CapturedErrors {
    static Port<TS<CanonicalError>> compose(Wiring &w, Port<TS<Int>> value) {
        return hgl::capture_node(wire<Required>(w, value)).error;
    }
};

struct HistoricalFailure {
    static NodeResult<void> eval(In<"value", TS<Int>>, Out<TS<Int>>) {
        return std::unexpected{CanonicalError{"value.missing_key", "historical diagnostic", ErrorContext{"original", "start", MIN_ST}}};
    }
};
struct CapturedHistorical {
    static Port<TS<CanonicalError>> compose(Wiring &w, Port<TS<Int>> value) {
        return hgl::capture_node(wire<HistoricalFailure>(w, value)).error;
    }
};
struct StartFailure {
    static void start() { throw ExecutionFailure{CanonicalError{"value.missing_key", "start failure", {}}}; }
    static void eval(In<"value", TS<Int>> value, Out<TS<Int>> out) { out.set(value.value()); }
};
template <typename Node> struct LifecycleChild {
    static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> value) { return wire<Node>(w, value); }
};
template <typename Node> struct MappedLifecycle {
    static Port<TSD<Int, TS<Int>>> compose(Wiring &w, Port<TSD<Int, TS<Int>>> value) {
        return wire<stdlib::map_>(w, fn<LifecycleChild<Node>>(), value).template as<TSD<Int, TS<Int>>>();
    }
};
template <typename Node> struct CapturedLifecycle {
    static Port<TSD<Int, TS<Int>>> compose(Wiring &w, Port<TSD<Int, TS<Int>>> value) {
        return hgl::capture_graph<TSD<Int, TS<Int>>, MappedLifecycle<Node>>(w, value).value;
    }
};

}

TEST_CASE("canonical capture preserves keyed siblings and removes retained child errors", "[capture][keyed]") {
    stdlib::register_standard_operators();
    const auto input = values<Value>(dict_delta<Int, TS<Int>>({{0, -1}, {1, 7}}),
                                     dict_delta<Int, TS<Int>>({{0, 8}, {1, 9}}),
                                     dict_delta<Int, TS<Int>>({}, {0}),
                                     dict_delta<Int, TS<Int>>({{0, -1}}));
    CHECK_OUTPUT(eval_node<KeyedValues>(input),
                 values<Value>(dict_delta<Int, TS<Int>>({{1, 7}}),
                               dict_delta<Int, TS<Int>>({{0, 8}, {1, 9}}),
                               dict_delta<Int, TS<Int>>({}, {0}),
                               dict_delta<Int, TS<Int>>({})));
    CHECK_OUTPUT(eval_node<KeyedErrors>(input),
                 values<Value>(dict_delta<Int, TS<Str>>({{0, "missing price"}}), none,
                               dict_delta<Int, TS<Str>>({}, {0}),
                               dict_delta<Int, TS<Str>>({{0, "missing price"}})));
}

TEST_CASE("captured graph aborts the current pass and preserves a later timer", "[capture][scheduler]") {
    CHECK_OUTPUT(eval_node<CapturedSchedule>(values<Int>(-1, none, none)), values<Int>(none, none, 9));
}

TEST_CASE("capture cannot consume stop failure", "[capture][lifecycle]") {
    bool escaped = false;
    try { (void)eval_node<CapturedStop>(values<Int>(7)); }
    catch (const std::exception &failure) {
        escaped = true;
        CHECK(hgl::execution_error_code(failure) == "value.missing_key");
    }
    CHECK(escaped);
}

TEST_CASE("equal captured diagnostics still tick and own distinct origins", "[capture][error]") {
    const auto errors = eval_node<CapturedErrors>(values<Int>(-1, -1, 7));
    REQUIRE(errors[0]);
    REQUIRE(errors[1]);
    CHECK(*errors[0] == *errors[1]);
    REQUIRE(errors[0]->context);
    REQUIRE(errors[1]->context);
    CHECK(errors[0]->context->evaluation_time != errors[1]->context->evaluation_time);
    const auto repeated = eval_node<CapturedErrors>(values<Int>(-1));
    REQUIRE(repeated[0]);
    CHECK(*errors[0] == *repeated[0]);
    CHECK(errors[0]->context->node != repeated[0]->context->node);
}

TEST_CASE("dynamic child lifecycle failure escapes evaluation capture", "[capture][keyed][lifecycle]") {
    stdlib::register_standard_operators();
    for (const bool starting : {true, false}) {
        bool escaped = false;
        try {
            const auto input = values<Value>(dict_delta<Int, TS<Int>>({{0, 7}}), dict_delta<Int, TS<Int>>({}, {0}));
            if (starting) { (void)eval_node<CapturedLifecycle<StartFailure>>(input); }
            else { (void)eval_node<CapturedLifecycle<StopFailure>>(input); }
        } catch (const std::exception &failure) {
            escaped = true;
            CHECK(hgl::execution_error_code(failure) == "value.missing_key");
            const auto original = canonicalize_failure({}, MIN_ST, "evaluate", failure);
            REQUIRE(original.context);
            CHECK(original.context->phase == (starting ? "start" : "stop"));
        }
        CHECK(escaped);
    }
}

TEST_CASE("new evaluation failure can retain older lifecycle provenance", "[capture][lifecycle]") {
    const auto errors = eval_node<CapturedHistorical>(values<Int>(1, 2));
    REQUIRE(errors[0]);
    REQUIRE(errors[1]);
    CHECK(errors[0]->context->phase == "start");
    CHECK(errors[1]->context->node == "original");
    CHECK(*errors[0] == *errors[1]);
}
