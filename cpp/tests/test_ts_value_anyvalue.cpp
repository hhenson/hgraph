#include <catch2/catch_test_macros.hpp>
#include "hgraph/types/v2/ts_value.h"
#include "hgraph/util/date_time.h"
#include "hgraph/types/ts_traits.h"

using namespace hgraph;

// Mock parent node for testing
struct MockParentNode : Notifiable, CurrentTimeProvider
{
    engine_time_t _current_time{min_start_time()};

    void notify(engine_time_t et) override
    {
    }

    [[nodiscard]] engine_time_t current_engine_time() const override
    {
        return _current_time;
    }

    void advance_time()
    {
        _current_time = _current_time + std::chrono::microseconds(1);
    }
};

TEST_CASE (
"TimeSeriesValueOutput with AnyValue"
,
"[ts_value][output][anyvalue]"
)
 {
    SECTION("Basic construction") {
        MockParentNode parent;
        TimeSeriesValueOutput output(&parent);
        REQUIRE_FALSE(output.valid());
    }

    SECTION("Set and get int value") {
        MockParentNode parent;
        TimeSeriesValueOutput output(&parent);

        parent.advance_time();
        AnyValue<> val;
        val.emplace<int>(42);
        output.set_value(val);

        REQUIRE(*output.value().get_if<int>() == 42);
        REQUIRE(output.valid());
    }

    SECTION("Set and get string value") {
        MockParentNode parent;
        TimeSeriesValueOutput output(&parent);

        parent.advance_time();
        AnyValue<> val;
        val.emplace<std::string>("test");
        output.set_value(val);

        REQUIRE(*output.value().get_if<std::string>() == "test");
        REQUIRE(output.valid());
    }

    SECTION("Multiple set operations") {
        MockParentNode parent;
        TimeSeriesValueOutput output(&parent);

        parent.advance_time();
        AnyValue<> val1;
        val1.emplace<int>(10);
        output.set_value(val1);

        parent.advance_time();
        AnyValue<> val2;
        val2.emplace<int>(20);
        output.set_value(val2);

        REQUIRE(*output.value().get_if<int>() == 20);
    }

    SECTION("Invalidate") {
        MockParentNode parent;
        TimeSeriesValueOutput output(&parent);

        parent.advance_time();
        AnyValue<> val;
        val.emplace<int>(42);
        output.set_value(val);
        REQUIRE(output.valid());

        parent.advance_time();
        output.invalidate();
        REQUIRE_FALSE(output.valid());
    }
}

TEST_CASE (
"TimeSeriesValueInput with AnyValue"
,
"[ts_value][input][anyvalue]"
)
 {
    SECTION("Non-bound input active state") {
        MockParentNode parent;
        TimeSeriesValueInput input(&parent);

        // Initially not active
        REQUIRE_FALSE(input.active());

        // Mark active
        input.mark_active();
        REQUIRE(input.active());

        // Mark passive
        input.mark_passive();
        REQUIRE_FALSE(input.active());
    }

    SECTION("Bind and read value") {
        MockParentNode parent;
        TimeSeriesValueOutput output(&parent);
        TimeSeriesValueInput input(&parent);

        parent.advance_time();
        AnyValue<> val;
        val.emplace<int>(42);
        output.set_value(val);

        input.bind_output(&output);

        REQUIRE(*input.value().get_if<int>() == 42);
        REQUIRE(input.valid());
    }

    SECTION("Multiple inputs share output") {
        MockParentNode parent;
        TimeSeriesValueOutput output(&parent);
        TimeSeriesValueInput input1(&parent);
        TimeSeriesValueInput input2(&parent);

        input1.bind_output(&output);
        input2.bind_output(&output);

        parent.advance_time();
        AnyValue<> val;
        val.emplace<int>(100);
        output.set_value(val);

        REQUIRE(*input1.value().get_if<int>() == 100);
        REQUIRE(*input2.value().get_if<int>() == 100);
        REQUIRE(input1.valid());
        REQUIRE(input2.valid());
    }

    SECTION("Input sees output changes") {
        MockParentNode parent;
        TimeSeriesValueOutput output(&parent);
        TimeSeriesValueInput input(&parent);

        input.bind_output(&output);

        parent.advance_time();
        AnyValue<> val1;
        val1.emplace<int>(10);
        output.set_value(val1);
        REQUIRE(*input.value().get_if<int>() == 10);

        parent.advance_time();
        AnyValue<> val2;
        val2.emplace<int>(20);
        output.set_value(val2);
        REQUIRE(*input.value().get_if<int>() == 20);
    }

    SECTION("Zero-copy sharing") {
        MockParentNode parent;
        TimeSeriesValueOutput output(&parent);
        TimeSeriesValueInput input(&parent);

        parent.advance_time();
        AnyValue<> val;
        val.emplace<std::string>("shared");
        output.set_value(val);

        input.bind_output(&output);

        // Both should reference the same AnyValue
        REQUIRE(&output.value() == &input.value());
    }

    SECTION("Active state preserved across bind_output") {
        MockParentNode parent;
        TimeSeriesValueOutput output1(&parent);
        TimeSeriesValueOutput output2(&parent);
        TimeSeriesValueInput input(&parent);

        // Initially bind to output1
        input.bind_output(&output1);
        REQUIRE_FALSE(input.active());

        // Mark active while bound to output1
        input.mark_active();
        REQUIRE(input.active());

        // Rebind to output2 - should preserve active state
        input.bind_output(&output2);
        REQUIRE(input.active());

        // Should no longer be subscribed to output1
        REQUIRE_FALSE(output1.get_impl()->active(reinterpret_cast<Notifiable*>(&input)));

        // Should now be subscribed to output2
        REQUIRE(output2.get_impl()->active(reinterpret_cast<Notifiable*>(&input)));

        // Mark passive on new binding
        input.mark_passive();
        REQUIRE_FALSE(input.active());
    }
}