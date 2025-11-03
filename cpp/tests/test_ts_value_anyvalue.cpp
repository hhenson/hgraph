#include <catch2/catch_test_macros.hpp>
#include "hgraph/types/v2/ts_value.h"
#include "hgraph/util/date_time.h"
#include "hgraph/types/ts_traits.h"

using namespace hgraph;

// Mock parent node for testing
struct MockParentNode : Notifiable, CurrentTimeProvider
{
    engine_time_t _current_time{min_start_time()};

    void notify(engine_time_t et) override {}

    [[nodiscard]] engine_time_t current_engine_time() const override { return _current_time; }

    void advance_time() { _current_time = _current_time + std::chrono::microseconds(1); }
};

TEST_CASE(
    "TSOutput with AnyValue"
    ,
    "[ts_value][output][anyvalue]"
    ) {
    SECTION("Basic construction") {
        MockParentNode parent;
        TSOutput       output(static_cast<Notifiable *>(&parent), typeid(int));
        REQUIRE_FALSE(output.valid());
    }

    SECTION("Set and get int value") {
        MockParentNode parent;
        TSOutput       output(static_cast<Notifiable *>(&parent), typeid(int));

        parent.advance_time();
        AnyValue<> val;
        val.emplace<int>(42);
        output.set_value(val);

        REQUIRE(*output.value().get_if<int>() == 42);
        REQUIRE(output.valid());
    }

    SECTION("Set and get string value") {
        MockParentNode parent;
        TSOutput       output(static_cast<Notifiable *>(&parent), typeid(std::string));

        parent.advance_time();
        AnyValue<> val;
        val.emplace<std::string>("test");
        output.set_value(val);

        REQUIRE(*output.value().get_if<std::string>() == "test");
        REQUIRE(output.valid());
    }

    SECTION("Multiple set operations") {
        MockParentNode parent;
        TSOutput       output(static_cast<Notifiable *>(&parent), typeid(int));

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
        TSOutput       output(static_cast<Notifiable *>(&parent), typeid(int));

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

TEST_CASE(
    "TSInput with AnyValue"
    ,
    "[ts_value][input][anyvalue]"
    ) {
    SECTION("Non-bound input active state") {
        MockParentNode parent;
        TSInput        input(static_cast<Notifiable *>(&parent), typeid(int));

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
        TSOutput       output(static_cast<Notifiable *>(&parent), typeid(int));
        TSInput        input(static_cast<Notifiable *>(&parent), typeid(int));

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
        TSOutput       output(static_cast<Notifiable *>(&parent), typeid(int));
        TSInput        input1(static_cast<Notifiable *>(&parent), typeid(int));
        TSInput        input2(static_cast<Notifiable *>(&parent), typeid(int));

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
        TSOutput       output(static_cast<Notifiable *>(&parent), typeid(int));
        TSInput        input(static_cast<Notifiable *>(&parent), typeid(int));

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
        TSOutput       output(static_cast<Notifiable *>(&parent), typeid(std::string));
        TSInput        input(static_cast<Notifiable *>(&parent), typeid(std::string));

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
        TSOutput       output1(static_cast<Notifiable *>(&parent), typeid(int));
        TSOutput       output2(static_cast<Notifiable *>(&parent), typeid(int));
        TSInput        input(static_cast<Notifiable *>(&parent), typeid(int));

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

    SECTION("Type mismatch on bind throws") {
        MockParentNode parent;
        TSOutput       output(static_cast<Notifiable *>(&parent), typeid(int));
        TSInput        input(static_cast<Notifiable *>(&parent), typeid(std::string));

        // Attempting to bind input expecting string to output providing int should throw
        REQUIRE_THROWS_AS(input.bind_output(&output), std::runtime_error);
    }

    SECTION("Type mismatch on set_value throws") {
        MockParentNode parent;
        TSOutput       output(static_cast<Notifiable *>(&parent), typeid(int));

        parent.advance_time();
        AnyValue<> val;
        val.emplace<std::string>("wrong type");

        // Attempting to set wrong type should throw
        REQUIRE_THROWS_AS(output.set_value(val), std::runtime_error);
    }
}
