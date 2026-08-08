// Unit tests for the NodeScheduler value/view split.
//
// NodeSchedulerState is the persistent per-node footprint; NodeScheduler is the
// on-demand borrowing view that carries the behaviour. These tests drive the
// view directly against a standalone state (with a null graph, so scheduling
// only mutates the state and does not try to re-arm a graph) to pin the full
// 2603-aligned interface: schedule (absolute / delta / tagged / immediate),
// query (is_scheduled / next_scheduled_time / has_tag / tag_time), cancel
// (pop_tag / un_schedule / reset), the post-eval advance, and the throw paths
// (missing state, wall-clock alarms without realtime support).

#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/util/date_time.h>

#include <catch2/catch_test_macros.hpp>

#include <stdexcept>

namespace
{
    using namespace hgraph;

    const DateTime           base = MIN_ST;  // MIN_ST is runtime-initialised
    constexpr TimeDelta one  = TimeDelta{1};
}  // namespace

TEST_CASE("node scheduler: a default (state-less) view is empty and refuses mutation")
{
    NodeScheduler sched;  // no state, no graph

    CHECK_FALSE(sched.is_scheduled());
    CHECK_FALSE(sched.is_scheduled_now());
    CHECK(sched.next_scheduled_time() == MIN_DT);
    CHECK_FALSE(sched.has_tag("x"));
    CHECK(sched.tag_time("x", base) == base);

    // Mutating operations require live state and must throw rather than no-op.
    CHECK_THROWS_AS(sched.schedule(base + one), std::logic_error);
    CHECK_THROWS_AS(sched.pop_tag("x"), std::logic_error);
    CHECK_THROWS_AS(sched.un_schedule(), std::logic_error);
    CHECK_THROWS_AS(sched.un_schedule("x"), std::logic_error);
    CHECK_THROWS_AS(sched.reset(), std::logic_error);
}

TEST_CASE("node scheduler: absolute and delta schedules track the earliest pending time")
{
    NodeSchedulerState state;
    NodeScheduler      sched{state, nullptr, 0, base};

    sched.schedule(base + TimeDelta{5});
    CHECK(sched.is_scheduled());
    CHECK(sched.next_scheduled_time() == base + TimeDelta{5});
    CHECK_FALSE(sched.is_scheduled_now());  // 5 ticks in the future

    // A delta is measured from the current evaluation time; the earlier event wins.
    sched.schedule(TimeDelta{3});
    CHECK(sched.next_scheduled_time() == base + TimeDelta{3});

    // Scheduling in the past / current cycle is ignored.
    sched.schedule(base);
    sched.schedule(base - one);
    CHECK(sched.next_scheduled_time() == base + TimeDelta{3});
}

TEST_CASE("node scheduler: a tag replaces its prior event and can be queried and popped")
{
    NodeSchedulerState state;
    NodeScheduler      sched{state, nullptr, 0, base};

    sched.schedule(base + TimeDelta{10}, "alarm");
    CHECK(sched.has_tag("alarm"));
    CHECK(sched.tag_time("alarm") == base + TimeDelta{10});

    // Re-scheduling the same tag moves it (does not add a second event).
    sched.schedule(base + TimeDelta{20}, "alarm");
    CHECK(sched.tag_time("alarm") == base + TimeDelta{20});
    CHECK(sched.next_scheduled_time() == base + TimeDelta{20});

    // pop_tag returns the time and removes both the event and the tag.
    CHECK(sched.pop_tag("alarm") == base + TimeDelta{20});
    CHECK_FALSE(sched.has_tag("alarm"));
    CHECK_FALSE(sched.is_scheduled());

    // popping an unknown tag returns the supplied default.
    CHECK(sched.pop_tag("missing", base + one) == base + one);
}

TEST_CASE("node scheduler: invalid tagged replacement leaves the previous event intact")
{
    NodeSchedulerState state;
    NodeScheduler      sched{state, nullptr, 0, base};

    sched.schedule(base + TimeDelta{10}, "alarm");

    sched.schedule(base, "alarm");
    CHECK(sched.has_tag("alarm"));
    CHECK(sched.tag_time("alarm") == base + TimeDelta{10});
    CHECK(sched.next_scheduled_time() == base + TimeDelta{10});

    sched.schedule(base - one, "alarm");
    CHECK(sched.has_tag("alarm"));
    CHECK(sched.tag_time("alarm") == base + TimeDelta{10});
    CHECK(sched.pop_tag("alarm") == base + TimeDelta{10});
}

TEST_CASE("node scheduler: start-time view can schedule the current cycle but not earlier")
{
    NodeSchedulerState state;
    NodeScheduler      sched{state, nullptr, 0, base, /*started=*/false};

    sched.schedule(base, "source");
    CHECK(sched.has_tag("source"));
    CHECK(sched.tag_time("source") == base);
    CHECK(sched.next_scheduled_time() == base);
    CHECK(sched.tag_is_scheduled_now("source"));

    sched.schedule(base - one, "source");
    CHECK(sched.has_tag("source"));
    CHECK(sched.tag_time("source") == base);
}

TEST_CASE("node scheduler: is_scheduled_now is true only when the earliest event is exactly now")
{
    NodeSchedulerState state;
    {
        NodeScheduler sched{state, nullptr, 0, base};
        sched.schedule(base + TimeDelta{3}, "tick");
    }

    // A view before the event: pending but not due this cycle.
    NodeScheduler before{state, nullptr, 0, base};
    CHECK(before.is_scheduled());
    CHECK_FALSE(before.is_scheduled_now());
    CHECK_FALSE(before.tag_is_scheduled_now("tick"));

    // A view positioned exactly at the event time (as the engine would be when it
    // fires the node): scheduled for this cycle. Matches Python's `== now`.
    NodeScheduler at_event{state, nullptr, 0, base + TimeDelta{3}};
    CHECK(at_event.is_scheduled_now());
    CHECK(at_event.tag_is_scheduled_now("tick"));
}

TEST_CASE("node scheduler: un_schedule cancels a tag or the earliest event; reset clears all")
{
    NodeSchedulerState state;
    NodeScheduler      sched{state, nullptr, 0, base};

    sched.schedule(base + TimeDelta{5}, "a");
    sched.schedule(base + TimeDelta{8}, "b");

    sched.un_schedule("a");  // cancel by tag
    CHECK_FALSE(sched.has_tag("a"));
    CHECK(sched.next_scheduled_time() == base + TimeDelta{8});

    sched.schedule(base + TimeDelta{3}, "c");
    sched.un_schedule();  // cancel the earliest (c at +3)
    CHECK_FALSE(sched.has_tag("c"));
    CHECK(sched.next_scheduled_time() == base + TimeDelta{8});

    sched.reset();
    CHECK_FALSE(sched.is_scheduled());
    CHECK_FALSE(sched.has_tag("b"));
}

TEST_CASE("node scheduler: advance consumes fired events up to the current time")
{
    NodeSchedulerState state;
    {
        NodeScheduler sched{state, nullptr, 0, base};
        sched.schedule(base + TimeDelta{2}, "early");
        sched.schedule(base + TimeDelta{5}, "late");
    }

    // A later view (now = base+2) consumes the fired "early" event and leaves "late".
    NodeScheduler at_two{state, nullptr, 0, base + TimeDelta{2}};
    at_two.advance();
    CHECK_FALSE(at_two.has_tag("early"));
    CHECK(at_two.has_tag("late"));
    CHECK(at_two.next_scheduled_time() == base + TimeDelta{5});

    // A view past all events consumes everything.
    NodeScheduler at_ten{state, nullptr, 0, base + TimeDelta{10}};
    at_ten.advance();
    CHECK_FALSE(at_ten.is_scheduled());
}

TEST_CASE("node scheduler: multiple untagged events accumulate and advance in time order")
{
    NodeSchedulerState state;
    {
        NodeScheduler sched{state, nullptr, 0, base};
        sched.schedule(base + TimeDelta{4});  // untagged
        sched.schedule(base + TimeDelta{2});  // untagged, earlier
        // Untagged schedules accumulate (they are not indexed by tag, so they do
        // not overwrite each other); the earliest wins.
        CHECK(sched.next_scheduled_time() == base + TimeDelta{2});
    }

    // Consume the first untagged event; the later one survives intact.
    NodeScheduler at_two{state, nullptr, 0, base + TimeDelta{2}};
    at_two.advance();
    CHECK(at_two.is_scheduled());
    CHECK(at_two.next_scheduled_time() == base + TimeDelta{4});

    // A tagged event coexisting with untagged ones is still queryable by tag.
    at_two.schedule(base + TimeDelta{6}, "named");
    CHECK(at_two.has_tag("named"));
    CHECK(at_two.tag_time("named") == base + TimeDelta{6});
    NodeScheduler at_five{state, nullptr, 0, base + TimeDelta{5}};
    at_five.advance();  // consumes the +4 untagged, leaves the +6 tag
    CHECK(at_five.has_tag("named"));
    CHECK(at_five.next_scheduled_time() == base + TimeDelta{6});
}

TEST_CASE("node scheduler: wall-clock alarms require realtime support")
{
    NodeSchedulerState state;
    NodeScheduler      sched{state, nullptr, 0, base};

    CHECK_THROWS_AS(sched.schedule(base + one, "wc", /*on_wall_clock=*/true), std::logic_error);
    CHECK_THROWS_AS(sched.schedule(one, "wc", /*on_wall_clock=*/true), std::logic_error);

    NodeScheduler realtime{state,
                           nullptr,
                           0,
                           base,
                           /*started=*/true,
                           EvaluationClockView{},
                           /*supports_wall_clock=*/true};

    realtime.schedule(base + one, "wc", /*on_wall_clock=*/true);
    CHECK(realtime.has_tag("wc"));
    CHECK(realtime.tag_time("wc") == base + one);

    realtime.schedule(one * 2, "wc", /*on_wall_clock=*/true);
    CHECK(realtime.tag_time("wc") == base + one * 2);
}
