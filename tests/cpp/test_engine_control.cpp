// C++-first engine control: the node injectable is a borrowed projection over
// the native executor, and stop_engine is a native sink wired through the
// ordinary operator registry.

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_test_macros.hpp>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    struct EngineControlProbe
    {
        static constexpr auto name              = "engine_control_probe";
        static constexpr bool schedule_on_start = true;

        static void eval(EngineControlView engine, Out<TS<Bool>> out)
        {
            const auto clock = engine.evaluation_clock();
            out.set(engine.valid() && engine.mode() == GraphExecutorMode::Simulation &&
                    engine.start_time() == MIN_ST && engine.end_time() == MAX_ET &&
                    !engine.stop_requested() && clock.valid() && clock.evaluation_time() == MIN_ST);
        }
    };

    struct StopAfterFirstGraph
    {
        static constexpr auto name = "stop_after_first_graph";

        static Port<SIGNAL> compose(Wiring &w, Port<SIGNAL> signal)
        {
            wire<hgraph::stdlib::stop_engine>(w, signal);
            return signal;
        }
    };
}  // namespace

TEST_CASE("engine control: static nodes receive the native executor projection")
{
    CHECK_OUTPUT(eval_node<EngineControlProbe>(), {true});

    NodeBuilder builder;
    builder.implementation<EngineControlProbe>();
    CHECK(builder.type().checked_plan().find_component("engine_control") == nullptr);
}

TEST_CASE("stop_engine: the current cycle completes and later cycles do not run")
{
    hgraph::stdlib::register_standard_operators();

    CHECK_OUTPUT(eval_node<StopAfterFirstGraph>(values<bool>(true, true, true)),
                 {true, none, none});
}

namespace
{
    // One-shot cycle-boundary notifications (2026-08-01): the C++-primary
    // facility behind python's add_*_evaluation_notification.
    std::vector<std::string> &notification_log()
    {
        static std::vector<std::string> log;
        return log;
    }

    struct NotificationProbe
    {
        static constexpr auto name = "notification_probe";

        static void eval(In<"trigger", TS<Int>> trigger, EngineControlView engine,
                         Out<TS<Int>> out)
        {
            const auto tick = trigger.value();
            notification_log().push_back("eval-" + std::to_string(tick));
            engine.add_after_evaluation_notification(
                [tick] { notification_log().push_back("after-" + std::to_string(tick)); });
            engine.add_before_evaluation_notification(
                [tick] { notification_log().push_back("before-next-" + std::to_string(tick)); });
            out.set(tick);
        }
    };

    struct ReentrantNotificationProbe
    {
        static constexpr auto name = "reentrant_notification_probe";

        static void eval(In<"trigger", TS<Int>> trigger, EngineControlView engine,
                         Out<TS<Int>> out)
        {
            notification_log().push_back("eval-order");
            engine.add_after_evaluation_notification([engine] {
                notification_log().push_back("after-first");
                engine.add_after_evaluation_notification(
                    [] { notification_log().push_back("nested-first"); });
            });
            engine.add_after_evaluation_notification([engine] {
                notification_log().push_back("after-second");
                engine.add_after_evaluation_notification(
                    [] { notification_log().push_back("nested-second"); });
            });
            out.set(trigger.value());
        }
    };

    struct ReentrantBeforeNotificationProbe
    {
        static constexpr auto name = "reentrant_before_notification_probe";

        static void eval(In<"trigger", TS<Int>> trigger, EngineControlView engine,
                         Out<TS<Int>> out)
        {
            const Int tick = trigger.value();
            notification_log().push_back("eval-before-" + std::to_string(tick));
            if (tick == 1)
            {
                engine.add_before_evaluation_notification([engine] {
                    notification_log().push_back("before-first");
                    engine.add_before_evaluation_notification(
                        [] { notification_log().push_back("nested-before-first"); });
                });
                engine.add_before_evaluation_notification([engine] {
                    notification_log().push_back("before-second");
                    engine.add_before_evaluation_notification(
                        [] { notification_log().push_back("nested-before-second"); });
                });
            }
            out.set(tick);
        }
    };

    struct StopNotificationProbe
    {
        static constexpr auto name              = "stop_notification_probe";
        static constexpr bool schedule_on_start = true;

        static void eval(Out<TS<Bool>> out) { out.set(true); }

        static void stop(EngineControlView engine)
        {
            engine.add_after_evaluation_notification(
                [] { notification_log().push_back("after-stop"); });
            engine.add_before_evaluation_notification(
                [] { notification_log().push_back("before-stop"); });
        }
    };

    struct FailingNotificationProbe
    {
        static constexpr auto name              = "failing_notification_probe";
        static constexpr bool schedule_on_start = true;

        static void eval(EngineControlView engine, Out<TS<Bool>>)
        {
            engine.add_after_evaluation_notification(
                [] { notification_log().push_back("after-error"); });
            throw std::runtime_error("notification probe failure");
        }
    };
}  // namespace

TEST_CASE("engine control: one-shot evaluation notifications fire at cycle boundaries")
{
    notification_log().clear();

    CHECK_OUTPUT(eval_node<NotificationProbe>(values<Int>(1, 2)), values<Int>(1, 2));

    // Clean shutdown drains the final before queue as well: this is the
    // historical final-tick deferred-release guarantee.
    const std::vector<std::string> expected{
        "eval-1", "after-1", "before-next-1", "eval-2", "after-2", "before-next-2"};
    CHECK(notification_log() == expected);
}

TEST_CASE("engine control: after notifications are LIFO and drain re-entrant work")
{
    notification_log().clear();

    CHECK_OUTPUT(eval_node<ReentrantNotificationProbe>(values<Int>(1)), values<Int>(1));

    const std::vector<std::string> expected{
        "eval-order", "after-second", "after-first", "nested-first", "nested-second"};
    CHECK(notification_log() == expected);
}

TEST_CASE("engine control: before notifications are FIFO and drain re-entrant work")
{
    notification_log().clear();

    CHECK_OUTPUT(eval_node<ReentrantBeforeNotificationProbe>(values<Int>(1, 2)),
                 values<Int>(1, 2));

    const std::vector<std::string> expected{
        "eval-before-1", "before-first", "before-second", "nested-before-first",
        "nested-before-second", "eval-before-2"};
    CHECK(notification_log() == expected);
}

TEST_CASE("engine control: stop-generated notifications drain before executor teardown")
{
    notification_log().clear();

    CHECK_OUTPUT(eval_node<StopNotificationProbe>(), {true});

    const std::vector<std::string> expected{"after-stop", "before-stop"};
    CHECK(notification_log() == expected);
}

TEST_CASE("engine control: a failed evaluation still drains its after notifications")
{
    notification_log().clear();

    CHECK_THROWS(eval_node<FailingNotificationProbe>());

    const std::vector<std::string> expected{"after-error"};
    CHECK(notification_log() == expected);
}
