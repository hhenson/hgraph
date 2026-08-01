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
}  // namespace

TEST_CASE("engine control: one-shot evaluation notifications fire at cycle boundaries")
{
    notification_log().clear();

    CHECK_OUTPUT(eval_node<NotificationProbe>(values<Int>(1, 2)), values<Int>(1, 2));

    // after-N at the end of N's cycle; before-next-N before the following
    // cycle; each exactly once (a re-queue would duplicate entries).
    const std::vector<std::string> expected{
        "eval-1", "after-1", "before-next-1", "eval-2", "after-2"};
    CHECK(std::vector<std::string>(notification_log().begin(),
                                   notification_log().begin() +
                                       std::min<std::size_t>(5, notification_log().size())) ==
          expected);
}
