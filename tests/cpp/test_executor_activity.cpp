// Lower-level ownership and scheduling tests for the executor activity contract.
#include <hgraph/runtime/executor.h>
#include <hgraph/runtime/graph.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <atomic>
#include <future>
#include <thread>
#include <vector>

namespace
{
    using namespace hgraph;
    using Catch::Matchers::ContainsSubstring;

    struct ActivityProbe
    {
        std::atomic<DateTime::duration::rep> requested{MAX_DT.time_since_epoch().count()};
        std::vector<DateTime> completions{};
        std::vector<bool> waits{};
        std::function<DateTime(bool)> on_next{};
        std::function<void(DateTime)> on_completed{};

        void request(DateTime time) { requested.store(time.time_since_epoch().count()); }
        ExecutorActivity handle()
        {
            static const ExecutorActivityOps ops{
                [](void *context, bool wait) {
                    auto &probe = *static_cast<ActivityProbe *>(context);
                    probe.waits.push_back(wait);
                    return probe.on_next ? probe.on_next(wait)
                                         : DateTime{TimeDelta{probe.requested.load()}};
                },
                [](void *context, DateTime time) {
                    auto &probe = *static_cast<ActivityProbe *>(context);
                    probe.completions.push_back(time);
                    probe.request(MAX_DT);
                    if (probe.on_completed) { probe.on_completed(time); }
                },
            };
            return ExecutorActivity{this, ops};
        }
    };

    GraphExecutorValue make_executor(GraphExecutorMode mode = GraphExecutorMode::Simulation,
                                     DateTime start = MIN_ST, TimeDelta duration = TimeDelta{100})
    {
        GraphExecutorBuilder builder;
        builder.graph_builder(GraphBuilder{}).mode(mode).start_time(start).end_time(start + duration);
        return builder.make_executor();
    }
}

TEST_CASE("executor activities: default handles and closed notifications are inert", "[executor_activity]")
{
    ExecutorActivity activity;
    CHECK(activity.next_time(false) == MAX_DT);
    CHECK(activity.next_time(true) == MAX_DT);
    CHECK_NOTHROW(activity.completed(MIN_ST));
    CHECK_NOTHROW(ExecutorActivityWake{}.notify());

    ActivityProbe probe;
    ExecutorActivityWake retained;
    {
        auto executor = make_executor(GraphExecutorMode::RealTime);
        retained = executor.view().engine_control().attach_activity(probe.handle());
    }
    CHECK_NOTHROW(retained.notify());
    auto moved = std::move(retained);
    CHECK_NOTHROW(retained.notify());
    CHECK_NOTHROW(moved.notify());
}

TEST_CASE("executor activities: registration rejects ambiguity and unsupported hosts", "[executor_activity]")
{
    ActivityProbe probe;
    auto executor = make_executor();
    auto control = executor.view().engine_control();
    CHECK_THROWS_AS(control.attach_activity(ExecutorActivity{}), std::invalid_argument);
    auto wake = control.attach_activity(probe.handle());
    CHECK_THROWS_WITH(control.attach_activity(probe.handle()), ContainsSubstring("already registered"));
    control.detach_activity(probe.handle());
    CHECK_NOTHROW(wake.notify());
    CHECK_THROWS_WITH(control.detach_activity(probe.handle()), ContainsSubstring("not registered"));

    auto external = make_executor(GraphExecutorMode::ExternallyDriven);
    CHECK_THROWS_WITH(external.view().engine_control().attach_activity(probe.handle()),
                      ContainsSubstring("ExternallyDriven"));
    CHECK_THROWS_AS(EngineControlView{}.attach_activity(probe.handle()), std::logic_error);
    CHECK_THROWS_AS(EngineControlView{}.detach_activity(probe.handle()), std::logic_error);
    const ExecutorActivityOps incomplete{nullptr, nullptr};
    CHECK_THROWS_AS((ExecutorActivity{&probe, incomplete}), std::invalid_argument);
}

TEST_CASE("executor activities: requested clocks run without formal inputs", "[executor_activity]")
{
    ActivityProbe probe;
    auto executor = make_executor();
    auto control = executor.view().engine_control();
    auto wake = control.attach_activity(probe.handle());
    static_cast<void>(wake);
    probe.request(MIN_ST);
    probe.on_completed = [&](DateTime time) {
        if (time < MIN_ST + TimeDelta{20}) { probe.request(time + TimeDelta{10}); }
    };
    executor.view().run();
    CHECK(probe.completions == std::vector<DateTime>{MIN_ST, MIN_ST + TimeDelta{10}, MIN_ST + TimeDelta{20}});
    REQUIRE_FALSE(probe.waits.empty());
    CHECK(probe.waits.back()); // Quiescence checked before simulation terminates.
    control.detach_activity(probe.handle());
}

TEST_CASE("executor activities: settle discovers work before simulation terminates", "[executor_activity]")
{
    ActivityProbe probe;
    bool discovered = false;
    probe.on_next = [&](bool wait) {
        if (wait && !discovered)
        {
            discovered = true;
            return MIN_ST + TimeDelta{50};
        }
        return MAX_DT;
    };
    auto executor = make_executor();
    auto wake = executor.view().engine_control().attach_activity(probe.handle());
    static_cast<void>(wake);
    executor.view().run();
    CHECK(probe.completions == std::vector<DateTime>{MIN_ST + TimeDelta{50}});
}

TEST_CASE("executor activities: the end bound is exclusive", "[executor_activity]")
{
    ActivityProbe probe;
    probe.request(MIN_ST + TimeDelta{100});
    auto executor = make_executor();
    auto wake = executor.view().engine_control().attach_activity(probe.handle());
    static_cast<void>(wake);
    executor.view().run();
    CHECK(probe.completions.empty());
}

TEST_CASE("executor activities: completion follows after notifications and errors do not grant", "[executor_activity]")
{
    for (const bool fail : {false, true})
    {
        ActivityProbe probe;
        auto executor = make_executor();
        auto control = executor.view().engine_control();
        auto wake = control.attach_activity(probe.handle());
        static_cast<void>(wake);
        probe.request(MIN_ST);
        bool after_completed = false;
        control.add_after_evaluation_notification([&] {
            after_completed = true;
            if (fail) { throw std::runtime_error("after failed"); }
        });
        probe.on_completed = [&](DateTime) { CHECK(after_completed); };
        if (fail)
        {
            CHECK_THROWS_WITH(executor.view().run(), "after failed");
            CHECK(probe.completions.empty());
        }
        else
        {
            executor.view().run();
            CHECK(probe.completions == std::vector<DateTime>{MIN_ST});
        }
    }
}

TEST_CASE("executor activities: failed workers and stale requests fail the owning run", "[executor_activity]")
{
    SECTION("worker error")
    {
        ActivityProbe probe;
        probe.on_next = [](bool) -> DateTime { throw std::runtime_error("worker failed"); };
        auto executor = make_executor();
        auto wake = executor.view().engine_control().attach_activity(probe.handle());
        static_cast<void>(wake);
        CHECK_THROWS_WITH(executor.view().run(), "worker failed");
        CHECK_FALSE(executor.view().graph().started());
    }
    SECTION("stale clock")
    {
        ActivityProbe probe;
        probe.request(MIN_ST);
        probe.on_completed = [&](DateTime time) { probe.request(time); };
        auto executor = make_executor();
        auto wake = executor.view().engine_control().attach_activity(probe.handle());
        static_cast<void>(wake);
        CHECK_THROWS_WITH(executor.view().run(), ContainsSubstring("already completed"));
        CHECK(probe.completions.size() == 1);
    }
}

TEST_CASE("executor activities: real time wakes to inspect child requests without a push tick", "[executor_activity]")
{
    ActivityProbe probe;
    const auto start = std::chrono::time_point_cast<TimeDelta>(engine_clock::now());
    const auto requested = start + TimeDelta{20'000};
    auto executor = make_executor(GraphExecutorMode::RealTime, start, TimeDelta{2'000'000});
    auto control = executor.view().engine_control();
    auto wake = control.attach_activity(probe.handle());
    probe.on_completed = [control](DateTime) { control.request_stop(); };
    std::jthread child([&probe, wake, requested] {
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
        probe.request(requested);
        wake.notify();
    });
    executor.view().run();
    CHECK(probe.completions == std::vector<DateTime>{requested});
    CHECK_FALSE(executor.view().push_queue_engine().is_push_update_pending());
}

TEST_CASE("executor activities: notification races safely with executor destruction", "[executor_activity]")
{
    ActivityProbe probe;
    auto executor = std::make_unique<GraphExecutorValue>(make_executor(GraphExecutorMode::RealTime));
    auto wake = executor->view().engine_control().attach_activity(probe.handle());
    std::atomic_bool started{false};
    std::jthread child([wake, &started](std::stop_token stop) {
        started.store(true);
        while (!stop.stop_requested()) { wake.notify(); }
    });
    while (!started.load()) { std::this_thread::yield(); }
    executor.reset();
    child.request_stop();
    child.join();
    CHECK_NOTHROW(wake.notify());
}

TEST_CASE("executor activities: real time drains late child requests before its end bound", "[executor_activity]")
{
    ActivityProbe probe;
    const auto start = std::chrono::time_point_cast<TimeDelta>(engine_clock::now());
    const auto requested = start + TimeDelta{500};
    bool discovered = false;
    probe.on_next = [&](bool wait) {
        if (wait && !discovered)
        {
            discovered = true;
            return requested;
        }
        return MAX_DT;
    };
    auto executor = make_executor(GraphExecutorMode::RealTime, start, TimeDelta{1'000});
    auto wake = executor.view().engine_control().attach_activity(probe.handle());
    static_cast<void>(wake);
    executor.view().run();
    CHECK(probe.completions == std::vector<DateTime>{requested});
}

TEST_CASE("executor activities: callbacks cannot invalidate active iteration", "[executor_activity]")
{
    ActivityProbe probe;
    auto executor = make_executor();
    auto control = executor.view().engine_control();
    auto wake = control.attach_activity(probe.handle());
    static_cast<void>(wake);
    probe.on_next = [&](bool) {
        control.detach_activity(probe.handle());
        return MAX_DT;
    };
    CHECK_THROWS_WITH(executor.view().run(), ContainsSubstring("from an activity callback"));
    CHECK_NOTHROW(control.detach_activity(probe.handle()));
}

TEST_CASE("executor activities: settling cannot override immediate drain termination", "[executor_activity]")
{
    for (const bool settle_only : {false, true})
    {
        CAPTURE(settle_only);
        ActivityProbe probe;
        auto executor = make_executor(GraphExecutorMode::RealTime, MIN_ST, TimeDelta{10'000});
        auto wake = executor.view().engine_control().attach_activity(probe.handle());
        static_cast<void>(wake);
        probe.request(MIN_ST);
        probe.on_next = [&](bool wait) {
            return settle_only && !wait ? MAX_DT : DateTime{TimeDelta{probe.requested.load()}};
        };
        probe.on_completed = [&](DateTime time) { probe.request(time + MIN_TD); };
        executor.view().run();
        REQUIRE_FALSE(probe.completions.empty());
        CHECK(probe.completions.size() <= 1025);
        CHECK(executor.view().evaluation_clock().evaluation_time() == MIN_ST + TimeDelta{10'000});
    }
}
