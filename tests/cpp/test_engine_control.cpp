// C++-first engine control: the node injectable is a borrowed projection over
// the native executor, and stop_engine is a native sink wired through the
// ordinary operator registry.

#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <compare>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <ostream>
#include <thread>

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

    struct AsyncWakeState
    {
        void install(AsyncNodeWakeSender value)
        {
            const std::lock_guard lock{mutex};
            sender = value;
        }

        void evaluated()
        {
            {
                const std::lock_guard lock{mutex};
                ++evaluations;
            }
            changed.notify_all();
        }

        [[nodiscard]] bool wait_for(std::size_t count,
                                    std::chrono::milliseconds timeout)
        {
            std::unique_lock lock{mutex};
            return changed.wait_for(lock, timeout,
                                    [&] { return evaluations >= count; });
        }

        void wake(std::size_t count)
        {
            AsyncNodeWakeSender value;
            {
                const std::lock_guard lock{mutex};
                value = sender;
            }
            for (std::size_t index = 0; index < count; ++index) { value.wake(); }
        }

        [[nodiscard]] std::size_t count() const
        {
            const std::lock_guard lock{mutex};
            return evaluations;
        }

        mutable std::mutex       mutex{};
        std::condition_variable  changed{};
        AsyncNodeWakeSender      sender{};
        std::size_t              evaluations{};
    };

    struct AsyncWakeHandle
    {
        std::shared_ptr<AsyncWakeState> value{};

        friend bool operator==(const AsyncWakeHandle &,
                               const AsyncWakeHandle &) noexcept = default;
        friend std::strong_ordering
        operator<=>(const AsyncWakeHandle &lhs,
                    const AsyncWakeHandle &rhs) noexcept
        {
            return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
                   reinterpret_cast<std::uintptr_t>(rhs.value.get());
        }
    };

    std::ostream &operator<<(std::ostream &stream,
                             const AsyncWakeHandle &value)
    {
        return stream << "AsyncWakeHandle(" << value.value.get() << ')';
    }

    struct AsyncWakeProbe
    {
        static constexpr auto name = "async_node_wake_probe";

        static void start(Scalar<"state", AsyncWakeHandle> state,
                          AsyncNodeWakeSender sender,
                          SingleShotScheduler scheduler)
        {
            state.value().value->install(sender);
            scheduler.schedule_now();
        }

        static void eval(Scalar<"state", AsyncWakeHandle> state)
        {
            state.value().value->evaluated();
        }
    };
}  // namespace

namespace std
{
    template <>
    struct hash<AsyncWakeHandle>
    {
        size_t operator()(const AsyncWakeHandle &value) const noexcept
        {
            return hash<const void *>{}(value.value.get());
        }
    };
}  // namespace std

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<AsyncWakeHandle>
    {
        static constexpr std::string_view value{"tests::AsyncWakeHandle"};
    };
}  // namespace hgraph::static_schema_detail

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

TEST_CASE("engine control: async wakes schedule an ordinary node once at a root boundary")
{
    AsyncWakeHandle state{std::make_shared<AsyncWakeState>()};
    Wiring wiring;
    wire<AsyncWakeProbe>(wiring, state);

    GraphExecutorBuilder builder;
    const DateTime now = std::chrono::time_point_cast<std::chrono::microseconds>(
        engine_clock::now());
    builder.graph_builder(std::move(wiring).finish())
        .mode(GraphExecutorMode::RealTime)
        .start_time(now - std::chrono::milliseconds{1})
        .end_time(now + std::chrono::seconds{5});
    auto executor = builder.make_executor();
    CHECK(executor.view().graph().schema()->push_source_nodes_end == 0);
    CHECK(executor.view().graph().schema()->waits_for_async_node_wakes);

    auto view = executor.view();
    AsyncGraphExecutorRun run{view};
    REQUIRE(state.value->wait_for(1, std::chrono::seconds{2}));
    state.value->wake(8);
    REQUIRE(state.value->wait_for(2, std::chrono::seconds{2}));
    CHECK(state.value->count() == 2);
    view.request_stop();
    run.join();
    state.value->wake(1);
    CHECK(state.value->count() == 2);
}

TEST_CASE("engine control: async wake teardown synchronizes with callback threads")
{
    AsyncWakeHandle state{std::make_shared<AsyncWakeState>()};
    Wiring wiring;
    wire<AsyncWakeProbe>(wiring, state);

    GraphExecutorBuilder builder;
    const DateTime now = std::chrono::time_point_cast<std::chrono::microseconds>(
        engine_clock::now());
    builder.graph_builder(std::move(wiring).finish())
        .mode(GraphExecutorMode::RealTime)
        .start_time(now - std::chrono::milliseconds{1})
        .end_time(now + std::chrono::seconds{5});
    auto executor = builder.make_executor();
    auto view     = executor.view();
    AsyncGraphExecutorRun run{view};
    REQUIRE(state.value->wait_for(1, std::chrono::seconds{2}));

    std::atomic running{true};
    std::thread callback([&] {
        while (running.load(std::memory_order_relaxed))
        {
            state.value->wake(1);
            std::this_thread::yield();
        }
    });
    REQUIRE(state.value->wait_for(2, std::chrono::seconds{2}));
    view.request_stop();
    run.join();
    running.store(false, std::memory_order_relaxed);
    callback.join();

    const auto evaluations = state.value->count();
    state.value->wake(1);
    CHECK(state.value->count() == evaluations);
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
