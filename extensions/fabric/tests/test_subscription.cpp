#include <hgraph/fabric/fabric.h>

#include <hgraph/lib/std/operators/impl/record_replay_memory_impl.h>
#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/types/subgraph_wiring.h>

#include <arrow/api.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

namespace hgraph::fabric::test_detail
{
    struct CapturedFrame
    {
        DateTime evaluation_time{MIN_DT};
        Frame    frame{};
    };

    struct CaptureState
    {
        void push(DateTime now, Frame frame)
        {
            std::unique_lock lock{mutex};
            values.push_back({now, std::move(frame)});
            if (block_at.has_value() && values.size() == *block_at)
            {
                blocked = true;
                changed.notify_all();
                changed.wait(lock, [&] { return released; });
            }
            lock.unlock();
            changed.notify_all();
        }

        void block_on(std::size_t count)
        {
            const std::lock_guard lock{mutex};
            block_at = count;
        }

        [[nodiscard]] bool wait_until_blocked(
            std::chrono::milliseconds timeout)
        {
            std::unique_lock lock{mutex};
            return changed.wait_for(lock, timeout, [&] { return blocked; });
        }

        void release()
        {
            {
                const std::lock_guard lock{mutex};
                released = true;
            }
            changed.notify_all();
        }

        [[nodiscard]] bool wait_for(std::size_t count,
                                    std::chrono::milliseconds timeout)
        {
            std::unique_lock lock{mutex};
            return changed.wait_for(lock, timeout,
                                    [&] { return values.size() >= count; });
        }

        [[nodiscard]] std::vector<CapturedFrame> snapshot() const
        {
            const std::lock_guard lock{mutex};
            return values;
        }

        mutable std::mutex       mutex{};
        std::condition_variable  changed{};
        std::vector<CapturedFrame> values{};
        std::optional<std::size_t> block_at{};
        bool                       blocked{};
        bool                       released{};
    };

    struct CaptureHandle
    {
        std::shared_ptr<CaptureState> value{};

        friend bool operator==(const CaptureHandle &,
                               const CaptureHandle &) noexcept = default;
        friend std::strong_ordering operator<=>(const CaptureHandle &lhs,
                                                const CaptureHandle &rhs) noexcept
        {
            return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
                   reinterpret_cast<std::uintptr_t>(rhs.value.get());
        }
    };

    inline std::ostream &operator<<(std::ostream &stream,
                                    const CaptureHandle &value)
    {
        return stream << "CaptureHandle(" << value.value.get() << ')';
    }

    struct CaptureFrameSink
    {
        static constexpr auto name = "hgraph.fabric.test.capture_frame";

        static void eval(In<"value", TS<Frame>> value,
                         Scalar<"capture", CaptureHandle> capture,
                         DateTime now)
        {
            capture.value().value->push(now, value.value());
        }
    };

    struct NestedLiveSubscription
    {
        static Port<TS<Frame>> compose(Wiring &wiring)
        {
            return subscribe_data(wiring, "nested-live",
                                  SubscriptionMode::Live);
        }
    };

    struct SubscribeGate
    {
        explicit SubscribeGate(Notifier configured)
            : inner(std::move(configured))
        {
        }

        NotificationSubscription subscribe()
        {
            auto result = inner.subscribe();
            std::unique_lock lock{mutex};
            subscribed = true;
            changed.notify_all();
            changed.wait(lock, [&] { return released; });
            return result;
        }

        [[nodiscard]] bool wait_until_subscribed(
            std::chrono::milliseconds timeout)
        {
            std::unique_lock lock{mutex};
            return changed.wait_for(lock, timeout,
                                    [&] { return subscribed; });
        }

        void release()
        {
            {
                const std::lock_guard lock{mutex};
                released = true;
            }
            changed.notify_all();
        }

        Notifier                 inner{};
        std::mutex               mutex{};
        std::condition_variable  changed{};
        bool                     subscribed{};
        bool                     released{};
    };

    [[nodiscard]] Notifier gated_notifier(
        const std::shared_ptr<SubscribeGate> &gate)
    {
        static const NotifierOps ops{
            [](void *context) {
                return static_cast<SubscribeGate *>(context)->subscribe();
            },
            [](void *context, RevisionNotification notification) {
                return static_cast<SubscribeGate *>(context)->inner.publish(
                    std::move(notification));
            },
        };
        return Notifier{gate, ops};
    }

    struct ControlledSubscription
    {
        void transition(NotificationSubscriptionState state)
        {
            std::function<void()> notify;
            {
                const std::scoped_lock lock{mutex};
                status = {state, status.generation + 1, {}};
                notify = waker;
            }
            if (notify) { notify(); }
        }

        mutable std::mutex mutex{};
        NotificationSubscriptionStatus status{};
        std::function<void()> waker{};
        bool closed{};
    };

    struct ControlledNotifier
    {
        std::shared_ptr<ControlledSubscription> subscription{};
        Notifier memory{make_memory_notifier()};
    };

    [[nodiscard]] const NotificationSubscriptionOps &
    controlled_subscription_ops()
    {
        static const NotificationSubscriptionOps ops{
            [](void *) -> std::optional<RevisionNotification> {
                return std::nullopt;
            },
            [](void *) noexcept -> std::size_t { return 0; },
            [](void *context) {
                auto &subscription =
                    *static_cast<ControlledSubscription *>(context);
                const std::scoped_lock lock{subscription.mutex};
                return subscription.status;
            },
            [](void *, const RevisionNotification &) {},
            [](void *context, std::function<void()> waker) {
                auto &subscription =
                    *static_cast<ControlledSubscription *>(context);
                const std::scoped_lock lock{subscription.mutex};
                subscription.waker = std::move(waker);
            },
            [](void *context) noexcept {
                auto &subscription =
                    *static_cast<ControlledSubscription *>(context);
                const std::scoped_lock lock{subscription.mutex};
                subscription.closed = true;
                subscription.waker = {};
                subscription.status.state =
                    NotificationSubscriptionState::Stopped;
                ++subscription.status.generation;
                subscription.status.message.clear();
            },
        };
        return ops;
    }

    [[nodiscard]] Notifier controlled_notifier(
        const std::shared_ptr<ControlledSubscription> &subscription)
    {
        static const NotifierOps ops{
            [](void *context) {
                auto &owner = *static_cast<ControlledNotifier *>(context);
                return NotificationSubscription{owner.subscription,
                                                controlled_subscription_ops()};
            },
            [](void *context, RevisionNotification notification) {
                return static_cast<ControlledNotifier *>(context)
                    ->memory.publish(std::move(notification));
            },
        };
        return Notifier{
            std::make_shared<ControlledNotifier>(
                ControlledNotifier{subscription, make_memory_notifier()}),
            ops};
    }
}  // namespace hgraph::fabric::test_detail

namespace std
{
    template <>
    struct hash<hgraph::fabric::test_detail::CaptureHandle>
    {
        size_t operator()(
            const hgraph::fabric::test_detail::CaptureHandle &value) const noexcept
        {
            return hash<const void *>{}(value.value.get());
        }
    };
}  // namespace std

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<fabric::test_detail::CaptureHandle>
    {
        static constexpr std::string_view value{
            "hgraph.fabric.test::CaptureHandle"};
    };
}  // namespace hgraph::static_schema_detail

namespace
{
    namespace hg = hgraph;
    namespace hgf = hgraph::fabric;
    namespace detail = hgraph::fabric::test_detail;
    namespace hgps = hgraph::persistence::store;

    constexpr hg::DateTime BASE_TIME{
        hg::TimeDelta{1'900'000'000'000'000}};

    [[nodiscard]] hg::Frame frame(std::int64_t value)
    {
        arrow::Int64Builder builder;
        REQUIRE(builder.Append(value).ok());
        auto array = builder.Finish();
        REQUIRE(array.ok());
        return hg::Frame{arrow::Table::Make(
            arrow::schema({arrow::field("value", arrow::int64())}),
            {std::move(array).ValueOrDie()})};
    }

    [[nodiscard]] std::int64_t value_of(const hg::Frame &value)
    {
        REQUIRE(value.has_value());
        const auto array = std::static_pointer_cast<arrow::Int64Array>(
            value.table->column(0)->chunk(0));
        return array->Value(0);
    }

    hgps::ObjectBytes seed(
        const hgf::FabricConfig &config, std::string data_id,
        hgf::RevisionId revision, hgf::DataVersion output_version,
        hg::DateTime as_of,
        std::vector<hgf::DataDependencyInput> dependencies = {},
        bool write_slot = true)
    {
        const std::string frame_key = hgf::data_version_key(
            config.prefix, data_id, output_version);
        if (!config.frames.contains(frame_key))
        {
            config.frames.write(frame_key, frame(output_version));
        }
        const auto encoded_value = hgf::make_data_revision(
            hgf::DataRevisionInput{
                .data_id = data_id,
                .revision = revision,
                .output_version = output_version,
                .dependencies = std::move(dependencies),
                .as_of = as_of,
            });
        auto encoded = hgf::encode_revision(encoded_value.view());
        if (write_slot)
        {
            REQUIRE(config.objects
                        .put_immutable(hgf::revision_key(
                                           config.prefix, data_id, revision),
                                       encoded)
                        .status == hgps::ImmutableWriteStatus::Created);
        }
        return encoded;
    }

    [[nodiscard]] hg::GraphExecutorValue make_executor(
        const hgf::FabricConfig &config, hgf::SubscriptionMode subscription_mode,
        hg::GraphExecutorMode executor_mode, hg::DateTime start,
        hg::DateTime end, detail::CaptureHandle capture,
        std::optional<hg::DateTime> as_of = {}, bool nested = false)
    {
        hg::stdlib::register_standard_operators();
        hgf::register_fabric_operators();
        hg::Wiring wiring;
        hgf::set_fabric_config(wiring.global_state(), config);
        hg::Port<hg::TS<hg::Frame>> source = nested
            ? hg::nested_<detail::NestedLiveSubscription>(wiring)
            : hgf::subscribe_data(wiring, nested ? "nested-live" : "data",
                                  subscription_mode, as_of);
        hg::wire<detail::CaptureFrameSink>(wiring, source, capture);

        hg::GraphExecutorBuilder builder;
        builder.graph_builder(std::move(wiring).finish())
            .mode(executor_mode)
            .start_time(start)
            .end_time(end);
        return builder.make_executor();
    }

    [[nodiscard]] std::vector<std::int64_t>
    values(const detail::CaptureHandle &capture)
    {
        std::vector<std::int64_t> result;
        for (const auto &item : capture.value->snapshot())
        {
            result.push_back(value_of(item.frame));
        }
        return result;
    }
}  // namespace

TEST_CASE("snapshot emits one recursively bounded image at graph start")
{
    auto config = hgf::make_memory_fabric_config("tests/subscription/snapshot");
    seed(config, "P", 1, 1, BASE_TIME + hg::TimeDelta{1});
    seed(config, "P", 2, 2, BASE_TIME + hg::TimeDelta{4});
    seed(config, "data", 1, 1, BASE_TIME + hg::TimeDelta{2}, {{"P", 1}});
    seed(config, "data", 2, 2, BASE_TIME + hg::TimeDelta{5}, {{"P", 2}});

    detail::CaptureHandle capture{std::make_shared<detail::CaptureState>()};
    auto executor = make_executor(
        config, hgf::SubscriptionMode::Snapshot,
        hg::GraphExecutorMode::Simulation, BASE_TIME + hg::TimeDelta{10},
        BASE_TIME + hg::TimeDelta{12}, capture,
        BASE_TIME + hg::TimeDelta{3});
    CHECK_FALSE(executor.view().graph().schema()->waits_for_async_node_wakes);
    executor.view().run();

    REQUIRE(values(capture) == std::vector<std::int64_t>{1});
    CHECK(capture.value->snapshot().front().evaluation_time ==
          BASE_TIME + hg::TimeDelta{10});
}

TEST_CASE("subscription surfaces corrupt accepted storage as a node failure")
{
    auto config = hgf::make_memory_fabric_config(
        "tests/subscription/corrupt");
    const auto revision = hgf::make_data_revision(hgf::DataRevisionInput{
        .data_id = "data",
        .revision = 1,
        .output_version = 1,
        .as_of = BASE_TIME + hg::TimeDelta{1},
    });
    REQUIRE(config.objects
                .put_immutable(
                    hgf::revision_key(config.prefix, "data", 1),
                    hgf::encode_revision(revision.view()))
                .status == hgps::ImmutableWriteStatus::Created);

    detail::CaptureHandle capture{std::make_shared<detail::CaptureState>()};
    auto executor = make_executor(
        config, hgf::SubscriptionMode::Snapshot,
        hg::GraphExecutorMode::Simulation, BASE_TIME + hg::TimeDelta{2},
        BASE_TIME + hg::TimeDelta{3}, capture,
        BASE_TIME + hg::TimeDelta{2});
    CHECK_THROWS_WITH(
        executor.view().run(),
        Catch::Matchers::ContainsSubstring("Corrupt") &&
            Catch::Matchers::ContainsSubstring("missing durable Frame"));
}

TEST_CASE("replay is seeded at start and excludes the end time")
{
    auto config = hgf::make_memory_fabric_config("tests/subscription/replay");
    seed(config, "data", 1, 1, BASE_TIME + hg::TimeDelta{1});
    seed(config, "data", 2, 2, BASE_TIME + hg::TimeDelta{2});
    seed(config, "data", 3, 3, BASE_TIME + hg::TimeDelta{3});
    seed(config, "data", 4, 4, BASE_TIME + hg::TimeDelta{5});

    detail::CaptureHandle capture{std::make_shared<detail::CaptureState>()};
    auto executor = make_executor(
        config, hgf::SubscriptionMode::Replay,
        hg::GraphExecutorMode::Simulation, BASE_TIME + hg::TimeDelta{2},
        BASE_TIME + hg::TimeDelta{5}, capture);
    CHECK_FALSE(executor.view().graph().schema()->waits_for_async_node_wakes);
    executor.view().run();

    CHECK(values(capture) == std::vector<std::int64_t>{2, 3});
    const auto observed = capture.value->snapshot();
    REQUIRE(observed.size() == 2);
    CHECK(observed[0].evaluation_time == BASE_TIME + hg::TimeDelta{2});
    CHECK(observed[1].evaluation_time == BASE_TIME + hg::TimeDelta{3});
}

TEST_CASE("replay advances hidden lineage without copying an unchanged Frame")
{
    auto config = hgf::make_memory_fabric_config(
        "tests/subscription/input-only");
    seed(config, "P", 1, 1, BASE_TIME + hg::TimeDelta{1});
    seed(config, "P", 2, 2, BASE_TIME + hg::TimeDelta{2});
    seed(config, "data", 1, 1, BASE_TIME + hg::TimeDelta{1}, {{"P", 1}});
    seed(config, "data", 2, 1, BASE_TIME + hg::TimeDelta{2}, {{"P", 2}});

    detail::CaptureHandle capture{std::make_shared<detail::CaptureState>()};
    auto executor = make_executor(
        config, hgf::SubscriptionMode::Replay,
        hg::GraphExecutorMode::Simulation, BASE_TIME + hg::TimeDelta{1},
        BASE_TIME + hg::TimeDelta{3}, capture);
    executor.view().run();

    CHECK(values(capture) == std::vector<std::int64_t>{1});
}

TEST_CASE("replay from MIN_ST opens dynamic ancestry without a future seed")
{
    auto config = hgf::make_memory_fabric_config("tests/subscription/min-st");
    seed(config, "A", 1, 1, BASE_TIME + hg::TimeDelta{1});
    seed(config, "B", 1, 1, BASE_TIME + hg::TimeDelta{2});
    seed(config, "data", 1, 1, BASE_TIME + hg::TimeDelta{1}, {{"A", 1}});
    seed(config, "data", 2, 2, BASE_TIME + hg::TimeDelta{3}, {{"B", 1}});

    detail::CaptureHandle capture{std::make_shared<detail::CaptureState>()};
    auto executor = make_executor(
        config, hgf::SubscriptionMode::Replay,
        hg::GraphExecutorMode::Simulation, hg::MIN_ST,
        BASE_TIME + hg::TimeDelta{4}, capture);
    executor.view().run();

    CHECK(values(capture) == std::vector<std::int64_t>{1, 2});
}

TEST_CASE("replay batches equal as-of timestamps across dynamic ancestry")
{
    auto config = hgf::make_memory_fabric_config(
        "tests/subscription/equal-as-of");
    seed(config, "P", 1, 1, BASE_TIME + hg::TimeDelta{1});
    seed(config, "data", 1, 1, BASE_TIME + hg::TimeDelta{1}, {{"P", 1}});
    seed(config, "P", 2, 2, BASE_TIME + hg::TimeDelta{2});
    seed(config, "data", 2, 2, BASE_TIME + hg::TimeDelta{2}, {{"P", 2}});

    detail::CaptureHandle capture{std::make_shared<detail::CaptureState>()};
    auto executor = make_executor(
        config, hgf::SubscriptionMode::Replay,
        hg::GraphExecutorMode::Simulation, BASE_TIME + hg::TimeDelta{1},
        BASE_TIME + hg::TimeDelta{3}, capture);
    executor.view().run();

    CHECK(values(capture) == std::vector<std::int64_t>{1, 2});
    const auto observed = capture.value->snapshot();
    REQUIRE(observed.size() == 2);
    CHECK(observed[1].evaluation_time == BASE_TIME + hg::TimeDelta{2});
}

TEST_CASE("replay emits a held cut when its dependency becomes knowable")
{
    auto config = hgf::make_memory_fabric_config(
        "tests/subscription/held-cut");
    seed(config, "P", 1, 1, BASE_TIME + hg::TimeDelta{1});
    seed(config, "data", 1, 1, BASE_TIME + hg::TimeDelta{1}, {{"P", 1}});
    seed(config, "data", 2, 2, BASE_TIME + hg::TimeDelta{2}, {{"P", 2}});
    seed(config, "P", 2, 2, BASE_TIME + hg::TimeDelta{3});

    detail::CaptureHandle capture{std::make_shared<detail::CaptureState>()};
    auto executor = make_executor(
        config, hgf::SubscriptionMode::Replay,
        hg::GraphExecutorMode::Simulation, BASE_TIME + hg::TimeDelta{1},
        BASE_TIME + hg::TimeDelta{4}, capture);
    executor.view().run();

    CHECK(values(capture) == std::vector<std::int64_t>{1, 2});
    const auto observed = capture.value->snapshot();
    REQUIRE(observed.size() == 2);
    CHECK(observed[1].evaluation_time == BASE_TIME + hg::TimeDelta{3});
}

TEST_CASE("Auto selects replay once for a simulation run")
{
    auto config = hgf::make_memory_fabric_config("tests/subscription/auto");
    seed(config, "data", 1, 1, BASE_TIME + hg::TimeDelta{1});
    seed(config, "data", 2, 2, BASE_TIME + hg::TimeDelta{2});

    detail::CaptureHandle capture{std::make_shared<detail::CaptureState>()};
    auto executor = make_executor(
        config, hgf::SubscriptionMode::Auto,
        hg::GraphExecutorMode::Simulation, BASE_TIME + hg::TimeDelta{1},
        BASE_TIME + hg::TimeDelta{3}, capture);
    executor.view().run();
    CHECK(values(capture) == std::vector<std::int64_t>{1, 2});
}

TEST_CASE("Auto honours the configured simulation strategy once in start")
{
    auto config = hgf::make_memory_fabric_config(
        "tests/subscription/auto-live", hgf::SubscriptionMode::Live,
        hgf::SubscriptionMode::Live);
    seed(config, "data", 1, 1, BASE_TIME + hg::TimeDelta{1});
    seed(config, "data", 2, 2, BASE_TIME + hg::TimeDelta{2});

    detail::CaptureHandle capture{std::make_shared<detail::CaptureState>()};
    auto executor = make_executor(
        config, hgf::SubscriptionMode::Auto,
        hg::GraphExecutorMode::Simulation, BASE_TIME + hg::TimeDelta{1},
        BASE_TIME + hg::TimeDelta{3}, capture);
    executor.view().run();

    CHECK(values(capture) == std::vector<std::int64_t>{2});
}

TEST_CASE("live subscribes before loading and emits the raced durable image")
{
    const auto now = std::chrono::time_point_cast<std::chrono::microseconds>(
        hg::engine_clock::now());
    auto config = hgf::make_memory_fabric_config(
        "tests/subscription/startup-race");
    auto gate = std::make_shared<detail::SubscribeGate>(config.notifications);
    config.notifications = detail::gated_notifier(gate);
    seed(config, "P", 1, 1, now - std::chrono::seconds{3});
    seed(config, "data", 1, 1, now - std::chrono::seconds{3}, {{"P", 1}});

    detail::CaptureHandle capture{std::make_shared<detail::CaptureState>()};
    auto executor = make_executor(
        config, hgf::SubscriptionMode::Live,
        hg::GraphExecutorMode::RealTime, now - std::chrono::milliseconds{1},
        now + std::chrono::seconds{5}, capture);
    CHECK(executor.view().graph().schema()->waits_for_async_node_wakes);
    auto view = executor.view();
    hg::testing::AsyncGraphExecutorRun run{view};
    REQUIRE(gate->wait_until_subscribed(std::chrono::seconds{2}));

    const auto parent = seed(config, "P", 2, 2,
                             now - std::chrono::seconds{2});
    const auto root = seed(config, "data", 2, 2,
                           now - std::chrono::seconds{2}, {{"P", 2}});
    REQUIRE(config.notifications.publish({"P", parent}).poll().status ==
            hgf::NotificationDeliveryStatus::Delivered);
    REQUIRE(config.notifications.publish({"data", root}).poll().status ==
            hgf::NotificationDeliveryStatus::Delivered);
    gate->release();

    REQUIRE(capture.value->wait_for(1, std::chrono::seconds{2}));
    view.request_stop();
    run.join();
    CHECK(values(capture) == std::vector<std::int64_t>{2});
}

TEST_CASE("slow live clients conflate and skip stale out-of-order notices")
{
    const auto now = std::chrono::time_point_cast<std::chrono::microseconds>(
        hg::engine_clock::now());
    auto config = hgf::make_memory_fabric_config(
        "tests/subscription/slow-client");
    seed(config, "data", 1, 1, now - std::chrono::seconds{4});

    detail::CaptureHandle capture{std::make_shared<detail::CaptureState>()};
    capture.value->block_on(2);
    auto executor = make_executor(
        config, hgf::SubscriptionMode::Live,
        hg::GraphExecutorMode::RealTime, now - std::chrono::milliseconds{1},
        now + std::chrono::seconds{5}, capture);
    auto view = executor.view();
    hg::testing::AsyncGraphExecutorRun run{view};
    REQUIRE(capture.value->wait_for(1, std::chrono::seconds{2}));

    const auto second = seed(config, "data", 2, 2,
                             now - std::chrono::seconds{3});
    REQUIRE(config.notifications.publish({"data", second}).poll().status ==
            hgf::NotificationDeliveryStatus::Delivered);
    REQUIRE(capture.value->wait_until_blocked(std::chrono::seconds{2}));

    const auto third = seed(config, "data", 3, 3,
                            now - std::chrono::seconds{2});
    const auto fourth = seed(config, "data", 4, 4,
                             now - std::chrono::seconds{1});
    REQUIRE(config.notifications.publish({"data", fourth}).poll().status ==
            hgf::NotificationDeliveryStatus::Delivered);
    REQUIRE(config.notifications.publish({"data", third}).poll().status ==
            hgf::NotificationDeliveryStatus::Delivered);
    capture.value->release();

    REQUIRE(capture.value->wait_for(3, std::chrono::seconds{2}));
    view.request_stop();
    run.join();
    CHECK(values(capture) == std::vector<std::int64_t>{1, 2, 4});
}

TEST_CASE("live notifications select the immutable winner over a losing payload")
{
    const auto now = std::chrono::time_point_cast<std::chrono::microseconds>(
        hg::engine_clock::now());
    auto config = hgf::make_memory_fabric_config(
        "tests/subscription/durable-winner");
    seed(config, "data", 1, 1, now - std::chrono::seconds{2});

    detail::CaptureHandle capture{std::make_shared<detail::CaptureState>()};
    auto executor = make_executor(
        config, hgf::SubscriptionMode::Live,
        hg::GraphExecutorMode::RealTime, now - std::chrono::milliseconds{1},
        now + std::chrono::seconds{5}, capture);
    auto view = executor.view();
    hg::testing::AsyncGraphExecutorRun run{view};
    REQUIRE(capture.value->wait_for(1, std::chrono::seconds{2}));

    static_cast<void>(seed(config, "data", 2, 2,
                           now - std::chrono::seconds{1}));
    const auto losing_value = hgf::make_data_revision(
        hgf::DataRevisionInput{
            .data_id = "data",
            .revision = 2,
            .output_version = 99,
            .as_of = now - std::chrono::seconds{1},
        });
    const auto losing_payload = hgf::encode_revision(losing_value.view());
    REQUIRE(config.notifications.publish({"data", losing_payload})
                .poll()
                .status == hgf::NotificationDeliveryStatus::Delivered);

    REQUIRE(capture.value->wait_for(2, std::chrono::seconds{2}));
    view.request_stop();
    run.join();
    CHECK(values(capture) == std::vector<std::int64_t>{1, 2});
}

TEST_CASE("live reconciles durable heads after each recovered transport generation")
{
    const auto now = std::chrono::time_point_cast<std::chrono::microseconds>(
        hg::engine_clock::now());
    auto config = hgf::make_memory_fabric_config(
        "tests/subscription/reconnect-reconcile");
    auto transport =
        std::make_shared<detail::ControlledSubscription>();
    config.notifications = detail::controlled_notifier(transport);
    seed(config, "data", 1, 1, now - std::chrono::seconds{2});

    detail::CaptureHandle capture{std::make_shared<detail::CaptureState>()};
    auto executor = make_executor(
        config, hgf::SubscriptionMode::Live,
        hg::GraphExecutorMode::RealTime, now - std::chrono::milliseconds{1},
        now + std::chrono::seconds{5}, capture);
    auto view = executor.view();
    hg::testing::AsyncGraphExecutorRun run{view};
    REQUIRE(capture.value->wait_for(1, std::chrono::seconds{2}));

    static_cast<void>(seed(config, "data", 2, 2,
                           now - std::chrono::seconds{1}));
    transport->transition(hgf::NotificationSubscriptionState::Recovering);
    CHECK_FALSE(capture.value->wait_for(2, std::chrono::milliseconds{20}));
    transport->transition(hgf::NotificationSubscriptionState::Live);

    REQUIRE(capture.value->wait_for(2, std::chrono::seconds{2}));
    view.request_stop();
    run.join();
    CHECK(values(capture) == std::vector<std::int64_t>{1, 2});
}

TEST_CASE("live retains an ahead proposal when a stale notice follows")
{
    const auto now = std::chrono::time_point_cast<std::chrono::microseconds>(
        hg::engine_clock::now());
    auto config = hgf::make_memory_fabric_config(
        "tests/subscription/ahead-conflation");
    seed(config, "data", 1, 1, now - std::chrono::seconds{4});

    detail::CaptureHandle capture{std::make_shared<detail::CaptureState>()};
    capture.value->block_on(1);
    auto executor = make_executor(
        config, hgf::SubscriptionMode::Live,
        hg::GraphExecutorMode::RealTime, now - std::chrono::milliseconds{1},
        now + std::chrono::seconds{5}, capture);
    auto view = executor.view();
    hg::testing::AsyncGraphExecutorRun run{view};
    REQUIRE(capture.value->wait_until_blocked(std::chrono::seconds{2}));

    const auto ahead = seed(config, "data", 3, 3,
                            now - std::chrono::seconds{2}, {}, false);
    const auto stale = seed(config, "data", 2, 2,
                            now - std::chrono::seconds{3});
    REQUIRE(config.notifications.publish({"data", ahead}).poll().status ==
            hgf::NotificationDeliveryStatus::Delivered);
    REQUIRE(config.notifications.publish({"data", stale}).poll().status ==
            hgf::NotificationDeliveryStatus::Delivered);
    capture.value->release();
    CHECK_FALSE(capture.value->wait_for(2, std::chrono::milliseconds{20}));

    REQUIRE(config.objects
                .put_immutable(hgf::revision_key(config.prefix, "data", 3),
                               ahead)
                .status == hgps::ImmutableWriteStatus::Created);
    REQUIRE(capture.value->wait_for(2, std::chrono::seconds{2}));
    view.request_stop();
    run.join();

    CHECK(values(capture) == std::vector<std::int64_t>{1, 3});
}

TEST_CASE("Auto selects live once and retries an ahead-of-storage notice")
{
    const auto now = std::chrono::time_point_cast<std::chrono::microseconds>(
        hg::engine_clock::now());
    auto config = hgf::make_memory_fabric_config("tests/subscription/live");
    seed(config, "data", 1, 1, now - std::chrono::seconds{2});

    detail::CaptureHandle capture{std::make_shared<detail::CaptureState>()};
    auto executor = make_executor(
        config, hgf::SubscriptionMode::Auto,
        hg::GraphExecutorMode::RealTime, now - std::chrono::milliseconds{1},
        now + std::chrono::seconds{5}, capture);
    auto view = executor.view();
    hg::testing::AsyncGraphExecutorRun run{view};
    REQUIRE(capture.value->wait_for(1, std::chrono::seconds{2}));

    const auto proposal = seed(config, "data", 2, 2,
                               now - std::chrono::seconds{1}, {}, false);
    REQUIRE(config.notifications.publish({"data", proposal}).poll().status ==
            hgf::NotificationDeliveryStatus::Delivered);
    CHECK_FALSE(capture.value->wait_for(2, std::chrono::milliseconds{20}));
    REQUIRE(config.objects
                .put_immutable(hgf::revision_key(config.prefix, "data", 2),
                               proposal)
                .status == hgps::ImmutableWriteStatus::Created);
    REQUIRE(capture.value->wait_for(2, std::chrono::seconds{2}));
    view.request_stop();
    run.join();

    CHECK(values(capture) == std::vector<std::int64_t>{1, 2});
}

TEST_CASE("live notification wake crosses a nested graph boundary")
{
    const auto now = std::chrono::time_point_cast<std::chrono::microseconds>(
        hg::engine_clock::now());
    auto config = hgf::make_memory_fabric_config("tests/subscription/nested");
    seed(config, "nested-live", 1, 1, now - std::chrono::seconds{2});

    detail::CaptureHandle capture{std::make_shared<detail::CaptureState>()};
    auto executor = make_executor(
        config, hgf::SubscriptionMode::Live,
        hg::GraphExecutorMode::RealTime, now - std::chrono::milliseconds{1},
        now + std::chrono::seconds{5}, capture, {}, true);
    auto view = executor.view();
    hg::testing::AsyncGraphExecutorRun run{view};
    REQUIRE(capture.value->wait_for(1, std::chrono::seconds{2}));

    const auto revision = seed(config, "nested-live", 2, 2,
                               now - std::chrono::seconds{1});
    REQUIRE(config.notifications.publish({"nested-live", revision}).poll().status ==
            hgf::NotificationDeliveryStatus::Delivered);
    REQUIRE(capture.value->wait_for(2, std::chrono::seconds{2}));
    view.request_stop();
    run.join();

    CHECK(values(capture) == std::vector<std::int64_t>{1, 2});
}
