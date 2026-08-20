#include <hgraph/fabric/kafka_notifier.h>
#include <hgraph/fabric/metadata_codec.h>
#include <hgraph/fabric/value_builders.h>

#include <hgraph/kafka/testing/fake_broker.h>
#include <hgraph/kafka/testing/mock_cluster.h>
#include <hgraph/kafka/value_builders.h>
#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/runtime.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>

namespace
{
    namespace hg  = hgraph;
    namespace hgf = hgraph::fabric;
    namespace hgk = hgraph::kafka;

    using namespace std::chrono_literals;

    inline hg::Value service_config{};
    inline hgk::testing::FakeBrokerPtr broker{};
    inline hgf::Notifier notifier{};
    inline hgf::KafkaNotifierConfig notifier_config{};

    [[nodiscard]] hg::GraphExecutorValue start_realtime(
        hg::GraphBuilder builder,
        hg::TimeDelta duration = hg::TimeDelta{5'000'000})
    {
        const hg::DateTime start = hg::testing::wall_now();
        hg::GraphExecutorBuilder executor_builder;
        executor_builder.graph_builder(std::move(builder))
            .mode(hg::GraphExecutorMode::RealTime)
            .start_time(start)
            .end_time(start + duration);
        return executor_builder.make_executor();
    }

    template <typename Predicate>
    [[nodiscard]] bool wait_until(Predicate &&predicate,
                                  std::chrono::milliseconds timeout = 2s)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (std::invoke(predicate)) { return true; }
            std::this_thread::sleep_for(1ms);
        }
        return std::invoke(predicate);
    }

    [[nodiscard]] hgraph::persistence::store::ObjectBytes notice_bytes(
        hg::Str data_id, hgf::RevisionId revision,
        hgf::DataVersion version = 0)
    {
        if (version == 0) { version = revision; }
        hg::Value value = hgf::make_data_revision(hgf::DataRevisionInput{
            .data_id = std::move(data_id),
            .revision = revision,
            .output_version = version,
            .as_of = hg::DateTime{hg::TimeDelta{
                1'900'000'000'000'000 + revision}},
        });
        return hgf::encode_revision(value.view());
    }

    [[nodiscard]] hg::Bytes bytes(
        const hgraph::persistence::store::ObjectBytes &value)
    {
        std::string result(value.size(), '\0');
        std::ranges::transform(value, result.begin(), [](std::byte item) {
            return static_cast<char>(item);
        });
        return hg::Bytes{std::move(result)};
    }

    [[nodiscard]] hg::Value subscription_key()
    {
        return hgk::subscription_key()
            .topics({notifier_config.topic})
            .group_id(notifier_config.group_id)
            .assignment_mode(hgk::KafkaAssignmentMode::Independent)
            .start(hgk::make_start_position(
                hgk::KafkaStartPositionKind::Committed,
                hgk::KafkaOffsetFallback::Earliest))
            .stop(hgk::make_stop_position(
                hgk::KafkaStopPositionKind::GraphLifetime))
            .commit_mode(hgk::KafkaCommitMode::Explicit)
            .recovery_clock(hgk::KafkaRecoveryClock::Arrival)
            .merge_policy(hgk::KafkaMergePolicy::Partition)
            .sharing_identity(notifier_config.sharing_identity)
            .build();
    }

    void emit(hg::Str data_id, hgf::RevisionId revision, hg::Int offset,
              hgk::KafkaSubscriptionState state =
                  hgk::KafkaSubscriptionState::Live,
              hgf::DataVersion version = 0)
    {
        auto encoded = notice_bytes(data_id, revision, version);
        broker->emit_subscription(
            subscription_key(),
            hgk::make_record(notifier_config.topic, 0, offset,
                             bytes(encoded), hg::Bytes{data_id}),
            hgk::make_cursor(notifier_config.sharing_identity, 1,
                             notifier_config.topic, 0, offset + 1),
            state);
    }

    [[nodiscard]] hg::Int revision_of(
        const hgf::RevisionNotification &notification)
    {
        return hgf::data_revision_input(
                   hgf::decode_revision(notification.revision).view())
            .revision;
    }

    struct KafkaNotifierGraph
    {
        static constexpr auto name = "hgraph.fabric.test.kafka_notifier";

        static void compose(hg::Wiring &wiring)
        {
            const auto path = hg::service::path("fabric-kafka");
            hgk::testing::register_fake_service(
                wiring, path, service_config.clone(), broker);
            notifier = hgf::wire_kafka_notifier(wiring, path,
                                                notifier_config);
        }
    };

    struct ProductionKafkaNotifierGraph
    {
        static constexpr auto name =
            "hgraph.fabric.test.production_kafka_notifier";

        static void compose(hg::Wiring &wiring)
        {
            notifier = hgf::register_kafka_notifier(
                wiring, hg::service::path("fabric-kafka-production"),
                service_config.clone(), notifier_config);
        }
    };

    struct RunningNotifier
    {
        RunningNotifier()
        {
            hg::stdlib::register_standard_operators();
            broker = std::make_shared<hgk::testing::FakeBroker>();
            notifier = {};
            service_config = hgk::service_config()
                                 .bootstrap_servers({"localhost:9092"})
                                 .client_id("fabric-kafka-test")
                                 .build();
            notifier_config = {
                .topic = "fabric-notices",
                .group_id = "fabric-notices-test",
                .sharing_identity = "fabric-notices-test",
                .pending_data_id_limit = 4,
                .outbound_record_limit = 4,
                .commit_partition_limit = 4,
            };
            executor = start_realtime(hg::build_graph<KafkaNotifierGraph>());
            subscription = notifier.subscribe();
            view = executor.view();
            runner.emplace(view);
            REQUIRE(broker->wait_until_attached(2s));
            REQUIRE(broker->wait_for_subscription_updates(1, 2s));
        }

        ~RunningNotifier()
        {
            subscription.close();
            view.request_stop();
            if (runner.has_value()) { runner->join(); }
            notifier.reset();
            broker.reset();
            service_config = {};
        }

        hg::GraphExecutorValue executor{};
        hgf::NotificationSubscription subscription{};
        hg::GraphExecutorView view{};
        std::optional<hg::testing::AsyncGraphExecutorRun> runner{};
    };

    struct RunningProductionNotifier
    {
        RunningProductionNotifier()
        {
            hg::stdlib::register_standard_operators();
            notifier = {};
            executor = start_realtime(
                hg::build_graph<ProductionKafkaNotifierGraph>(),
                hg::TimeDelta{15'000'000});
            subscription = notifier.subscribe();
            view = executor.view();
            runner.emplace(view);
        }

        ~RunningProductionNotifier()
        {
            subscription.close();
            view.request_stop();
            if (runner.has_value()) { runner->join(); }
            notifier.reset();
        }

        [[nodiscard]] bool wait_for_live()
        {
            return wait_until(
                [&] {
                    return subscription.status().state ==
                           hgf::NotificationSubscriptionState::Live;
                },
                10s);
        }

        [[nodiscard]] std::optional<hgf::RevisionNotification>
        wait_for_revision(hgf::RevisionId expected)
        {
            std::optional<hgf::RevisionNotification> result;
            const bool found = wait_until(
                [&] {
                    while (auto candidate = subscription.try_pop())
                    {
                        if (revision_of(*candidate) == expected)
                        {
                            result = std::move(*candidate);
                            return true;
                        }
                        subscription.acknowledge(*candidate);
                    }
                    return false;
                },
                10s);
            return found ? std::move(result) : std::nullopt;
        }

        hg::GraphExecutorValue executor{};
        hgf::NotificationSubscription subscription{};
        hg::GraphExecutorView view{};
        std::optional<hg::testing::AsyncGraphExecutorRun> runner{};
    };
}  // namespace

TEST_CASE("Kafka fabric profile requires non-lossy idempotent transport")
{
    const hg::Value valid = hgk::service_config()
                                .bootstrap_servers({"localhost:9092"})
                                .client_id("fabric-profile")
                                .build();
    REQUIRE_NOTHROW(hgf::require_kafka_fabric_profile(valid.view()));

    const hg::Value non_idempotent =
        hgk::service_config()
            .bootstrap_servers({"localhost:9092"})
            .client_id("fabric-profile")
            .idempotent_producer(false)
            .build();
    REQUIRE_THROWS_WITH(
        hgf::require_kafka_fabric_profile(non_idempotent.view()),
        Catch::Matchers::ContainsSubstring("idempotent"));

    const hg::Value lossy =
        hgk::service_config()
            .bootstrap_servers({"localhost:9092"})
            .client_id("fabric-profile")
            .inbound_overflow(hgk::KafkaOverflowAction::Drop)
            .build();
    REQUIRE_THROWS_WITH(
        hgf::require_kafka_fabric_profile(lossy.view()),
        Catch::Matchers::ContainsSubstring("dropping inbound"));
}

TEST_CASE("Kafka notifier publishes the complete revision and correlates delivery")
{
    RunningNotifier running;
    const auto payload = notice_bytes("prices", 7, 41);
    auto delivery = notifier.publish({"prices", payload});

    REQUIRE(broker->wait_for_publications(1, 2s));
    REQUIRE(wait_until([&] {
        return delivery.poll().status !=
               hgf::NotificationDeliveryStatus::Pending;
    }));
    REQUIRE(delivery.poll().status ==
            hgf::NotificationDeliveryStatus::Delivered);

    const auto publications = broker->published_records();
    REQUIRE(publications.size() == 1);
    const auto record = publications.front().record.view().as_bundle();
    REQUIRE(record.at("key").checked_as<hg::Bytes>() == hg::Bytes{"prices"});
    REQUIRE(record.at("value").checked_as<hg::Bytes>() == bytes(payload));
}

TEST_CASE("Kafka notifier bounds delivery correlation before graph startup")
{
    hg::stdlib::register_standard_operators();
    broker = std::make_shared<hgk::testing::FakeBroker>();
    service_config = hgk::service_config()
                         .bootstrap_servers({"localhost:9092"})
                         .client_id("fabric-kafka-bound")
                         .build();
    notifier_config = {
        .topic = "fabric-notices",
        .group_id = "fabric-notices-bound",
        .sharing_identity = "fabric-notices-bound",
        .pending_data_id_limit = 4,
        .outbound_record_limit = 1,
        .commit_partition_limit = 4,
    };

    auto executor = start_realtime(hg::build_graph<KafkaNotifierGraph>());
    auto first = notifier.publish({"prices", notice_bytes("prices", 1)});
    auto rejected = notifier.publish({"positions", notice_bytes("positions", 1)});
    REQUIRE(first.poll().status == hgf::NotificationDeliveryStatus::Pending);
    REQUIRE(rejected.poll().status == hgf::NotificationDeliveryStatus::Failed);
    REQUIRE_THAT(rejected.poll().message,
                 Catch::Matchers::ContainsSubstring("outbound record limit"));

    notifier.reset();
    executor = {};
    REQUIRE(first.poll().status == hgf::NotificationDeliveryStatus::Failed);
    broker.reset();
    service_config = {};
}

TEST_CASE("Kafka notifier retains notices before Fabric claims live ingress")
{
    hg::stdlib::register_standard_operators();
    broker = std::make_shared<hgk::testing::FakeBroker>();
    service_config = hgk::service_config()
                         .bootstrap_servers({"localhost:9092"})
                         .client_id("fabric-kafka-startup-race")
                         .build();
    notifier_config = {
        .topic = "fabric-notices",
        .group_id = "fabric-notices-startup-race",
        .sharing_identity = "fabric-notices-startup-race",
        .pending_data_id_limit = 4,
        .outbound_record_limit = 4,
        .commit_partition_limit = 4,
    };

    auto executor = start_realtime(hg::build_graph<KafkaNotifierGraph>());
    auto view = executor.view();
    hg::testing::AsyncGraphExecutorRun runner{view};
    REQUIRE(broker->wait_for_subscription_updates(1, 2s));
    emit("prices", 1, 0);
    REQUIRE(wait_until([&] {
        const auto commits = broker->committed_cursors();
        return !commits.empty() &&
               commits.back()
                       .view()
                       .as_bundle()
                       .at("next_offset")
                       .checked_as<hg::Int>() == 0;
    }));

    auto subscription = notifier.subscribe();
    REQUIRE(subscription.pending() == 1);
    REQUIRE(subscription.status().state ==
            hgf::NotificationSubscriptionState::Live);
    auto retained = subscription.try_pop();
    REQUIRE(retained.has_value());
    REQUIRE(revision_of(*retained) == 1);
    subscription.acknowledge(*retained);
    REQUIRE(wait_until([&] {
        const auto commits = broker->committed_cursors();
        return !commits.empty() &&
               commits.back()
                       .view()
                       .as_bundle()
                       .at("next_offset")
                       .checked_as<hg::Int>() == 1;
    }));

    subscription.close();
    view.request_stop();
    runner.join();
    notifier.reset();
    broker.reset();
    service_config = {};
}

TEST_CASE("Kafka notifier conflates by newest revision and commits only acknowledgements")
{
    RunningNotifier running;

    emit("prices", 2, 0, hgk::KafkaSubscriptionState::Recovering);
    REQUIRE(wait_until([&] { return running.subscription.pending() == 1; }));
    REQUIRE(running.subscription.status().state ==
            hgf::NotificationSubscriptionState::Recovering);

    emit("prices", 3, 1);
    emit("prices", 2, 2);
    // A second key is a FIFO transport barrier proving that the stale record
    // has reached the adapter before the test acknowledges the winner.
    emit("positions", 1, 3);
    REQUIRE(wait_until([&] { return running.subscription.pending() == 2; }));
    REQUIRE(wait_until([&] {
        return running.subscription.status().state ==
               hgf::NotificationSubscriptionState::Live;
    }));
    REQUIRE(wait_until([&] {
        const auto commits = broker->committed_cursors();
        return !commits.empty() &&
               commits.back()
                       .view()
                       .as_bundle()
                       .at("next_offset")
                       .checked_as<hg::Int>() == 1;
    }));

    auto notification = running.subscription.try_pop();
    REQUIRE(notification.has_value());
    REQUIRE(revision_of(*notification) == 3);
    REQUIRE(running.subscription.pending() == 1);

    const auto before_ack = broker->committed_cursors();
    REQUIRE(before_ack.back()
                .view()
                .as_bundle()
                .at("next_offset")
                .checked_as<hg::Int>() == 1);

    running.subscription.acknowledge(*notification);
    const bool committed = wait_until([&] {
        const auto commits = broker->committed_cursors();
        return !commits.empty() &&
               commits.back()
                       .view()
                       .as_bundle()
                       .at("next_offset")
                       .checked_as<hg::Int>() == 3;
    });
    std::vector<hg::Int> committed_offsets;
    for (const auto &cursor : broker->committed_cursors())
    {
        committed_offsets.push_back(cursor.view()
                                        .as_bundle()
                                        .at("next_offset")
                                        .checked_as<hg::Int>());
    }
    CAPTURE(committed_offsets);
    REQUIRE(committed);

    auto barrier = running.subscription.try_pop();
    REQUIRE(barrier.has_value());
    REQUIRE(barrier->data_id == "positions");
    running.subscription.acknowledge(*barrier);
}

TEST_CASE("Kafka notifier clears obsolete receipts when the consumer recovers")
{
    RunningNotifier running;

    emit("prices", 2, 0);
    REQUIRE(wait_until([&] { return running.subscription.pending() == 1; }));
    const auto prior_generation = running.subscription.status().generation;

    emit("positions", 1, 1, hgk::KafkaSubscriptionState::Retrying);
    REQUIRE(wait_until([&] {
        return running.subscription.status().state ==
               hgf::NotificationSubscriptionState::Retrying;
    }));
    REQUIRE(running.subscription.pending() == 1);

    emit("positions", 2, 2);
    REQUIRE(wait_until([&] {
        return running.subscription.status().state ==
                   hgf::NotificationSubscriptionState::Live &&
               running.subscription.status().generation > prior_generation;
    }));
    auto recovered = running.subscription.try_pop();
    REQUIRE(recovered.has_value());
    REQUIRE(recovered->data_id == "positions");
    REQUIRE(revision_of(*recovered) == 2);
    running.subscription.acknowledge(*recovered);
}

TEST_CASE("Kafka notifier fails closed at its distinct pending-id bound")
{
    RunningNotifier running;
    notifier_config.pending_data_id_limit = 1;
    // Rebuild because the wiring-time limits are already captured by the
    // running adapter.
    running.subscription.close();
    running.view.request_stop();
    running.runner->join();
    running.runner.reset();
    notifier.reset();

    broker = std::make_shared<hgk::testing::FakeBroker>();
    running.executor = start_realtime(hg::build_graph<KafkaNotifierGraph>());
    running.subscription = notifier.subscribe();
    running.view = running.executor.view();
    running.runner.emplace(running.view);
    REQUIRE(broker->wait_for_subscription_updates(1, 2s));

    emit("prices", 1, 0);
    emit("positions", 1, 1);
    REQUIRE(wait_until([&] {
        return running.subscription.status().state ==
               hgf::NotificationSubscriptionState::Failed;
    }));
    REQUIRE_THAT(running.subscription.status().message,
                 Catch::Matchers::ContainsSubstring("pending data-id limit"));
}

TEST_CASE("Kafka notifier exposes fatal transport diagnostics as typed status")
{
    RunningNotifier running;
    broker->emit_event(hgk::make_event(
        hgk::KafkaSeverity::Fatal, "consumer", "poll", "fabric-kafka",
        "broker authentication failed", 29, false, true));

    REQUIRE(wait_until([&] {
        return running.subscription.status().state ==
               hgf::NotificationSubscriptionState::Failed;
    }));
    REQUIRE_THAT(running.subscription.status().message,
                 Catch::Matchers::ContainsSubstring(
                     "broker authentication failed"));

    emit("prices", 1, 0);
    REQUIRE(wait_until([&] { return running.subscription.pending() == 1; }));
    REQUIRE(running.subscription.status().state ==
            hgf::NotificationSubscriptionState::Failed);
    REQUIRE_THAT(running.subscription.status().message,
                 Catch::Matchers::ContainsSubstring(
                     "broker authentication failed"));
}

TEST_CASE("production Kafka notifier recovers and resumes after graph restart")
{
    hgk::testing::MockCluster cluster;
    cluster.create_topic("fabric-production-notices");
    cluster.fail_next_fetch(hgk::testing::MockConsumeError::Retriable);
    service_config =
        hgk::service_config()
            .bootstrap_servers({cluster.bootstrap_servers()})
            .client_id("fabric-production-notifier")
            .consumer_failure_policy(hgk::KafkaFailurePolicy::Report)
            .build();
    notifier_config = {
        .topic = "fabric-production-notices",
        .group_id = "fabric-production-notifier",
        .sharing_identity = "fabric-production-notifier",
        .pending_data_id_limit = 16,
        .outbound_record_limit = 16,
        .commit_partition_limit = 4,
    };

    {
        RunningProductionNotifier running;
        REQUIRE(running.wait_for_live());
        REQUIRE(running.subscription.status().generation > 0);

        const auto first_bytes = notice_bytes("prices", 1);
        auto delivery = notifier.publish({"prices", first_bytes});
        REQUIRE(wait_until(
            [&] {
                return delivery.poll().status !=
                       hgf::NotificationDeliveryStatus::Pending;
            },
            10s));
        REQUIRE(delivery.poll().status ==
                hgf::NotificationDeliveryStatus::Delivered);
        auto first = running.wait_for_revision(1);
        REQUIRE(first.has_value());
        running.subscription.acknowledge(*first);
        std::this_thread::sleep_for(100ms);
    }

    {
        RunningProductionNotifier running;
        REQUIRE(running.wait_for_live());

        const auto second_bytes = notice_bytes("prices", 2);
        auto delivery = notifier.publish({"prices", second_bytes});
        REQUIRE(wait_until(
            [&] {
                return delivery.poll().status !=
                       hgf::NotificationDeliveryStatus::Pending;
            },
            10s));
        REQUIRE(delivery.poll().status ==
                hgf::NotificationDeliveryStatus::Delivered);
        auto second = running.wait_for_revision(2);
        REQUIRE(second.has_value());
        running.subscription.acknowledge(*second);
    }

    notifier.reset();
    service_config = {};
}

TEST_CASE("production Kafka notifier surfaces permanent delivery failure")
{
    hgk::testing::MockCluster cluster;
    cluster.create_topic("fabric-production-failure");
    cluster.fail_next_produce(hgk::testing::MockProduceError::Permanent);
    service_config =
        hgk::service_config()
            .bootstrap_servers({cluster.bootstrap_servers()})
            .client_id("fabric-production-failure")
            .producer_failure_policy(hgk::KafkaFailurePolicy::Report)
            .build();
    notifier_config = {
        .topic = "fabric-production-failure",
        .group_id = "fabric-production-failure",
        .sharing_identity = "fabric-production-failure",
        .pending_data_id_limit = 4,
        .outbound_record_limit = 4,
        .commit_partition_limit = 4,
    };

    {
        RunningProductionNotifier running;
        REQUIRE(running.wait_for_live());
        auto delivery = notifier.publish(
            {"prices", notice_bytes("prices", 1)});
        REQUIRE(wait_until(
            [&] {
                return delivery.poll().status !=
                       hgf::NotificationDeliveryStatus::Pending;
            },
            10s));
        REQUIRE(delivery.poll().status ==
                hgf::NotificationDeliveryStatus::Failed);
        REQUIRE_FALSE(delivery.poll().message.empty());
        REQUIRE(running.subscription.pending() == 0);
    }

    notifier.reset();
    service_config = {};
}

TEST_CASE("production Kafka notifier round-trips through an external broker")
{
    const char *bootstrap =
        std::getenv("HGRAPH_FABRIC_KAFKA_INTEGRATION_BOOTSTRAP");
    const char *topic = std::getenv("HGRAPH_FABRIC_KAFKA_INTEGRATION_TOPIC");
    if (bootstrap == nullptr && topic == nullptr) { return; }

    REQUIRE(bootstrap != nullptr);
    REQUIRE(topic != nullptr);
    const hg::Str topic_name{topic};
    const hg::Str identity =
        hg::Str{"hgraph-fabric-integration-"} + topic_name;
    service_config =
        hgk::service_config()
            .bootstrap_servers({hg::Str{bootstrap}})
            .client_id("hgraph-fabric-real-broker")
            .build();
    notifier_config = {
        .topic = topic_name,
        .group_id = identity,
        .sharing_identity = identity,
        .pending_data_id_limit = 16,
        .outbound_record_limit = 16,
        .commit_partition_limit = 16,
    };

    const auto revision = static_cast<hgf::RevisionId>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    {
        RunningProductionNotifier running;
        REQUIRE(running.wait_for_live());
        auto delivery = notifier.publish(
            {"prices", notice_bytes("prices", revision)});
        REQUIRE(wait_until(
            [&] {
                return delivery.poll().status !=
                       hgf::NotificationDeliveryStatus::Pending;
            },
            10s));
        REQUIRE(delivery.poll().status ==
                hgf::NotificationDeliveryStatus::Delivered);
        auto received = running.wait_for_revision(revision);
        REQUIRE(received.has_value());
        running.subscription.acknowledge(*received);
        std::this_thread::sleep_for(100ms);
    }

    notifier.reset();
    service_config = {};
}
