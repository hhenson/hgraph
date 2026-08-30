#include <hgraph/fabric/fabric.h>
#include <hgraph/fabric/kafka.h>

#include <hgraph/kafka/service.h>
#include <hgraph/kafka/testing/fake_broker.h>
#include <hgraph/kafka/testing/mock_cluster.h>
#include <hgraph/kafka/value_builders.h>

#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/static_node.h>
#include <hgraph/util/environment.h>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/table.h>

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {
namespace hg = hgraph;
namespace hgf = hgraph::fabric;
namespace hgk = hgraph::kafka;

using namespace std::chrono_literals;

constexpr std::string_view TOPIC{"hgraph-fabric-transport-test"};
hg::Value kafka_config{};
std::int64_t observed_value{};
std::size_t observed_count{};
hgk::testing::FakeBrokerPtr fake_broker{};
std::vector<std::int64_t> observed_sequence{};
std::mutex observed_sequence_mutex{};
hg::Value actual_kafka_config{};
hg::Value actual_notice_record{};
hg::Value actual_delivery_report{};
hg::Value actual_audit_key{};
hg::Str actual_topic{};
std::filesystem::path actual_control_dir{};
std::vector<std::int64_t> actual_live_values{};
std::map<hg::Str, hg::Str> actual_diagnostics{};
std::map<hg::Str, hgf::FabricDiagnosticEventInput> actual_events{};

struct ActualBrokerRecord {
  hg::Int partition{};
  hg::Int offset{};
  hg::Bytes key{};
  hgf::DataRevisionInput revision{};
};

std::vector<ActualBrokerRecord> actual_broker_records{};

template <typename G>
[[nodiscard]] hg::GraphBuilder build_realtime_graph() {
  return hg::build_graph<G>(hg::WiringOptions{.is_realtime = true});
}

[[nodiscard]] std::filesystem::path marker(std::string_view name) {
  return actual_control_dir / name;
}

void write_marker(std::string_view name) {
  std::ofstream output{marker(name), std::ios::trunc};
  if (!output) {
    throw std::runtime_error("failed to write Fabric broker test marker");
  }
  output << name << '\n';
}

template <typename Predicate>
[[nodiscard]] bool wait_until(Predicate &&predicate,
                              std::chrono::steady_clock::duration timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (std::forward<Predicate>(predicate)()) {
      return true;
    }
    std::this_thread::sleep_for(10ms);
  }
  return std::forward<Predicate>(predicate)();
}

[[nodiscard]] hg::Frame frame(std::int64_t value) {
  arrow::Int64Builder builder;
  if (!builder.Append(value).ok()) {
    throw std::runtime_error("failed to append Fabric Kafka test value");
  }
  auto array = builder.Finish();
  if (!array.ok()) {
    throw std::runtime_error("failed to finish Fabric Kafka test value");
  }
  return hg::Frame{
      arrow::Table::Make(arrow::schema({arrow::field("value", arrow::int64())}),
                         {std::move(array).ValueOrDie()})};
}

[[nodiscard]] std::int64_t frame_value(const hg::Frame &value) {
  const auto values = std::static_pointer_cast<arrow::Int64Array>(
      value.table->column(0)->chunk(0));
  return values->Value(0);
}

[[nodiscard]] hgf::DataRevisionInput seed(const hgf::FabricConfig &config,
                                          hgf::RevisionId revision,
                                          hgf::DataVersion version) {
  config.frames.write(hgf::data_version_key(config.prefix, "prices", version),
                      frame(version));
  hg::Value value = hgf::make_data_revision(hgf::DataRevisionInput{
      .data_id = "prices",
      .revision = revision,
      .output_version = version,
      .as_of = hg::DateTime{hg::TimeDelta{1'800'000'000'000'000 + revision}},
  });
  const auto decoded = hgf::data_revision_input(value.view());
  const auto revision_write = config.objects.put_immutable(
      hgf::revision_key(config.prefix, "prices", revision),
      config.values.encode(value.view()));
  if (revision_write.status ==
      hg::persistence::store::ImmutableWriteStatus::Conflict) {
    throw std::runtime_error("Fabric Kafka test revision conflicted");
  }
  const auto as_of_write = config.objects.put_immutable(
      hgf::as_of_key(config.prefix, "prices", decoded.as_of),
      hgf::encode_reference(config.values, hgf::MetadataObjectKind::AsOf, revision));
  if (as_of_write.status ==
      hg::persistence::store::ImmutableWriteStatus::Conflict) {
    throw std::runtime_error("Fabric Kafka test as-of entry conflicted");
  }
  const std::string latest_key = hgf::latest_key(config.prefix, "prices");
  const auto current = config.objects.get(latest_key);
  const auto latest = config.objects.compare_exchange_ref(
      latest_key,
      current.has_value()
          ? std::optional<std::string_view>{current->version_token}
          : std::nullopt,
      hgf::encode_reference(config.values, hgf::MetadataObjectKind::Latest, revision));
  if (!latest.exchanged) {
    throw std::runtime_error("Fabric Kafka test latest update lost a race");
  }
  return decoded;
}

[[nodiscard]] hg::Bytes revision_bytes(const hgf::DataRevisionInput &revision) {
  const auto encoded =
      config.values.encode(hgf::make_data_revision(revision).view());
  return hg::Bytes{std::string{reinterpret_cast<const char *>(encoded.data()),
                               encoded.size()}};
}

struct PublishedFrameSource {
  static constexpr auto name = "hgraph.fabric.kafka.test.frame";
  static constexpr bool schedule_on_start = true;

  static void eval(hg::Out<hg::TS<hg::Frame>> out) { out.set(frame(71)); }
};

struct CaptureLiveFrame {
  static constexpr auto name = "hgraph.fabric.kafka.test.capture";

  static void eval(hg::NodeView node,
                   hg::In<"value", hg::TS<hg::Frame>> value) {
    observed_value = frame_value(value.value());
    ++observed_count;
    node.graph().executor().request_stop();
  }
};

struct CaptureLiveSequence {
  static constexpr auto name = "hgraph.fabric.kafka.test.capture_sequence";

  static void eval(hg::NodeView node,
                   hg::In<"value", hg::TS<hg::Frame>> value) {
    std::scoped_lock lock{observed_sequence_mutex};
    observed_sequence.push_back(frame_value(value.value()));
    if (observed_sequence.size() == 2) {
      node.graph().executor().request_stop();
    }
  }
};

struct ActualNoticeSource {
  static constexpr auto name = "hgraph.fabric.kafka.test.actual_notice";
  static constexpr bool schedule_on_start = true;

  static void eval(hg::Out<hg::TS<hgk::KafkaProduceRecord>> out) {
    out.apply(actual_notice_record.view());
  }
};

struct CaptureActualDelivery {
  static constexpr auto name = "hgraph.fabric.kafka.test.actual_delivery";

  static void eval(hg::NodeView node,
                   hg::In<"report", hg::TS<hgk::KafkaDeliveryReport>> report) {
    actual_delivery_report = report.base().value().clone();
    node.graph().executor().request_stop();
  }
};

struct ActualNoticeGraph {
  static constexpr auto name = "hgraph.fabric.kafka.test.actual_notice_graph";

  static void compose(hg::Wiring &wiring) {
    const auto path = hg::service::path("fabric-broker-seed-kafka");
    hgk::register_service(wiring, path, actual_kafka_config.clone());
    auto request = hgk::publish_request(wiring, actual_topic,
                                        hg::wire<ActualNoticeSource>(wiring));
    static_cast<void>(hg::wire<CaptureActualDelivery>(
        wiring, hgk::publish(wiring, path, request)));
  }
};

struct ActualBrokerFrameSource {
  static constexpr auto name = "hgraph.fabric.kafka.test.actual_frame";
  static constexpr bool schedule_on_start = true;

  static void eval(hg::NodeScheduler scheduler, hg::State<hg::Bool> emitted,
                   hg::Out<hg::TS<hg::Frame>> out) {
    if (emitted.get()) {
      return;
    }
    if (!std::filesystem::exists(marker("broker-stopped"))) {
      scheduler.schedule(hg::TimeDelta{10'000});
      return;
    }
    out.set(frame(2));
    emitted.set(true);
  }
};

struct CaptureActualLiveFrame {
  static constexpr auto name = "hgraph.fabric.kafka.test.actual_live_capture";

  static void eval(hg::NodeView node,
                   hg::In<"value", hg::TS<hg::Frame>> value) {
    const auto observed = frame_value(value.value());
    actual_live_values.push_back(observed);
    if (actual_live_values.size() == 1) {
      write_marker("initial-ready");
    }
    if (observed == 2) {
      write_marker("live-complete");
      node.graph().executor().request_stop();
    }
  }
};

struct CaptureActualKafkaEvent {
  static constexpr auto name = "hgraph.fabric.kafka.test.actual_event_capture";

  static void eval(hg::In<"event", hg::TS<hgk::KafkaEvent>> event) {
    const auto fields = event.base().value().as_bundle();
    if (fields.at("category").checked_as<hg::Str>() == "delivery" &&
        fields.at("retriable").checked_as<hg::Bool>()) {
      write_marker("delivery-failed-retriable");
    }
  }
};

struct CaptureActualDiagnostics {
  static constexpr auto name =
      "hgraph.fabric.kafka.test.actual_diagnostics_capture";

  static void eval(hg::In<"values", hgf::FabricDiagnostics> values) {
    const auto metrics = values.template field<"metrics">();
    for (const auto &[key, value] : metrics.valid_items()) {
      actual_diagnostics.insert_or_assign(key.checked_as<hg::Str>(),
                                          value.value());
    }
    const auto events = values.template field<"events">();
    for (const auto &[key, value] : events.valid_items()) {
      const auto fields = value.base().value().as_bundle();
      actual_events.insert_or_assign(
          key.checked_as<hg::Str>(),
          hgf::FabricDiagnosticEventInput{
              .component = fields.at("component").checked_as<hg::Str>(),
              .category = fields.at("category").checked_as<hg::Str>(),
              .message = fields.at("message").checked_as<hg::Str>(),
              .retriable = fields.at("retriable").checked_as<hg::Bool>(),
              .fatal = fields.at("fatal").checked_as<hg::Bool>(),
              .occurrences = fields.at("occurrences").checked_as<hg::Int>(),
          });
    }
  }
};

struct ActualBrokerGraph {
  static constexpr auto name = "hgraph.fabric.kafka.test.actual_broker_graph";

  static void compose(hg::Wiring &wiring) {
    hgf::register_kafka_transport(wiring, actual_topic,
                                  "fabric-broker-conformance",
                                  actual_kafka_config.clone());
    auto subscribed =
        hgf::subscribe_data(wiring, "prices");
    hgf::publish_data(wiring, "prices",
                      hg::wire<ActualBrokerFrameSource>(wiring));
    static_cast<void>(hg::wire<CaptureActualLiveFrame>(wiring, subscribed));
    static_cast<void>(hg::wire<CaptureActualKafkaEvent>(
        wiring, hgk::events(wiring, hg::service::path(
                                        hgf::DEFAULT_KAFKA_SERVICE_PATH))));
    static_cast<void>(
        hg::wire<CaptureActualDiagnostics>(wiring, hgf::diagnostics(wiring)));
  }
};

struct CaptureActualBrokerRecords {
  static constexpr auto name = "hgraph.fabric.kafka.test.actual_record_capture";

  static void eval(hg::NodeView node,
                   hg::In<"subscription", hgk::KafkaSubscriptionOutput,
                          hg::InputValidity::Unchecked>
                       subscription) {
    auto record = subscription.template field<"record">();
    if (record.valid() && record.modified()) {
      const auto fields = record.base().value().concrete().as_bundle();
      const auto payload = fields.at("value").checked_as<hg::Bytes>();
      actual_broker_records.push_back(ActualBrokerRecord{
          .partition = fields.at("partition").checked_as<hg::Int>(),
          .offset = fields.at("offset").checked_as<hg::Int>(),
          .key = fields.at("key").checked_as<hg::Bytes>(),
          .revision = hgf::data_revision_input(
              config.values.decode(hgf::data_revision_meta(), 
                  std::as_bytes(
                      std::span{payload.data.data(), payload.data.size()}))
                  .view()),
      });
    }
    auto state = subscription.template field<"state">();
    if (state.valid() && state.modified() &&
        state.value() == hgk::KafkaSubscriptionState::BoundedComplete) {
      node.graph().executor().request_stop();
    }
  }
};

struct ActualBrokerAuditGraph {
  static constexpr auto name = "hgraph.fabric.kafka.test.actual_audit_graph";

  static void compose(hg::Wiring &wiring) {
    const auto path = hg::service::path("fabric-broker-audit-kafka");
    hgk::register_service(wiring, path, actual_kafka_config.clone());
    auto key = hg::wire<hg::stdlib::const_, hg::TS<hgk::KafkaSubscriptionKey>>(
        wiring, actual_audit_key.clone());
    static_cast<void>(hg::wire<CaptureActualBrokerRecords>(
        wiring, hgk::subscribe(wiring, path, key)));
  }
};

struct KafkaFabricGraph {
  static constexpr auto name = "hgraph.fabric.kafka.test.graph";

  static void compose(hg::Wiring &wiring) {
    hgf::register_kafka_transport(wiring, hg::Str{TOPIC},
                                  hg::Str{"fabric-transport-test"},
                                  kafka_config.clone());
    auto subscribed =
        hgf::subscribe_data(wiring, "prices");
    hgf::publish_data(wiring, "prices", hg::wire<PublishedFrameSource>(wiring));
    static_cast<void>(hg::wire<CaptureLiveFrame>(wiring, subscribed));
  }
};

struct KafkaFabricFakeGraph {
  static constexpr auto name = "hgraph.fabric.kafka.test.fake_graph";

  static void compose(hg::Wiring &wiring) {
    const auto kafka_path = hg::service::path(hgf::DEFAULT_KAFKA_SERVICE_PATH);
    hgk::testing::register_fake_service(wiring, kafka_path,
                                        kafka_config.clone(), fake_broker);
    hgf::wire_kafka_transport(
        wiring, hg::service::path(hgf::DEFAULT_SERVICE_PATH), kafka_path,
        hg::Str{TOPIC}, hg::Str{"fabric-fake-transport"});
    auto subscribed =
        hgf::subscribe_data(wiring, "prices");
    static_cast<void>(hg::wire<CaptureLiveSequence>(wiring, subscribed));
  }
};

template <typename Fn> void require_invalid(Fn &&fn) {
  CHECK_THROWS_AS(std::forward<Fn>(fn)(), std::invalid_argument);
}
} // namespace

TEST_CASE("Fabric validates its stricter Kafka service profile") {
  hgk::register_kafka_types();
  const auto valid =
      hgk::service_config().bootstrap_servers({"broker:9092"}).build();
  CHECK_NOTHROW(hgf::require_fabric_kafka_profile(valid));
  require_invalid([] {
    hgf::require_fabric_kafka_profile(hgk::service_config()
                                          .bootstrap_servers({"broker:9092"})
                                          .idempotent_producer(false)
                                          .build());
  });
  require_invalid([] {
    hgf::require_fabric_kafka_profile(
        hgk::service_config()
            .bootstrap_servers({"broker:9092"})
            .inbound_overflow(hgk::KafkaOverflowAction::Drop)
            .build());
  });
  require_invalid([] {
    hgf::require_fabric_kafka_profile(
        hgk::service_config()
            .bootstrap_servers({"broker:9092"})
            .outbound_overflow(hgk::KafkaOverflowAction::Drop)
            .build());
  });
}

TEST_CASE("Fabric Kafka subscription consumes the complete topic with explicit "
          "commits") {
  hgk::register_kafka_types();
  const hg::Value key =
      hgf::fabric_kafka_subscription_key("fabric-topic", "fabric-identity");
  const auto fields = key.view().as_bundle();
  CHECK(fields.at("selector_kind").checked_as<hgk::KafkaSelectorKind>() ==
        hgk::KafkaSelectorKind::Topics);
  CHECK(fields.at("assignment_mode").checked_as<hgk::KafkaAssignmentMode>() ==
        hgk::KafkaAssignmentMode::Independent);
  CHECK(fields.at("commit_mode").checked_as<hgk::KafkaCommitMode>() ==
        hgk::KafkaCommitMode::Explicit);
  CHECK(fields.at("key_filter").data() == nullptr);
  const auto start = fields.at("start_position").as_bundle();
  CHECK(start.at("kind").checked_as<hgk::KafkaStartPositionKind>() ==
        hgk::KafkaStartPositionKind::Committed);
  CHECK(start.at("fallback").checked_as<hgk::KafkaOffsetFallback>() ==
        hgk::KafkaOffsetFallback::Earliest);
}

TEST_CASE("Kafka queue wakes live Fabric through the Kafka root push source") {
  hg::stdlib::register_standard_operators();
  hgf::register_fabric_operators();
  hgk::register_kafka_types();

  hgk::testing::MockCluster cluster;
  cluster.create_topic(hg::Str{TOPIC}, 3, 1);
  kafka_config = hgk::service_config()
                     .bootstrap_servers({cluster.bootstrap_servers()})
                     .client_id("hgraph-fabric-transport-test")
                     .build();
  observed_value = 0;
  observed_count = 0;

  auto graph = build_realtime_graph<KafkaFabricGraph>();
  CHECK(graph.root_type().schema()->push_source_nodes_end > 0);
  auto config = hgf::make_memory_fabric_config("tests/kafka-transport");
  hgf::set_fabric_config(graph.global_state(), config);

  const hg::DateTime start = hg::testing::wall_now();
  hg::GraphExecutorBuilder builder;
  builder.graph_builder(std::move(graph))
      .mode(hg::GraphExecutorMode::RealTime)
      .start_time(start)
      .end_time(start + hg::TimeDelta{10'000'000});
  auto executor = builder.make_executor();
  executor.view().run();

  CHECK(observed_count == 1);
  CHECK(observed_value == 71);
  const auto latest =
      config.objects.get(hgf::latest_key(config.prefix, "prices"));
  REQUIRE(latest.has_value());
  CHECK(hgf::revision_reference_value(config.values, hgf::MetadataObjectKind::Latest, latest->data) == 1);
  kafka_config = {};
}

TEST_CASE("Kafka lifecycle establishes the durable image before "
          "notification-driven live updates") {
  hg::stdlib::register_standard_operators();
  hgf::register_fabric_operators();
  hgk::register_kafka_types();

  fake_broker = std::make_shared<hgk::testing::FakeBroker>();
  kafka_config = hgk::service_config()
                     .bootstrap_servers({"fake:9092"})
                     .client_id("fabric-fake")
                     .build();
  auto config = hgf::make_memory_fabric_config("tests/kafka-lifecycle");
  const auto first = seed(config, 1, 1);
  {
    std::scoped_lock lock{observed_sequence_mutex};
    observed_sequence.clear();
  }

  auto graph = build_realtime_graph<KafkaFabricFakeGraph>();
  hgf::set_fabric_config(graph.global_state(), config);
  const hg::DateTime start = hg::testing::wall_now();
  hg::GraphExecutorBuilder builder;
  builder.graph_builder(std::move(graph))
      .mode(hg::GraphExecutorMode::RealTime)
      .start_time(start)
      .end_time(start + hg::TimeDelta{10'000'000});
  auto executor = builder.make_executor();
  auto view = executor.view();
  hg::testing::AsyncGraphExecutorRun runner{view};

  REQUIRE(fake_broker->wait_until_attached(2s));
  REQUIRE(fake_broker->wait_for_subscription_updates(1, 2s));
  const hg::Value subscription_key = hgf::fabric_kafka_subscription_key(
      hg::Str{TOPIC}, "fabric-fake-transport");
  fake_broker->emit_subscription(
      subscription_key.clone(),
      hgk::make_record(hg::Str{TOPIC}, 0, 0, revision_bytes(first),
                       hg::Bytes{"prices"}),
      hgk::make_cursor("fabric-fake-transport", 1, hg::Str{TOPIC}, 0, 1),
      hgk::KafkaSubscriptionState::Recovering);
  REQUIRE(fake_broker->wait_for_commits(1, 2s));
  REQUIRE(wait_until(
      [] {
        std::scoped_lock lock{observed_sequence_mutex};
        return observed_sequence == std::vector<std::int64_t>{1};
      },
      2s));

  const auto second = seed(config, 2, 2);
  fake_broker->emit_subscription(
      subscription_key.clone(),
      hgk::make_record(hg::Str{TOPIC}, 0, 1, revision_bytes(second),
                       hg::Bytes{"prices"}),
      hgk::make_cursor("fabric-fake-transport", 1, hg::Str{TOPIC}, 0, 2),
      hgk::KafkaSubscriptionState::Live);
  runner.join();

  std::vector<std::int64_t> sequence;
  {
    std::scoped_lock lock{observed_sequence_mutex};
    sequence = observed_sequence;
  }
  CHECK((sequence == std::vector<std::int64_t>{1, 2}));
  CHECK(fake_broker->committed_cursors().size() == 2);
  CHECK(fake_broker->attach_count() == 1);
  CHECK(fake_broker->wait_until_detached(2s));
  fake_broker.reset();
  kafka_config = {};
}

TEST_CASE("manual Kafka assignment recovers after every broker disconnects") {
  hg::stdlib::register_standard_operators();
  hgf::register_fabric_operators();
  hgk::register_kafka_types();

  hgk::testing::MockCluster cluster{1};
  const hg::Str topic{"hgraph-fabric-manual-reconnect"};
  cluster.create_topic(topic, 1, 1);
  auto config = hgf::make_memory_fabric_config("tests/kafka-manual-reconnect");
  const auto first = seed(config, 1, 1);
  cluster.seed_record(topic, revision_bytes(first), hg::Bytes{"prices"});

  actual_topic = topic;
  actual_control_dir =
      std::filesystem::temp_directory_path() /
      ("hgraph-fabric-kafka-reconnect-" +
       std::to_string(
           std::chrono::steady_clock::now().time_since_epoch().count()));
  std::filesystem::create_directories(actual_control_dir);
  actual_kafka_config = hgk::service_config()
                            .bootstrap_servers({cluster.bootstrap_servers()})
                            .client_id("hgraph-fabric-manual-reconnect")
                            .ingress_limit(8)
                            .outbound_limit(2)
                            .shutdown_drain_timeout(2s)
                            .producer_option("message.timeout.ms", "750")
                            .build();
  actual_live_values.clear();
  actual_diagnostics.clear();
  actual_events.clear();

  auto graph = build_realtime_graph<ActualBrokerGraph>();
  hgf::set_fabric_config(graph.global_state(), config);
  const hg::DateTime start = hg::testing::wall_now();
  hg::GraphExecutorBuilder builder;
  builder.graph_builder(std::move(graph))
      .mode(hg::GraphExecutorMode::RealTime)
      .start_time(start)
      .end_time(start + hg::TimeDelta{20'000'000});
  auto executor = builder.make_executor();
  auto view = executor.view();
  hg::testing::AsyncGraphExecutorRun runner{view};

  if (!wait_until(
          [] { return std::filesystem::exists(marker("initial-ready")); },
          5s)) {
    view.request_stop();
    runner.join();
    FAIL("mock broker subscription did not expose the initial image");
  }
  cluster.stop_brokers();
  write_marker("broker-stopped");
  const bool second_is_durable = wait_until(
      [&config] {
        const auto latest =
            config.objects.get(hgf::latest_key(config.prefix, "prices"));
        return latest.has_value() &&
               hgf::revision_reference_value(config.values, hgf::MetadataObjectKind::Latest, latest->data) == 2;
      },
      5s);
  if (!second_is_durable ||
      !wait_until(
          [] {
            return std::filesystem::exists(marker("delivery-failed-retriable"));
          },
          5s)) {
    cluster.start_brokers();
    write_marker("broker-restarted");
    view.request_stop();
    runner.join();
    FAIL("mock outage did not retain a retriable durable publication");
  }
  write_marker("publication-durable");
  cluster.start_brokers();
  write_marker("broker-restarted");
  runner.join();

  CHECK((actual_live_values == std::vector<std::int64_t>{1, 2}));
  CHECK(std::filesystem::exists(marker("live-complete")));
  std::error_code ignored;
  std::filesystem::remove_all(actual_control_dir, ignored);
  actual_kafka_config = {};
}

TEST_CASE("actual broker preserves Fabric recovery, retry, and ordering",
          "[.kafka-broker]") {
  const auto bootstrap =
      hg::environment_variable("HGRAPH_FABRIC_KAFKA_INTEGRATION_BOOTSTRAP");
  const auto topic =
      hg::environment_variable("HGRAPH_FABRIC_KAFKA_INTEGRATION_TOPIC");
  const auto control =
      hg::environment_variable("HGRAPH_FABRIC_KAFKA_INTEGRATION_CONTROL_DIR");
  if (!bootstrap && !topic && !control) {
    SKIP("run through run_kafka_broker_conformance.py");
  }
  REQUIRE(bootstrap.has_value());
  REQUIRE(topic.has_value());
  REQUIRE(control.has_value());

  hg::stdlib::register_standard_operators();
  hgf::register_fabric_operators();
  hgk::register_kafka_types();

  actual_topic = *topic;
  actual_control_dir = *control;
  std::filesystem::create_directories(actual_control_dir);
  actual_kafka_config = hgk::service_config()
                            .bootstrap_servers({*bootstrap})
                            .client_id("hgraph-fabric-broker-conformance")
                            .ingress_limit(8)
                            .outbound_limit(2)
                            .shutdown_drain_timeout(2s)
                            .common_option("reconnect.backoff.ms", "100")
                            .common_option("reconnect.backoff.max.ms", "500")
                            .producer_option("message.timeout.ms", "1500")
                            .build();

  auto config = hgf::make_memory_fabric_config("tests/actual-kafka-broker");
  const auto first = seed(config, 1, 1);
  actual_notice_record = hgk::make_produce_record(
      revision_bytes(first), hg::Bytes{"prices"}, {}, std::nullopt,
      std::nullopt, "fabric-broker-seed");
  actual_delivery_report = {};

  {
    const hg::DateTime start = hg::testing::wall_now();
    hg::GraphExecutorBuilder builder;
    builder.graph_builder(build_realtime_graph<ActualNoticeGraph>())
        .mode(hg::GraphExecutorMode::RealTime)
        .start_time(start)
        .end_time(start + hg::TimeDelta{15'000'000});
    builder.make_executor().view().run();
  }
  REQUIRE(actual_delivery_report.view().data() != nullptr);
  CHECK(actual_delivery_report.view()
            .as_bundle()
            .at("status")
            .checked_as<hgk::KafkaDeliveryStatus>() ==
        hgk::KafkaDeliveryStatus::Delivered);

  actual_live_values.clear();
  actual_diagnostics.clear();
  actual_events.clear();
  auto graph = build_realtime_graph<ActualBrokerGraph>();
  hgf::set_fabric_config(graph.global_state(), config);
  const hg::DateTime start = hg::testing::wall_now();
  hg::GraphExecutorBuilder builder;
  builder.graph_builder(std::move(graph))
      .mode(hg::GraphExecutorMode::RealTime)
      .start_time(start)
      .end_time(start + hg::TimeDelta{45'000'000});
  auto executor = builder.make_executor();
  auto view = executor.view();
  hg::testing::AsyncGraphExecutorRun runner{view};

  if (!wait_until(
          [] { return std::filesystem::exists(marker("initial-ready")); },
          20s)) {
    view.request_stop();
    runner.join();
    FAIL("Fabric did not expose its durable image after broker subscription");
  }
  if (!wait_until(
          [] { return std::filesystem::exists(marker("broker-stopped")); },
          20s)) {
    view.request_stop();
    runner.join();
    FAIL("broker controller did not stop the broker");
  }
  const bool second_is_durable = wait_until(
      [&config] {
        const auto latest =
            config.objects.get(hgf::latest_key(config.prefix, "prices"));
        return latest.has_value() &&
               hgf::revision_reference_value(config.values, hgf::MetadataObjectKind::Latest, latest->data) == 2;
      },
      20s);
  if (!second_is_durable) {
    view.request_stop();
    runner.join();
    FAIL("Fabric did not durably accept the outage publication");
  }
  write_marker("publication-durable");
  if (!wait_until(
          [] {
            return std::filesystem::exists(marker("delivery-failed-retriable"));
          },
          20s)) {
    view.request_stop();
    runner.join();
    FAIL("Kafka did not report a retriable delivery failure during outage");
  }
  if (!wait_until(
          [] { return std::filesystem::exists(marker("broker-restarted")); },
          20s)) {
    view.request_stop();
    runner.join();
    FAIL("broker controller did not restart the broker");
  }
  runner.join();

  CHECK((actual_live_values == std::vector<std::int64_t>{1, 2}));
  CHECK(std::filesystem::exists(marker("live-complete")));
  CHECK(actual_diagnostics.at("publication.queue_limit_per_data_id") == "1024");
  CHECK(actual_diagnostics.at("live.notice_limit_per_session") == "4096");
  CHECK(actual_diagnostics.at("transport.notification.retried") != "0");
  const auto retriable = std::ranges::find_if(
      actual_events, [](const auto &entry) { return entry.second.retriable; });
  REQUIRE(retriable != actual_events.end());
  CHECK_FALSE(retriable->second.fatal);

  actual_audit_key =
      hgk::subscription_key()
          .topics({actual_topic})
          .group_id("fabric-broker-audit")
          .assignment_mode(hgk::KafkaAssignmentMode::Independent)
          .start(
              hgk::make_start_position(hgk::KafkaStartPositionKind::Earliest))
          .stop(hgk::make_stop_position(hgk::KafkaStopPositionKind::Snapshot))
          .sharing_identity("fabric-broker-audit")
          .build();
  actual_broker_records.clear();
  {
    const hg::DateTime audit_start = hg::testing::wall_now();
    hg::GraphExecutorBuilder audit_builder;
    audit_builder.graph_builder(build_realtime_graph<ActualBrokerAuditGraph>())
        .mode(hg::GraphExecutorMode::RealTime)
        .start_time(audit_start)
        .end_time(audit_start + hg::TimeDelta{20'000'000});
    audit_builder.make_executor().view().run();
  }

  REQUIRE(actual_broker_records.size() >= 2);
  CHECK(actual_broker_records.front().revision.revision == 1);
  CHECK(actual_broker_records.back().revision.revision == 2);
  for (std::size_t index = 0; index < actual_broker_records.size(); ++index) {
    const auto &record = actual_broker_records[index];
    CHECK(record.key.data == "prices");
    CHECK(record.revision.data_id == "prices");
    CHECK(record.partition == actual_broker_records.front().partition);
    if (index > 0) {
      CHECK(record.offset > actual_broker_records[index - 1].offset);
      CHECK(record.revision.revision >=
            actual_broker_records[index - 1].revision.revision);
    }
  }

  actual_kafka_config = {};
  actual_notice_record = {};
  actual_delivery_report = {};
  actual_audit_key = {};
}
