#include <hgraph/fabric/fabric.h>
#include <hgraph/fabric/kafka.h>

#include <hgraph/kafka/testing/fake_broker.h>
#include <hgraph/kafka/testing/mock_cluster.h>
#include <hgraph/kafka/value_builders.h>

#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/static_node.h>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/table.h>

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
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
      hgf::encode_revision(value.view()));
  if (revision_write.status ==
      hg::persistence::store::ImmutableWriteStatus::Conflict) {
    throw std::runtime_error("Fabric Kafka test revision conflicted");
  }
  const auto as_of_write = config.objects.put_immutable(
      hgf::as_of_key(config.prefix, "prices", decoded.as_of),
      hgf::encode_revision_reference(hgf::MetadataObjectKind::AsOf, revision));
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
      hgf::encode_revision_reference(hgf::MetadataObjectKind::Latest,
                                     revision));
  if (!latest.exchanged) {
    throw std::runtime_error("Fabric Kafka test latest update lost a race");
  }
  return decoded;
}

[[nodiscard]] hg::Bytes revision_bytes(const hgf::DataRevisionInput &revision) {
  const auto encoded =
      hgf::encode_revision(hgf::make_data_revision(revision).view());
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
    observed_sequence.push_back(frame_value(value.value()));
    if (observed_sequence.size() == 2) {
      node.graph().executor().request_stop();
    }
  }
};

struct KafkaFabricGraph {
  static constexpr auto name = "hgraph.fabric.kafka.test.graph";

  static void compose(hg::Wiring &wiring) {
    hgf::register_kafka_transport(wiring, hg::Str{TOPIC},
                                  hg::Str{"fabric-transport-test"},
                                  kafka_config.clone());
    auto subscribed =
        hgf::subscribe_data(wiring, "prices", hgf::SubscriptionMode::Live);
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
        hgf::subscribe_data(wiring, "prices", hgf::SubscriptionMode::Live);
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

  auto graph = hg::build_graph<KafkaFabricGraph>();
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
  CHECK(hgf::decode_revision_reference(hgf::MetadataObjectKind::Latest,
                                       latest->data) == 1);
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
  observed_sequence.clear();

  auto graph = hg::build_graph<KafkaFabricFakeGraph>();
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

  const auto second = seed(config, 2, 2);
  fake_broker->emit_subscription(
      subscription_key.clone(),
      hgk::make_record(hg::Str{TOPIC}, 0, 1, revision_bytes(second),
                       hg::Bytes{"prices"}),
      hgk::make_cursor("fabric-fake-transport", 1, hg::Str{TOPIC}, 0, 2),
      hgk::KafkaSubscriptionState::Live);
  runner.join();

  CHECK((observed_sequence == std::vector<std::int64_t>{1, 2}));
  CHECK(fake_broker->committed_cursors().size() == 2);
  CHECK(fake_broker->attach_count() == 1);
  CHECK(fake_broker->wait_until_detached(2s));
  fake_broker.reset();
  kafka_config = {};
}
