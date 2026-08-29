#include <hgraph/kafka/service.h>
#include <hgraph/kafka/testing/mock_cluster.h>
#include <hgraph/kafka/value_builders.h>
#include <hgraph/persistence/frame_store.h>
#include <hgraph/persistence/object_store.h>

#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/table_codec.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace {
namespace hg = hgraph;
namespace hgf = hgraph::persistence::store;
namespace hgk = hgraph::kafka;

using namespace std::chrono_literals;

constexpr std::string_view FABRIC_TOPIC{"hgraph-fabric-dependency-probe"};
constexpr std::string_view DATA_ID{"prices/reference"};
constexpr std::string_view REVISION_PAYLOAD{"revision-1"};
constexpr std::string_view CANDIDATE_TOKEN{"prices/reference:1"};

hg::Value service_config_value{};
hg::Value subscription_key_value{};

struct KafkaObservation {
  bool recovering{};
  bool live{};
  bool delivered{};
  bool received{};
  std::string last_event{};
};

KafkaObservation kafka_observation{};

void require(bool condition, std::string message) {
  if (!condition) {
    throw std::runtime_error(std::move(message));
  }
}

template <typename Fn> void require_invalid(Fn &&fn, std::string message) {
  try {
    std::forward<Fn>(fn)();
  } catch (const std::invalid_argument &) {
    return;
  }
  throw std::runtime_error(std::move(message));
}

[[nodiscard]] std::span<const std::byte>
bytes_of(std::string_view value) noexcept {
  return std::as_bytes(std::span{value.data(), value.size()});
}

/** Validate the immutable Kafka choices required by RFC 0026.
 *
 * This consumer-side profile is intentionally not part of hgraph-kafka:
 * Drop remains a valid policy for general Kafka graphs, while fabric must
 * reject it before wiring its service. The future fabric configuration
 * owns this validation and can implement it entirely through public value
 * schemas, as this installed probe demonstrates.
 */
void require_fabric_service_profile(const hg::Value &config) {
  if (config.schema() !=
      hg::scalar_descriptor<hgk::KafkaServiceConfig>::value_meta()) {
    throw std::invalid_argument("fabric requires KafkaServiceConfig");
  }

  const auto root = config.view().as_bundle();
  const auto consumer = root.at("consumer_defaults").as_bundle();
  const auto producer = root.at("producer").as_bundle();
  const auto acknowledgements =
      producer.at("acknowledgements").checked_as<hg::Str>();

  if (!producer.at("idempotent").checked_as<hg::Bool>() ||
      (acknowledgements != "all" && acknowledgements != "-1")) {
    throw std::invalid_argument("fabric requires idempotent Kafka production "
                                "with acknowledgements=all");
  }
  if (consumer.at("inbound_overflow").checked_as<hgk::KafkaOverflowAction>() !=
      hgk::KafkaOverflowAction::Fail) {
    throw std::invalid_argument("fabric Kafka ingress cannot drop records");
  }

  const auto outbound =
      producer.at("overflow").checked_as<hgk::KafkaOverflowAction>();
  const auto staged =
      producer.at("stage_overflow").checked_as<hgk::KafkaOverflowAction>();
  if (outbound == hgk::KafkaOverflowAction::Drop ||
      (outbound == hgk::KafkaOverflowAction::Stage &&
       staged != hgk::KafkaOverflowAction::Fail)) {
    throw std::invalid_argument("fabric Kafka publication cannot drop records");
  }
}

/** Validate the no-gap notification subscription selected by RFC 0026.
 *
 * Committed-with-earliest-fallback retains a proposal which Kafka
 * acknowledges just before its immutable revision slot becomes visible.
 * Independent assignment consumes every partition for this fabric
 * instance, and explicit commits remain an optimisation performed only
 * after durable validation; offsets never become authoritative state.
 */
void require_fabric_subscription_profile(const hg::Value &key,
                                         std::string_view topic) {
  if (key.schema() !=
      hg::scalar_descriptor<hgk::KafkaSubscriptionKey>::value_meta()) {
    throw std::invalid_argument("fabric requires KafkaSubscriptionKey");
  }

  const auto fields = key.view().as_bundle();
  const auto topics = fields.at("topics").as_list();
  const auto start = fields.at("start_position").as_bundle();
  const auto stop = fields.at("stop_position").as_bundle();

  if (fields.at("selector_kind").checked_as<hgk::KafkaSelectorKind>() !=
          hgk::KafkaSelectorKind::Topics ||
      topics.size() != 1 || topics.front().checked_as<hg::Str>() != topic) {
    throw std::invalid_argument(
        "fabric must consume its complete configured topic");
  }
  if (fields.at("assignment_mode").checked_as<hgk::KafkaAssignmentMode>() !=
      hgk::KafkaAssignmentMode::Independent) {
    throw std::invalid_argument(
        "fabric must receive every configured topic partition");
  }
  if (fields.at("group_id").checked_as<hg::Str>().empty() ||
      fields.at("sharing_identity").checked_as<hg::Str>().empty()) {
    throw std::invalid_argument(
        "fabric requires stable Kafka group and subscription identities");
  }
  if (start.at("kind").checked_as<hgk::KafkaStartPositionKind>() !=
          hgk::KafkaStartPositionKind::Committed ||
      start.at("fallback").checked_as<hgk::KafkaOffsetFallback>() !=
          hgk::KafkaOffsetFallback::Earliest) {
    throw std::invalid_argument(
        "fabric requires committed Kafka startup with earliest fallback");
  }
  if (stop.at("kind").checked_as<hgk::KafkaStopPositionKind>() !=
          hgk::KafkaStopPositionKind::Unbounded ||
      fields.at("commit_mode").checked_as<hgk::KafkaCommitMode>() !=
          hgk::KafkaCommitMode::Explicit) {
    throw std::invalid_argument(
        "fabric requires an unbounded subscription with explicit commits");
  }
  if (fields.at("key_filter").data() != nullptr) {
    throw std::invalid_argument(
        "fabric filters its dynamic data-id closure after Kafka ingress");
  }
}

void check_profile_validation(std::string_view bootstrap_servers) {
  service_config_value =
      hgk::service_config()
          .bootstrap_servers({hg::Str{bootstrap_servers}})
          .client_id(hg::Str{"hgraph-fabric-dependency-probe"})
          .build();
  require_fabric_service_profile(service_config_value);

  require_invalid(
      [&] {
        require_fabric_service_profile(
            hgk::service_config()
                .bootstrap_servers({hg::Str{bootstrap_servers}})
                .idempotent_producer(false)
                .build());
      },
      "fabric profile accepted non-idempotent Kafka production");
  require_invalid(
      [&] {
        require_fabric_service_profile(
            hgk::service_config()
                .bootstrap_servers({hg::Str{bootstrap_servers}})
                .idempotent_producer(false)
                .producer_acknowledgements(hg::Str{"1"})
                .build());
      },
      "fabric profile accepted Kafka acknowledgements=1");
  require_invalid(
      [&] {
        require_fabric_service_profile(
            hgk::service_config()
                .bootstrap_servers({hg::Str{bootstrap_servers}})
                .inbound_overflow(hgk::KafkaOverflowAction::Drop)
                .build());
      },
      "fabric profile accepted dropping Kafka ingress");
  require_invalid(
      [&] {
        require_fabric_service_profile(
            hgk::service_config()
                .bootstrap_servers({hg::Str{bootstrap_servers}})
                .outbound_overflow(hgk::KafkaOverflowAction::Drop)
                .build());
      },
      "fabric profile accepted dropping Kafka publication");
  require_invalid(
      [&] {
        require_fabric_service_profile(
            hgk::service_config()
                .bootstrap_servers({hg::Str{bootstrap_servers}})
                .outbound_overflow(hgk::KafkaOverflowAction::Stage,
                                   hgk::KafkaOverflowAction::Drop)
                .build());
      },
      "fabric profile accepted dropping a full Kafka staging queue");

  subscription_key_value =
      hgk::subscription_key()
          .topics({hg::Str{FABRIC_TOPIC}})
          .group_id(hg::Str{"hgraph-fabric-dependency-probe"})
          .assignment_mode(hgk::KafkaAssignmentMode::Independent)
          .start(
              hgk::make_start_position(hgk::KafkaStartPositionKind::Committed,
                                       hgk::KafkaOffsetFallback::Earliest))
          .stop(hgk::make_stop_position(hgk::KafkaStopPositionKind::Unbounded))
          .commit_mode(hgk::KafkaCommitMode::Explicit)
          .sharing_identity(hg::Str{"hgraph-fabric-dependency-probe"})
          .build();
  require_fabric_subscription_profile(subscription_key_value, FABRIC_TOPIC);

  require_invalid(
      [&] {
        require_fabric_subscription_profile(
            hgk::subscription_key()
                .topics({hg::Str{FABRIC_TOPIC}})
                .group_id(hg::Str{"unsafe-fabric-latest"})
                .assignment_mode(hgk::KafkaAssignmentMode::Independent)
                .start(hgk::make_start_position(
                    hgk::KafkaStartPositionKind::Latest))
                .stop(hgk::make_stop_position(
                    hgk::KafkaStopPositionKind::Unbounded))
                .commit_mode(hgk::KafkaCommitMode::Explicit)
                .sharing_identity(hg::Str{"unsafe-fabric-latest"})
                .build(),
            FABRIC_TOPIC);
      },
      "fabric profile accepted a latest-only Kafka subscription");
  require_invalid(
      [&] {
        require_fabric_subscription_profile(
            hgk::subscription_key()
                .topics({hg::Str{FABRIC_TOPIC}})
                .group_id(hg::Str{"unsafe-fabric-group"})
                .assignment_mode(hgk::KafkaAssignmentMode::Group)
                .start(hgk::make_start_position(
                    hgk::KafkaStartPositionKind::Committed,
                    hgk::KafkaOffsetFallback::Earliest))
                .stop(hgk::make_stop_position(
                    hgk::KafkaStopPositionKind::Unbounded))
                .commit_mode(hgk::KafkaCommitMode::Explicit)
                .sharing_identity(hg::Str{"unsafe-fabric-group"})
                .build(),
            FABRIC_TOPIC);
      },
      "fabric profile accepted partition-sharing group assignment");
  require_invalid(
      [&] {
        require_fabric_subscription_profile(
            hgk::subscription_key()
                .topics({hg::Str{FABRIC_TOPIC}})
                .group_id(hg::Str{"unsafe-fabric-filter"})
                .assignment_mode(hgk::KafkaAssignmentMode::Independent)
                .start(hgk::make_start_position(
                    hgk::KafkaStartPositionKind::Committed,
                    hgk::KafkaOffsetFallback::Earliest))
                .stop(hgk::make_stop_position(
                    hgk::KafkaStopPositionKind::Unbounded))
                .commit_mode(hgk::KafkaCommitMode::Explicit)
                .sharing_identity(hg::Str{"unsafe-fabric-filter"})
                .key_filter(hg::Bytes{std::string{DATA_ID}})
                .build(),
            FABRIC_TOPIC);
      },
      "fabric profile accepted Kafka-side data-id filtering");
}

void check_persistence_contract() {
  auto object_store = hgf::make_object_store(hgf::ObjectStoreConfig{});
  const auto revision = bytes_of(REVISION_PAYLOAD);
  const auto created = object_store.put_immutable(
      "fabric/prices/revision/00000000000000000001", revision);
  require(created.status == hgf::ImmutableWriteStatus::Created,
          "installed object store did not create immutable revision metadata");
  require(
      object_store
              .put_immutable("fabric/prices/revision/00000000000000000001",
                             revision)
              .status == hgf::ImmutableWriteStatus::Unchanged,
      "installed object store did not recognise an idempotent immutable retry");
  require(
      object_store
              .put_immutable("fabric/prices/revision/00000000000000000001",
                             bytes_of("conflict"))
              .status == hgf::ImmutableWriteStatus::Conflict,
      "installed object store did not reject conflicting immutable metadata");

  const auto loaded =
      object_store.get("fabric/prices/revision/00000000000000000001");
  require(loaded.has_value() && loaded->data.size() == revision.size() &&
              std::equal(loaded->data.begin(), loaded->data.end(),
                         revision.begin()),
          "installed object store did not read immutable revision metadata");
  const auto page = object_store.list("fabric/prices/revision/", {}, 1);
  require(page.objects.size() == 1 &&
              page.objects.front().key ==
                  "fabric/prices/revision/00000000000000000001",
          "installed object store did not list ordered revision metadata");

  const auto first_head = object_store.compare_exchange_ref(
      "fabric/prices/latest", {}, bytes_of("1"));
  require(first_head.exchanged && first_head.current.has_value(),
          "installed object store did not create the latest reference");
  const auto next_head = object_store.compare_exchange_ref(
      "fabric/prices/latest", first_head.current->version_token, bytes_of("2"));
  require(
      next_head.exchanged && next_head.current.has_value(),
      "installed object store did not compare/exchange the latest reference");

  hg::FrameRecorder recorder{
      hg::table_converter(hg::scalar_descriptor<hg::Int>::value_meta())};
  const hg::Value value{hg::Int{42}};
  recorder.append(hg::MIN_ST, hg::MIN_ST, value.view());
  auto frame_store = hgf::make_frame_store(hgf::FrameStoreConfig{});
  frame_store.write("fabric/prices/data/00000000000000000001",
                    recorder.finish());
  const hg::Frame frame =
      frame_store.read("fabric/prices/data/00000000000000000001");
  require(hg::frame_rows(frame) == 1 &&
              hg::read_row(hg::table_converter(
                               hg::scalar_descriptor<hg::Int>::value_meta()),
                           frame, 0)
                      .view()
                      .checked_as<hg::Int>() == hg::Int{42},
          "installed Frame store did not round-trip the fabric payload");
}

/** Test-only sink joining the asynchronous subscribe and publish results.
 *
 * It performs O(1) field checks per tick, retains only four booleans and
 * requests graph stop after both broker paths complete. A primitive sink
 * is appropriate here because this is an installed-SDK assertion boundary,
 * not reusable graph behavior or an operator implementation.
 */
struct FabricKafkaProbeCapture {
  static constexpr auto name = "fabric_kafka_probe_capture";

  static void eval(hg::NodeView node,
                   hg::In<"subscription", hgk::KafkaSubscriptionOutput,
                          hg::InputValidity::Unchecked>
                       subscription,
                   hg::In<"delivery", hg::TS<hgk::KafkaDeliveryReport>,
                          hg::InputValidity::Unchecked>
                       delivery) {
    if (delivery.valid() && delivery.modified()) {
      const auto report = delivery.base().value().as_bundle();
      require(report.at("status").checked_as<hgk::KafkaDeliveryStatus>() ==
                  hgk::KafkaDeliveryStatus::Delivered,
              "installed Kafka producer did not report broker delivery");
      require(report.at("user_token").checked_as<hg::Str>() == CANDIDATE_TOKEN,
              "installed Kafka delivery report lost candidate correlation");
      kafka_observation.delivered = true;
    }

    const auto state = subscription.template field<"state">();
    if (state.valid() && state.modified()) {
      kafka_observation.recovering =
          kafka_observation.recovering ||
          state.value() == hgk::KafkaSubscriptionState::Recovering;
      kafka_observation.live =
          kafka_observation.live ||
          state.value() == hgk::KafkaSubscriptionState::Live;
    }

    const auto record = subscription.template field<"record">();
    if (record.valid() && record.modified()) {
      const auto fields = record.base().value().concrete().as_bundle();
      require(fields.at("topic").checked_as<hg::Str>() == FABRIC_TOPIC,
              "installed Kafka subscription received the wrong topic");
      require(fields.at("key").checked_as<hg::Bytes>().data == DATA_ID,
              "installed Kafka subscription lost the canonical data-id key");
      require(fields.at("value").checked_as<hg::Bytes>().data ==
                  REVISION_PAYLOAD,
              "installed Kafka subscription changed the revision payload");
      kafka_observation.received = true;
    }

    if (kafka_observation.recovering && kafka_observation.live &&
        kafka_observation.delivered && kafka_observation.received) {
      node.graph().executor().request_stop();
    }
  }
};

/** Test-only diagnostic sink retaining the last transport event so a
 * failed installed probe reports useful public API context. Cost and
 * storage are O(message size) only when the diagnostic stream ticks. */
struct FabricKafkaEventCapture {
  static constexpr auto name = "fabric_kafka_event_capture";

  static void eval(hg::In<"event", hg::TS<hgk::KafkaEvent>> event) {
    const auto fields = event.base().value().as_bundle();
    kafka_observation.last_event = fields.at("category").checked_as<hg::Str>() +
                                   ": " +
                                   fields.at("message").checked_as<hg::Str>();
  }
};

struct FabricKafkaDependencyGraph {
  static constexpr auto name = "fabric_kafka_dependency_graph";

  static void compose(hg::Wiring &wiring) {
    const auto path = hg::service::path("fabric-dependency-probe");
    hgk::register_service(wiring, path, service_config_value.clone());

    auto key = hg::wire<hg::stdlib::const_, hg::TS<hgk::KafkaSubscriptionKey>>(
        wiring, subscription_key_value.clone());
    auto subscription = hgk::subscribe(wiring, path, key);

    auto candidate =
        hg::wire<hg::stdlib::const_, hg::TS<hgk::KafkaProduceRecord>>(
            wiring, hgk::make_produce_record(
                        hg::Bytes{std::string{REVISION_PAYLOAD}},
                        hg::Bytes{std::string{DATA_ID}}, {}, std::nullopt,
                        std::nullopt, hg::Str{CANDIDATE_TOKEN}));
    auto delivery = hgk::publish(
        wiring, path,
        hgk::publish_request(wiring, hg::Str{FABRIC_TOPIC}, candidate));

    static_cast<void>(
        hg::wire<FabricKafkaProbeCapture>(wiring, subscription, delivery));
    static_cast<void>(
        hg::wire<FabricKafkaEventCapture>(wiring, hgk::events(wiring, path)));
  }
};

void check_kafka_contract(hgk::testing::MockCluster &cluster) {
  kafka_observation = {};
  check_profile_validation(cluster.bootstrap_servers());

  const hg::DateTime start = hg::testing::wall_now();
  hg::GraphExecutorBuilder builder;
  builder
      .graph_builder(hg::build_graph<FabricKafkaDependencyGraph>(
          hg::WiringOptions{.is_realtime = true}))
      .mode(hg::GraphExecutorMode::RealTime)
      .start_time(start)
      .end_time(start + hg::TimeDelta{15'000'000});
  hg::GraphExecutorValue executor = builder.make_executor();
  auto view = executor.view();
  view.run();

  const std::string diagnostic =
      kafka_observation.last_event.empty()
          ? std::string{}
          : " (last Kafka event: " + kafka_observation.last_event + ')';
  require(kafka_observation.recovering,
          "installed Kafka subscription did not expose Recovering" +
              diagnostic);
  require(kafka_observation.live,
          "installed Kafka subscription did not expose Live" + diagnostic);
  require(kafka_observation.delivered,
          "installed Kafka producer did not expose correlated delivery" +
              diagnostic);
  require(kafka_observation.received,
          "installed Kafka subscription did not receive the keyed revision" +
              diagnostic);
}
} // namespace

int main() {
  try {
    hg::stdlib::register_standard_operators();
    hgk::register_kafka_types();
    [[maybe_unused]] auto clear_probe_values = hg::make_scope_exit([] noexcept {
      service_config_value = {};
      subscription_key_value = {};
    });

    check_persistence_contract();

    hgk::testing::MockCluster cluster;
    cluster.create_topic(hg::Str{FABRIC_TOPIC}, 3, 1);
    check_kafka_contract(cluster);

    std::cout << "hgraph fabric dependency consumer passed\n";
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "hgraph fabric dependency consumer failed: " << error.what()
              << '\n';
    return 1;
  }
}
