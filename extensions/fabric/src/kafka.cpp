#include <hgraph/fabric/kafka.h>

#include <hgraph/fabric/metadata_codec.h>
#include <hgraph/fabric/value_builders.h>

#include <hgraph/kafka/service.h>
#include <hgraph/kafka/value_builders.h>

#include <hgraph/lib/std/operators/collection.h>
#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/types/static_node.h>

#include <charconv>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace hgraph::fabric {
namespace {
inline constexpr char TOKEN_SEPARATOR{'\n'};

[[nodiscard]] std::span<const std::byte>
bytes_view(const Bytes &value) noexcept {
  return std::as_bytes(std::span{value.data.data(), value.data.size()});
}

[[nodiscard]] Bytes kafka_bytes(const persistence::store::ObjectBytes &value) {
  return Bytes{
      std::string{reinterpret_cast<const char *>(value.data()), value.size()}};
}

[[nodiscard]] Str delivery_token(std::string_view data_id,
                                 RevisionId revision) {
  return Str{data_id} + TOKEN_SEPARATOR + std::to_string(revision);
}

[[nodiscard]] std::pair<Str, RevisionId>
parse_delivery_token(std::string_view token) {
  const auto separator = token.rfind(TOKEN_SEPARATOR);
  if (separator == std::string_view::npos) {
    throw std::invalid_argument("fabric Kafka delivery token is malformed");
  }
  Str data_id{token.substr(0, separator)};
  require_data_id(data_id);
  RevisionId revision{};
  const std::string_view encoded = token.substr(separator + 1);
  const auto [end, error] = std::from_chars(
      encoded.data(), encoded.data() + encoded.size(), revision);
  if (error != std::errc{} || end != encoded.data() + encoded.size() ||
      revision <= 0) {
    throw std::invalid_argument(
        "fabric Kafka delivery token has an invalid revision");
  }
  return {std::move(data_id), revision};
}

struct FabricKafkaDecodeNode {
  static constexpr auto name = "hgraph.fabric.kafka.decode_revision";

  /** O(payload size) per Kafka record; no retained state. The complete
      revision is canonicalised once into shared storage before it enters the
      Fabric service edge. */
  static void eval(In<"record", TS<Shared<kafka::KafkaRecord>>> record,
                   Out<TS<Shared<DataRevision>>> out) {
    const auto fields = record.base().value().concrete().as_bundle();
    const auto key = fields.at("key");
    const auto payload = fields.at("value");
    if (key.data() == nullptr || payload.data() == nullptr) {
      throw std::invalid_argument(
          "fabric Kafka record requires a key and revision payload");
    }
    const Bytes &key_bytes = key.checked_as<Bytes>();
    const Bytes &payload_bytes = payload.checked_as<Bytes>();
    require_metadata_within_limit(payload_bytes.data.size());
    Value revision = notification_codec().decode(data_revision_meta(),
                                                 bytes_view(payload_bytes));
    const DataRevisionInput decoded = data_revision_input(revision.view());
    // A broker record is as untrusted as a stored document.
    validate_data_revision(decoded);
    if (key_bytes.data != decoded.data_id) {
      throw std::invalid_argument(
          "fabric Kafka record key does not match its revision data id");
    }
    out.apply(revision.view());
  }
};

struct FabricKafkaValidatedCursorNode {
  static constexpr auto name = "hgraph.fabric.kafka.validated_cursor";

  static void eval(In<"revision", TS<Shared<DataRevision>>> revision,
                   In<"cursor", TS<kafka::KafkaCursor>> cursor,
                   Out<TS<kafka::KafkaCursor>> out) {
    if (revision.modified() && cursor.valid()) {
      out.apply(cursor.base().value());
    }
  }
};

struct FabricKafkaProduceRecordNode {
  static constexpr auto name = "hgraph.fabric.kafka.encode_revision";

  /** O(revision metadata size) per durable publication; no retained state. */
  static void eval(In<"revision", TS<Shared<DataRevision>>> revision,
                   Out<TS<kafka::KafkaProduceRecord>> out) {
    const DataRevisionInput decoded =
        data_revision_input(revision.base().value().concrete());
    persistence::store::ObjectBytes payload;
    notification_codec().encode(revision.base().value().concrete(), payload);
    Value record = kafka::make_produce_record(
        kafka_bytes(payload), Bytes{decoded.data_id}, {}, std::nullopt,
        std::nullopt, delivery_token(decoded.data_id, decoded.revision));
    out.apply(record.view());
  }
};

struct FabricKafkaDeliveryNode {
  static constexpr auto name = "hgraph.fabric.kafka.delivery";

  static void eval(In<"report", TS<kafka::KafkaDeliveryReport>> report,
                   Out<FabricNotificationDelivery> out) {
    const auto fields = report.base().value().as_bundle();
    auto [data_id, revision] =
        parse_delivery_token(fields.at("user_token").checked_as<Str>());
    const auto status =
        fields.at("status").checked_as<kafka::KafkaDeliveryStatus>();
    const bool delivered = status == kafka::KafkaDeliveryStatus::Delivered;
    const bool retriable =
        !delivered && fields.at("retriable").checked_as<Bool>();
    out.template field<"data_id">().set(std::move(data_id));
    out.template field<"revision">().set(revision);
    out.template field<"delivered">().set(delivered);
    out.template field<"retriable">().set(retriable);
    out.template field<"message">().set(fields.at("message").checked_as<Str>());
  }
};

struct FabricKafkaControlNode {
  static constexpr auto name = "hgraph.fabric.kafka.control";

  static void eval(In<"state", TS<kafka::KafkaSubscriptionState>> state,
                   Out<FabricTransportControl> out) {
    const auto value = state.value();
    const bool ready = value == kafka::KafkaSubscriptionState::Recovering ||
                       value == kafka::KafkaSubscriptionState::Live;
    const bool reconcile = ready;
    const bool failed = value == kafka::KafkaSubscriptionState::Failed;
    out.template field<"ready">().set(ready);
    out.template field<"reconcile">().set(reconcile);
    out.template field<"failed">().set(failed);
    out.template field<"message">().set(
        failed ? Str{"fabric Kafka subscription failed"} : Str{});
  }
};

struct FabricKafkaEventNode {
  static constexpr auto name = "hgraph.fabric.kafka.event";

  static void eval(In<"event", TS<kafka::KafkaEvent>> event,
                   Out<FabricTransportEvent> out) {
    const auto fields = event.base().value().as_bundle();
    out.template field<"component">().set(
        fields.at("component").checked_as<Str>());
    out.template field<"category">().set(
        fields.at("category").checked_as<Str>());
    out.template field<"message">().set(fields.at("message").checked_as<Str>());
    out.template field<"retriable">().set(
        fields.at("retriable").checked_as<Bool>());
    out.template field<"fatal">().set(fields.at("fatal").checked_as<Bool>());
  }
};

void require_topic_and_identity(std::string_view topic,
                                std::string_view identity) {
  if (topic.empty() || topic.find('\0') != std::string_view::npos) {
    throw std::invalid_argument(
        "fabric Kafka topic must be non-empty and contain no NUL bytes");
  }
  if (identity.empty()) {
    throw std::invalid_argument("fabric Kafka identity must be non-empty");
  }
}
} // namespace

void require_fabric_kafka_profile(const Value &service_config) {
  if (service_config.schema() !=
      scalar_descriptor<kafka::KafkaServiceConfig>::value_meta()) {
    throw std::invalid_argument("fabric requires KafkaServiceConfig");
  }
  const auto root = service_config.view().as_bundle();
  const auto consumer = root.at("consumer_defaults").as_bundle();
  const auto producer = root.at("producer").as_bundle();
  const Str acknowledgements =
      producer.at("acknowledgements").checked_as<Str>();
  if (!producer.at("idempotent").checked_as<Bool>() ||
      (acknowledgements != "all" && acknowledgements != "-1")) {
    throw std::invalid_argument("fabric requires idempotent Kafka production "
                                "with acknowledgements=all");
  }
  if (consumer.at("inbound_overflow")
          .checked_as<kafka::KafkaOverflowAction>() !=
      kafka::KafkaOverflowAction::Fail) {
    throw std::invalid_argument("fabric Kafka ingress cannot drop records");
  }
  const auto overflow =
      producer.at("overflow").checked_as<kafka::KafkaOverflowAction>();
  const auto stage_overflow =
      producer.at("stage_overflow").checked_as<kafka::KafkaOverflowAction>();
  if (overflow == kafka::KafkaOverflowAction::Drop ||
      (overflow == kafka::KafkaOverflowAction::Stage &&
       stage_overflow != kafka::KafkaOverflowAction::Fail)) {
    throw std::invalid_argument("fabric Kafka publication cannot drop records");
  }
}

Value fabric_kafka_subscription_key(Str topic, Str identity) {
  require_topic_and_identity(topic, identity);
  return kafka::subscription_key()
      .topics({std::move(topic)})
      .group_id(identity)
      .assignment_mode(kafka::KafkaAssignmentMode::Independent)
      .start(
          kafka::make_start_position(kafka::KafkaStartPositionKind::Committed,
                                     kafka::KafkaOffsetFallback::Earliest))
      .stop(kafka::make_stop_position(kafka::KafkaStopPositionKind::Unbounded))
      .commit_mode(kafka::KafkaCommitMode::Explicit)
      .sharing_identity(std::move(identity))
      .build();
}

void wire_kafka_transport(Wiring &wiring, service::ServicePath fabric_path,
                          service::ServicePath kafka_path, Str topic,
                          Str identity) {
  require_topic_and_identity(topic, identity);
  register_service(wiring, fabric_path, FabricNotificationMode::GraphTransport);

  auto key = wire<stdlib::const_, TS<kafka::KafkaSubscriptionKey>>(
      wiring, fabric_kafka_subscription_key(topic, identity));
  auto subscription = kafka::subscribe(wiring, kafka_path, key);
  auto record = wire<stdlib::getattr_, TS<Shared<kafka::KafkaRecord>>>(
      wiring, subscription, Str{"record"});
  auto cursor = wire<stdlib::getattr_, TS<kafka::KafkaCursor>>(
      wiring, subscription, Str{"cursor"});
  auto state = wire<stdlib::getattr_, TS<kafka::KafkaSubscriptionState>>(
      wiring, subscription, Str{"state"});

  auto revision = wire<FabricKafkaDecodeNode>(wiring, record);
  submit_notice(wiring, revision, fabric_path);
  kafka::commit(wiring, kafka_path,
                wire<FabricKafkaValidatedCursorNode>(wiring, revision, cursor));
  submit_transport_control(wiring, wire<FabricKafkaControlNode>(wiring, state),
                           fabric_path);

  auto outgoing = notification_requests(wiring, fabric_path);
  auto produce_record = wire<FabricKafkaProduceRecordNode>(wiring, outgoing);
  auto report =
      kafka::publish(wiring, kafka_path,
                     kafka::publish_request(wiring, topic, produce_record));
  submit_notification_delivery(
      wiring, wire<FabricKafkaDeliveryNode>(wiring, report), fabric_path);

  submit_transport_event(
      wiring,
      wire<FabricKafkaEventNode>(wiring, kafka::events(wiring, kafka_path)),
      fabric_path);
}

void register_kafka_transport(Wiring &wiring, service::ServicePath fabric_path,
                              service::ServicePath kafka_path, Str topic,
                              Str identity, Value kafka_service_config) {
  if (!wiring.is_realtime()) {
    throw std::invalid_argument(
        "Fabric Kafka transport requires real-time graph wiring");
  }
  require_fabric_kafka_profile(kafka_service_config);
  kafka::register_service(wiring, kafka_path, std::move(kafka_service_config));
  wire_kafka_transport(wiring, std::move(fabric_path), std::move(kafka_path),
                       std::move(topic), std::move(identity));
}

void register_kafka_transport(Wiring &wiring, Str topic, Str identity,
                              Value kafka_service_config) {
  register_kafka_transport(wiring, service::path(DEFAULT_SERVICE_PATH),
                           service::path(DEFAULT_KAFKA_SERVICE_PATH),
                           std::move(topic), std::move(identity),
                           std::move(kafka_service_config));
}
} // namespace hgraph::fabric
