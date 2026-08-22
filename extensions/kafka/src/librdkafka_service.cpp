#include <hgraph/kafka/service.h>
#include <hgraph/kafka/value_builders.h>

#include "detail/service_transport.h"

#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/util/scope.h>

#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wignored-qualifiers"
#endif
#if __has_include(<librdkafka/rdkafka.h>)
#include <librdkafka/rdkafka.h>
#else
#include <rdkafka.h>
#endif
#if defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <compare>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

namespace hgraph::kafka::detail {
namespace {
[[nodiscard]] Value
subscription_envelope(const KafkaTransportBindings &bindings, Value key,
                      std::optional<Value> record,
                      std::optional<Value> cursor, KafkaSubscriptionState state,
                      std::optional<DateTime> evaluation_time,
                      Bool recovery = false) {
  return subscription_transport_event(bindings, std::move(key),
                                      std::move(record), std::move(cursor), state,
                                      evaluation_time, recovery);
}

[[nodiscard]] Value delivery_envelope(const KafkaTransportBindings &bindings,
                                      Int request_id, Value report) {
  return delivery_transport_event(bindings, request_id, std::move(report));
}

[[nodiscard]] Value event_envelope(const KafkaTransportBindings &bindings,
                                   Value event, Bool stop_graph) {
  return service_transport_event(bindings, std::move(event), stop_graph);
}

[[nodiscard]] bool present(const ValueView &value) noexcept {
  return value.data() != nullptr;
}

[[nodiscard]] std::vector<Str> strings(ValueView value) {
  std::vector<Str> result;
  for (const auto item : value.as_list()) {
    result.push_back(item.checked_as<Str>());
  }
  return result;
}

using Options = std::vector<std::pair<Str, Str>>;

void validate_option_sets(const Options &common, const Options &specific,
                          bool consumer);

[[nodiscard]] Options options(ValueView value) {
  Options result;
  for (const auto item : value.as_list()) {
    const auto fields = item.as_bundle();
    result.emplace_back(fields.at("name").checked_as<Str>(),
                        fields.at("value").checked_as<Str>());
  }
  return result;
}

struct RuntimeConfig {
  std::vector<Str> bootstrap_servers{};
  Str client_id{};
  Options common_options{};
  Options consumer_options{};
  Options producer_options{};
  bool idempotent{};
  Str acknowledgements{};
  std::int64_t retries{};
  std::int64_t linger_ms{};
  std::int64_t batch_record_limit{};
  std::size_t ingress_records{};
  std::size_t outbound_records{};
  KafkaOverflowAction inbound_overflow{KafkaOverflowAction::Fail};
  KafkaFailurePolicy consumer_failure_policy{KafkaFailurePolicy::Report};
  KafkaOverflowAction outbound_overflow{KafkaOverflowAction::Stage};
  KafkaOverflowAction stage_overflow{KafkaOverflowAction::Fail};
  std::chrono::milliseconds shutdown_drain_timeout{5'000};
  KafkaFailurePolicy producer_failure_policy{KafkaFailurePolicy::Report};
};

[[nodiscard]] std::size_t positive_limit(ValueView field,
                                         std::string_view name) {
  const Int value = field.checked_as<Int>();
  if (value <= 0) {
    throw std::invalid_argument("Kafka " + std::string{name} +
                                " must be positive");
  }
  return static_cast<std::size_t>(value);
}

[[nodiscard]] RuntimeConfig parse_config(const Value &value) {
  if (value.schema() != scalar_descriptor<KafkaServiceConfig>::value_meta()) {
    throw std::invalid_argument("Kafka service requires KafkaServiceConfig");
  }
  const auto root = value.view().as_bundle();
  const auto connection = root.at("connection").as_bundle();
  const auto consumer = root.at("consumer_defaults").as_bundle();
  const auto producer = root.at("producer").as_bundle();

  RuntimeConfig result;
  result.bootstrap_servers = strings(connection.at("bootstrap_servers"));
  result.client_id = connection.at("client_id").checked_as<Str>();
  result.common_options = options(connection.at("options"));
  result.consumer_options = options(consumer.at("options"));
  result.producer_options = options(producer.at("options"));
  result.idempotent = producer.at("idempotent").checked_as<Bool>();
  result.acknowledgements = producer.at("acknowledgements").checked_as<Str>();
  result.retries = producer.at("retries").checked_as<Int>();
  result.linger_ms = producer.at("linger_ms").checked_as<Int>();
  result.batch_record_limit =
      producer.at("batch_record_limit").checked_as<Int>();
  result.ingress_records = positive_limit(consumer.at("ingress_record_limit"),
                                          "ingress record limit");
  result.outbound_records = positive_limit(producer.at("outbound_record_limit"),
                                           "outbound record limit");
  result.inbound_overflow =
      consumer.at("inbound_overflow").checked_as<KafkaOverflowAction>();
  result.consumer_failure_policy =
      consumer.at("failure_policy").checked_as<KafkaFailurePolicy>();
  result.outbound_overflow =
      producer.at("overflow").checked_as<KafkaOverflowAction>();
  result.stage_overflow =
      producer.at("stage_overflow").checked_as<KafkaOverflowAction>();
  const Int shutdown_drain_timeout_ms =
      producer.at("shutdown_drain_timeout_ms").checked_as<Int>();
  if (shutdown_drain_timeout_ms < 0) {
    throw std::invalid_argument(
        "Kafka shutdown drain timeout must be non-negative");
  }
  result.shutdown_drain_timeout =
      std::chrono::milliseconds{shutdown_drain_timeout_ms};
  result.producer_failure_policy =
      producer.at("failure_policy").checked_as<KafkaFailurePolicy>();
  if (result.bootstrap_servers.empty()) {
    throw std::invalid_argument("Kafka service requires bootstrap servers");
  }
  if (result.inbound_overflow == KafkaOverflowAction::Stage) {
    throw std::invalid_argument("Kafka inbound overflow cannot use Stage");
  }
  if (result.stage_overflow == KafkaOverflowAction::Stage) {
    throw std::invalid_argument("Kafka stage overflow must be Fail or Drop");
  }
  if (result.acknowledgements != "0" && result.acknowledgements != "1" &&
      result.acknowledgements != "all" && result.acknowledgements != "-1") {
    throw std::invalid_argument(
        "Kafka acknowledgements must be 0, 1, all, or -1");
  }
  if (result.idempotent && result.acknowledgements != "all" &&
      result.acknowledgements != "-1") {
    throw std::invalid_argument(
        "Kafka idempotence requires all acknowledgements");
  }
  if (result.retries < 0 || result.linger_ms < 0 ||
      result.batch_record_limit <= 0) {
    throw std::invalid_argument(
        "Kafka producer retry/batch settings are out of range");
  }
  validate_option_sets(result.common_options, result.consumer_options, true);
  validate_option_sets(result.common_options, result.producer_options, false);
  return result;
}

[[nodiscard]] Str joined_bootstrap_servers(const std::vector<Str> &servers) {
  Str result;
  for (const auto &server : servers) {
    if (!result.empty()) {
      result.push_back(',');
    }
    result += server;
  }
  return result;
}

void set_conf(rd_kafka_conf_t *conf, std::string_view name,
              std::string_view value) {
  char error[512]{};
  if (rd_kafka_conf_set(conf, std::string{name}.c_str(),
                        std::string{value}.c_str(), error,
                        sizeof(error)) != RD_KAFKA_CONF_OK) {
    throw std::invalid_argument("Invalid librdkafka option '" +
                                std::string{name} + "': " + error);
  }
}

void reject_owned_option(std::string_view name, bool consumer) {
  const bool common_owned =
      name == "bootstrap.servers" || name == "client.id" || name == "opaque";
  const bool consumer_owned =
      consumer &&
      (name == "group.id" || name == "enable.auto.commit" ||
       name == "enable.auto.offset.store" || name == "auto.offset.reset" ||
       name == "queued.max.messages.kbytes");
  const bool producer_owned =
      !consumer &&
      (name == "enable.idempotence" || name == "transactional.id" ||
       name == "acks" || name == "retries" || name == "linger.ms" ||
       name == "batch.num.messages" || name == "queue.buffering.max.messages" ||
       name == "queue.buffering.max.kbytes");
  if (common_owned || consumer_owned || producer_owned) {
    throw std::invalid_argument("Kafka option '" + std::string{name} +
                                "' is owned by the service contract");
  }
}

void apply_options(rd_kafka_conf_t *conf, const Options &values,
                   bool consumer) {
  std::vector<std::string_view> seen;
  for (const auto &[name, value] : values) {
    if (std::ranges::find(seen, name) != seen.end()) {
      throw std::invalid_argument("Kafka option '" + name +
                                  "' is configured more than once");
    }
    seen.push_back(name);
    reject_owned_option(name, consumer);
    set_conf(conf, name, value);
  }
}

void validate_option_sets(const Options &common, const Options &specific,
                          bool consumer) {
  std::vector<std::string_view> seen;
  const auto validate = [&](const Options &values) {
    for (const auto &[name, value] : values) {
      static_cast<void>(value);
      if (name.empty()) {
        throw std::invalid_argument("Kafka option names cannot be empty");
      }
      if (std::ranges::find(seen, name) != seen.end()) {
        throw std::invalid_argument("Kafka option '" + name +
                                    "' is configured more than once");
      }
      seen.push_back(name);
      reject_owned_option(name, consumer);
    }
  };
  validate(common);
  validate(specific);
}

struct KafkaConfDeleter {
  void operator()(rd_kafka_conf_t *value) const noexcept {
    if (value) {
      rd_kafka_conf_destroy(value);
    }
  }
};

using KafkaConfPtr = std::unique_ptr<rd_kafka_conf_t, KafkaConfDeleter>;

[[nodiscard]] bool retriable_error(rd_kafka_resp_err_t error) noexcept {
  switch (error) {
  case RD_KAFKA_RESP_ERR__TIMED_OUT:
  case RD_KAFKA_RESP_ERR__MSG_TIMED_OUT:
  case RD_KAFKA_RESP_ERR__TRANSPORT:
  case RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN:
  case RD_KAFKA_RESP_ERR__WAIT_COORD:
  case RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT:
  case RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS:
  case RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE:
  case RD_KAFKA_RESP_ERR_NOT_COORDINATOR:
  case RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS:
  case RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS_AFTER_APPEND:
  case RD_KAFKA_RESP_ERR_NETWORK_EXCEPTION:
  case RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE:
  case RD_KAFKA_RESP_ERR_NOT_LEADER_OR_FOLLOWER:
  case RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE:
  case RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE:
  case RD_KAFKA_RESP_ERR_KAFKA_STORAGE_ERROR:
    return true;
  default:
    return false;
  }
}

struct SubscriptionSpec {
  struct Offset {
    Str topic{};
    std::int32_t partition{};
    std::int64_t offset{};
  };

  KafkaSelectorKind selector{KafkaSelectorKind::Topics};
  std::vector<Str> topics{};
  Str pattern{};
  std::vector<std::pair<Str, std::int32_t>> partitions{};
  Str group_id{};
  KafkaAssignmentMode assignment_mode{KafkaAssignmentMode::Group};
  KafkaStartPositionKind start_kind{KafkaStartPositionKind::Committed};
  KafkaOffsetFallback start_fallback{KafkaOffsetFallback::Earliest};
  std::optional<std::int64_t> start_timestamp_ms{};
  std::vector<Offset> start_offsets{};
  KafkaStopPositionKind stop_kind{KafkaStopPositionKind::Unbounded};
  std::optional<std::int64_t> stop_timestamp_ms{};
  std::vector<Offset> stop_offsets{};
  Str isolation{};
  KafkaCommitMode commit_mode{KafkaCommitMode::Explicit};
  KafkaRecoveryClock recovery_clock{KafkaRecoveryClock::Arrival};
  KafkaMergePolicy merge_policy{KafkaMergePolicy::Partition};
  std::optional<std::string> key_filter{};
  Str identity{};
};

[[nodiscard]] std::vector<SubscriptionSpec::Offset>
position_offsets(ValueView value) {
  std::vector<SubscriptionSpec::Offset> result;
  for (const auto item : value.as_list()) {
    const auto fields = item.as_bundle();
    const Int partition = fields.at("partition").checked_as<Int>();
    const Int offset = fields.at("offset").checked_as<Int>();
    if (partition < 0 || partition > std::numeric_limits<std::int32_t>::max() ||
        offset < 0) {
      throw std::invalid_argument("Kafka partition offset is out of range");
    }
    result.push_back(SubscriptionSpec::Offset{
        fields.at("topic").checked_as<Str>(),
        static_cast<std::int32_t>(partition),
        static_cast<std::int64_t>(offset),
    });
  }
  return result;
}

[[nodiscard]] SubscriptionSpec parse_subscription(ValueView value) {
  const auto fields = value.as_bundle();
  SubscriptionSpec result;
  result.selector = fields.at("selector_kind").checked_as<KafkaSelectorKind>();
  if (present(fields.at("topics"))) {
    result.topics = strings(fields.at("topics"));
  }
  if (present(fields.at("topic_pattern"))) {
    result.pattern = fields.at("topic_pattern").checked_as<Str>();
  }
  if (present(fields.at("partitions"))) {
    for (const auto item : fields.at("partitions").as_list()) {
      const auto partition_fields = item.as_bundle();
      const Int partition = partition_fields.at("partition").checked_as<Int>();
      if (partition < 0 ||
          partition > std::numeric_limits<std::int32_t>::max()) {
        throw std::invalid_argument(
            "Kafka subscription partition is out of range");
      }
      result.partitions.emplace_back(
          partition_fields.at("topic").checked_as<Str>(),
          static_cast<std::int32_t>(partition));
    }
  }
  result.group_id = fields.at("group_id").checked_as<Str>();
  result.assignment_mode =
      fields.at("assignment_mode").checked_as<KafkaAssignmentMode>();
  const auto start = fields.at("start_position").as_bundle();
  result.start_kind = start.at("kind").checked_as<KafkaStartPositionKind>();
  result.start_fallback =
      start.at("fallback").checked_as<KafkaOffsetFallback>();
  if (present(start.at("timestamp"))) {
    result.start_timestamp_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            start.at("timestamp").checked_as<DateTime>().time_since_epoch())
            .count();
  }
  result.start_offsets = position_offsets(start.at("offsets"));
  const auto stop = fields.at("stop_position").as_bundle();
  result.stop_kind = stop.at("kind").checked_as<KafkaStopPositionKind>();
  if (present(stop.at("timestamp"))) {
    result.stop_timestamp_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            stop.at("timestamp").checked_as<DateTime>().time_since_epoch())
            .count();
  }
  result.stop_offsets = position_offsets(stop.at("offsets"));
  result.isolation = fields.at("isolation_level").checked_as<Str>();
  result.commit_mode = fields.at("commit_mode").checked_as<KafkaCommitMode>();
  result.recovery_clock =
      fields.at("recovery_clock").checked_as<KafkaRecoveryClock>();
  result.merge_policy =
      fields.at("merge_policy").checked_as<KafkaMergePolicy>();
  if (present(fields.at("key_filter"))) {
    result.key_filter = fields.at("key_filter").checked_as<Bytes>().data;
  }
  result.identity = fields.at("sharing_identity").checked_as<Str>();
  if (result.identity.empty()) {
    // The cursor identity is also the commit-routing key. Derive it
    // from the complete immutable subscription key so two sessions
    // which differ only by start/stop, isolation, merge, or buffer
    // semantics can never route acknowledgements to one another.
    result.identity = result.group_id + ":" + std::to_string(value.hash());
  }
  if (result.group_id.empty()) {
    throw std::invalid_argument(
        "Kafka subscription requires an explicit group id");
  }
  if (result.selector == KafkaSelectorKind::Topics && result.topics.empty()) {
    throw std::invalid_argument(
        "Kafka topic subscription requires at least one topic");
  }
  if (result.selector == KafkaSelectorKind::Pattern && result.pattern.empty()) {
    throw std::invalid_argument(
        "Kafka pattern subscription requires a pattern");
  }
  if (result.selector == KafkaSelectorKind::Partitions &&
      result.partitions.empty()) {
    throw std::invalid_argument(
        "Kafka partition subscription requires partitions");
  }
  const int selector_count = !result.topics.empty() + !result.pattern.empty() +
                             !result.partitions.empty();
  if (selector_count != 1) {
    throw std::invalid_argument(
        "Kafka subscription requires exactly one topic selector");
  }
  if (result.assignment_mode == KafkaAssignmentMode::Independent &&
      result.selector == KafkaSelectorKind::Pattern) {
    throw std::invalid_argument(
        "Independent Kafka assignment requires explicit topics or partitions");
  }
  if ((result.start_kind == KafkaStartPositionKind::Timestamp &&
       !result.start_timestamp_ms.has_value()) ||
      (result.start_kind == KafkaStartPositionKind::Offsets &&
       result.start_offsets.empty())) {
    throw std::invalid_argument(
        "Kafka start position is missing its timestamp or offsets");
  }
  if ((result.stop_kind == KafkaStopPositionKind::Timestamp &&
       !result.stop_timestamp_ms.has_value()) ||
      (result.stop_kind == KafkaStopPositionKind::Offsets &&
       result.stop_offsets.empty())) {
    throw std::invalid_argument(
        "Kafka stop position is missing its timestamp or offsets");
  }
  if (result.isolation != "read_uncommitted" &&
      result.isolation != "read_committed") {
    throw std::invalid_argument(
        "Kafka isolation level must be read_uncommitted or read_committed");
  }
  return result;
}

struct ProduceRecord {
  struct Header {
    Str name{};
    std::optional<std::string> value{};
  };

  Int request_id{};
  Int sequence{};
  Str topic{};
  Str user_token{};
  std::optional<std::string> value{};
  std::optional<std::string> key{};
  std::vector<Header> headers{};
  std::optional<std::int64_t> timestamp_ms{};
  std::int32_t partition{RD_KAFKA_PARTITION_UA};
};

[[nodiscard]] ProduceRecord parse_produce_record(Int request_id, Int sequence,
                                                 Str topic,
                                                 const Value &value) {
  const auto fields = value.view().as_bundle();
  ProduceRecord result;
  result.request_id = request_id;
  result.sequence = sequence;
  result.topic = std::move(topic);
  result.user_token = fields.at("user_token").checked_as<Str>();
  if (present(fields.at("value"))) {
    result.value = fields.at("value").checked_as<Bytes>().data;
  }
  if (present(fields.at("key"))) {
    result.key = fields.at("key").checked_as<Bytes>().data;
  }
  for (const auto header : fields.at("headers").as_list()) {
    const auto header_fields = header.as_bundle();
    ProduceRecord::Header item;
    item.name = header_fields.at("name").checked_as<Str>();
    if (present(header_fields.at("value"))) {
      item.value = header_fields.at("value").checked_as<Bytes>().data;
    }
    result.headers.push_back(std::move(item));
  }
  if (present(fields.at("timestamp"))) {
    const auto timestamp = fields.at("timestamp").checked_as<DateTime>();
    result.timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              timestamp.time_since_epoch())
                              .count();
  }
  if (present(fields.at("partition"))) {
    const Int partition = fields.at("partition").checked_as<Int>();
    if (partition < 0 || partition > std::numeric_limits<std::int32_t>::max()) {
      throw std::invalid_argument("Kafka partition is out of range");
    }
    result.partition = static_cast<std::int32_t>(partition);
  }
  return result;
}

[[nodiscard]] KafkaTimestampType
timestamp_type(rd_kafka_timestamp_type_t value) noexcept {
  switch (value) {
  case RD_KAFKA_TIMESTAMP_CREATE_TIME:
    return KafkaTimestampType::CreateTime;
  case RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME:
    return KafkaTimestampType::LogAppendTime;
  case RD_KAFKA_TIMESTAMP_NOT_AVAILABLE:
    return KafkaTimestampType::NotAvailable;
  }
  return KafkaTimestampType::NotAvailable;
}

using PositionBoundary = std::tuple<Str, std::int32_t, std::int64_t>;

[[nodiscard]] const PositionBoundary *
find_boundary(const std::vector<PositionBoundary> &boundaries,
              std::string_view topic, std::int32_t partition) noexcept {
  const auto found =
      std::find_if(boundaries.begin(), boundaries.end(), [&](const auto &item) {
        return std::get<0>(item) == topic && std::get<1>(item) == partition;
      });
  return found == boundaries.end() ? nullptr : &*found;
}

} // namespace

class KafkaRuntime;

class ConsumerSession {
public:
  ConsumerSession(KafkaRuntime &owner, Value key, SubscriptionSpec spec);
  ~ConsumerSession();

  ConsumerSession(const ConsumerSession &) = delete;
  ConsumerSession &operator=(const ConsumerSession &) = delete;

  void start();
  void wait_until_preloaded();
  void stop() noexcept;
  void commit(Value cursor);
  void coordinate_record_time_recovery() noexcept {
    record_time_recovery_participant_ = true;
  }
  [[nodiscard]] bool uses_record_time_recovery() const noexcept {
    return spec_.recovery_clock == KafkaRecoveryClock::RecordTimestamp;
  }
  [[nodiscard]] bool matches_key(const ValueView &key) const {
    return key_.view().equals(key);
  }
  [[nodiscard]] const Str &identity() const noexcept { return spec_.identity; }
  [[nodiscard]] KafkaCommitMode commit_mode() const noexcept {
    return spec_.commit_mode;
  }

private:
  struct BufferedRecord {
    Value record{};
    Value cursor{};
    std::optional<DateTime> timestamp{};
    Str topic{};
    std::int32_t partition{};
    std::int64_t offset{};
  };

  static void rebalance_callback(rd_kafka_t *consumer,
                                 rd_kafka_resp_err_t error,
                                 rd_kafka_topic_partition_list_t *partitions,
                                 void *opaque) noexcept;
  static void error_callback(rd_kafka_t *, int error, const char *,
                             void *opaque) noexcept;

  void run() noexcept;
  [[nodiscard]] bool uses_manual_assignment() const noexcept;
  void poll_manual_reconnect(rd_kafka_t *consumer);
  void configure_assignment(rd_kafka_t *consumer, rd_kafka_resp_err_t error,
                            rd_kafka_topic_partition_list_t *partitions);
  void handle_poll_error(rd_kafka_resp_err_t error, const char *message);
  void consume(rd_kafka_message_t *message);
  void process_commits(rd_kafka_t *consumer);
  void check_positions(rd_kafka_t *consumer);
  void prepare_recovery_flush(rd_kafka_t *consumer,
                              bool bounded_after_recovery);
  void flush_recovery_records(rd_kafka_t *consumer);
  void buffer_recovery_record(BufferedRecord record);
  [[nodiscard]] bool buffers_recovery() const noexcept;
  void resolve_start_positions(rd_kafka_t *consumer,
                               rd_kafka_topic_partition_list_t *partitions);
  void
  resolve_stop_positions(rd_kafka_t *consumer,
                         const rd_kafka_topic_partition_list_t *partitions);
  [[nodiscard]] bool record_is_before_boundaries(
      const rd_kafka_message_t *message,
      const std::vector<PositionBoundary> &boundaries) const noexcept;
  [[nodiscard]] bool positions_reached(
      const rd_kafka_topic_partition_list_t *positions,
      const std::vector<PositionBoundary> &boundaries) const noexcept;
  void complete_bounded();
  void emit_state(KafkaSubscriptionState state,
                  std::optional<DateTime> evaluation_time = std::nullopt);
  void complete_preload(Str error = {});
  [[nodiscard]] bool preload_is_complete() const;
  void abandon_record_time_recovery() noexcept;

  KafkaRuntime &owner_;
  Value key_{};
  SubscriptionSpec spec_{};
  std::atomic<bool> stopping_{};
  std::atomic<bool> reconnect_requested_{};
  std::thread thread_{};
  std::mutex commands_mutex_{};
  std::deque<Value> commits_{};
  Int assignment_generation_{};
  bool recovering_{};
  bool live_{};
  bool reconnecting_{};
  std::chrono::steady_clock::time_point reconnect_probe_after_{};
  std::vector<PositionBoundary> recovery_ends_{};
  std::vector<PositionBoundary> stop_ends_{};
  std::vector<PositionBoundary> assignment_starts_{};
  std::vector<std::pair<Str, std::int32_t>> assigned_{};
  std::vector<PositionBoundary> committed_{};
  bool bounded_complete_{};
  bool failed_{};
  bool recovery_ready_{};
  bool recovery_paused_{};
  bool bounded_after_recovery_{};
  std::deque<BufferedRecord> recovery_records_{};
  std::optional<DateTime> last_recovery_evaluation_time_{};
  bool record_time_recovery_participant_{};
  bool record_time_recovery_arrived_{};
  bool record_time_recovery_finished_{};
  mutable std::mutex preload_mutex_{};
  std::condition_variable preload_changed_{};
  bool preload_complete_{};
  Str preload_error_{};
};

class KafkaRuntime : public std::enable_shared_from_this<KafkaRuntime> {
public:
  using Output = std::function<bool(Value)>;

  KafkaRuntime(RuntimeConfig config, Str path,
               KafkaTransportBindings bindings, Output output,
               DateTime graph_start_time, bool simulation)
      : config_{std::move(config)}, path_{std::move(path)},
        bindings_{std::move(bindings)},
        output_{std::move(output)},
        graph_start_ms_{std::chrono::duration_cast<std::chrono::milliseconds>(
                            graph_start_time.time_since_epoch())
                            .count()},
        simulation_{simulation} {
    if (!output_) {
      throw std::invalid_argument("Kafka runtime requires an output target");
    }
  }

  ~KafkaRuntime() { stop(); }

  void start() { accepting_ = true; }

  void stop() noexcept {
    if (!accepting_.exchange(false) && !producer_) {
      return;
    }

    for (auto &session : sessions_) {
      session->stop();
    }
    sessions_.clear();
    {
      std::lock_guard lock{producer_mutex_};
      producer_stopping_ = true;
    }
    producer_changed_.notify_all();
    if (producer_thread_.joinable()) {
      producer_thread_.join();
    }
    if (producer_) {
      rd_kafka_destroy(producer_);
      producer_ = nullptr;
    }
  }

  void add_subscriptions(std::vector<Value> keys) {
    std::vector<std::unique_ptr<ConsumerSession>> additions;
    additions.reserve(keys.size());
    for (auto &key : keys) {
      SubscriptionSpec spec = parse_subscription(key.view());
      if (spec.stop_kind == KafkaStopPositionKind::GraphLifetime) {
        spec.stop_kind = simulation_ ? KafkaStopPositionKind::Snapshot
                                     : KafkaStopPositionKind::Unbounded;
      }
      const auto identity_is_live = [&](const auto &session) {
        return session->identity() == spec.identity;
      };
      if (std::ranges::any_of(sessions_, identity_is_live) ||
          std::ranges::any_of(additions, identity_is_live)) {
        throw std::invalid_argument("Kafka subscription identity '" +
                                    spec.identity +
                                    "' is already live with a different key");
      }
      if (simulation_ &&
          spec.recovery_clock != KafkaRecoveryClock::RecordTimestamp) {
        throw std::invalid_argument(
            "Kafka simulation subscriptions require RecordTimestamp recovery");
      }
      if (simulation_ && spec.stop_kind == KafkaStopPositionKind::Unbounded) {
        throw std::invalid_argument(
            "Kafka simulation subscriptions must have a bounded stop position");
      }
      if (simulation_ && spec.merge_policy !=
                             KafkaMergePolicy::TimestampTopicPartitionOffset) {
        throw std::invalid_argument(
            "Kafka simulation subscriptions require "
            "TimestampTopicPartitionOffset recovery ordering");
      }
      if (simulation_ && spec.commit_mode == KafkaCommitMode::OnGraphDelivery) {
        throw std::invalid_argument(
            "Kafka simulation subscriptions cannot commit on graph delivery");
      }
      additions.push_back(std::make_unique<ConsumerSession>(
          *this, std::move(key), std::move(spec)));
    }

    const auto record_time_participants =
        simulation_ ? std::size_t{0}
                    : static_cast<std::size_t>(std::ranges::count_if(
                          additions, [](const auto &session) {
                            return session->uses_record_time_recovery();
                          }));
    sessions_.reserve(sessions_.size() + additions.size());
    begin_record_time_recovery(record_time_participants);
    for (auto &session : additions) {
      if (!simulation_ && session->uses_record_time_recovery()) {
        session->coordinate_record_time_recovery();
      }
    }

    auto rollback = make_scope_exit<true>([&] {
      for (auto &session : additions) {
        if (session) {
          session->stop();
        }
      }
    });
    for (auto &session : additions) {
      session->start();
    }
    if (simulation_) {
      for (auto &session : additions) {
        session->wait_until_preloaded();
      }
    }
    for (auto &session : additions) {
      sessions_.push_back(std::move(session));
    }
    rollback.release();
  }

  void remove_subscription(const ValueView &key) {
    const auto found = std::find_if(
        sessions_.begin(), sessions_.end(),
        [&](const auto &session) { return session->matches_key(key); });
    if (found == sessions_.end()) {
      return;
    }
    (*found)->stop();
    sessions_.erase(found);
    static_cast<void>(
        output_(subscription_removed_transport_event(bindings_, key.clone())));
  }

  void publish(Int request_id, Str topic, Value record) {
    if (simulation_) {
      throw std::invalid_argument(
          "Kafka publishing is not supported by a simulation executor");
    }
    ensure_producer_started();
    const Int sequence = ++sequence_;
    ProduceRecord parsed =
        parse_produce_record(request_id, sequence, std::move(topic), record);
    {
      std::lock_guard lock{producer_mutex_};
      const bool records_full =
          producer_queue_.size() >= config_.outbound_records;
      if (!accepting_ || records_full) {
        const KafkaOverflowAction action =
            config_.outbound_overflow == KafkaOverflowAction::Stage
                ? config_.stage_overflow
                : config_.outbound_overflow;
        const bool dropped = action == KafkaOverflowAction::Drop;
        const bool stop_graph = !dropped && config_.producer_failure_policy ==
                                                KafkaFailurePolicy::StopGraph;
        emit_delivery(
            request_id,
            make_delivery_report(parsed.user_token, sequence, parsed.topic,
                                 dropped ? KafkaDeliveryStatus::Dropped
                                         : KafkaDeliveryStatus::EnqueueRejected,
                                 parsed.partition, std::nullopt,
                                 RD_KAFKA_RESP_ERR__QUEUE_FULL, true, false,
                                 Str{"outbound queue is full"}));
        emit_event(dropped ? KafkaSeverity::Warning : KafkaSeverity::Error,
                   Str{"producer"}, Str{"queue_overflow"},
                   RD_KAFKA_RESP_ERR__QUEUE_FULL, true, false,
                   dropped ? Str{"outbound record was dropped because the "
                                 "staging queue is full"}
                           : Str{"outbound staging queue is full"},
                   {}, parsed.user_token, stop_graph);
        return;
      }
      producer_queue_.push_back(std::move(parsed));
    }
    producer_changed_.notify_one();
  }

  void explicit_commit(Value cursor) {
    if (simulation_) {
      throw std::invalid_argument(
          "Kafka commits are not supported by a simulation executor");
    }
    route_commit(std::move(cursor), true);
  }
  void graph_delivered(Value cursor) { route_commit(std::move(cursor), false); }

  [[nodiscard]] Int next_assignment_generation() noexcept {
    return ++assignment_generation_;
  }

  [[nodiscard]] const RuntimeConfig &config() const noexcept { return config_; }
  [[nodiscard]] const Str &path() const noexcept { return path_; }
  [[nodiscard]] std::int64_t graph_start_ms() const noexcept {
    return graph_start_ms_;
  }
  [[nodiscard]] bool simulation() const noexcept { return simulation_; }

  bool
  emit_subscription(Value key, std::optional<Value> record,
                    std::optional<Value> cursor, KafkaSubscriptionState state,
                    std::optional<DateTime> evaluation_time = std::nullopt) {
    return output_(subscription_envelope(
        bindings_, std::move(key), std::move(record), std::move(cursor), state,
        evaluation_time));
  }

  bool emit_recovery_subscription(Value key, std::optional<Value> record,
                                  std::optional<Value> cursor,
                                  KafkaSubscriptionState state,
                                  std::optional<DateTime> evaluation_time) {
    return output_(subscription_envelope(
        bindings_, std::move(key), std::move(record), std::move(cursor), state,
        evaluation_time, !simulation_));
  }

  void begin_record_time_recovery(std::size_t participants) {
    if (participants == 0) {
      return;
    }
    std::lock_guard lock{recovery_mutex_};
    if (participants > std::numeric_limits<std::size_t>::max() -
                           record_time_recoveries_pending_ ||
        participants > std::numeric_limits<std::size_t>::max() -
                           record_time_recovery_flushes_pending_) {
      throw std::overflow_error(
          "Kafka record-time recovery participant count overflowed");
    }
    record_time_recoveries_pending_ += participants;
    record_time_recovery_flushes_pending_ += participants;
  }

  void record_time_recovery_ready(std::optional<DateTime> tail) {
    std::lock_guard lock{recovery_mutex_};
    if (record_time_recoveries_pending_ == 0) {
      throw std::logic_error(
          "Kafka record-time recovery participant completed twice");
    }
    if (tail.has_value() && (!record_time_recovery_tail_.has_value() ||
                             *tail > *record_time_recovery_tail_)) {
      record_time_recovery_tail_ = *tail;
    }
    --record_time_recoveries_pending_;
  }

  void finish_record_time_recovery() {
    bool release{};
    {
      std::lock_guard lock{recovery_mutex_};
      if (record_time_recovery_flushes_pending_ == 0) {
        throw std::logic_error(
            "Kafka record-time recovery flush completed twice");
      }
      release = --record_time_recovery_flushes_pending_ == 0;
    }
    if (release) {
      static_cast<void>(output_(recovery_barrier_transport_event(bindings_)));
    }
  }

  void cancel_record_time_recovery(bool already_ready) noexcept {
    try {
      bool release{};
      {
        std::lock_guard lock{recovery_mutex_};
        if (!already_ready && record_time_recoveries_pending_ != 0) {
          --record_time_recoveries_pending_;
        }
        if (record_time_recovery_flushes_pending_ != 0) {
          release = --record_time_recovery_flushes_pending_ == 0;
        }
      }
      if (release) {
        static_cast<void>(output_(recovery_barrier_transport_event(bindings_)));
      }
    } catch (...) {
    }
  }

  [[nodiscard]] std::pair<bool, std::optional<DateTime>>
  record_time_recovery_status() const {
    std::lock_guard lock{recovery_mutex_};
    return {record_time_recoveries_pending_ == 0, record_time_recovery_tail_};
  }

  void emit_subscription_state(
      Value key, KafkaSubscriptionState state,
      std::optional<DateTime> evaluation_time = std::nullopt) noexcept {
    try {
      static_cast<void>(output_(subscription_envelope(
          bindings_, std::move(key), std::nullopt, std::nullopt, state,
          evaluation_time)));
    } catch (...) {
    }
  }

  bool emit_delivery(Int request_id, Value report) noexcept {
    try {
      return output_(delivery_envelope(bindings_, request_id,
                                       std::move(report)));
    } catch (...) {
      return false;
    }
  }

  void emit_event(KafkaSeverity severity, Str component, Str category,
                  Int error_code, Bool retriable, Bool fatal, Str message,
                  Str subscription_identity = {}, Str publisher_identity = {},
                  Bool stop_graph = false) noexcept {
    try {
      Value event = make_event(
          severity, std::move(component), std::move(category), path_,
          std::move(message), error_code, retriable, fatal,
          std::move(subscription_identity), std::move(publisher_identity));
      static_cast<void>(
          output_(event_envelope(bindings_, std::move(event), stop_graph)));
    } catch (...) {
    }
  }

private:
  struct DeliveryOpaque {
    KafkaRuntime *runtime{};
    Int request_id{};
    Int sequence{};
    Str topic{};
    Str user_token{};
  };

  static void delivery_callback(rd_kafka_t *producer,
                                const rd_kafka_message_t *message,
                                void *) noexcept {
    std::unique_ptr<DeliveryOpaque> opaque{
        static_cast<DeliveryOpaque *>(message->_private)};
    if (!opaque || !opaque->runtime) {
      return;
    }
    try {
      const bool delivered = message->err == RD_KAFKA_RESP_ERR_NO_ERROR;
      char fatal_message[512]{};
      const auto fatal_error =
          rd_kafka_fatal_error(producer, fatal_message, sizeof(fatal_message));
      const bool fatal =
          !delivered && fatal_error != RD_KAFKA_RESP_ERR_NO_ERROR;
      const bool retriable = !fatal && retriable_error(message->err);
      const char *produce_error =
          delivered ? nullptr : rd_kafka_message_produce_errstr(message);
      const Str error_message =
          delivered
              ? Str{}
              : Str{produce_error != nullptr ? produce_error
                                             : rd_kafka_err2str(message->err)};
      opaque->runtime->emit_delivery(
          opaque->request_id,
          make_delivery_report(
              opaque->user_token, opaque->sequence, opaque->topic,
              delivered ? KafkaDeliveryStatus::Delivered
                        : (retriable ? KafkaDeliveryStatus::RetriableFailure
                                     : KafkaDeliveryStatus::PermanentFailure),
              message->partition >= 0
                  ? std::optional<Int>{static_cast<Int>(message->partition)}
                  : std::nullopt,
              delivered && message->offset >= 0
                  ? std::optional<Int>{static_cast<Int>(message->offset)}
                  : std::nullopt,
              static_cast<Int>(message->err), retriable, fatal, error_message));
      if (!delivered) {
        opaque->runtime->emit_event(
            fatal ? KafkaSeverity::Fatal : KafkaSeverity::Error,
            Str{"producer"}, Str{"delivery"}, static_cast<Int>(message->err),
            retriable, fatal, error_message, {}, opaque->user_token,
            !retriable && opaque->runtime->config().producer_failure_policy ==
                              KafkaFailurePolicy::StopGraph);
      }
    } catch (...) {
    }
  }

  static void producer_error_callback(rd_kafka_t *, int error, const char *,
                                      void *opaque) noexcept {
    auto *runtime = static_cast<KafkaRuntime *>(opaque);
    if (!runtime) {
      return;
    }
    runtime->emit_event(
        error == RD_KAFKA_RESP_ERR__FATAL ? KafkaSeverity::Fatal
                                          : KafkaSeverity::Error,
        Str{"producer"}, Str{"client_error"}, static_cast<Int>(error),
        retriable_error(static_cast<rd_kafka_resp_err_t>(error)),
        error == RD_KAFKA_RESP_ERR__FATAL,
        Str{rd_kafka_err2str(static_cast<rd_kafka_resp_err_t>(error))}, {}, {},
        error == RD_KAFKA_RESP_ERR__FATAL &&
            runtime->config().producer_failure_policy ==
                KafkaFailurePolicy::StopGraph);
  }

  void create_producer() {
    KafkaConfPtr conf{rd_kafka_conf_new()};
    set_conf(conf.get(), "bootstrap.servers",
             joined_bootstrap_servers(config_.bootstrap_servers));
    if (!config_.client_id.empty()) {
      set_conf(conf.get(), "client.id", config_.client_id);
    }
    set_conf(conf.get(), "enable.idempotence",
             config_.idempotent ? "true" : "false");
    set_conf(conf.get(), "acks", config_.acknowledgements);
    set_conf(conf.get(), "retries", std::to_string(config_.retries));
    set_conf(conf.get(), "linger.ms", std::to_string(config_.linger_ms));
    set_conf(conf.get(), "batch.num.messages",
             std::to_string(config_.batch_record_limit));
    set_conf(conf.get(), "queue.buffering.max.messages",
             std::to_string(config_.outbound_records));
    apply_options(conf.get(), config_.common_options, false);
    apply_options(conf.get(), config_.producer_options, false);
    rd_kafka_conf_set_opaque(conf.get(), this);
    rd_kafka_conf_set_dr_msg_cb(conf.get(), &delivery_callback);
    rd_kafka_conf_set_error_cb(conf.get(), &producer_error_callback);

    char error[512]{};
    producer_ =
        rd_kafka_new(RD_KAFKA_PRODUCER, conf.get(), error, sizeof(error));
    if (!producer_) {
      throw std::runtime_error(Str{"Unable to create Kafka producer: "} +
                               error);
    }
    static_cast<void>(conf.release());
  }

  void ensure_producer_started() {
    if (producer_) {
      return;
    }
    create_producer();
    try {
      producer_thread_ = std::thread{[this] { producer_loop(); }};
    } catch (...) {
      rd_kafka_destroy(producer_);
      producer_ = nullptr;
      throw;
    }
  }

  void producer_loop() noexcept {
    while (true) {
      std::optional<ProduceRecord> record;
      {
        std::unique_lock lock{producer_mutex_};
        producer_changed_.wait_for(lock, std::chrono::milliseconds{25}, [&] {
          return producer_stopping_ || !producer_queue_.empty();
        });
        if (!producer_queue_.empty()) {
          record.emplace(std::move(producer_queue_.front()));
          producer_queue_.pop_front();
        } else if (producer_stopping_) {
          break;
        }
      }
      try {
        if (record.has_value() && !produce(*record)) {
          {
            std::lock_guard lock{producer_mutex_};
            producer_queue_.push_front(std::move(*record));
          }
          rd_kafka_poll(producer_, 10);
        } else {
          rd_kafka_poll(producer_, 0);
        }
      } catch (const std::exception &exception) {
        if (record.has_value()) {
          static_cast<void>(emit_delivery(
              record->request_id,
              make_delivery_report(
                  record->user_token, record->sequence, record->topic,
                  KafkaDeliveryStatus::PermanentFailure, record->partition,
                  std::nullopt, 0, false, true, exception.what())));
        }
        emit_event(KafkaSeverity::Fatal, Str{"producer"}, Str{"worker"}, 0,
                   false, true, exception.what(), {},
                   record.has_value() ? record->user_token : Str{},
                   config_.producer_failure_policy ==
                       KafkaFailurePolicy::StopGraph);
        producer_stopping_ = true;
      }
    }
    const auto timeout_ms = static_cast<int>(
        std::min<std::int64_t>(config_.shutdown_drain_timeout.count(),
                               std::numeric_limits<int>::max()));
    const auto flush_error = rd_kafka_flush(producer_, timeout_ms);
    if (flush_error != RD_KAFKA_RESP_ERR_NO_ERROR) {
      emit_event(
          KafkaSeverity::Error, Str{"producer"}, Str{"shutdown_timeout"},
          flush_error, true, false,
          Str{"Kafka producer did not drain before the shutdown timeout"}, {},
          {}, config_.producer_failure_policy == KafkaFailurePolicy::StopGraph);
    }
  }

  [[nodiscard]] bool produce(ProduceRecord &record) {
    auto opaque = std::make_unique<DeliveryOpaque>(
        DeliveryOpaque{this, record.request_id, record.sequence, record.topic,
                       record.user_token});
    rd_kafka_headers_t *headers = rd_kafka_headers_new(record.headers.size());
    if (!headers) {
      throw std::bad_alloc{};
    }
    for (const auto &header : record.headers) {
      const void *data = header.value.has_value()
                             ? static_cast<const void *>(header.value->data())
                             : nullptr;
      const std::size_t size =
          header.value.has_value() ? header.value->size() : 0;
      const auto error = rd_kafka_header_add(headers, header.name.c_str(),
                                             header.name.size(), data, size);
      if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_headers_destroy(headers);
        emit_delivery(record.request_id,
                      make_delivery_report(
                          record.user_token, record.sequence, record.topic,
                          KafkaDeliveryStatus::EnqueueRejected,
                          record.partition, std::nullopt, error, false, false,
                          Str{rd_kafka_err2str(error)}));
        emit_event(
            KafkaSeverity::Error, Str{"producer"}, Str{"header"}, error, false,
            false, Str{rd_kafka_err2str(error)}, {}, record.user_token,
            config_.producer_failure_policy == KafkaFailurePolicy::StopGraph);
        return true;
      }
    }

    const void *payload = record.value.has_value()
                              ? static_cast<const void *>(record.value->data())
                              : nullptr;
    const std::size_t payload_size =
        record.value.has_value() ? record.value->size() : 0;
    const void *key = record.key.has_value()
                          ? static_cast<const void *>(record.key->data())
                          : nullptr;
    const std::size_t key_size =
        record.key.has_value() ? record.key->size() : 0;

    std::array<rd_kafka_vu_t, 8> arguments{};
    std::size_t count{};
    arguments[count].vtype = RD_KAFKA_VTYPE_TOPIC;
    arguments[count++].u.cstr = record.topic.c_str();
    arguments[count].vtype = RD_KAFKA_VTYPE_PARTITION;
    arguments[count++].u.i32 = record.partition;
    arguments[count].vtype = RD_KAFKA_VTYPE_MSGFLAGS;
    arguments[count++].u.i = RD_KAFKA_MSG_F_COPY;
    arguments[count].vtype = RD_KAFKA_VTYPE_VALUE;
    arguments[count].u.mem.ptr = const_cast<void *>(payload);
    arguments[count++].u.mem.size = payload_size;
    arguments[count].vtype = RD_KAFKA_VTYPE_KEY;
    arguments[count].u.mem.ptr = const_cast<void *>(key);
    arguments[count++].u.mem.size = key_size;
    if (record.timestamp_ms.has_value()) {
      arguments[count].vtype = RD_KAFKA_VTYPE_TIMESTAMP;
      arguments[count++].u.i64 = *record.timestamp_ms;
    }
    arguments[count].vtype = RD_KAFKA_VTYPE_HEADERS;
    arguments[count++].u.headers = headers;
    arguments[count].vtype = RD_KAFKA_VTYPE_OPAQUE;
    arguments[count++].u.ptr = opaque.get();

    rd_kafka_error_t *produce_error =
        rd_kafka_produceva(producer_, arguments.data(), count);
    const rd_kafka_resp_err_t error = produce_error != nullptr
                                          ? rd_kafka_error_code(produce_error)
                                          : RD_KAFKA_RESP_ERR_NO_ERROR;
    if (error == RD_KAFKA_RESP_ERR_NO_ERROR) {
      static_cast<void>(opaque.release());
      return true;
    }

    const bool retriable = rd_kafka_error_is_retriable(produce_error) != 0;
    const bool fatal = rd_kafka_error_is_fatal(produce_error) != 0;
    const Str error_message = rd_kafka_error_string(produce_error);
    rd_kafka_error_destroy(produce_error);
    rd_kafka_headers_destroy(headers);
    if (error == RD_KAFKA_RESP_ERR__QUEUE_FULL &&
        config_.outbound_overflow == KafkaOverflowAction::Stage &&
        !producer_stopping_) {
      return false;
    }
    const KafkaOverflowAction action =
        config_.outbound_overflow == KafkaOverflowAction::Stage
            ? config_.stage_overflow
            : config_.outbound_overflow;
    const bool dropped = error == RD_KAFKA_RESP_ERR__QUEUE_FULL &&
                         action == KafkaOverflowAction::Drop;
    emit_delivery(
        record.request_id,
        make_delivery_report(record.user_token, record.sequence, record.topic,
                             dropped ? KafkaDeliveryStatus::Dropped
                                     : KafkaDeliveryStatus::EnqueueRejected,
                             record.partition, std::nullopt, error, retriable,
                             fatal, std::move(error_message)));
    emit_event(fatal     ? KafkaSeverity::Fatal
               : dropped ? KafkaSeverity::Warning
                         : KafkaSeverity::Error,
               Str{"producer"},
               error == RD_KAFKA_RESP_ERR__QUEUE_FULL ? Str{"queue_overflow"}
                                                      : Str{"enqueue"},
               error, retriable, fatal,
               dropped ? Str{"outbound record was dropped"}
                       : Str{rd_kafka_err2str(error)},
               {}, record.user_token,
               !dropped && config_.producer_failure_policy ==
                               KafkaFailurePolicy::StopGraph);
    return true;
  }

  void route_commit(Value cursor, bool explicit_request) {
    const auto fields = cursor.view().as_bundle();
    const Str identity = fields.at("subscription_identity").checked_as<Str>();
    const auto found = std::find_if(
        sessions_.begin(), sessions_.end(),
        [&](const auto &session) { return session->identity() == identity; });
    if (found == sessions_.end()) {
      emit_event(KafkaSeverity::Warning, Str{"consumer"}, Str{"stale_commit"},
                 0, false, false,
                 Str{"cursor does not identify a live subscription"}, identity);
      return;
    }
    const auto mode = (*found)->commit_mode();
    if ((explicit_request && mode == KafkaCommitMode::Explicit) ||
        (!explicit_request && mode == KafkaCommitMode::OnGraphDelivery)) {
      (*found)->commit(std::move(cursor));
    }
  }

  RuntimeConfig config_{};
  Str path_{};
  KafkaTransportBindings bindings_{};
  Output output_{};
  std::int64_t graph_start_ms_{};
  bool simulation_{};
  std::atomic<bool> accepting_{};
  std::vector<std::unique_ptr<ConsumerSession>> sessions_{};
  mutable std::mutex recovery_mutex_{};
  std::size_t record_time_recoveries_pending_{};
  std::size_t record_time_recovery_flushes_pending_{};
  std::optional<DateTime> record_time_recovery_tail_{};
  rd_kafka_t *producer_{};
  std::thread producer_thread_{};
  std::mutex producer_mutex_{};
  std::condition_variable producer_changed_{};
  std::deque<ProduceRecord> producer_queue_{};
  std::atomic<bool> producer_stopping_{};
  std::atomic<Int> sequence_{};
  std::atomic<Int> assignment_generation_{};
};

ConsumerSession::ConsumerSession(KafkaRuntime &owner, Value key,
                                 SubscriptionSpec spec)
    : owner_{owner}, key_{std::move(key)}, spec_{std::move(spec)} {}

ConsumerSession::~ConsumerSession() { stop(); }

void ConsumerSession::start() {
  emit_state(KafkaSubscriptionState::Starting);
  if (owner_.simulation()) {
    run();
  } else {
    thread_ = std::thread{[this] { run(); }};
  }
}

void ConsumerSession::wait_until_preloaded() {
  std::unique_lock lock{preload_mutex_};
  if (!preload_changed_.wait_for(lock, std::chrono::seconds{30},
                                 [&] { return preload_complete_; })) {
    lock.unlock();
    stop();
    throw std::runtime_error("Kafka simulation subscription '" +
                             spec_.identity +
                             "' did not preload within 30 seconds");
  }
  if (!preload_error_.empty()) {
    throw std::runtime_error(preload_error_);
  }
}

void ConsumerSession::complete_preload(Str error) {
  {
    std::lock_guard lock{preload_mutex_};
    if (preload_complete_) {
      return;
    }
    preload_error_ = std::move(error);
    preload_complete_ = true;
  }
  preload_changed_.notify_all();
}

bool ConsumerSession::preload_is_complete() const {
  std::lock_guard lock{preload_mutex_};
  return preload_complete_;
}

void ConsumerSession::stop() noexcept {
  stopping_ = true;
  if (thread_.joinable()) {
    thread_.join();
  }
  abandon_record_time_recovery();
}

void ConsumerSession::abandon_record_time_recovery() noexcept {
  if (!record_time_recovery_participant_ || record_time_recovery_finished_) {
    return;
  }
  owner_.cancel_record_time_recovery(record_time_recovery_arrived_);
  record_time_recovery_arrived_ = true;
  record_time_recovery_finished_ = true;
  record_time_recovery_participant_ = false;
}

void ConsumerSession::commit(Value cursor) {
  std::lock_guard lock{commands_mutex_};
  commits_.push_back(std::move(cursor));
}

void ConsumerSession::rebalance_callback(
    rd_kafka_t *consumer, rd_kafka_resp_err_t error,
    rd_kafka_topic_partition_list_t *partitions, void *opaque) noexcept {
  auto *session = static_cast<ConsumerSession *>(opaque);
  if (!session) {
    return;
  }
  try {
    session->configure_assignment(consumer, error, partitions);
  } catch (const std::exception &exception) {
    session->failed_ = true;
    session->stopping_ = true;
    session->complete_preload(exception.what());
    session->owner_.emit_event(
        KafkaSeverity::Error, Str{"consumer"}, Str{"rebalance"}, error, true,
        false, exception.what(), session->spec_.identity, {},
        session->owner_.config().consumer_failure_policy ==
            KafkaFailurePolicy::StopGraph);
  }
}

void ConsumerSession::error_callback(rd_kafka_t *, int error, const char *,
                                     void *opaque) noexcept {
  auto *session = static_cast<ConsumerSession *>(opaque);
  if (!session) {
    return;
  }
  const auto kafka_error = static_cast<rd_kafka_resp_err_t>(error);
  const bool retriable = retriable_error(kafka_error);
  session->owner_.emit_event(
      error == RD_KAFKA_RESP_ERR__FATAL ? KafkaSeverity::Fatal
                                        : KafkaSeverity::Error,
      Str{"consumer"}, Str{"client_error"}, error, retriable,
      error == RD_KAFKA_RESP_ERR__FATAL, Str{rd_kafka_err2str(kafka_error)},
      session->spec_.identity, {},
      error == RD_KAFKA_RESP_ERR__FATAL &&
          session->owner_.config().consumer_failure_policy ==
              KafkaFailurePolicy::StopGraph);
  if (retriable && session->uses_manual_assignment()) {
    session->reconnect_requested_.store(true, std::memory_order_release);
  }
  if (error == RD_KAFKA_RESP_ERR__FATAL) {
    session->failed_ = true;
    session->stopping_ = true;
    session->complete_preload(
        rd_kafka_err2str(static_cast<rd_kafka_resp_err_t>(error)));
  }
}

void ConsumerSession::run() noexcept {
  auto release_recovery_participation =
      make_scope_exit<true>([this] { abandon_record_time_recovery(); });
  rd_kafka_t *consumer = nullptr;
  try {
    KafkaConfPtr conf{rd_kafka_conf_new()};
    const auto &config = owner_.config();
    set_conf(conf.get(), "bootstrap.servers",
             joined_bootstrap_servers(config.bootstrap_servers));
    if (!config.client_id.empty()) {
      set_conf(conf.get(), "client.id", config.client_id);
    }
    set_conf(conf.get(), "group.id", spec_.group_id);
    set_conf(conf.get(), "enable.auto.commit", "false");
    set_conf(conf.get(), "enable.auto.offset.store", "false");
    set_conf(conf.get(), "isolation.level", spec_.isolation);
    switch (spec_.start_fallback) {
    case KafkaOffsetFallback::Earliest:
      set_conf(conf.get(), "auto.offset.reset", "earliest");
      break;
    case KafkaOffsetFallback::Latest:
      set_conf(conf.get(), "auto.offset.reset", "latest");
      break;
    case KafkaOffsetFallback::Fail:
      set_conf(conf.get(), "auto.offset.reset", "error");
      break;
    }
    apply_options(conf.get(), config.common_options, true);
    apply_options(conf.get(), config.consumer_options, true);
    rd_kafka_conf_set_opaque(conf.get(), this);
    rd_kafka_conf_set_rebalance_cb(conf.get(), &rebalance_callback);
    rd_kafka_conf_set_error_cb(conf.get(), &error_callback);

    char error[512]{};
    consumer =
        rd_kafka_new(RD_KAFKA_CONSUMER, conf.get(), error, sizeof(error));
    if (!consumer) {
      throw std::runtime_error(Str{"Unable to create Kafka consumer: "} +
                               error);
    }
    static_cast<void>(conf.release());
    if (rd_kafka_poll_set_consumer(consumer) != RD_KAFKA_RESP_ERR_NO_ERROR) {
      throw std::runtime_error("Unable to configure Kafka consumer poll queue");
    }

    if (spec_.selector == KafkaSelectorKind::Partitions) {
      auto *partitions = rd_kafka_topic_partition_list_new(
          static_cast<int>(spec_.partitions.size()));
      for (const auto &[topic, partition] : spec_.partitions) {
        rd_kafka_topic_partition_list_add(partitions, topic.c_str(), partition);
      }
      try {
        configure_assignment(consumer, RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS,
                             partitions);
      } catch (...) {
        rd_kafka_topic_partition_list_destroy(partitions);
        throw;
      }
      rd_kafka_topic_partition_list_destroy(partitions);
    } else if (spec_.assignment_mode == KafkaAssignmentMode::Independent) {
      const rd_kafka_metadata_t *metadata{};
      const auto metadata_error =
          rd_kafka_metadata(consumer, 0, nullptr, &metadata, 5'000);
      if (metadata_error != RD_KAFKA_RESP_ERR_NO_ERROR || !metadata) {
        throw std::runtime_error(
            "Unable to discover Kafka partitions for independent assignment: " +
            Str{rd_kafka_err2str(metadata_error)});
      }
      auto *partitions = rd_kafka_topic_partition_list_new(8);
      try {
        for (const auto &topic : spec_.topics) {
          const rd_kafka_metadata_topic_t *found{};
          for (int index = 0; index < metadata->topic_cnt; ++index) {
            if (topic == metadata->topics[index].topic) {
              found = &metadata->topics[index];
              break;
            }
          }
          if (!found || found->err != RD_KAFKA_RESP_ERR_NO_ERROR ||
              found->partition_cnt == 0) {
            throw std::runtime_error("No Kafka partitions found for topic '" +
                                     topic + "'");
          }
          for (int index = 0; index < found->partition_cnt; ++index) {
            rd_kafka_topic_partition_list_add(partitions, topic.c_str(),
                                              found->partitions[index].id);
          }
        }
        configure_assignment(consumer, RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS,
                             partitions);
      } catch (...) {
        rd_kafka_topic_partition_list_destroy(partitions);
        rd_kafka_metadata_destroy(metadata);
        throw;
      }
      rd_kafka_topic_partition_list_destroy(partitions);
      rd_kafka_metadata_destroy(metadata);
    } else {
      std::vector<Str> selectors = spec_.topics;
      if (spec_.selector == KafkaSelectorKind::Pattern) {
        selectors = {spec_.pattern.front() == '^' ? spec_.pattern
                                                  : '^' + spec_.pattern};
      }
      auto *topics =
          rd_kafka_topic_partition_list_new(static_cast<int>(selectors.size()));
      for (const auto &topic : selectors) {
        rd_kafka_topic_partition_list_add(topics, topic.c_str(),
                                          RD_KAFKA_PARTITION_UA);
      }
      const auto subscribe_error = rd_kafka_subscribe(consumer, topics);
      rd_kafka_topic_partition_list_destroy(topics);
      if (subscribe_error != RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw std::runtime_error(rd_kafka_err2str(subscribe_error));
      }
    }

    while (!stopping_) {
      poll_manual_reconnect(consumer);
      process_commits(consumer);
      check_positions(consumer);
      if (stopping_) {
        break;
      }
      if (recovery_ready_) {
        flush_recovery_records(consumer);
        if (recovery_ready_) {
          rd_kafka_message_t *pending = rd_kafka_consumer_poll(consumer, 10);
          if (pending) {
            if (pending->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
              // Recovery partitions are paused and restored to
              // their snapshot before this phase. If an already
              // queued live record still escapes that purge,
              // rewind it so it is fetched after the replay drain.
              static_cast<void>(rd_kafka_seek(pending->rkt, pending->partition,
                                              pending->offset, 0));
            } else if (pending->err != RD_KAFKA_RESP_ERR__PARTITION_EOF) {
              handle_poll_error(pending->err, rd_kafka_message_errstr(pending));
            }
            rd_kafka_message_destroy(pending);
          }
          continue;
        }
      }
      rd_kafka_message_t *message = rd_kafka_consumer_poll(consumer, 50);
      if (message) {
        if (message->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
          consume(message);
        } else if (message->err != RD_KAFKA_RESP_ERR__PARTITION_EOF) {
          handle_poll_error(message->err, rd_kafka_message_errstr(message));
        }
        rd_kafka_message_destroy(message);
      }
      check_positions(consumer);
      if (owner_.simulation() && preload_is_complete()) {
        break;
      }
    }
    process_commits(consumer);
    static_cast<void>(rd_kafka_consumer_close(consumer));
    rd_kafka_destroy(consumer);
    consumer = nullptr;
    if (failed_) {
      complete_preload("Kafka consumer failed during preload");
    }
    emit_state(failed_ ? KafkaSubscriptionState::Failed
                       : KafkaSubscriptionState::Stopped);
  } catch (const std::exception &exception) {
    if (consumer) {
      static_cast<void>(rd_kafka_consumer_close(consumer));
      rd_kafka_destroy(consumer);
    }
    owner_.emit_event(KafkaSeverity::Error, Str{"consumer"}, Str{"lifecycle"},
                      0, false, false, exception.what(), spec_.identity, {},
                      owner_.config().consumer_failure_policy ==
                          KafkaFailurePolicy::StopGraph);
    complete_preload(exception.what());
    emit_state(KafkaSubscriptionState::Failed);
  }
}

void ConsumerSession::handle_poll_error(rd_kafka_resp_err_t error,
                                        const char *message) {
  const bool retriable = retriable_error(error);
  const bool fatal = !retriable;
  owner_.emit_event(fatal ? KafkaSeverity::Fatal : KafkaSeverity::Error,
                    Str{"consumer"}, Str{"poll"}, error, retriable, fatal,
                    Str{message}, spec_.identity, {},
                    fatal && owner_.config().consumer_failure_policy ==
                                 KafkaFailurePolicy::StopGraph);
  if (fatal) {
    failed_ = true;
    stopping_ = true;
    complete_preload(message);
  } else if (uses_manual_assignment()) {
    reconnect_requested_.store(true, std::memory_order_release);
  }
}

bool ConsumerSession::uses_manual_assignment() const noexcept {
  return spec_.selector == KafkaSelectorKind::Partitions ||
         spec_.assignment_mode == KafkaAssignmentMode::Independent;
}

void ConsumerSession::poll_manual_reconnect(rd_kafka_t *consumer) {
  // Normal polling pays one atomic load. Metadata probing and assignment
  // reconstruction occur only after a manual assignment loses connectivity.
  if (!reconnecting_ && !reconnect_requested_.load(std::memory_order_acquire)) {
    return;
  }
  if (reconnect_requested_.exchange(false, std::memory_order_acq_rel) &&
      !reconnecting_) {
    reconnecting_ = true;
    recovering_ = false;
    live_ = false;
    emit_state(KafkaSubscriptionState::Retrying);
    reconnect_probe_after_ = std::chrono::steady_clock::time_point{};
  }
  if (!reconnecting_ ||
      std::chrono::steady_clock::now() < reconnect_probe_after_) {
    return;
  }
  reconnect_probe_after_ =
      std::chrono::steady_clock::now() + std::chrono::milliseconds{250};

  const rd_kafka_metadata_t *metadata{};
  const auto metadata_error =
      rd_kafka_metadata(consumer, 0, nullptr, &metadata, 100);
  if (metadata_error != RD_KAFKA_RESP_ERR_NO_ERROR || metadata == nullptr) {
    if (metadata) {
      rd_kafka_metadata_destroy(metadata);
    }
    return;
  }
  rd_kafka_metadata_destroy(metadata);

  rd_kafka_topic_partition_list_t *partitions{};
  if (rd_kafka_assignment(consumer, &partitions) !=
          RD_KAFKA_RESP_ERR_NO_ERROR ||
      partitions == nullptr || partitions->cnt == 0) {
    if (partitions) {
      rd_kafka_topic_partition_list_destroy(partitions);
    }
    return;
  }
  try {
    configure_assignment(consumer, RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS,
                         partitions);
    reconnecting_ = false;
  } catch (const std::exception &exception) {
    owner_.emit_event(KafkaSeverity::Error, Str{"consumer"}, Str{"reconnect"},
                      0, true, false, exception.what(), spec_.identity);
  }
  rd_kafka_topic_partition_list_destroy(partitions);
}

void ConsumerSession::configure_assignment(
    rd_kafka_t *consumer, rd_kafka_resp_err_t error,
    rd_kafka_topic_partition_list_t *partitions) {
  if (error == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
    // Assignment generations are runtime-wide rather than
    // session-local. A removed and later re-added subscription must
    // never recreate generation 1 and accept a cursor retained from
    // the former session.
    assignment_generation_ = owner_.next_assignment_generation();
    recovery_ends_.clear();
    stop_ends_.clear();
    assignment_starts_.clear();
    assigned_.clear();
    recovery_records_.clear();
    recovery_ready_ = false;
    recovery_paused_ = false;
    bounded_after_recovery_ = false;
    last_recovery_evaluation_time_.reset();
    bounded_complete_ = false;
    for (int index = 0; index < partitions->cnt; ++index) {
      const auto &partition = partitions->elems[index];
      assigned_.emplace_back(partition.topic, partition.partition);
      std::int64_t low{};
      std::int64_t high{};
      if (rd_kafka_query_watermark_offsets(
              consumer, partition.topic, partition.partition, &low, &high,
              5'000) == RD_KAFKA_RESP_ERR_NO_ERROR) {
        recovery_ends_.emplace_back(partition.topic, partition.partition, high);
      } else {
        throw std::runtime_error(
            "Unable to query Kafka partition watermark for " +
            Str{partition.topic} + ':' + std::to_string(partition.partition));
      }
    }
    resolve_stop_positions(consumer, partitions);
    resolve_start_positions(consumer, partitions);
    for (int index = 0; index < partitions->cnt; ++index) {
      const auto &partition = partitions->elems[index];
      if (partition.offset >= 0) {
        assignment_starts_.emplace_back(partition.topic, partition.partition,
                                        partition.offset);
      }
    }
    const auto assign_error = rd_kafka_assign(consumer, partitions);
    if (assign_error != RD_KAFKA_RESP_ERR_NO_ERROR) {
      throw std::runtime_error(rd_kafka_err2str(assign_error));
    }
    recovering_ = true;
    live_ = false;
    emit_state(KafkaSubscriptionState::Recovering);
    const auto starts_reached = [&](const std::vector<PositionBoundary> &ends) {
      return !ends.empty() &&
             std::all_of(ends.begin(), ends.end(), [&](const auto &end) {
               const auto *start = find_boundary(
                   assignment_starts_, std::get<0>(end), std::get<1>(end));
               return start != nullptr &&
                      std::get<2>(*start) >= std::get<2>(end);
             });
    };
    if (starts_reached(stop_ends_)) {
      if (buffers_recovery()) {
        prepare_recovery_flush(consumer, true);
      } else {
        complete_bounded();
      }
    } else if (starts_reached(recovery_ends_)) {
      if (buffers_recovery()) {
        prepare_recovery_flush(consumer, false);
      } else {
        recovering_ = false;
        live_ = true;
        emit_state(KafkaSubscriptionState::Live);
        complete_preload();
      }
    }
  } else if (error == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) {
    // Cursors already accepted from the graph are the only positions
    // this session is allowed to advance. Flush those before changing
    // the generation and releasing ownership of the assignment.
    process_commits(consumer);
    ++assignment_generation_;
    recovering_ = false;
    live_ = false;
    recovery_ends_.clear();
    stop_ends_.clear();
    assignment_starts_.clear();
    assigned_.clear();
    recovery_records_.clear();
    recovery_ready_ = false;
    recovery_paused_ = false;
    bounded_after_recovery_ = false;
    last_recovery_evaluation_time_.reset();
    static_cast<void>(rd_kafka_assign(consumer, nullptr));
    emit_state(KafkaSubscriptionState::Retrying);
  } else {
    static_cast<void>(rd_kafka_assign(consumer, nullptr));
    throw std::runtime_error(rd_kafka_err2str(error));
  }
}

void ConsumerSession::resolve_start_positions(
    rd_kafka_t *consumer, rd_kafka_topic_partition_list_t *partitions) {
  const auto explicit_offset =
      [&](std::string_view topic,
          std::int32_t partition) -> std::optional<std::int64_t> {
    const auto found = std::find_if(
        spec_.start_offsets.begin(), spec_.start_offsets.end(),
        [&](const auto &item) {
          return item.topic == topic && item.partition == partition;
        });
    return found == spec_.start_offsets.end()
               ? std::nullopt
               : std::optional<std::int64_t>{found->offset};
  };

  const bool timestamp_start =
      spec_.start_kind == KafkaStartPositionKind::Timestamp ||
      spec_.start_kind == KafkaStartPositionKind::GraphStartTime;
  if (timestamp_start) {
    const std::int64_t timestamp =
        spec_.start_kind == KafkaStartPositionKind::GraphStartTime
            ? owner_.graph_start_ms()
            : *spec_.start_timestamp_ms;
    for (int index = 0; index < partitions->cnt; ++index) {
      partitions->elems[index].offset = timestamp;
    }
    const auto error = rd_kafka_offsets_for_times(consumer, partitions, 5'000);
    if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
      throw std::runtime_error(
          Str{"Unable to resolve Kafka start timestamp: "} +
          rd_kafka_err2str(error));
    }
  }
  if (spec_.start_kind == KafkaStartPositionKind::Committed) {
    // A newly started broker may acknowledge topic traffic before its group
    // coordinator is ready.  Committed-offset discovery is a startup/recovery
    // operation, so retry coordinator transitions within the existing
    // five-second resolution budget instead of terminating the subscription.
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds{5'000};
    rd_kafka_resp_err_t error{};
    do {
      error = rd_kafka_committed(consumer, partitions, 500);
      if (error == RD_KAFKA_RESP_ERR_NO_ERROR || !retriable_error(error) ||
          stopping_.load(std::memory_order_relaxed) ||
          std::chrono::steady_clock::now() >= deadline) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds{50});
    } while (true);
    if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
      throw std::runtime_error(
          Str{"Unable to resolve committed Kafka offsets: "} +
          rd_kafka_err2str(error));
    }
  }

  for (int index = 0; index < partitions->cnt; ++index) {
    auto &partition = partitions->elems[index];
    switch (spec_.start_kind) {
    case KafkaStartPositionKind::Earliest: {
      std::int64_t low{};
      std::int64_t high{};
      const auto error = rd_kafka_query_watermark_offsets(
          consumer, partition.topic, partition.partition, &low, &high, 5'000);
      if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw std::runtime_error(
            "Unable to resolve earliest Kafka offset for " +
            Str{partition.topic} + ':' + std::to_string(partition.partition));
      }
      partition.offset = low;
      break;
    }
    case KafkaStartPositionKind::Latest: {
      const auto *boundary =
          find_boundary(recovery_ends_, partition.topic, partition.partition);
      partition.offset = std::get<2>(*boundary);
      break;
    }
    case KafkaStartPositionKind::Committed:
      if (partition.err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw std::runtime_error(
            "Unable to resolve committed Kafka offset for " +
            Str{partition.topic} + ':' + std::to_string(partition.partition) +
            ": " + rd_kafka_err2str(partition.err));
      }
      if (partition.offset < 0) {
        if (spec_.start_fallback == KafkaOffsetFallback::Fail) {
          throw std::runtime_error(
              "Kafka subscription has no committed offset for " +
              Str{partition.topic} + ':' + std::to_string(partition.partition));
        }
        if (spec_.start_fallback == KafkaOffsetFallback::Latest) {
          const auto *boundary = find_boundary(recovery_ends_, partition.topic,
                                               partition.partition);
          partition.offset = std::get<2>(*boundary);
        } else {
          std::int64_t low{};
          std::int64_t high{};
          const auto error = rd_kafka_query_watermark_offsets(
              consumer, partition.topic, partition.partition, &low, &high,
              5'000);
          if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
            throw std::runtime_error("Unable to resolve earliest Kafka "
                                     "committed fallback offset for " +
                                     Str{partition.topic} + ':' +
                                     std::to_string(partition.partition));
          }
          partition.offset = low;
        }
      }
      break;
    case KafkaStartPositionKind::Offsets: {
      const auto offset = explicit_offset(partition.topic, partition.partition);
      if (!offset.has_value()) {
        throw std::invalid_argument(
            "Kafka start offsets do not cover assigned partition " +
            Str{partition.topic} + ':' + std::to_string(partition.partition));
      }
      partition.offset = *offset;
      break;
    }
    case KafkaStartPositionKind::Timestamp:
    case KafkaStartPositionKind::GraphStartTime:
      if (partition.err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw std::runtime_error("Unable to resolve Kafka start offset for " +
                                 Str{partition.topic} + ':' +
                                 std::to_string(partition.partition) + ": " +
                                 rd_kafka_err2str(partition.err));
      }
      if (partition.offset < 0) {
        if (spec_.start_fallback == KafkaOffsetFallback::Fail) {
          throw std::runtime_error(
              "Kafka start timestamp has no matching offset for " +
              Str{partition.topic} + ':' + std::to_string(partition.partition));
        }
        if (spec_.start_fallback == KafkaOffsetFallback::Latest) {
          const auto *boundary = find_boundary(recovery_ends_, partition.topic,
                                               partition.partition);
          partition.offset = std::get<2>(*boundary);
        } else {
          std::int64_t low{};
          std::int64_t high{};
          const auto error = rd_kafka_query_watermark_offsets(
              consumer, partition.topic, partition.partition, &low, &high,
              5'000);
          if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
            throw std::runtime_error(
                "Unable to resolve earliest Kafka fallback offset for " +
                Str{partition.topic} + ':' +
                std::to_string(partition.partition));
          }
          partition.offset = low;
        }
      }
      break;
    }
  }
}

void ConsumerSession::resolve_stop_positions(
    rd_kafka_t *consumer, const rd_kafka_topic_partition_list_t *partitions) {
  if (spec_.stop_kind == KafkaStopPositionKind::Unbounded) {
    return;
  }
  if (spec_.stop_kind == KafkaStopPositionKind::Snapshot) {
    stop_ends_ = recovery_ends_;
    return;
  }

  if (spec_.stop_kind == KafkaStopPositionKind::Offsets) {
    for (int index = 0; index < partitions->cnt; ++index) {
      const auto &partition = partitions->elems[index];
      const auto found =
          std::find_if(spec_.stop_offsets.begin(), spec_.stop_offsets.end(),
                       [&](const auto &item) {
                         return item.topic == partition.topic &&
                                item.partition == partition.partition;
                       });
      if (found == spec_.stop_offsets.end()) {
        throw std::invalid_argument(
            "Kafka stop offsets do not cover assigned partition " +
            Str{partition.topic} + ':' + std::to_string(partition.partition));
      }
      stop_ends_.emplace_back(found->topic, found->partition, found->offset);
    }
    return;
  }

  auto *resolved = rd_kafka_topic_partition_list_copy(partitions);
  for (int index = 0; index < resolved->cnt; ++index) {
    resolved->elems[index].offset = *spec_.stop_timestamp_ms;
  }
  const auto error = rd_kafka_offsets_for_times(consumer, resolved, 5'000);
  if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
    rd_kafka_topic_partition_list_destroy(resolved);
    throw std::runtime_error(Str{"Unable to resolve Kafka stop timestamp: "} +
                             rd_kafka_err2str(error));
  }
  for (int index = 0; index < resolved->cnt; ++index) {
    const auto &partition = resolved->elems[index];
    if (partition.err != RD_KAFKA_RESP_ERR_NO_ERROR) {
      const Str message = "Unable to resolve Kafka stop offset for " +
                          Str{partition.topic} + ':' +
                          std::to_string(partition.partition) + ": " +
                          rd_kafka_err2str(partition.err);
      rd_kafka_topic_partition_list_destroy(resolved);
      throw std::runtime_error(message);
    }
    const auto *snapshot =
        find_boundary(recovery_ends_, partition.topic, partition.partition);
    const std::int64_t offset =
        partition.offset < 0 ? std::get<2>(*snapshot) : partition.offset;
    stop_ends_.emplace_back(partition.topic, partition.partition, offset);
  }
  rd_kafka_topic_partition_list_destroy(resolved);
}

bool ConsumerSession::record_is_before_boundaries(
    const rd_kafka_message_t *message,
    const std::vector<PositionBoundary> &boundaries) const noexcept {
  if (boundaries.empty()) {
    return true;
  }
  const auto *boundary = find_boundary(
      boundaries, rd_kafka_topic_name(message->rkt), message->partition);
  return boundary != nullptr && message->offset < std::get<2>(*boundary);
}

bool ConsumerSession::positions_reached(
    const rd_kafka_topic_partition_list_t *positions,
    const std::vector<PositionBoundary> &boundaries) const noexcept {
  if (boundaries.empty()) {
    return false;
  }
  for (const auto &boundary : boundaries) {
    const auto *position = rd_kafka_topic_partition_list_find(
        const_cast<rd_kafka_topic_partition_list_t *>(positions),
        std::get<0>(boundary).c_str(), std::get<1>(boundary));
    if (position == nullptr || position->offset < 0 ||
        position->offset < std::get<2>(boundary)) {
      return false;
    }
  }
  return true;
}

void ConsumerSession::complete_bounded() {
  if (bounded_complete_) {
    return;
  }
  bounded_complete_ = true;
  recovering_ = false;
  live_ = false;
  emit_state(
      KafkaSubscriptionState::BoundedComplete,
      last_recovery_evaluation_time_.has_value()
          ? std::optional<DateTime>{*last_recovery_evaluation_time_ + MIN_TD}
          : std::nullopt);
  complete_preload();
  // BoundedComplete is queued behind any final record/cursor. Keep the
  // consumer owner alive until ordinary subscription or graph teardown
  // so OnGraphDelivery and same-graph explicit commits produced while
  // those cursors drain can still be serviced. Subsequent records remain
  // suppressed by stop_ends_ and bounded_complete_.
}

void ConsumerSession::consume(rd_kafka_message_t *message) {
  if (!record_is_before_boundaries(message, stop_ends_) ||
      (recovering_ && !record_is_before_boundaries(message, recovery_ends_))) {
    return;
  }

  std::optional<Bytes> payload;
  if (message->payload != nullptr) {
    payload = Bytes{
        std::string{static_cast<const char *>(message->payload), message->len}};
  }
  std::optional<Bytes> key;
  if (message->key != nullptr) {
    key = Bytes{
        std::string{static_cast<const char *>(message->key), message->key_len}};
  }

  if (spec_.key_filter.has_value() &&
      (!key.has_value() || key->data != *spec_.key_filter)) {
    return;
  }

  std::vector<KafkaHeaderInput> headers;
  rd_kafka_headers_t *raw_headers{};
  if (rd_kafka_message_headers(message, &raw_headers) ==
      RD_KAFKA_RESP_ERR_NO_ERROR) {
    for (std::size_t index = 0;; ++index) {
      const char *name{};
      const void *value{};
      std::size_t size{};
      if (rd_kafka_header_get_all(raw_headers, index, &name, &value, &size) !=
          RD_KAFKA_RESP_ERR_NO_ERROR) {
        break;
      }
      headers.emplace_back(Str{name},
                           value != nullptr
                               ? std::optional<Bytes>{Bytes{std::string{
                                     static_cast<const char *>(value), size}}}
                               : std::nullopt);
    }
  }

  rd_kafka_timestamp_type_t raw_timestamp_type{};
  const std::int64_t timestamp_ms =
      rd_kafka_message_timestamp(message, &raw_timestamp_type);
  std::optional<DateTime> timestamp;
  if (timestamp_ms >= 0) {
    timestamp = DateTime{std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::milliseconds{timestamp_ms})};
  }

  const Str topic = rd_kafka_topic_name(message->rkt);
  Value record =
      make_record(topic, message->partition, message->offset,
                  std::move(payload), std::move(key), std::move(headers),
                  timestamp, timestamp_type(raw_timestamp_type));
  Value cursor = make_cursor(spec_.identity, assignment_generation_, topic,
                             message->partition, message->offset + 1);
  if (recovering_ && buffers_recovery()) {
    buffer_recovery_record(BufferedRecord{
        .record = std::move(record),
        .cursor = std::move(cursor),
        .timestamp = timestamp,
        .topic = topic,
        .partition = message->partition,
        .offset = message->offset,
    });
    return;
  }
  if (!owner_.emit_subscription(key_.clone(), std::move(record),
                                std::move(cursor),
                                recovering_ ? KafkaSubscriptionState::Recovering
                                            : KafkaSubscriptionState::Live)) {
    // Standard blocking sends fail only after graph teardown closes the
    // receiver. This is lifecycle completion, not overflow or record loss in
    // a running graph.
    stopping_ = true;
  }
}

bool ConsumerSession::buffers_recovery() const noexcept {
  return spec_.recovery_clock == KafkaRecoveryClock::RecordTimestamp ||
         spec_.merge_policy == KafkaMergePolicy::TimestampTopicPartitionOffset;
}

void ConsumerSession::buffer_recovery_record(BufferedRecord record) {
  const bool records_full =
      recovery_records_.size() >= owner_.config().ingress_records;
  if (!records_full) {
    recovery_records_.push_back(std::move(record));
    return;
  }

  const bool dropped =
      owner_.config().inbound_overflow == KafkaOverflowAction::Drop;
  owner_.emit_event(dropped ? KafkaSeverity::Warning : KafkaSeverity::Fatal,
                    Str{"consumer"}, Str{"recovery_overflow"},
                    RD_KAFKA_RESP_ERR__QUEUE_FULL, false, !dropped,
                    dropped ? Str{"Kafka recovery record was dropped because "
                                  "the bounded replay buffer is full"}
                            : Str{"bounded Kafka recovery buffer is full"},
                    spec_.identity, {},
                    !dropped && owner_.config().consumer_failure_policy ==
                                    KafkaFailurePolicy::StopGraph);
  if (!dropped) {
    failed_ = true;
    stopping_ = true;
  }
}

void ConsumerSession::prepare_recovery_flush(rd_kafka_t *consumer,
                                             bool bounded_after_recovery) {
  if (recovery_ready_) {
    return;
  }
  if (spec_.merge_policy == KafkaMergePolicy::TimestampTopicPartitionOffset) {
    std::stable_sort(recovery_records_.begin(), recovery_records_.end(),
                     [](const BufferedRecord &lhs, const BufferedRecord &rhs) {
                       return std::tuple{lhs.timestamp.value_or(MIN_DT),
                                         lhs.topic, lhs.partition, lhs.offset} <
                              std::tuple{rhs.timestamp.value_or(MIN_DT),
                                         rhs.topic, rhs.partition, rhs.offset};
                     });
  }
  bounded_after_recovery_ = bounded_after_recovery;
  recovery_ready_ = true;

  rd_kafka_topic_partition_list_t *partitions{};
  if (rd_kafka_assignment(consumer, &partitions) ==
          RD_KAFKA_RESP_ERR_NO_ERROR &&
      partitions && partitions->cnt > 0) {
    const auto error = rd_kafka_pause_partitions(consumer, partitions);
    if (error == RD_KAFKA_RESP_ERR_NO_ERROR) {
      recovery_paused_ = true;
    } else {
      owner_.emit_event(KafkaSeverity::Error, Str{"consumer"},
                        Str{"flow_control"}, error, true, false,
                        Str{rd_kafka_err2str(error)}, spec_.identity);
    }
  }
  if (partitions) {
    rd_kafka_topic_partition_list_destroy(partitions);
  }
}

void ConsumerSession::flush_recovery_records(rd_kafka_t *consumer) {
  while (!recovery_records_.empty()) {
    auto &front = recovery_records_.front();
    const bool coordinated_record_time = record_time_recovery_participant_;

    std::optional<DateTime> evaluation_time;
    if (spec_.recovery_clock == KafkaRecoveryClock::RecordTimestamp) {
      const DateTime graph_start{std::chrono::duration_cast<TimeDelta>(
          std::chrono::milliseconds{owner_.graph_start_ms()})};
      DateTime candidate = front.timestamp.value_or(
          last_recovery_evaluation_time_.value_or(graph_start) + MIN_TD);
      if (candidate < graph_start) {
        candidate = graph_start;
      }
      if (last_recovery_evaluation_time_.has_value() &&
          candidate <= *last_recovery_evaluation_time_) {
        candidate = *last_recovery_evaluation_time_ + MIN_TD;
      }
      last_recovery_evaluation_time_ = candidate;
      evaluation_time = candidate;
    }

    const bool pushed =
        coordinated_record_time
            ? owner_.emit_recovery_subscription(
                  key_.clone(), std::move(front.record),
                  std::move(front.cursor), KafkaSubscriptionState::Recovering,
                  evaluation_time)
            : owner_.emit_subscription(key_.clone(), std::move(front.record),
                                       std::move(front.cursor),
                                       KafkaSubscriptionState::Recovering,
                                       evaluation_time);
    if (!pushed) {
      stopping_ = true;
      recovery_records_.clear();
      return;
    }
    recovery_records_.erase(recovery_records_.begin());
  }

  const bool coordinated_record_time = record_time_recovery_participant_;
  if (coordinated_record_time) {
    if (!record_time_recovery_arrived_) {
      owner_.record_time_recovery_ready(last_recovery_evaluation_time_);
      record_time_recovery_arrived_ = true;
    }
    auto [released, recovery_tail] = owner_.record_time_recovery_status();
    if (!released) {
      return;
    }
    if (recovery_tail.has_value() &&
        (!last_recovery_evaluation_time_.has_value() ||
         *recovery_tail > *last_recovery_evaluation_time_)) {
      last_recovery_evaluation_time_ = *recovery_tail;
    }
  }

  recovery_ready_ = false;
  recovering_ = false;
  if (recovery_paused_) {
    rd_kafka_topic_partition_list_t *partitions{};
    if (rd_kafka_assignment(consumer, &partitions) ==
            RD_KAFKA_RESP_ERR_NO_ERROR &&
        partitions && partitions->cnt > 0) {
      const auto error = rd_kafka_resume_partitions(consumer, partitions);
      if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
        owner_.emit_event(KafkaSeverity::Error, Str{"consumer"},
                          Str{"flow_control"}, error, true, false,
                          Str{rd_kafka_err2str(error)}, spec_.identity);
      }
    }
    if (partitions) {
      rd_kafka_topic_partition_list_destroy(partitions);
    }
    recovery_paused_ = false;
  }
  if (bounded_after_recovery_) {
    complete_bounded();
    if (coordinated_record_time) {
      owner_.finish_record_time_recovery();
      record_time_recovery_finished_ = true;
      record_time_recovery_participant_ = false;
    }
    return;
  }
  live_ = true;
  emit_state(
      KafkaSubscriptionState::Live,
      last_recovery_evaluation_time_.has_value()
          ? std::optional<DateTime>{*last_recovery_evaluation_time_ + MIN_TD}
          : std::nullopt);
  if (coordinated_record_time) {
    owner_.finish_record_time_recovery();
    record_time_recovery_finished_ = true;
    record_time_recovery_participant_ = false;
  }
  complete_preload();
}

void ConsumerSession::process_commits(rd_kafka_t *consumer) {
  std::deque<Value> commits;
  {
    std::lock_guard lock{commands_mutex_};
    commits.swap(commits_);
  }
  for (auto &cursor : commits) {
    const auto fields = cursor.view().as_bundle();
    const Int generation = fields.at("assignment_generation").checked_as<Int>();
    const Str topic = fields.at("topic").checked_as<Str>();
    const Int raw_partition = fields.at("partition").checked_as<Int>();
    const Int raw_offset = fields.at("next_offset").checked_as<Int>();
    if (raw_partition < 0 ||
        raw_partition > std::numeric_limits<std::int32_t>::max() ||
        raw_offset < 0) {
      owner_.emit_event(KafkaSeverity::Warning, Str{"consumer"},
                        Str{"stale_commit"}, 0, false, false,
                        Str{"cursor partition or offset is invalid"},
                        spec_.identity);
      continue;
    }
    const auto partition = static_cast<std::int32_t>(raw_partition);
    const auto offset = static_cast<std::int64_t>(raw_offset);
    if (generation != assignment_generation_) {
      owner_.emit_event(KafkaSeverity::Warning, Str{"consumer"},
                        Str{"stale_commit"}, 0, false, false,
                        Str{"cursor assignment generation is stale"},
                        spec_.identity);
      continue;
    }
    const bool currently_assigned =
        std::ranges::any_of(assigned_, [&](const auto &item) {
          return item.first == topic && item.second == partition;
        });
    if (!currently_assigned) {
      owner_.emit_event(KafkaSeverity::Warning, Str{"consumer"},
                        Str{"stale_commit"}, 0, false, false,
                        Str{"cursor partition is not currently assigned"},
                        spec_.identity);
      continue;
    }
    auto prior = std::find_if(
        committed_.begin(), committed_.end(), [&](const auto &item) {
          return std::get<0>(item) == topic && std::get<1>(item) == partition;
        });
    if (prior != committed_.end() && offset <= std::get<2>(*prior)) {
      continue;
    }

    auto *positions = rd_kafka_topic_partition_list_new(1);
    auto *position =
        rd_kafka_topic_partition_list_add(positions, topic.c_str(), partition);
    position->offset = offset;
    const auto error = rd_kafka_commit(consumer, positions, 0);
    rd_kafka_topic_partition_list_destroy(positions);
    if (error == RD_KAFKA_RESP_ERR_NO_ERROR) {
      if (prior == committed_.end()) {
        committed_.emplace_back(topic, partition, offset);
      } else {
        std::get<2>(*prior) = offset;
      }
    } else {
      owner_.emit_event(KafkaSeverity::Error, Str{"consumer"}, Str{"commit"},
                        error, true, false, rd_kafka_err2str(error),
                        spec_.identity);
    }
  }
}

void ConsumerSession::check_positions(rd_kafka_t *consumer) {
  if ((!recovering_ || recovery_ends_.empty()) && stop_ends_.empty()) {
    return;
  }
  rd_kafka_topic_partition_list_t *positions{};
  if (rd_kafka_assignment(consumer, &positions) != RD_KAFKA_RESP_ERR_NO_ERROR ||
      !positions) {
    return;
  }
  static_cast<void>(rd_kafka_position(consumer, positions));

  const bool stop_reached = positions_reached(positions, stop_ends_);
  const bool recovery_reached =
      recovering_ && positions_reached(positions, recovery_ends_);
  if (recovery_reached) {
    auto *seeks = rd_kafka_topic_partition_list_new(positions->cnt);
    for (int index = 0; index < positions->cnt; ++index) {
      const auto &position = positions->elems[index];
      const auto *boundary =
          find_boundary(recovery_ends_, position.topic, position.partition);
      if (boundary != nullptr && position.offset > std::get<2>(*boundary)) {
        auto *seek = rd_kafka_topic_partition_list_add(seeks, position.topic,
                                                       position.partition);
        seek->offset = std::get<2>(*boundary);
      }
    }
    if (seeks->cnt > 0) {
      rd_kafka_error_t *error =
          rd_kafka_seek_partitions(consumer, seeks, 5'000);
      if (error) {
        const Str message = rd_kafka_error_string(error);
        rd_kafka_error_destroy(error);
        rd_kafka_topic_partition_list_destroy(seeks);
        rd_kafka_topic_partition_list_destroy(positions);
        throw std::runtime_error("Unable to restore Kafka recovery boundary: " +
                                 message);
      }
    }
    rd_kafka_topic_partition_list_destroy(seeks);
    if (buffers_recovery()) {
      prepare_recovery_flush(consumer, stop_reached);
    } else if (stop_reached) {
      complete_bounded();
    } else {
      recovering_ = false;
      live_ = true;
      emit_state(KafkaSubscriptionState::Live);
    }
  } else if (stop_reached) {
    if (buffers_recovery()) {
      prepare_recovery_flush(consumer, true);
    } else {
      complete_bounded();
    }
  }
  rd_kafka_topic_partition_list_destroy(positions);
}

void ConsumerSession::emit_state(KafkaSubscriptionState state,
                                 std::optional<DateTime> evaluation_time) {
  owner_.emit_subscription_state(key_.clone(), state, evaluation_time);
}

class KafkaRuntimeResource {
public:
  void install(std::shared_ptr<KafkaRuntime> runtime) {
    std::lock_guard lock{mutex_};
    if (runtime_) {
      throw std::logic_error("Kafka runtime resource was installed twice");
    }
    runtime_ = std::move(runtime);
  }

  [[nodiscard]] std::shared_ptr<KafkaRuntime> get() const {
    std::lock_guard lock{mutex_};
    return runtime_;
  }

  [[nodiscard]] std::shared_ptr<KafkaRuntime> take() noexcept {
    std::lock_guard lock{mutex_};
    return std::exchange(runtime_, {});
  }

private:
  mutable std::mutex mutex_{};
  std::shared_ptr<KafkaRuntime> runtime_{};
};

struct KafkaRuntimeHandle {
  std::shared_ptr<KafkaRuntimeResource> value{};

  friend bool operator==(const KafkaRuntimeHandle &,
                         const KafkaRuntimeHandle &) noexcept = default;
  friend std::strong_ordering
  operator<=>(const KafkaRuntimeHandle &lhs,
              const KafkaRuntimeHandle &rhs) noexcept {
    return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
           reinterpret_cast<std::uintptr_t>(rhs.value.get());
  }
};

inline std::ostream &operator<<(std::ostream &stream,
                                const KafkaRuntimeHandle &value) {
  return stream << "KafkaRuntimeHandle(" << value.value.get() << ')';
}

struct RuntimeConfigHandle {
  std::shared_ptr<const RuntimeConfig> value{};

  friend bool operator==(const RuntimeConfigHandle &,
                         const RuntimeConfigHandle &) noexcept = default;
  friend std::strong_ordering
  operator<=>(const RuntimeConfigHandle &lhs,
              const RuntimeConfigHandle &rhs) noexcept {
    return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
           reinterpret_cast<std::uintptr_t>(rhs.value.get());
  }
};

inline std::ostream &operator<<(std::ostream &stream,
                                const RuntimeConfigHandle &value) {
  return stream << "RuntimeConfigHandle(" << value.value.get() << ')';
}

class SimulationTransportQueue {
public:
  explicit SimulationTransportQueue(KafkaTransportBindings bindings)
      : bindings_{std::move(bindings)} {}

  [[nodiscard]] bool push(Value value) {
    std::lock_guard lock{mutex_};
    const auto position =
        std::upper_bound(values_.begin(), values_.end(), value,
                         [](const Value &lhs, const Value &rhs) {
                           const auto lhs_time = evaluation_time(lhs.view());
                           const auto rhs_time = evaluation_time(rhs.view());
                           if (!lhs_time.has_value()) {
                             return rhs_time.has_value();
                           }
                           return rhs_time.has_value() && *lhs_time < *rhs_time;
                         });
    values_.insert(position, std::move(value));
    return true;
  }

  [[nodiscard]] std::optional<DateTime> next_time() const {
    std::lock_guard lock{mutex_};
    return values_.empty() ? std::nullopt
                           : evaluation_time(values_.front().view());
  }

  [[nodiscard]] std::vector<Value> pop_batch() {
    std::lock_guard lock{mutex_};
    if (values_.empty()) {
      return {};
    }
    std::vector<Value> result;
    const auto batch_time = evaluation_time(values_.front().view());
    const bool batch_subscriptions =
        batch_time.has_value() &&
        values_.front()
                .view()
                .as_bundle()
                .at("kind")
                .checked_as<KafkaTransportEventKind>() ==
            KafkaTransportEventKind::Subscription;
    result.push_back(std::move(values_.front()));
    values_.pop_front();
    while (batch_subscriptions && !values_.empty() &&
           evaluation_time(values_.front().view()) == batch_time &&
           values_.front()
                   .view()
                   .as_bundle()
                   .at("kind")
                   .checked_as<KafkaTransportEventKind>() ==
               KafkaTransportEventKind::Subscription) {
      result.push_back(std::move(values_.front()));
      values_.pop_front();
    }
    return result;
  }

  [[nodiscard]] bool empty() const {
    std::lock_guard lock{mutex_};
    return values_.empty();
  }

  [[nodiscard]] Value build_batch(const std::vector<Value> &batch) const {
    ListBuilder builder{bindings_.event};
    for (const auto &value : batch) {
      builder.push_back_copy(value.view().data());
    }
    ListStorage storage = builder.build_storage();
    return Value{bindings_.batch, &storage};
  }

  [[nodiscard]] const KafkaTransportBindings &bindings() const noexcept {
    return bindings_;
  }

private:
  [[nodiscard]] static std::optional<DateTime>
  evaluation_time(const ValueView &value) {
    const auto field = value.as_bundle().at("evaluation_time");
    return field.data() == nullptr
               ? std::nullopt
               : std::optional<DateTime>{field.checked_as<DateTime>()};
  }

  mutable std::mutex mutex_{};
  std::deque<Value> values_{};
  KafkaTransportBindings bindings_{};
};

struct SimulationTransportQueueHandle {
  std::shared_ptr<SimulationTransportQueue> value{};

  friend bool
  operator==(const SimulationTransportQueueHandle &,
             const SimulationTransportQueueHandle &) noexcept = default;
  friend std::strong_ordering
  operator<=>(const SimulationTransportQueueHandle &lhs,
              const SimulationTransportQueueHandle &rhs) noexcept {
    return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
           reinterpret_cast<std::uintptr_t>(rhs.value.get());
  }
};

inline std::ostream &operator<<(std::ostream &stream,
                                const SimulationTransportQueueHandle &value) {
  return stream << "SimulationTransportQueueHandle(" << value.value.get()
                << ')';
}
} // namespace hgraph::kafka::detail

namespace std {
template <> struct hash<hgraph::kafka::detail::KafkaRuntimeHandle> {
  size_t operator()(
      const hgraph::kafka::detail::KafkaRuntimeHandle &value) const noexcept {
    return hash<const void *>{}(value.value.get());
  }
};

template <> struct hash<hgraph::kafka::detail::RuntimeConfigHandle> {
  size_t operator()(
      const hgraph::kafka::detail::RuntimeConfigHandle &value) const noexcept {
    return hash<const void *>{}(value.value.get());
  }
};

template <> struct hash<hgraph::kafka::detail::SimulationTransportQueueHandle> {
  size_t operator()(const hgraph::kafka::detail::SimulationTransportQueueHandle
                        &value) const noexcept {
    return hash<const void *>{}(value.value.get());
  }
};
} // namespace std

namespace hgraph::static_schema_detail {
template <> struct scalar_name<kafka::detail::KafkaRuntimeHandle> {
  static constexpr std::string_view value{
      "hgraph.kafka.internal::KafkaRuntimeHandle"};
};

template <> struct scalar_name<kafka::detail::RuntimeConfigHandle> {
  static constexpr std::string_view value{
      "hgraph.kafka.internal::RuntimeConfigHandle"};
};

template <> struct scalar_name<kafka::detail::SimulationTransportQueueHandle> {
  static constexpr std::string_view value{
      "hgraph.kafka.internal::SimulationTransportQueueHandle"};
};
} // namespace hgraph::static_schema_detail

namespace hgraph::kafka {
namespace {
using KafkaSubscriptionsInput =
    In<"subscriptions", TSS<KafkaSubscriptionKey>, InputValidity::Unchecked>;
using KafkaPublishInput =
    In<"publish", TSD<Int, KafkaPublishRequest>, InputValidity::Unchecked>;
using KafkaCommitInput =
    In<"commits", TSD<Int, TS<KafkaCursor>>, InputValidity::Unchecked>;

[[nodiscard]] std::shared_ptr<detail::KafkaRuntime>
live_runtime(Scalar<"runtime", detail::KafkaRuntimeHandle> runtime) {
  if (!runtime.value().value) {
    throw std::logic_error("Kafka runtime resource is not configured");
  }
  auto value = runtime.value().value->get();
  if (!value) {
    throw std::logic_error("Kafka command evaluated before runtime start");
  }
  return value;
}

void process_subscription_commands(KafkaSubscriptionsInput &subscriptions,
                                   detail::KafkaRuntime &runtime) {
  if (!subscriptions.modified()) {
    return;
  }
  const auto &erased = static_cast<const TSSInputView &>(subscriptions);
  for (const auto key : erased.removed()) {
    runtime.remove_subscription(key);
  }
  std::vector<Value> additions;
  for (const auto key : erased.added()) {
    additions.push_back(key.clone());
  }
  if (!additions.empty()) {
    runtime.add_subscriptions(std::move(additions));
  }
}

/** Graph-to-runtime subscription boundary. A sink is required for the external
 *  side effect; it processes only the TSS delta and starts/stops the affected
 *  consumer sessions. Cost is O(A + R) per tick for additions A and removals
 *  R; removal may wait for the corresponding owner thread to join. */
struct KafkaSubscriptionCommandSink {
  static constexpr auto name = "kafka_subscription_commands";

  static void eval(KafkaSubscriptionsInput subscriptions,
                   Scalar<"runtime", detail::KafkaRuntimeHandle> runtime) {
    process_subscription_commands(subscriptions, *live_runtime(runtime));
  }
};

/** Graph-to-runtime publish boundary. It processes only modified publish
 *  requests and stages each valid record for the producer task. Cost is O(M)
 *  per tick for M modified requests; producer creation is lazy and one-off. */
struct KafkaPublishSink {
  static constexpr auto name = "kafka_publish_commands";

  static void eval(KafkaPublishInput publish_requests,
                   Scalar<"runtime", detail::KafkaRuntimeHandle> runtime) {
    if (!publish_requests.modified()) {
      return;
    }
    auto task = live_runtime(runtime);
    for (const auto &[request_id_view, request] :
         publish_requests.modified_items()) {
      auto record = request.template field<"record">();
      auto topic = request.template field<"topic">();
      if (!record.modified() || !record.valid()) {
        continue;
      }
      if (!topic.valid()) {
        throw std::invalid_argument(
            "Kafka publish record requires a valid topic");
      }
      task->publish(request_id_view.checked_as<Int>(), topic.value(),
                    record.base().value().clone());
    }
  }
};

/** Graph-to-runtime explicit-commit boundary. It forwards only modified valid
 *  cursors to their consumer sessions, with O(M) work per modified tick. */
struct KafkaCommitSink {
  static constexpr auto name = "kafka_commit_commands";

  static void eval(KafkaCommitInput commits,
                   Scalar<"runtime", detail::KafkaRuntimeHandle> runtime) {
    if (!commits.modified()) {
      return;
    }
    auto task = live_runtime(runtime);
    for (const auto &[request_id, cursor] : commits.modified_items()) {
      static_cast<void>(request_id);
      if (cursor.valid() && cursor.modified()) {
        task->explicit_commit(cursor.base().value().clone());
      }
    }
  }
};

/** Protocol acknowledgement boundary for OnGraphDelivery. It observes the
 *  graph-side subscription projection, never sender admission, and forwards
 *  only cursors that the graph has received. Cost is O(M) per modified tick. */
struct KafkaGraphDeliveryCommitSink {
  static constexpr auto name = "kafka_graph_delivery_commit";

  static void
  eval(In<"subscriptions", TSD<KafkaSubscriptionKey, KafkaSubscriptionOutput>,
          InputValidity::Unchecked>
           subscriptions,
       Scalar<"runtime", detail::KafkaRuntimeHandle> runtime) {
    if (!subscriptions.modified()) {
      return;
    }
    auto task = live_runtime(runtime);
    for (const auto &[key, update] : subscriptions.modified_items()) {
      static_cast<void>(key);
      auto cursor = update.template field<"cursor">();
      if (cursor.valid() && cursor.modified()) {
        task->graph_delivered(cursor.base().value().clone());
      }
    }
  }
};

struct KafkaRealtimeTransportTag {};

[[nodiscard]] Port<TS<detail::KafkaTransportEvent>>
wire_realtime_transport(Wiring &w, detail::RuntimeConfigHandle config, Str path,
                        detail::KafkaRuntimeHandle runtime,
                        detail::KafkaTransportBindingsHandle bindings) {
  return detail::wire_transport_source<KafkaRealtimeTransportTag>(
      w,
      [config = std::move(config), path = std::move(path), runtime,
       bindings](PushSourceSender sender, const NodeView &, DateTime now) {
        auto task = std::make_shared<detail::KafkaRuntime>(
            *config.value, path, *bindings.value,
            [sender = std::move(sender)](Value value) {
              return sender.send_blocking(std::move(value));
            },
            now, false);
        task->start();
        auto rollback = make_scope_exit<true>([&] { task->stop(); });
        runtime.value->install(task);
        rollback.release();
      },
      [runtime](const NodeView &) {
        if (auto task = runtime.value->take()) {
          task->stop();
        }
      });
}

/** Simulation subscription primitive. A runtime node is required because the
 *  finite Kafka read is a tick-time external operation; it runs synchronously
 *  on the graph thread, creates no worker, and signals replay readiness after
 *  processing the subscription delta. Cost is the bounded recovery read. */
struct KafkaSimulationSubscriptionNode {
  static constexpr auto name = "kafka_simulation_subscription_commands";

  static void eval(KafkaSubscriptionsInput subscriptions,
                   Scalar<"runtime", detail::KafkaRuntimeHandle> runtime,
                   Out<TS<Int>> ready) {
    process_subscription_commands(subscriptions, *live_runtime(runtime));
    ready.set(Int{1});
  }
};

/** Graph-owned scheduled source for finite simulation history. Start installs
 *  the simulation runtime without a sender or worker; eval releases retained
 *  envelopes at their Kafka timestamps; stop releases the runtime. */
struct KafkaSimulationReplayNode {
  static constexpr auto name = "kafka_simulation_replay";
  using signature_args = std::tuple<
      In<"ready", TS<Int>>, Scalar<"config", detail::RuntimeConfigHandle>,
      Scalar<"path", Str>, Scalar<"runtime", detail::KafkaRuntimeHandle>,
      Scalar<"queue", detail::SimulationTransportQueueHandle>,
      SingleShotScheduler, Out<TS<detail::KafkaTransportEventBatch>>>;

  static void
  start(Scalar<"config", detail::RuntimeConfigHandle> config,
        Scalar<"path", Str> path,
        Scalar<"runtime", detail::KafkaRuntimeHandle> runtime,
        Scalar<"queue", detail::SimulationTransportQueueHandle> queue,
        SingleShotScheduler scheduler) {
    auto task = std::make_shared<detail::KafkaRuntime>(
        *config.value().value, path.value(), queue.value().value->bindings(),
        [queue = queue.value().value](Value value) {
          return queue->push(std::move(value));
        },
        scheduler.now(), true);
    task->start();
    auto rollback = make_scope_exit<true>([&] { task->stop(); });
    runtime.value().value->install(task);
    rollback.release();
  }

  /** Replays the next transport event at its recorded graph time. Kafka reads
   *  occur synchronously when subscription commands arrive; simulation owns
   *  no worker thread and never enters the push-source path.
   *
   *  Per tick: O(1) dequeue plus transport-envelope projection. Retained
   *  memory is O(n) in the bounded replay history. */
  static void
  eval(In<"ready", TS<Int>>,
       Scalar<"queue", detail::SimulationTransportQueueHandle> queue,
       SingleShotScheduler scheduler,
       Out<TS<detail::KafkaTransportEventBatch>> out) {
    const auto next_time = queue.value().value->next_time();
    if (next_time.has_value() && *next_time > scheduler.now()) {
      scheduler.schedule(*next_time);
      return;
    }
    auto batch = queue.value().value->pop_batch();
    if (batch.empty()) {
      return;
    }
    const Value value = queue.value().value->build_batch(batch);
    out.apply(value.view());
    const auto following_time = queue.value().value->next_time();
    if (following_time.has_value() && *following_time > scheduler.now()) {
      scheduler.schedule(*following_time);
    } else if (!queue.value().value->empty()) {
      scheduler.schedule(MIN_TD);
    }
  }

  static void stop(Scalar<"runtime", detail::KafkaRuntimeHandle> runtime) {
    if (auto task = runtime.value().value->take()) {
      task->stop();
    }
  }
};

struct KafkaServiceImpl {
  static constexpr auto name = "kafka_service_impl";

  static void compose(Wiring &w, Scalar<"config", Value> config,
                      Scalar<"simulation", Bool> simulation,
                      Scalar<"path", Str> path) {
    register_kafka_types();
    const auto parsed = detail::parse_config(config.value());
    detail::RuntimeConfigHandle runtime_config{
        std::make_shared<const detail::RuntimeConfig>(parsed)};
    const auto binding = service::path(path.value());
    auto subscription_keys =
        service::impl_input<KafkaSubscriptionService>(w, binding);
    auto publish_requests =
        service::impl_input<KafkaPublishService>(w, binding);
    auto commit_requests = service::impl_input<KafkaCommitService>(w, binding);

    detail::KafkaRuntimeHandle runtime{
        std::make_shared<detail::KafkaRuntimeResource>()};
    const auto transport_bindings = detail::make_transport_bindings();
    detail::ServiceOutputs outputs = [&] {
      if (simulation.value()) {
        detail::SimulationTransportQueueHandle queue{
            std::make_shared<detail::SimulationTransportQueue>(
                *transport_bindings.value)};
        auto ready = wire<KafkaSimulationSubscriptionNode>(w, subscription_keys,
                                                           runtime);
        static_cast<void>(wire<KafkaPublishSink>(w, publish_requests, runtime));
        static_cast<void>(wire<KafkaCommitSink>(w, commit_requests, runtime));
        auto replay =
            wire<KafkaSimulationReplayNode>(w, ready, runtime_config,
                                            path.value(), runtime, queue)
                .template as<TS<detail::KafkaTransportEventBatch>>();
        return detail::wire_simulation_service_outputs(
            w, replay, transport_bindings);
      }
      auto source = wire_realtime_transport(w, runtime_config, path.value(),
                                            runtime, transport_bindings);
      static_cast<void>(
          wire<KafkaSubscriptionCommandSink>(w, subscription_keys, runtime));
      static_cast<void>(wire<KafkaPublishSink>(w, publish_requests, runtime));
      static_cast<void>(wire<KafkaCommitSink>(w, commit_requests, runtime));
      return detail::wire_service_outputs(w, source, transport_bindings);
    }();
    static_cast<void>(
        wire<KafkaGraphDeliveryCommitSink>(w, outputs.subscriptions, runtime));

    service::impl_output<KafkaSubscriptionService>(w, binding,
                                                   outputs.subscriptions);
    service::impl_output<KafkaPublishService>(w, binding, outputs.deliveries);
    service::impl_output<KafkaEventService>(w, binding, outputs.events);
  }
};
} // namespace

void register_service(Wiring &w, service::ServicePath path,
                      Value service_config) {
  service::register_services<KafkaServiceImpl, KafkaSubscriptionService,
                             KafkaPublishService, KafkaCommitService,
                             KafkaEventService>(
      w, std::move(path), std::move(service_config), Bool{!w.is_realtime()});
}
} // namespace hgraph::kafka
