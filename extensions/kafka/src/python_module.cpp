#include <hgraph/kafka/service.h>
#include <hgraph/kafka/testing/mock_cluster.h>
#include <hgraph/kafka/value_builders.h>

#include <hgraph/python/native_scalar_registration.h>
#include <hgraph/types/operator_dispatch.h>

#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>

#include <stdexcept>
#include <utility>

namespace nb = nanobind;

namespace hgraph::kafka {
namespace {
struct RegisterKafkaServiceOperator
    : Operator<"hgraph.kafka.register_service",
               Scalar<"config", ScalarVar<"Config">>, Scalar<"path", Str>> {};

struct RegisterKafkaServiceGraph {
  static constexpr auto name = "hgraph.kafka.register_service_impl";

  static void compose(Wiring &w, Scalar<"config", ScalarVar<"Config">> config,
                      Scalar<"path", Str> path) {
    Value value = config.value().clone();
    if (value.schema() != scalar_descriptor<KafkaServiceConfig>::value_meta()) {
      throw std::invalid_argument(
          "register_kafka_service requires KafkaServiceConfig, received " +
          std::string{value.schema() != nullptr
                          ? value.schema()->name()
                          : std::string_view{"<unbound>"}});
    }
    KafkaServiceMode mode = KafkaServiceMode::RealTime;
    const ValueView configured_mode =
        w.global_state().get("__evaluation_mode__");
    if (configured_mode.valid()) {
      const Str selected = configured_mode.checked_as<Str>();
      if (selected == "simulation") {
        mode = KafkaServiceMode::Simulation;
      } else if (selected != "real_time") {
        throw std::invalid_argument(
            "Kafka service received an unsupported graph evaluation mode");
      }
    }
    register_service(w, service::path(path.value()), std::move(value), mode);
  }
};
} // namespace
} // namespace hgraph::kafka

NB_MODULE(_hgraph_kafka, module) {
  using namespace hgraph;
  using namespace hgraph::kafka;

  auto timestamp_type =
      nb::enum_<KafkaTimestampType>(module, "KafkaTimestampType")
          .value("NOT_AVAILABLE", KafkaTimestampType::NotAvailable)
          .value("CREATE_TIME", KafkaTimestampType::CreateTime)
          .value("LOG_APPEND_TIME", KafkaTimestampType::LogAppendTime);

  auto subscription_state =
      nb::enum_<KafkaSubscriptionState>(module, "KafkaSubscriptionState")
          .value("STARTING", KafkaSubscriptionState::Starting)
          .value("RECOVERING", KafkaSubscriptionState::Recovering)
          .value("LIVE", KafkaSubscriptionState::Live)
          .value("BOUNDED_COMPLETE", KafkaSubscriptionState::BoundedComplete)
          .value("RETRYING", KafkaSubscriptionState::Retrying)
          .value("STOPPED", KafkaSubscriptionState::Stopped)
          .value("FAILED", KafkaSubscriptionState::Failed);

  auto delivery_status =
      nb::enum_<KafkaDeliveryStatus>(module, "KafkaDeliveryStatus")
          .value("ENQUEUE_REJECTED", KafkaDeliveryStatus::EnqueueRejected)
          .value("DROPPED", KafkaDeliveryStatus::Dropped)
          .value("DELIVERED", KafkaDeliveryStatus::Delivered)
          .value("RETRIABLE_FAILURE", KafkaDeliveryStatus::RetriableFailure)
          .value("PERMANENT_FAILURE", KafkaDeliveryStatus::PermanentFailure);

  auto severity = nb::enum_<KafkaSeverity>(module, "KafkaSeverity")
                      .value("DEBUG", KafkaSeverity::Debug)
                      .value("INFO", KafkaSeverity::Info)
                      .value("WARNING", KafkaSeverity::Warning)
                      .value("ERROR", KafkaSeverity::Error)
                      .value("FATAL", KafkaSeverity::Fatal);

  auto commit_mode =
      nb::enum_<KafkaCommitMode>(module, "KafkaCommitMode")
          .value("NONE", KafkaCommitMode::None)
          .value("ON_GRAPH_DELIVERY", KafkaCommitMode::OnGraphDelivery)
          .value("EXPLICIT", KafkaCommitMode::Explicit);

  auto assignment_mode =
      nb::enum_<KafkaAssignmentMode>(module, "KafkaAssignmentMode")
          .value("GROUP", KafkaAssignmentMode::Group)
          .value("INDEPENDENT", KafkaAssignmentMode::Independent);

  auto selector_kind = nb::enum_<KafkaSelectorKind>(module, "KafkaSelectorKind")
                           .value("TOPICS", KafkaSelectorKind::Topics)
                           .value("PATTERN", KafkaSelectorKind::Pattern)
                           .value("PARTITIONS", KafkaSelectorKind::Partitions);

  auto start_kind =
      nb::enum_<KafkaStartPositionKind>(module, "KafkaStartPositionKind")
          .value("EARLIEST", KafkaStartPositionKind::Earliest)
          .value("LATEST", KafkaStartPositionKind::Latest)
          .value("COMMITTED", KafkaStartPositionKind::Committed)
          .value("TIMESTAMP", KafkaStartPositionKind::Timestamp)
          .value("OFFSETS", KafkaStartPositionKind::Offsets)
          .value("GRAPH_START_TIME", KafkaStartPositionKind::GraphStartTime);

  auto stop_kind =
      nb::enum_<KafkaStopPositionKind>(module, "KafkaStopPositionKind")
          .value("UNBOUNDED", KafkaStopPositionKind::Unbounded)
          .value("SNAPSHOT", KafkaStopPositionKind::Snapshot)
          .value("GRAPH_LIFETIME", KafkaStopPositionKind::GraphLifetime)
          .value("TIMESTAMP", KafkaStopPositionKind::Timestamp)
          .value("OFFSETS", KafkaStopPositionKind::Offsets);

  auto offset_fallback =
      nb::enum_<KafkaOffsetFallback>(module, "KafkaOffsetFallback")
          .value("EARLIEST", KafkaOffsetFallback::Earliest)
          .value("LATEST", KafkaOffsetFallback::Latest)
          .value("FAIL", KafkaOffsetFallback::Fail);

  auto recovery_clock =
      nb::enum_<KafkaRecoveryClock>(module, "KafkaRecoveryClock")
          .value("ARRIVAL", KafkaRecoveryClock::Arrival)
          .value("RECORD_TIMESTAMP", KafkaRecoveryClock::RecordTimestamp);

  auto merge_policy =
      nb::enum_<KafkaMergePolicy>(module, "KafkaMergePolicy")
          .value("PARTITION", KafkaMergePolicy::Partition)
          .value("TIMESTAMP_TOPIC_PARTITION_OFFSET",
                 KafkaMergePolicy::TimestampTopicPartitionOffset);

  auto overflow_action =
      nb::enum_<KafkaOverflowAction>(module, "KafkaOverflowAction")
          .value("FAIL", KafkaOverflowAction::Fail)
          .value("DROP", KafkaOverflowAction::Drop)
          .value("STAGE", KafkaOverflowAction::Stage);

  auto failure_policy =
      nb::enum_<KafkaFailurePolicy>(module, "KafkaFailurePolicy")
          .value("REPORT", KafkaFailurePolicy::Report)
          .value("STOP_GRAPH", KafkaFailurePolicy::StopGraph);

  // Keyed installer (RFC 0025 checkpoint 3): the extension's ENTIRE
  // registration — native types, operator overloads, AND the python
  // scalar associations (the captured class handles) — so a registry
  // reset-and-rebuild replays this extension exactly as it replays core.
  hgraph::OperatorRegistry::instance().register_installer(
      "hgraph.kafka", [=] {
        register_kafka_types();
        register_graph_overload<RegisterKafkaServiceOperator,
                                RegisterKafkaServiceGraph>();
        python_bridge::register_native_scalar_type<KafkaTimestampType>(timestamp_type);
        python_bridge::register_native_scalar_type<KafkaSubscriptionState>(subscription_state);
        python_bridge::register_native_scalar_type<KafkaDeliveryStatus>(delivery_status);
        python_bridge::register_native_scalar_type<KafkaSeverity>(severity);
        python_bridge::register_native_scalar_type<KafkaCommitMode>(commit_mode);
        python_bridge::register_native_scalar_type<KafkaAssignmentMode>(assignment_mode);
        python_bridge::register_native_scalar_type<KafkaSelectorKind>(selector_kind);
        python_bridge::register_native_scalar_type<KafkaStartPositionKind>(start_kind);
        python_bridge::register_native_scalar_type<KafkaStopPositionKind>(stop_kind);
        python_bridge::register_native_scalar_type<KafkaOffsetFallback>(offset_fallback);
        python_bridge::register_native_scalar_type<KafkaRecoveryClock>(recovery_clock);
        python_bridge::register_native_scalar_type<KafkaMergePolicy>(merge_policy);
        python_bridge::register_native_scalar_type<KafkaOverflowAction>(overflow_action);
        python_bridge::register_native_scalar_type<KafkaFailurePolicy>(failure_policy);
      });
  hgraph::OperatorRegistry::instance().run_installers();

  nb::enum_<testing::MockProduceError>(module, "MockProduceError")
      .value("RETRIABLE", testing::MockProduceError::Retriable)
      .value("PERMANENT", testing::MockProduceError::Permanent);

  nb::enum_<testing::MockConsumeError>(module, "MockConsumeError")
      .value("RETRIABLE", testing::MockConsumeError::Retriable)
      .value("PERMANENT", testing::MockConsumeError::Permanent);

  nb::class_<testing::MockCluster>(module, "MockCluster")
      .def(nb::init<std::int32_t>(), nb::arg("broker_count") = 3)
      .def("bootstrap_servers", &testing::MockCluster::bootstrap_servers)
      .def("create_topic", &testing::MockCluster::create_topic,
           nb::arg("topic"), nb::arg("partitions") = 1,
           nb::arg("replication_factor") = 1)
      .def("fail_next_produce", &testing::MockCluster::fail_next_produce,
           nb::arg("error"), nb::arg("count") = 1)
      .def("fail_next_fetch", &testing::MockCluster::fail_next_fetch,
           nb::arg("error"), nb::arg("count") = 1)
      .def(
          "seed_record",
          [](testing::MockCluster &cluster, Str topic, nb::handle value,
             nb::handle key, nb::iterable header_values, std::int32_t partition,
             nb::handle timestamp_ms) {
            const auto bytes = [](nb::handle object) -> std::optional<Bytes> {
              if (object.is_none()) {
                return std::nullopt;
              }
              if (!nb::isinstance<nb::bytes>(object)) {
                throw nb::type_error(
                    "Kafka payloads, keys, and headers must be bytes or None");
              }
              const auto raw = nb::borrow<nb::bytes>(object);
              return Bytes{std::string{raw.c_str(), raw.size()}};
            };

            std::vector<KafkaHeaderInput> headers;
            for (nb::handle item : header_values) {
              const auto pair = nb::cast<nb::tuple>(item);
              if (pair.size() != 2) {
                throw nb::value_error(
                    "Kafka test headers must be (name, value) pairs");
              }
              headers.emplace_back(nb::cast<Str>(pair[0]), bytes(pair[1]));
            }
            std::optional<DateTime> timestamp;
            if (!timestamp_ms.is_none()) {
              timestamp = DateTime{std::chrono::duration_cast<TimeDelta>(
                  std::chrono::milliseconds{
                      nb::cast<std::int64_t>(timestamp_ms)})};
            }
            cluster.seed_record(std::move(topic), bytes(value), bytes(key),
                                std::move(headers), partition, timestamp);
          },
          nb::arg("topic"), nb::arg("value") = nb::none(),
          nb::arg("key") = nb::none(), nb::arg("headers") = nb::tuple(),
          nb::arg("partition") = 0, nb::arg("timestamp_ms") = nb::none());
}
