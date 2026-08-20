#include <hgraph/kafka/testing/mock_cluster.h>

#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wignored-qualifiers"
#endif
#if __has_include(<librdkafka/rdkafka.h>)
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafka_mock.h>
#else
#include <rdkafka.h>
#include <rdkafka_mock.h>
#endif
#if defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

#include <array>
#include <chrono>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

namespace hgraph::kafka::testing {
namespace {
struct ConfigDeleter {
  void operator()(rd_kafka_conf_t *value) const noexcept {
    if (value) {
      rd_kafka_conf_destroy(value);
    }
  }
};

using ConfigPtr = std::unique_ptr<rd_kafka_conf_t, ConfigDeleter>;
} // namespace

struct MockCluster::Impl {
  rd_kafka_t *owner{};
  rd_kafka_mock_cluster_t *cluster{};
  rd_kafka_t *producer{};
};

MockCluster::MockCluster(std::int32_t broker_count)
    : impl_{std::make_unique<Impl>()} {
  if (broker_count <= 0) {
    throw std::invalid_argument("Kafka mock cluster needs a broker");
  }
  ConfigPtr config{rd_kafka_conf_new()};
  char error[512]{};
  impl_->owner =
      rd_kafka_new(RD_KAFKA_PRODUCER, config.get(), error, sizeof(error));
  if (!impl_->owner) {
    throw std::runtime_error(Str{"Unable to create mock Kafka owner: "} +
                             error);
  }
  static_cast<void>(config.release());
  impl_->cluster = rd_kafka_mock_cluster_new(impl_->owner, broker_count);
  if (!impl_->cluster) {
    rd_kafka_destroy(impl_->owner);
    impl_->owner = nullptr;
    throw std::runtime_error("Unable to create librdkafka mock cluster");
  }
  rd_kafka_mock_group_initial_rebalance_delay_ms(impl_->cluster, 0);

  ConfigPtr producer_config{rd_kafka_conf_new()};
  if (rd_kafka_conf_set(producer_config.get(), "bootstrap.servers",
                        rd_kafka_mock_cluster_bootstraps(impl_->cluster), error,
                        sizeof(error)) != RD_KAFKA_CONF_OK) {
    rd_kafka_mock_cluster_destroy(impl_->cluster);
    rd_kafka_destroy(impl_->owner);
    throw std::runtime_error(error);
  }
  impl_->producer = rd_kafka_new(RD_KAFKA_PRODUCER, producer_config.get(),
                                 error, sizeof(error));
  if (!impl_->producer) {
    rd_kafka_mock_cluster_destroy(impl_->cluster);
    rd_kafka_destroy(impl_->owner);
    throw std::runtime_error(Str{"Unable to create mock Kafka producer: "} +
                             error);
  }
  static_cast<void>(producer_config.release());
}

MockCluster::~MockCluster() {
  if (!impl_) {
    return;
  }
  if (impl_->producer) {
    static_cast<void>(rd_kafka_flush(impl_->producer, 5'000));
    rd_kafka_destroy(impl_->producer);
  }
  if (impl_->cluster) {
    rd_kafka_mock_cluster_destroy(impl_->cluster);
  }
  if (impl_->owner) {
    rd_kafka_destroy(impl_->owner);
  }
}

Str MockCluster::bootstrap_servers() const {
  return rd_kafka_mock_cluster_bootstraps(impl_->cluster);
}

void MockCluster::create_topic(Str topic, std::int32_t partitions,
                               std::int32_t replication_factor) {
  const auto error = rd_kafka_mock_topic_create(impl_->cluster, topic.c_str(),
                                                partitions, replication_factor);
  if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw std::runtime_error(Str{"Unable to create mock Kafka topic: "} +
                             rd_kafka_err2str(error));
  }
}

void MockCluster::seed_record(Str topic, std::optional<Bytes> value,
                              std::optional<Bytes> key,
                              std::vector<KafkaHeaderInput> header_values,
                              std::int32_t partition,
                              std::optional<DateTime> timestamp) {
  rd_kafka_headers_t *headers = rd_kafka_headers_new(header_values.size());
  for (const auto &[name, header_value] : header_values) {
    const void *data =
        header_value.has_value()
            ? static_cast<const void *>(header_value->data.data())
            : nullptr;
    const std::size_t size =
        header_value.has_value() ? header_value->data.size() : 0;
    const auto error =
        rd_kafka_header_add(headers, name.c_str(), name.size(), data, size);
    if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
      rd_kafka_headers_destroy(headers);
      throw std::runtime_error(rd_kafka_err2str(error));
    }
  }

  const void *payload = value.has_value()
                            ? static_cast<const void *>(value->data.data())
                            : nullptr;
  const std::size_t payload_size = value.has_value() ? value->data.size() : 0;
  const void *key_data =
      key.has_value() ? static_cast<const void *>(key->data.data()) : nullptr;
  const std::size_t key_size = key.has_value() ? key->data.size() : 0;

  std::array<rd_kafka_vu_t, 7> arguments{};
  std::size_t count{};
  arguments[count].vtype = RD_KAFKA_VTYPE_TOPIC;
  arguments[count++].u.cstr = topic.c_str();
  arguments[count].vtype = RD_KAFKA_VTYPE_PARTITION;
  arguments[count++].u.i32 = partition;
  arguments[count].vtype = RD_KAFKA_VTYPE_MSGFLAGS;
  arguments[count++].u.i = RD_KAFKA_MSG_F_COPY;
  arguments[count].vtype = RD_KAFKA_VTYPE_VALUE;
  arguments[count].u.mem.ptr = const_cast<void *>(payload);
  arguments[count++].u.mem.size = payload_size;
  arguments[count].vtype = RD_KAFKA_VTYPE_KEY;
  arguments[count].u.mem.ptr = const_cast<void *>(key_data);
  arguments[count++].u.mem.size = key_size;
  if (timestamp.has_value()) {
    arguments[count].vtype = RD_KAFKA_VTYPE_TIMESTAMP;
    arguments[count++].u.i64 =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            timestamp->time_since_epoch())
            .count();
  }
  arguments[count].vtype = RD_KAFKA_VTYPE_HEADERS;
  arguments[count++].u.headers = headers;

  rd_kafka_error_t *produce_error =
      rd_kafka_produceva(impl_->producer, arguments.data(), count);
  if (produce_error) {
    const Str message = rd_kafka_error_string(produce_error);
    rd_kafka_error_destroy(produce_error);
    rd_kafka_headers_destroy(headers);
    throw std::runtime_error(message);
  }
  const auto flush_error = rd_kafka_flush(impl_->producer, 5'000);
  if (flush_error != RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw std::runtime_error(rd_kafka_err2str(flush_error));
  }
}

void MockCluster::fail_next_produce(MockProduceError error, std::size_t count) {
  if (count == 0) {
    return;
  }
  if (count > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
    throw std::invalid_argument("Kafka mock failure count is out of range");
  }
  const auto response = error == MockProduceError::Retriable
                            ? RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS
                            : RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED;
  std::vector<rd_kafka_resp_err_t> responses(count, response);
  rd_kafka_mock_push_request_errors_array(impl_->cluster, 0, responses.size(),
                                          responses.data());
}

void MockCluster::fail_next_fetch(MockConsumeError error, std::size_t count) {
  if (count == 0) {
    return;
  }
  if (count > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
    throw std::invalid_argument("Kafka mock failure count is out of range");
  }
  const auto response = error == MockConsumeError::Retriable
                            ? RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION
                            : RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED;
  std::vector<rd_kafka_resp_err_t> responses(count, response);
  // Fetch is Kafka protocol API key 1. librdkafka's public mock API accepts
  // the numeric protocol key rather than exporting its internal enum.
  rd_kafka_mock_push_request_errors_array(impl_->cluster, 1, responses.size(),
                                          responses.data());
}

void MockCluster::fail_next_committed_offset_fetch(std::size_t count) {
  if (count == 0) {
    return;
  }
  if (count > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
    throw std::invalid_argument("Kafka mock failure count is out of range");
  }
  std::vector<rd_kafka_resp_err_t> responses(
      count, RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE);
  // OffsetFetch is Kafka protocol API key 9.  This reproduces the startup
  // window in which topic traffic is available before the group coordinator.
  rd_kafka_mock_push_request_errors_array(impl_->cluster, 9, responses.size(),
                                          responses.data());
}

void MockCluster::stop_brokers() {
  const auto error = rd_kafka_mock_broker_set_down(impl_->cluster, -1);
  if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw std::runtime_error(Str{"Unable to stop mock Kafka brokers: "} +
                             rd_kafka_err2str(error));
  }
}

void MockCluster::start_brokers() {
  const auto error = rd_kafka_mock_broker_set_up(impl_->cluster, -1);
  if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw std::runtime_error(Str{"Unable to start mock Kafka brokers: "} +
                             rd_kafka_err2str(error));
  }
}
} // namespace hgraph::kafka::testing
