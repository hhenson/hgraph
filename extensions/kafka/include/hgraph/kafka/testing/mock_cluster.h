#ifndef HGRAPH_KAFKA_TESTING_MOCK_CLUSTER_H
#define HGRAPH_KAFKA_TESTING_MOCK_CLUSTER_H

#include <hgraph/kafka/export.h>
#include <hgraph/kafka/value_builders.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace hgraph::kafka::testing {
enum class MockProduceError : std::int64_t {
  Retriable,
  Permanent,
};

enum class MockConsumeError : std::int64_t {
  Retriable,
  Permanent,
};

/** In-process librdkafka mock cluster for deterministic connector tests.
 * The public testing surface does not expose librdkafka handles. */
class HGRAPH_KAFKA_EXPORT MockCluster {
public:
  explicit MockCluster(std::int32_t broker_count = 3);
  ~MockCluster();

  MockCluster(const MockCluster &) = delete;
  MockCluster &operator=(const MockCluster &) = delete;

  [[nodiscard]] Str bootstrap_servers() const;
  void create_topic(Str topic, std::int32_t partitions = 1,
                    std::int32_t replication_factor = 1);
  void seed_record(Str topic, std::optional<Bytes> value,
                   std::optional<Bytes> key = std::nullopt,
                   std::vector<KafkaHeaderInput> headers = {},
                   std::int32_t partition = 0,
                   std::optional<DateTime> timestamp = std::nullopt);
  void fail_next_produce(MockProduceError error, std::size_t count = 1);
  void fail_next_fetch(MockConsumeError error, std::size_t count = 1);

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
} // namespace hgraph::kafka::testing

#endif // HGRAPH_KAFKA_TESTING_MOCK_CLUSTER_H
