#ifndef HGRAPH_KAFKA_VALUE_BUILDERS_H
#define HGRAPH_KAFKA_VALUE_BUILDERS_H

#include <hgraph/kafka/export.h>
#include <hgraph/kafka/types.h>
#include <hgraph/types/value/value.h>

#include <chrono>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph::kafka
{
    using KafkaHeaderInput = std::pair<Str, std::optional<Bytes>>;
    using KafkaOptionInput = std::pair<Str, Str>;

    struct KafkaPartitionOffsetInput
    {
        Str topic{};
        Int partition{};
        Int offset{};
    };

    struct KafkaTopicPartitionInput
    {
        Str topic{};
        Int partition{};
    };

    class HGRAPH_KAFKA_EXPORT ServiceConfigBuilder
    {
      public:
        ServiceConfigBuilder &bootstrap_servers(std::vector<Str> value);
        ServiceConfigBuilder &client_id(Str value);
        ServiceConfigBuilder &idempotent_producer(bool value);
        ServiceConfigBuilder &producer_acknowledgements(Str value);
        ServiceConfigBuilder &producer_retries(Int value);
        ServiceConfigBuilder &producer_linger(std::chrono::milliseconds value);
        ServiceConfigBuilder &producer_batch_record_limit(Int value);
        ServiceConfigBuilder &ingress_limits(Int records, Int bytes);
        ServiceConfigBuilder &outbound_limits(Int records, Int bytes);
        ServiceConfigBuilder &inbound_overflow(KafkaOverflowAction value);
        ServiceConfigBuilder &consumer_failure_policy(KafkaFailurePolicy value);
        ServiceConfigBuilder &outbound_overflow(KafkaOverflowAction value,
                                                KafkaOverflowAction stage_full = KafkaOverflowAction::Fail);
        ServiceConfigBuilder &shutdown_drain_timeout(std::chrono::milliseconds value);
        ServiceConfigBuilder &producer_failure_policy(KafkaFailurePolicy value);
        ServiceConfigBuilder &common_option(Str name, Str value);
        ServiceConfigBuilder &consumer_option(Str name, Str value);
        ServiceConfigBuilder &producer_option(Str name, Str value);

        [[nodiscard]] Value build() const;

      private:
        std::vector<Str>              bootstrap_servers_{};
        Str                           client_id_{};
        bool                          idempotent_producer_{true};
        Str                           producer_acknowledgements_{"all"};
        Int                           producer_retries_{2'147'483'647};
        Int                           producer_linger_ms_{5};
        Int                           producer_batch_record_limit_{1'000};
        Int                           ingress_record_limit_{10'000};
        Int                           ingress_byte_limit_{64 * 1024 * 1024};
        Int                           outbound_record_limit_{10'000};
        Int                           outbound_byte_limit_{64 * 1024 * 1024};
        std::vector<KafkaOptionInput> common_options_{};
        std::vector<KafkaOptionInput> consumer_options_{};
        std::vector<KafkaOptionInput> producer_options_{};
        KafkaOverflowAction           inbound_overflow_{KafkaOverflowAction::Fail};
        KafkaFailurePolicy            consumer_failure_policy_{KafkaFailurePolicy::Report};
        KafkaOverflowAction           outbound_overflow_{KafkaOverflowAction::Stage};
        KafkaOverflowAction           stage_overflow_{KafkaOverflowAction::Fail};
        Int                           shutdown_drain_timeout_ms_{5'000};
        KafkaFailurePolicy            producer_failure_policy_{KafkaFailurePolicy::Report};
    };

    [[nodiscard]] HGRAPH_KAFKA_EXPORT ServiceConfigBuilder service_config();

    class HGRAPH_KAFKA_EXPORT SubscriptionKeyBuilder
    {
      public:
        SubscriptionKeyBuilder &topics(std::vector<Str> value);
        SubscriptionKeyBuilder &topic_pattern(Str value);
        SubscriptionKeyBuilder &partitions(std::vector<KafkaTopicPartitionInput> value);
        SubscriptionKeyBuilder &group_id(Str value);
        SubscriptionKeyBuilder &assignment_mode(KafkaAssignmentMode value);
        SubscriptionKeyBuilder &start(Value value);
        SubscriptionKeyBuilder &stop(Value value);
        SubscriptionKeyBuilder &commit_mode(KafkaCommitMode value);
        SubscriptionKeyBuilder &sharing_identity(Str value);
        SubscriptionKeyBuilder &isolation_level(Str value);
        SubscriptionKeyBuilder &recovery_clock(KafkaRecoveryClock value);
        SubscriptionKeyBuilder &merge_policy(KafkaMergePolicy value);
        SubscriptionKeyBuilder &key_filter(std::optional<Bytes> value);

        [[nodiscard]] Value build() const;

      private:
        KafkaSelectorKind                     selector_{KafkaSelectorKind::Topics};
        std::vector<Str>                      topics_{};
        Str                                   topic_pattern_{};
        std::vector<KafkaTopicPartitionInput> partitions_{};
        Str                                   group_id_{};
        KafkaAssignmentMode                   assignment_mode_{KafkaAssignmentMode::Group};
        Value                                 start_{};
        Value                                 stop_{};
        KafkaCommitMode                       commit_mode_{KafkaCommitMode::Explicit};
        Str                                   sharing_identity_{};
        Str                                   isolation_level_{"read_uncommitted"};
        KafkaRecoveryClock                    recovery_clock_{KafkaRecoveryClock::Arrival};
        KafkaMergePolicy                      merge_policy_{KafkaMergePolicy::Partition};
        std::optional<Bytes>                  key_filter_{};
    };

    [[nodiscard]] HGRAPH_KAFKA_EXPORT SubscriptionKeyBuilder subscription_key();

    [[nodiscard]] HGRAPH_KAFKA_EXPORT Value make_start_position(KafkaStartPositionKind  kind,
                                                                KafkaOffsetFallback     fallback  = KafkaOffsetFallback::Earliest,
                                                                std::optional<DateTime> timestamp = std::nullopt,
                                                                std::vector<KafkaPartitionOffsetInput> offsets = {});

    [[nodiscard]] HGRAPH_KAFKA_EXPORT Value make_stop_position(KafkaStopPositionKind                  kind,
                                                               std::optional<DateTime>                timestamp = std::nullopt,
                                                               std::vector<KafkaPartitionOffsetInput> offsets   = {});

    [[nodiscard]] HGRAPH_KAFKA_EXPORT Value make_service_config(
        std::vector<Str> bootstrap_servers, Str client_id = {}, bool idempotent_producer = true, Int ingress_record_limit = 10'000,
        Int ingress_byte_limit = 64 * 1024 * 1024, Int outbound_record_limit = 10'000, Int outbound_byte_limit = 64 * 1024 * 1024,
        std::vector<KafkaOptionInput> common_options = {}, std::vector<KafkaOptionInput> consumer_options = {},
        std::vector<KafkaOptionInput> producer_options = {}, KafkaOverflowAction inbound_overflow = KafkaOverflowAction::Fail,
        KafkaFailurePolicy  consumer_failure_policy = KafkaFailurePolicy::Report,
        KafkaOverflowAction outbound_overflow       = KafkaOverflowAction::Stage,
        KafkaOverflowAction stage_overflow = KafkaOverflowAction::Fail, Int shutdown_drain_timeout_ms = 5'000,
        KafkaFailurePolicy producer_failure_policy = KafkaFailurePolicy::Report, Str producer_acknowledgements = "all",
        Int producer_retries = 2'147'483'647, Int producer_linger_ms = 5, Int producer_batch_record_limit = 1'000);

    [[nodiscard]] HGRAPH_KAFKA_EXPORT Value make_subscription_key(
        std::vector<Str> topics, Str group_id, Str start_position = "committed:earliest", Str stop_position = "unbounded",
        KafkaCommitMode commit_mode = KafkaCommitMode::Explicit, Str sharing_identity = {},
        Str isolation_level = "read_uncommitted", Str recovery_clock = "arrival", Str merge_policy = "partition",
        std::optional<Bytes> key_filter = std::nullopt, KafkaAssignmentMode assignment_mode = KafkaAssignmentMode::Group);

    [[nodiscard]] HGRAPH_KAFKA_EXPORT Value make_subscription_key(
        std::vector<Str> topics, Str group_id, Value start_position, Value stop_position,
        KafkaCommitMode commit_mode = KafkaCommitMode::Explicit, Str sharing_identity = {},
        Str isolation_level = "read_uncommitted", KafkaRecoveryClock recovery_clock = KafkaRecoveryClock::Arrival,
        KafkaMergePolicy merge_policy = KafkaMergePolicy::Partition, std::optional<Bytes> key_filter = std::nullopt,
        KafkaAssignmentMode assignment_mode = KafkaAssignmentMode::Group);

    [[nodiscard]] HGRAPH_KAFKA_EXPORT Value make_pattern_subscription_key(
        Str topic_pattern, Str group_id, Value start_position, Value stop_position,
        KafkaCommitMode commit_mode = KafkaCommitMode::Explicit, Str sharing_identity = {},
        Str isolation_level = "read_uncommitted", KafkaRecoveryClock recovery_clock = KafkaRecoveryClock::Arrival,
        KafkaMergePolicy merge_policy = KafkaMergePolicy::Partition, std::optional<Bytes> key_filter = std::nullopt,
        KafkaAssignmentMode assignment_mode = KafkaAssignmentMode::Group);

    [[nodiscard]] HGRAPH_KAFKA_EXPORT Value make_partition_subscription_key(
        std::vector<KafkaTopicPartitionInput> partitions, Str group_id, Value start_position, Value stop_position,
        KafkaCommitMode commit_mode = KafkaCommitMode::Explicit, Str sharing_identity = {},
        Str isolation_level = "read_uncommitted", KafkaRecoveryClock recovery_clock = KafkaRecoveryClock::Arrival,
        KafkaMergePolicy merge_policy = KafkaMergePolicy::Partition, std::optional<Bytes> key_filter = std::nullopt,
        KafkaAssignmentMode assignment_mode = KafkaAssignmentMode::Group);

    [[nodiscard]] HGRAPH_KAFKA_EXPORT Value make_produce_record(std::optional<Bytes> value, std::optional<Bytes> key = std::nullopt,
                                                                std::vector<KafkaHeaderInput> headers   = {},
                                                                std::optional<DateTime>       timestamp = std::nullopt,
                                                                std::optional<Int> partition = std::nullopt, Str user_token = {});

    [[nodiscard]] HGRAPH_KAFKA_EXPORT Value make_record(Str topic, Int partition, Int offset, std::optional<Bytes> value,
                                                        std::optional<Bytes>          key       = std::nullopt,
                                                        std::vector<KafkaHeaderInput> headers   = {},
                                                        std::optional<DateTime>       timestamp = std::nullopt,
                                                        KafkaTimestampType timestamp_type       = KafkaTimestampType::NotAvailable);

    [[nodiscard]] HGRAPH_KAFKA_EXPORT Value make_cursor(Str subscription_identity, Int assignment_generation, Str topic,
                                                        Int partition, Int next_offset);

    [[nodiscard]] HGRAPH_KAFKA_EXPORT Value make_delivery_report(Str user_token, Int sequence, Str topic,
                                                                 KafkaDeliveryStatus status,
                                                                 std::optional<Int>  partition = std::nullopt,
                                                                 std::optional<Int> offset = std::nullopt, Int error_code = 0,
                                                                 Bool retriable = false, Bool fatal = false, Str message = {});

    [[nodiscard]] HGRAPH_KAFKA_EXPORT Value make_event(KafkaSeverity severity, Str component, Str category, Str service_path,
                                                       Str message, Int error_code = 0, Bool retriable = false, Bool fatal = false,
                                                       Str subscription_identity = {}, Str publisher_identity = {});
}  // namespace hgraph::kafka

#endif  // HGRAPH_KAFKA_VALUE_BUILDERS_H
