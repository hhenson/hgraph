#ifndef HGRAPH_KAFKA_TYPES_H
#define HGRAPH_KAFKA_TYPES_H

#include <hgraph/kafka/export.h>

#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_schema.h>

#include <cstdint>
#include <ostream>
#include <string_view>

namespace hgraph::kafka
{
    enum class KafkaTimestampType : std::int64_t {
        NotAvailable,
        CreateTime,
        LogAppendTime,
    };

    enum class KafkaSubscriptionState : std::int64_t {
        Starting,
        Recovering,
        Live,
        BoundedComplete,
        Retrying,
        Stopped,
        Failed,
    };

    enum class KafkaDeliveryStatus : std::int64_t {
        EnqueueRejected,
        Dropped,
        Delivered,
        RetriableFailure,
        PermanentFailure,
    };

    enum class KafkaSeverity : std::int64_t {
        Debug,
        Info,
        Warning,
        Error,
        Fatal,
    };

    enum class KafkaCommitMode : std::int64_t {
        None,
        OnGraphDelivery,
        Explicit,
    };

    enum class KafkaAssignmentMode : std::int64_t {
        Group,
        Independent,
    };

    enum class KafkaSelectorKind : std::int64_t {
        Topics,
        Pattern,
        Partitions,
    };

    enum class KafkaStartPositionKind : std::int64_t {
        Earliest,
        Latest,
        Committed,
        Timestamp,
        Offsets,
        GraphStartTime,
    };

    enum class KafkaStopPositionKind : std::int64_t {
        Unbounded,
        Snapshot,
        Timestamp,
        Offsets,
        GraphLifetime,
    };

    enum class KafkaOffsetFallback : std::int64_t {
        Earliest,
        Latest,
        Fail,
    };

    enum class KafkaRecoveryClock : std::int64_t {
        Arrival,
        RecordTimestamp,
    };

    enum class KafkaMergePolicy : std::int64_t {
        Partition,
        TimestampTopicPartitionOffset,
    };

    enum class KafkaOverflowAction : std::int64_t {
        Fail,
        Drop,
        Stage,
    };

    namespace detail
    {
        [[nodiscard]] constexpr std::string_view enum_name(KafkaTimestampType value) noexcept {
            switch (value) {
                case KafkaTimestampType::NotAvailable: return "NotAvailable";
                case KafkaTimestampType::CreateTime: return "CreateTime";
                case KafkaTimestampType::LogAppendTime: return "LogAppendTime";
            }
            return "Unknown";
        }

        [[nodiscard]] constexpr std::string_view enum_name(KafkaSubscriptionState value) noexcept {
            switch (value) {
                case KafkaSubscriptionState::Starting: return "Starting";
                case KafkaSubscriptionState::Recovering: return "Recovering";
                case KafkaSubscriptionState::Live: return "Live";
                case KafkaSubscriptionState::BoundedComplete: return "BoundedComplete";
                case KafkaSubscriptionState::Retrying: return "Retrying";
                case KafkaSubscriptionState::Stopped: return "Stopped";
                case KafkaSubscriptionState::Failed: return "Failed";
            }
            return "Unknown";
        }

        [[nodiscard]] constexpr std::string_view enum_name(KafkaDeliveryStatus value) noexcept {
            switch (value) {
                case KafkaDeliveryStatus::EnqueueRejected: return "EnqueueRejected";
                case KafkaDeliveryStatus::Dropped: return "Dropped";
                case KafkaDeliveryStatus::Delivered: return "Delivered";
                case KafkaDeliveryStatus::RetriableFailure: return "RetriableFailure";
                case KafkaDeliveryStatus::PermanentFailure: return "PermanentFailure";
            }
            return "Unknown";
        }

        [[nodiscard]] constexpr std::string_view enum_name(KafkaSeverity value) noexcept {
            switch (value) {
                case KafkaSeverity::Debug: return "Debug";
                case KafkaSeverity::Info: return "Info";
                case KafkaSeverity::Warning: return "Warning";
                case KafkaSeverity::Error: return "Error";
                case KafkaSeverity::Fatal: return "Fatal";
            }
            return "Unknown";
        }

        [[nodiscard]] constexpr std::string_view enum_name(KafkaCommitMode value) noexcept {
            switch (value) {
                case KafkaCommitMode::None: return "None";
                case KafkaCommitMode::OnGraphDelivery: return "OnGraphDelivery";
                case KafkaCommitMode::Explicit: return "Explicit";
            }
            return "Unknown";
        }

        [[nodiscard]] constexpr std::string_view enum_name(KafkaAssignmentMode value) noexcept {
            switch (value) {
                case KafkaAssignmentMode::Group: return "Group";
                case KafkaAssignmentMode::Independent: return "Independent";
            }
            return "Unknown";
        }

        [[nodiscard]] constexpr std::string_view enum_name(KafkaSelectorKind value) noexcept {
            switch (value) {
                case KafkaSelectorKind::Topics: return "Topics";
                case KafkaSelectorKind::Pattern: return "Pattern";
                case KafkaSelectorKind::Partitions: return "Partitions";
            }
            return "Unknown";
        }

        [[nodiscard]] constexpr std::string_view enum_name(KafkaStartPositionKind value) noexcept {
            switch (value) {
                case KafkaStartPositionKind::Earliest: return "Earliest";
                case KafkaStartPositionKind::Latest: return "Latest";
                case KafkaStartPositionKind::Committed: return "Committed";
                case KafkaStartPositionKind::Timestamp: return "Timestamp";
                case KafkaStartPositionKind::Offsets: return "Offsets";
                case KafkaStartPositionKind::GraphStartTime: return "GraphStartTime";
            }
            return "Unknown";
        }

        [[nodiscard]] constexpr std::string_view enum_name(KafkaStopPositionKind value) noexcept {
            switch (value) {
                case KafkaStopPositionKind::Unbounded: return "Unbounded";
                case KafkaStopPositionKind::Snapshot: return "Snapshot";
                case KafkaStopPositionKind::GraphLifetime: return "GraphLifetime";
                case KafkaStopPositionKind::Timestamp: return "Timestamp";
                case KafkaStopPositionKind::Offsets: return "Offsets";
            }
            return "Unknown";
        }

        [[nodiscard]] constexpr std::string_view enum_name(KafkaOffsetFallback value) noexcept {
            switch (value) {
                case KafkaOffsetFallback::Earliest: return "Earliest";
                case KafkaOffsetFallback::Latest: return "Latest";
                case KafkaOffsetFallback::Fail: return "Fail";
            }
            return "Unknown";
        }

        [[nodiscard]] constexpr std::string_view enum_name(KafkaRecoveryClock value) noexcept {
            switch (value) {
                case KafkaRecoveryClock::Arrival: return "Arrival";
                case KafkaRecoveryClock::RecordTimestamp: return "RecordTimestamp";
            }
            return "Unknown";
        }

        [[nodiscard]] constexpr std::string_view enum_name(KafkaMergePolicy value) noexcept {
            switch (value) {
                case KafkaMergePolicy::Partition: return "Partition";
                case KafkaMergePolicy::TimestampTopicPartitionOffset: return "TimestampTopicPartitionOffset";
            }
            return "Unknown";
        }

        [[nodiscard]] constexpr std::string_view enum_name(KafkaOverflowAction value) noexcept {
            switch (value) {
                case KafkaOverflowAction::Fail: return "Fail";
                case KafkaOverflowAction::Drop: return "Drop";
                case KafkaOverflowAction::Stage: return "Stage";
            }
            return "Unknown";
        }

    }  // namespace detail

    template <typename T>
        requires requires(T value) { detail::enum_name(value); }
    inline std::ostream &operator<<(std::ostream &stream, T value) {
        return stream << detail::enum_name(value);
    }
}  // namespace hgraph::kafka

namespace hgraph::static_schema_detail
{
    template <> struct scalar_name<kafka::KafkaTimestampType>
    {
        static constexpr std::string_view value{"hgraph.kafka::KafkaTimestampType"};
    };

    template <> struct scalar_name<kafka::KafkaSubscriptionState>
    {
        static constexpr std::string_view value{"hgraph.kafka::KafkaSubscriptionState"};
    };

    template <> struct scalar_name<kafka::KafkaDeliveryStatus>
    {
        static constexpr std::string_view value{"hgraph.kafka::KafkaDeliveryStatus"};
    };

    template <> struct scalar_name<kafka::KafkaSeverity>
    {
        static constexpr std::string_view value{"hgraph.kafka::KafkaSeverity"};
    };

    template <> struct scalar_name<kafka::KafkaCommitMode>
    {
        static constexpr std::string_view value{"hgraph.kafka::KafkaCommitMode"};
    };

    template <> struct scalar_name<kafka::KafkaAssignmentMode>
    {
        static constexpr std::string_view value{"hgraph.kafka::KafkaAssignmentMode"};
    };

    template <> struct scalar_name<kafka::KafkaSelectorKind>
    {
        static constexpr std::string_view value{"hgraph.kafka::KafkaSelectorKind"};
    };

    template <> struct scalar_name<kafka::KafkaStartPositionKind>
    {
        static constexpr std::string_view value{"hgraph.kafka::KafkaStartPositionKind"};
    };

    template <> struct scalar_name<kafka::KafkaStopPositionKind>
    {
        static constexpr std::string_view value{"hgraph.kafka::KafkaStopPositionKind"};
    };

    template <> struct scalar_name<kafka::KafkaOffsetFallback>
    {
        static constexpr std::string_view value{"hgraph.kafka::KafkaOffsetFallback"};
    };

    template <> struct scalar_name<kafka::KafkaRecoveryClock>
    {
        static constexpr std::string_view value{"hgraph.kafka::KafkaRecoveryClock"};
    };

    template <> struct scalar_name<kafka::KafkaMergePolicy>
    {
        static constexpr std::string_view value{"hgraph.kafka::KafkaMergePolicy"};
    };

    template <> struct scalar_name<kafka::KafkaOverflowAction>
    {
        static constexpr std::string_view value{"hgraph.kafka::KafkaOverflowAction"};
    };

}  // namespace hgraph::static_schema_detail

#if HGRAPH_ENABLE_PYTHON_USER_NODES
namespace hgraph
{
    #define HGRAPH_KAFKA_PYTHON_ENUM_CONVERSION(EnumType)                                                                          \
        template <> struct python_conversion_traits<kafka::EnumType>                                                               \
        {                                                                                                                          \
            static nb::object      to_python(const kafka::EnumType &value) { return nb::cast(value); }                             \
            static kafka::EnumType from_python(nb::handle source) {                                                                \
                if (nb::hasattr(source, "value")) { source = source.attr("value"); }                                               \
                return static_cast<kafka::EnumType>(nb::cast<std::int64_t>(source));                                               \
            }                                                                                                                      \
        }

    HGRAPH_KAFKA_PYTHON_ENUM_CONVERSION(KafkaTimestampType);
    HGRAPH_KAFKA_PYTHON_ENUM_CONVERSION(KafkaSubscriptionState);
    HGRAPH_KAFKA_PYTHON_ENUM_CONVERSION(KafkaDeliveryStatus);
    HGRAPH_KAFKA_PYTHON_ENUM_CONVERSION(KafkaSeverity);
    HGRAPH_KAFKA_PYTHON_ENUM_CONVERSION(KafkaCommitMode);
    HGRAPH_KAFKA_PYTHON_ENUM_CONVERSION(KafkaAssignmentMode);
    HGRAPH_KAFKA_PYTHON_ENUM_CONVERSION(KafkaSelectorKind);
    HGRAPH_KAFKA_PYTHON_ENUM_CONVERSION(KafkaStartPositionKind);
    HGRAPH_KAFKA_PYTHON_ENUM_CONVERSION(KafkaStopPositionKind);
    HGRAPH_KAFKA_PYTHON_ENUM_CONVERSION(KafkaOffsetFallback);
    HGRAPH_KAFKA_PYTHON_ENUM_CONVERSION(KafkaRecoveryClock);
    HGRAPH_KAFKA_PYTHON_ENUM_CONVERSION(KafkaMergePolicy);
    HGRAPH_KAFKA_PYTHON_ENUM_CONVERSION(KafkaOverflowAction);
    #undef HGRAPH_KAFKA_PYTHON_ENUM_CONVERSION
}  // namespace hgraph
#endif

namespace hgraph::kafka
{
    using KafkaHeader = Bundle<"hgraph.kafka::KafkaHeader", Field<"name", Str>, Field<"value", Bytes>>;

    using KafkaOption = Bundle<"hgraph.kafka::KafkaOption", Field<"name", Str>, Field<"value", Str>>;

    using KafkaTopicPartition = Bundle<"hgraph.kafka::KafkaTopicPartition", Field<"topic", Str>, Field<"partition", Int>>;

    using KafkaPartitionOffset =
        Bundle<"hgraph.kafka::KafkaPartitionOffset", Field<"topic", Str>, Field<"partition", Int>, Field<"offset", Int>>;

    using KafkaStartPosition =
        Bundle<"hgraph.kafka::KafkaStartPosition", Field<"kind", KafkaStartPositionKind>, Field<"fallback", KafkaOffsetFallback>,
               Field<"timestamp", DateTime>, Field<"offsets", HomogeneousTuple<KafkaPartitionOffset>>>;

    using KafkaStopPosition = Bundle<"hgraph.kafka::KafkaStopPosition", Field<"kind", KafkaStopPositionKind>,
                                     Field<"timestamp", DateTime>, Field<"offsets", HomogeneousTuple<KafkaPartitionOffset>>>;

    using KafkaConnectionConfig = Bundle<"hgraph.kafka::KafkaConnectionConfig", Field<"bootstrap_servers", HomogeneousTuple<Str>>,
                                         Field<"client_id", Str>, Field<"options", HomogeneousTuple<KafkaOption>>>;

    using KafkaConsumerDefaults =
        Bundle<"hgraph.kafka::KafkaConsumerDefaults", Field<"ingress_record_limit", Int>,
               Field<"inbound_overflow", KafkaOverflowAction>, Field<"options", HomogeneousTuple<KafkaOption>>>;

    using KafkaProducerOptions =
        Bundle<"hgraph.kafka::KafkaProducerOptions", Field<"idempotent", Bool>, Field<"acknowledgements", Str>,
               Field<"retries", Int>, Field<"linger_ms", Int>, Field<"batch_record_limit", Int>,
               Field<"outbound_record_limit", Int>, Field<"overflow", KafkaOverflowAction>,
               Field<"stage_overflow", KafkaOverflowAction>, Field<"shutdown_drain_timeout_ms", Int>,
               Field<"options", HomogeneousTuple<KafkaOption>>>;

    using KafkaServiceConfig = Bundle<"hgraph.kafka::KafkaServiceConfig", Field<"connection", KafkaConnectionConfig>,
                                      Field<"consumer_defaults", KafkaConsumerDefaults>, Field<"producer", KafkaProducerOptions>>;

    using KafkaSubscriptionKey =
        Bundle<"hgraph.kafka::KafkaSubscriptionKey", Field<"selector_kind", KafkaSelectorKind>,
               Field<"topics", HomogeneousTuple<Str>>, Field<"topic_pattern", Str>,
               Field<"partitions", HomogeneousTuple<KafkaTopicPartition>>, Field<"group_id", Str>,
               Field<"assignment_mode", KafkaAssignmentMode>, Field<"start_position", KafkaStartPosition>,
               Field<"stop_position", KafkaStopPosition>, Field<"isolation_level", Str>, Field<"commit_mode", KafkaCommitMode>,
               Field<"recovery_clock", KafkaRecoveryClock>, Field<"merge_policy", KafkaMergePolicy>, Field<"key_filter", Bytes>,
               Field<"sharing_identity", Str>>;

    using KafkaRecord = Bundle<"hgraph.kafka::KafkaRecord", Field<"topic", Str>, Field<"partition", Int>, Field<"offset", Int>,
                               Field<"timestamp", DateTime>, Field<"timestamp_type", KafkaTimestampType>, Field<"key", Bytes>,
                               Field<"value", Bytes>, Field<"headers", HomogeneousTuple<KafkaHeader>>>;

    using KafkaProduceRecord = Bundle<"hgraph.kafka::KafkaProduceRecord", Field<"value", Bytes>, Field<"key", Bytes>,
                                      Field<"headers", HomogeneousTuple<KafkaHeader>>, Field<"timestamp", DateTime>,
                                      Field<"partition", Int>, Field<"user_token", Str>>;

    using KafkaCursor =
        Bundle<"hgraph.kafka::KafkaCursor", Field<"subscription_identity", Str>, Field<"assignment_generation", Int>,
               Field<"topic", Str>, Field<"partition", Int>, Field<"next_offset", Int>>;

    using KafkaDeliveryReport =
        Bundle<"hgraph.kafka::KafkaDeliveryReport", Field<"user_token", Str>, Field<"sequence", Int>, Field<"topic", Str>,
               Field<"partition", Int>, Field<"offset", Int>, Field<"status", KafkaDeliveryStatus>, Field<"error_code", Int>,
               Field<"retriable", Bool>, Field<"fatal", Bool>, Field<"message", Str>>;

    using KafkaEvent =
        Bundle<"hgraph.kafka::KafkaEvent", Field<"severity", KafkaSeverity>, Field<"component", Str>, Field<"category", Str>,
               Field<"error_code", Int>, Field<"retriable", Bool>, Field<"fatal", Bool>, Field<"service_path", Str>,
               Field<"subscription_identity", Str>, Field<"publisher_identity", Str>, Field<"message", Str>>;

    using KafkaSubscriptionOutput = TSB<"hgraph.kafka::KafkaSubscriptionOutput",
                                        Field<"record", TS<Shared<KafkaRecord>>>, Field<"cursor", TS<KafkaCursor>>,
                                        Field<"state", TS<KafkaSubscriptionState>>>;

    using KafkaPublishRequest =
        TSB<"hgraph.kafka::KafkaPublishRequest", Field<"topic", TS<Str>>, Field<"record", TS<KafkaProduceRecord>>>;

    HGRAPH_KAFKA_EXPORT void register_kafka_types();
}  // namespace hgraph::kafka

#endif  // HGRAPH_KAFKA_TYPES_H
