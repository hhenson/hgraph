#include <hgraph/kafka/value_builders.h>

#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/value/value_builder.h>

#include <algorithm>
#include <initializer_list>
#include <stdexcept>
#include <string>
#include <utility>

namespace hgraph::kafka
{
    namespace
    {
        template <typename T> [[nodiscard]] Value atomic(T value) {
            static_cast<void>(scalar_descriptor<T>::value_meta());
            return Value{std::move(value)};
        }

        template <typename Schema> [[nodiscard]] Value bundle(std::vector<std::pair<std::string_view, Value>> fields) {
            const auto   *meta = scalar_descriptor<Schema>::value_meta();
            BundleBuilder builder{ValuePlanFactory::instance().type_for(meta)};
            for (auto &[name, field] : fields) {
                if (field.has_value()) { builder.set(name, std::move(field)); }
            }
            return builder.build();
        }

        template <typename Element> [[nodiscard]] Value homogeneous_tuple(std::vector<Value> values) {
            const auto element_binding = ValuePlanFactory::instance().type_for(scalar_descriptor<Element>::value_meta());
            const auto tuple_binding =
                ValuePlanFactory::instance().type_for(scalar_descriptor<HomogeneousTuple<Element>>::value_meta());
            if (!element_binding || !tuple_binding) { throw std::logic_error("Kafka tuple schema did not resolve"); }

            ListBuilder builder{element_binding};
            for (const Value &value : values) {
                if (value.schema() != element_binding.schema()) {
                    throw std::invalid_argument("Kafka tuple element schema mismatch");
                }
                builder.push_back_copy(value.view().data());
            }
            ListStorage storage = builder.build_storage();
            return Value{tuple_binding, &storage};
        }

        [[nodiscard]] Value strings(std::vector<Str> values) {
            static_cast<void>(scalar_descriptor<Str>::value_meta());
            std::vector<Value> erased;
            erased.reserve(values.size());
            for (auto &value : values) { erased.push_back(atomic(std::move(value))); }
            return homogeneous_tuple<Str>(std::move(erased));
        }

        [[nodiscard]] Value options(std::vector<KafkaOptionInput> values) {
            std::vector<Value> erased;
            erased.reserve(values.size());
            for (auto &[name, value] : values) {
                if (name.empty()) { throw std::invalid_argument("Kafka option names cannot be empty"); }
                erased.push_back(bundle<KafkaOption>({
                    {"name", atomic(std::move(name))},
                    {"value", atomic(std::move(value))},
                }));
            }
            return homogeneous_tuple<KafkaOption>(std::move(erased));
        }

        [[nodiscard]] Value headers(std::vector<KafkaHeaderInput> values) {
            std::vector<Value> erased;
            erased.reserve(values.size());
            for (auto &[name, value] : values) {
                if (name.empty()) { throw std::invalid_argument("Kafka header names cannot be empty"); }
                std::vector<std::pair<std::string_view, Value>> fields;
                fields.emplace_back("name", atomic(std::move(name)));
                if (value.has_value()) { fields.emplace_back("value", atomic(std::move(*value))); }
                erased.push_back(bundle<KafkaHeader>(std::move(fields)));
            }
            return homogeneous_tuple<KafkaHeader>(std::move(erased));
        }

        [[nodiscard]] Value partition_offsets(std::vector<KafkaPartitionOffsetInput> values) {
            std::vector<Value> erased;
            erased.reserve(values.size());
            for (auto &[topic, partition, offset] : values) {
                if (topic.empty() || partition < 0 || offset < 0) {
                    throw std::invalid_argument("Kafka partition offsets must be non-negative");
                }
                erased.push_back(bundle<KafkaPartitionOffset>({
                    {"topic", atomic(std::move(topic))},
                    {"partition", atomic(partition)},
                    {"offset", atomic(offset)},
                }));
            }
            return homogeneous_tuple<KafkaPartitionOffset>(std::move(erased));
        }

        [[nodiscard]] Value topic_partitions(std::vector<KafkaTopicPartitionInput> values) {
            std::vector<Value> erased;
            erased.reserve(values.size());
            for (auto &[topic, partition] : values) {
                if (topic.empty() || partition < 0) {
                    throw std::invalid_argument("Kafka explicit partitions require a topic and non-negative partition");
                }
                erased.push_back(bundle<KafkaTopicPartition>({
                    {"topic", atomic(std::move(topic))},
                    {"partition", atomic(partition)},
                }));
            }
            return homogeneous_tuple<KafkaTopicPartition>(std::move(erased));
        }

        [[nodiscard]] Value subscription_key_value(KafkaSelectorKind selector, std::vector<Str> topics, Str topic_pattern,
                                                   std::vector<KafkaTopicPartitionInput> partitions, Str group_id,
                                                   Value start_position, Value stop_position, KafkaCommitMode commit_mode,
                                                   KafkaAssignmentMode assignment_mode, Str sharing_identity, Str isolation_level,
                                                   KafkaRecoveryClock recovery_clock, KafkaMergePolicy merge_policy,
                                                   std::optional<Bytes> key_filter) {
            if (group_id.empty()) { throw std::invalid_argument("Kafka subscription requires a group id"); }
            switch (selector) {
                case KafkaSelectorKind::Topics:
                    if (topics.empty() || std::ranges::any_of(topics, [](const Str &topic) { return topic.empty(); })) {
                        throw std::invalid_argument("Kafka topic subscription requires non-empty topics");
                    }
                    break;
                case KafkaSelectorKind::Pattern:
                    if (topic_pattern.empty()) { throw std::invalid_argument("Kafka pattern subscription requires a pattern"); }
                    break;
                case KafkaSelectorKind::Partitions:
                    if (partitions.empty()) { throw std::invalid_argument("Kafka partition subscription requires partitions"); }
                    break;
            }
            if (start_position.schema() != scalar_descriptor<KafkaStartPosition>::value_meta() ||
                stop_position.schema() != scalar_descriptor<KafkaStopPosition>::value_meta()) {
                throw std::invalid_argument("Kafka subscription positions have the wrong schema");
            }
            if (isolation_level != "read_uncommitted" && isolation_level != "read_committed") {
                throw std::invalid_argument("Kafka isolation level must be read_uncommitted or read_committed");
            }
            if (assignment_mode == KafkaAssignmentMode::Independent && selector == KafkaSelectorKind::Pattern) {
                throw std::invalid_argument("Independent Kafka assignment requires explicit topics or partitions");
            }

            std::vector<std::pair<std::string_view, Value>> fields{
                {"selector_kind", atomic(selector)},          {"group_id", atomic(std::move(group_id))},
                {"assignment_mode", atomic(assignment_mode)}, {"start_position", std::move(start_position)},
                {"stop_position", std::move(stop_position)},  {"isolation_level", atomic(std::move(isolation_level))},
                {"commit_mode", atomic(commit_mode)},         {"recovery_clock", atomic(recovery_clock)},
                {"merge_policy", atomic(merge_policy)},       {"sharing_identity", atomic(std::move(sharing_identity))},
            };
            if (!topics.empty()) { fields.emplace_back("topics", strings(std::move(topics))); }
            if (!topic_pattern.empty()) { fields.emplace_back("topic_pattern", atomic(std::move(topic_pattern))); }
            if (!partitions.empty()) { fields.emplace_back("partitions", topic_partitions(std::move(partitions))); }
            if (key_filter.has_value()) { fields.emplace_back("key_filter", atomic(std::move(*key_filter))); }
            return bundle<KafkaSubscriptionKey>(std::move(fields));
        }

        [[nodiscard]] Value legacy_start_position(const Str &value) {
            if (value == "earliest") { return make_start_position(KafkaStartPositionKind::Earliest); }
            if (value == "latest") { return make_start_position(KafkaStartPositionKind::Latest); }
            if (value == "graph_start" || value == "graph-start") {
                return make_start_position(KafkaStartPositionKind::GraphStartTime);
            }
            if (value == "committed:earliest") {
                return make_start_position(KafkaStartPositionKind::Committed, KafkaOffsetFallback::Earliest);
            }
            if (value == "committed:latest") {
                return make_start_position(KafkaStartPositionKind::Committed, KafkaOffsetFallback::Latest);
            }
            if (value == "committed:fail") {
                return make_start_position(KafkaStartPositionKind::Committed, KafkaOffsetFallback::Fail);
            }
            throw std::invalid_argument("Unsupported Kafka start position: " + value);
        }

        [[nodiscard]] Value legacy_stop_position(const Str &value) {
            if (value == "unbounded") { return make_stop_position(KafkaStopPositionKind::Unbounded); }
            if (value == "snapshot") { return make_stop_position(KafkaStopPositionKind::Snapshot); }
            throw std::invalid_argument("Unsupported Kafka stop position: " + value);
        }
    }  // namespace

    ServiceConfigBuilder &ServiceConfigBuilder::bootstrap_servers(std::vector<Str> value) {
        bootstrap_servers_ = std::move(value);
        return *this;
    }

    ServiceConfigBuilder &ServiceConfigBuilder::client_id(Str value) {
        client_id_ = std::move(value);
        return *this;
    }

    ServiceConfigBuilder &ServiceConfigBuilder::idempotent_producer(bool value) {
        idempotent_producer_ = value;
        return *this;
    }

    ServiceConfigBuilder &ServiceConfigBuilder::producer_acknowledgements(Str value) {
        producer_acknowledgements_ = std::move(value);
        return *this;
    }

    ServiceConfigBuilder &ServiceConfigBuilder::producer_retries(Int value) {
        producer_retries_ = value;
        return *this;
    }

    ServiceConfigBuilder &ServiceConfigBuilder::producer_linger(std::chrono::milliseconds value) {
        producer_linger_ms_ = value.count();
        return *this;
    }

    ServiceConfigBuilder &ServiceConfigBuilder::producer_batch_record_limit(Int value) {
        producer_batch_record_limit_ = value;
        return *this;
    }

    ServiceConfigBuilder &ServiceConfigBuilder::ingress_limit(Int records) {
        ingress_record_limit_ = records;
        return *this;
    }

    ServiceConfigBuilder &ServiceConfigBuilder::outbound_limit(Int records) {
        outbound_record_limit_ = records;
        return *this;
    }

    ServiceConfigBuilder &ServiceConfigBuilder::inbound_overflow(KafkaOverflowAction value) {
        inbound_overflow_ = value;
        return *this;
    }

    ServiceConfigBuilder &ServiceConfigBuilder::consumer_failure_policy(KafkaFailurePolicy value) {
        consumer_failure_policy_ = value;
        return *this;
    }

    ServiceConfigBuilder &ServiceConfigBuilder::outbound_overflow(KafkaOverflowAction value, KafkaOverflowAction stage_full) {
        outbound_overflow_ = value;
        stage_overflow_    = stage_full;
        return *this;
    }

    ServiceConfigBuilder &ServiceConfigBuilder::shutdown_drain_timeout(std::chrono::milliseconds value) {
        shutdown_drain_timeout_ms_ = value.count();
        return *this;
    }

    ServiceConfigBuilder &ServiceConfigBuilder::producer_failure_policy(KafkaFailurePolicy value) {
        producer_failure_policy_ = value;
        return *this;
    }

    ServiceConfigBuilder &ServiceConfigBuilder::common_option(Str name, Str value) {
        common_options_.emplace_back(std::move(name), std::move(value));
        return *this;
    }

    ServiceConfigBuilder &ServiceConfigBuilder::consumer_option(Str name, Str value) {
        consumer_options_.emplace_back(std::move(name), std::move(value));
        return *this;
    }

    ServiceConfigBuilder &ServiceConfigBuilder::producer_option(Str name, Str value) {
        producer_options_.emplace_back(std::move(name), std::move(value));
        return *this;
    }

    Value ServiceConfigBuilder::build() const {
        return make_service_config(
            bootstrap_servers_, client_id_, idempotent_producer_, ingress_record_limit_, outbound_record_limit_, common_options_,
            consumer_options_, producer_options_, inbound_overflow_,
            consumer_failure_policy_, outbound_overflow_, stage_overflow_, shutdown_drain_timeout_ms_, producer_failure_policy_,
            producer_acknowledgements_, producer_retries_, producer_linger_ms_, producer_batch_record_limit_);
    }

    ServiceConfigBuilder service_config() { return {}; }

    SubscriptionKeyBuilder &SubscriptionKeyBuilder::topics(std::vector<Str> value) {
        selector_ = KafkaSelectorKind::Topics;
        topics_   = std::move(value);
        topic_pattern_.clear();
        partitions_.clear();
        return *this;
    }

    SubscriptionKeyBuilder &SubscriptionKeyBuilder::topic_pattern(Str value) {
        selector_ = KafkaSelectorKind::Pattern;
        topics_.clear();
        topic_pattern_ = std::move(value);
        partitions_.clear();
        return *this;
    }

    SubscriptionKeyBuilder &SubscriptionKeyBuilder::partitions(std::vector<KafkaTopicPartitionInput> value) {
        selector_ = KafkaSelectorKind::Partitions;
        topics_.clear();
        topic_pattern_.clear();
        partitions_ = std::move(value);
        return *this;
    }

    SubscriptionKeyBuilder &SubscriptionKeyBuilder::group_id(Str value) {
        group_id_ = std::move(value);
        return *this;
    }

    SubscriptionKeyBuilder &SubscriptionKeyBuilder::assignment_mode(KafkaAssignmentMode value) {
        assignment_mode_ = value;
        return *this;
    }

    SubscriptionKeyBuilder &SubscriptionKeyBuilder::start(Value value) {
        start_ = std::move(value);
        return *this;
    }

    SubscriptionKeyBuilder &SubscriptionKeyBuilder::stop(Value value) {
        stop_ = std::move(value);
        return *this;
    }

    SubscriptionKeyBuilder &SubscriptionKeyBuilder::commit_mode(KafkaCommitMode value) {
        commit_mode_ = value;
        return *this;
    }

    SubscriptionKeyBuilder &SubscriptionKeyBuilder::sharing_identity(Str value) {
        sharing_identity_ = std::move(value);
        return *this;
    }

    SubscriptionKeyBuilder &SubscriptionKeyBuilder::isolation_level(Str value) {
        isolation_level_ = std::move(value);
        return *this;
    }

    SubscriptionKeyBuilder &SubscriptionKeyBuilder::recovery_clock(KafkaRecoveryClock value) {
        recovery_clock_ = value;
        return *this;
    }

    SubscriptionKeyBuilder &SubscriptionKeyBuilder::merge_policy(KafkaMergePolicy value) {
        merge_policy_ = value;
        return *this;
    }

    SubscriptionKeyBuilder &SubscriptionKeyBuilder::key_filter(std::optional<Bytes> value) {
        key_filter_ = std::move(value);
        return *this;
    }

    Value SubscriptionKeyBuilder::build() const {
        Value start = start_.has_value() ? start_.clone()
                                         : make_start_position(KafkaStartPositionKind::Committed, KafkaOffsetFallback::Earliest);
        Value stop  = stop_.has_value() ? stop_.clone() : make_stop_position(KafkaStopPositionKind::Unbounded);
        switch (selector_) {
            case KafkaSelectorKind::Topics:
                return make_subscription_key(topics_, group_id_, std::move(start), std::move(stop), commit_mode_, sharing_identity_,
                                             isolation_level_, recovery_clock_, merge_policy_, key_filter_, assignment_mode_);
            case KafkaSelectorKind::Pattern:
                return make_pattern_subscription_key(topic_pattern_, group_id_, std::move(start), std::move(stop), commit_mode_,
                                                     sharing_identity_, isolation_level_, recovery_clock_, merge_policy_,
                                                     key_filter_, assignment_mode_);
            case KafkaSelectorKind::Partitions:
                return make_partition_subscription_key(partitions_, group_id_, std::move(start), std::move(stop), commit_mode_,
                                                       sharing_identity_, isolation_level_, recovery_clock_, merge_policy_,
                                                       key_filter_, assignment_mode_);
        }
        throw std::logic_error("Unknown Kafka selector kind");
    }

    SubscriptionKeyBuilder subscription_key() { return {}; }

    void register_kafka_types() {
        static_cast<void>(scalar_descriptor<KafkaTimestampType>::value_meta());
        static_cast<void>(scalar_descriptor<KafkaSubscriptionState>::value_meta());
        static_cast<void>(scalar_descriptor<KafkaDeliveryStatus>::value_meta());
        static_cast<void>(scalar_descriptor<KafkaSeverity>::value_meta());
        static_cast<void>(scalar_descriptor<KafkaCommitMode>::value_meta());
        static_cast<void>(scalar_descriptor<KafkaAssignmentMode>::value_meta());
        static_cast<void>(scalar_descriptor<KafkaSelectorKind>::value_meta());
        static_cast<void>(scalar_descriptor<KafkaStartPositionKind>::value_meta());
        static_cast<void>(scalar_descriptor<KafkaStopPositionKind>::value_meta());
        static_cast<void>(scalar_descriptor<KafkaOffsetFallback>::value_meta());
        static_cast<void>(scalar_descriptor<KafkaRecoveryClock>::value_meta());
        static_cast<void>(scalar_descriptor<KafkaMergePolicy>::value_meta());
        static_cast<void>(scalar_descriptor<KafkaOverflowAction>::value_meta());
        static_cast<void>(scalar_descriptor<KafkaFailurePolicy>::value_meta());

        static_cast<void>(scalar_descriptor<KafkaHeader>::value_meta());
        static_cast<void>(scalar_descriptor<KafkaOption>::value_meta());
        static_cast<void>(scalar_descriptor<KafkaTopicPartition>::value_meta());
        static_cast<void>(scalar_descriptor<KafkaPartitionOffset>::value_meta());
        static_cast<void>(scalar_descriptor<KafkaStartPosition>::value_meta());
        static_cast<void>(scalar_descriptor<KafkaStopPosition>::value_meta());
        static_cast<void>(scalar_descriptor<KafkaConnectionConfig>::value_meta());
        static_cast<void>(scalar_descriptor<KafkaConsumerDefaults>::value_meta());
        static_cast<void>(scalar_descriptor<KafkaProducerOptions>::value_meta());
        static_cast<void>(scalar_descriptor<KafkaServiceConfig>::value_meta());
        static_cast<void>(scalar_descriptor<KafkaSubscriptionKey>::value_meta());
        static_cast<void>(scalar_descriptor<KafkaRecord>::value_meta());
        static_cast<void>(scalar_descriptor<KafkaProduceRecord>::value_meta());
        static_cast<void>(scalar_descriptor<KafkaCursor>::value_meta());
        static_cast<void>(scalar_descriptor<KafkaDeliveryReport>::value_meta());
        static_cast<void>(scalar_descriptor<KafkaEvent>::value_meta());
        static_cast<void>(schema_descriptor<KafkaSubscriptionOutput>::ts_meta());
        static_cast<void>(schema_descriptor<KafkaPublishRequest>::ts_meta());
    }

    Value make_start_position(KafkaStartPositionKind kind, KafkaOffsetFallback fallback, std::optional<DateTime> timestamp,
                              std::vector<KafkaPartitionOffsetInput> offsets) {
        register_kafka_types();
        if (kind == KafkaStartPositionKind::Timestamp && !timestamp.has_value()) {
            throw std::invalid_argument("Kafka timestamp start requires a timestamp");
        }
        if (kind == KafkaStartPositionKind::Offsets && offsets.empty()) {
            throw std::invalid_argument("Kafka offset start requires partition offsets");
        }
        if (kind != KafkaStartPositionKind::Timestamp && timestamp.has_value()) {
            throw std::invalid_argument("Kafka start timestamp is only valid for Timestamp");
        }
        if (kind != KafkaStartPositionKind::Offsets && !offsets.empty()) {
            throw std::invalid_argument("Kafka start offsets are only valid for Offsets");
        }
        std::vector<std::pair<std::string_view, Value>> fields{
            {"kind", atomic(kind)},
            {"fallback", atomic(fallback)},
            {"offsets", partition_offsets(std::move(offsets))},
        };
        if (timestamp.has_value()) { fields.emplace_back("timestamp", atomic(*timestamp)); }
        return bundle<KafkaStartPosition>(std::move(fields));
    }

    Value make_stop_position(KafkaStopPositionKind kind, std::optional<DateTime> timestamp,
                             std::vector<KafkaPartitionOffsetInput> offsets) {
        register_kafka_types();
        if (kind == KafkaStopPositionKind::Timestamp && !timestamp.has_value()) {
            throw std::invalid_argument("Kafka timestamp stop requires a timestamp");
        }
        if (kind == KafkaStopPositionKind::Offsets && offsets.empty()) {
            throw std::invalid_argument("Kafka offset stop requires partition offsets");
        }
        if (kind != KafkaStopPositionKind::Timestamp && timestamp.has_value()) {
            throw std::invalid_argument("Kafka stop timestamp is only valid for Timestamp");
        }
        if (kind != KafkaStopPositionKind::Offsets && !offsets.empty()) {
            throw std::invalid_argument("Kafka stop offsets are only valid for Offsets");
        }
        std::vector<std::pair<std::string_view, Value>> fields{
            {"kind", atomic(kind)},
            {"offsets", partition_offsets(std::move(offsets))},
        };
        if (timestamp.has_value()) { fields.emplace_back("timestamp", atomic(*timestamp)); }
        return bundle<KafkaStopPosition>(std::move(fields));
    }

    Value make_service_config(std::vector<Str> bootstrap_servers, Str client_id, bool idempotent_producer, Int ingress_record_limit,
                              Int outbound_record_limit,
                              std::vector<KafkaOptionInput> common_options, std::vector<KafkaOptionInput> consumer_options,
                              std::vector<KafkaOptionInput> producer_options, KafkaOverflowAction inbound_overflow,
                              KafkaFailurePolicy consumer_failure_policy, KafkaOverflowAction outbound_overflow,
                              KafkaOverflowAction stage_overflow, Int shutdown_drain_timeout_ms,
                              KafkaFailurePolicy producer_failure_policy, Str producer_acknowledgements, Int producer_retries,
                              Int producer_linger_ms, Int producer_batch_record_limit) {
        register_kafka_types();
        if (bootstrap_servers.empty()) {
            throw std::invalid_argument("Kafka service config requires at least one bootstrap server");
        }
        if (std::ranges::any_of(bootstrap_servers, [](const Str &server) { return server.empty(); })) {
            throw std::invalid_argument("Kafka bootstrap servers cannot be empty");
        }
        if (ingress_record_limit <= 0 || outbound_record_limit <= 0) {
            throw std::invalid_argument("Kafka queue limits must be positive");
        }
        if (shutdown_drain_timeout_ms < 0) { throw std::invalid_argument("Kafka shutdown drain timeout must be non-negative"); }
        if (producer_acknowledgements != "0" && producer_acknowledgements != "1" && producer_acknowledgements != "all" &&
            producer_acknowledgements != "-1") {
            throw std::invalid_argument("Kafka acknowledgements must be 0, 1, all, or -1");
        }
        if (idempotent_producer && producer_acknowledgements != "all" && producer_acknowledgements != "-1") {
            throw std::invalid_argument("Kafka idempotence requires all acknowledgements");
        }
        if (producer_retries < 0 || producer_linger_ms < 0 || producer_batch_record_limit <= 0) {
            throw std::invalid_argument("Kafka producer retry/batch settings are out of range");
        }
        if (inbound_overflow == KafkaOverflowAction::Stage) {
            throw std::invalid_argument("Kafka inbound overflow cannot use Stage");
        }
        if (stage_overflow == KafkaOverflowAction::Stage) {
            throw std::invalid_argument("Kafka stage overflow must be Fail or Drop");
        }

        Value connection = bundle<KafkaConnectionConfig>({
            {"bootstrap_servers", strings(std::move(bootstrap_servers))},
            {"client_id", atomic(std::move(client_id))},
            {"options", options(std::move(common_options))},
        });
        Value consumer   = bundle<KafkaConsumerDefaults>({
            {"ingress_record_limit", atomic(ingress_record_limit)},
            {"inbound_overflow", atomic(inbound_overflow)},
            {"failure_policy", atomic(consumer_failure_policy)},
            {"options", options(std::move(consumer_options))},
        });
        Value producer   = bundle<KafkaProducerOptions>({
            {"idempotent", atomic(Bool{idempotent_producer})},
            {"acknowledgements", atomic(std::move(producer_acknowledgements))},
            {"retries", atomic(producer_retries)},
            {"linger_ms", atomic(producer_linger_ms)},
            {"batch_record_limit", atomic(producer_batch_record_limit)},
            {"outbound_record_limit", atomic(outbound_record_limit)},
            {"overflow", atomic(outbound_overflow)},
            {"stage_overflow", atomic(stage_overflow)},
            {"shutdown_drain_timeout_ms", atomic(shutdown_drain_timeout_ms)},
            {"failure_policy", atomic(producer_failure_policy)},
            {"options", options(std::move(producer_options))},
        });
        return bundle<KafkaServiceConfig>({
            {"connection", std::move(connection)},
            {"consumer_defaults", std::move(consumer)},
            {"producer", std::move(producer)},
        });
    }

    Value make_subscription_key(std::vector<Str> topics, Str group_id, Str start_position, Str stop_position,
                                KafkaCommitMode commit_mode, Str sharing_identity, Str isolation_level, Str recovery_clock,
                                Str merge_policy, std::optional<Bytes> key_filter, KafkaAssignmentMode assignment_mode) {
        KafkaRecoveryClock typed_recovery_clock{};
        if (recovery_clock == "arrival") {
            typed_recovery_clock = KafkaRecoveryClock::Arrival;
        } else if (recovery_clock == "record_timestamp") {
            typed_recovery_clock = KafkaRecoveryClock::RecordTimestamp;
        } else {
            throw std::invalid_argument("Unsupported Kafka recovery clock: " + recovery_clock);
        }

        KafkaMergePolicy typed_merge_policy{};
        if (merge_policy == "partition") {
            typed_merge_policy = KafkaMergePolicy::Partition;
        } else if (merge_policy == "timestamp_topic_partition_offset") {
            typed_merge_policy = KafkaMergePolicy::TimestampTopicPartitionOffset;
        } else {
            throw std::invalid_argument("Unsupported Kafka merge policy: " + merge_policy);
        }
        return make_subscription_key(std::move(topics), std::move(group_id), legacy_start_position(start_position),
                                     legacy_stop_position(stop_position), commit_mode, std::move(sharing_identity),
                                     std::move(isolation_level), typed_recovery_clock, typed_merge_policy, std::move(key_filter),
                                     assignment_mode);
    }

    Value make_subscription_key(std::vector<Str> topics, Str group_id, Value start_position, Value stop_position,
                                KafkaCommitMode commit_mode, Str sharing_identity, Str isolation_level,
                                KafkaRecoveryClock recovery_clock, KafkaMergePolicy merge_policy, std::optional<Bytes> key_filter,
                                KafkaAssignmentMode assignment_mode) {
        register_kafka_types();
        if (topics.empty()) { throw std::invalid_argument("Kafka topic subscription requires at least one topic"); }
        if (std::ranges::any_of(topics, [](const Str &topic) { return topic.empty(); })) {
            throw std::invalid_argument("Kafka subscription topics cannot be empty");
        }
        return subscription_key_value(KafkaSelectorKind::Topics, std::move(topics), {}, {}, std::move(group_id),
                                      std::move(start_position), std::move(stop_position), commit_mode, assignment_mode,
                                      std::move(sharing_identity), std::move(isolation_level), recovery_clock, merge_policy,
                                      std::move(key_filter));
    }

    Value make_pattern_subscription_key(Str topic_pattern, Str group_id, Value start_position, Value stop_position,
                                        KafkaCommitMode commit_mode, Str sharing_identity, Str isolation_level,
                                        KafkaRecoveryClock recovery_clock, KafkaMergePolicy merge_policy,
                                        std::optional<Bytes> key_filter, KafkaAssignmentMode assignment_mode) {
        register_kafka_types();
        if (topic_pattern.empty()) { throw std::invalid_argument("Kafka pattern subscription requires a pattern"); }
        return subscription_key_value(KafkaSelectorKind::Pattern, {}, std::move(topic_pattern), {}, std::move(group_id),
                                      std::move(start_position), std::move(stop_position), commit_mode, assignment_mode,
                                      std::move(sharing_identity), std::move(isolation_level), recovery_clock, merge_policy,
                                      std::move(key_filter));
    }

    Value make_partition_subscription_key(std::vector<KafkaTopicPartitionInput> partitions, Str group_id, Value start_position,
                                          Value stop_position, KafkaCommitMode commit_mode, Str sharing_identity,
                                          Str isolation_level, KafkaRecoveryClock recovery_clock, KafkaMergePolicy merge_policy,
                                          std::optional<Bytes> key_filter, KafkaAssignmentMode assignment_mode) {
        register_kafka_types();
        if (partitions.empty()) { throw std::invalid_argument("Kafka partition subscription requires partitions"); }
        return subscription_key_value(KafkaSelectorKind::Partitions, {}, {}, std::move(partitions), std::move(group_id),
                                      std::move(start_position), std::move(stop_position), commit_mode, assignment_mode,
                                      std::move(sharing_identity), std::move(isolation_level), recovery_clock, merge_policy,
                                      std::move(key_filter));
    }

    Value make_produce_record(std::optional<Bytes> value, std::optional<Bytes> key, std::vector<KafkaHeaderInput> header_values,
                              std::optional<DateTime> timestamp, std::optional<Int> partition, Str user_token) {
        register_kafka_types();
        if (partition.has_value() && *partition < 0) { throw std::invalid_argument("Kafka partition must be non-negative"); }
        std::vector<std::pair<std::string_view, Value>> fields{
            {"headers", headers(std::move(header_values))},
            {"user_token", atomic(std::move(user_token))},
        };
        if (value.has_value()) { fields.emplace_back("value", atomic(std::move(*value))); }
        if (key.has_value()) { fields.emplace_back("key", atomic(std::move(*key))); }
        if (timestamp.has_value()) { fields.emplace_back("timestamp", atomic(*timestamp)); }
        if (partition.has_value()) { fields.emplace_back("partition", atomic(*partition)); }
        return bundle<KafkaProduceRecord>(std::move(fields));
    }

    Value make_record(Str topic, Int partition, Int offset, std::optional<Bytes> value, std::optional<Bytes> key,
                      std::vector<KafkaHeaderInput> header_values, std::optional<DateTime> timestamp,
                      KafkaTimestampType timestamp_type) {
        register_kafka_types();
        if (topic.empty() || partition < 0 || offset < 0) {
            throw std::invalid_argument("Kafka records require a topic and non-negative partition/offset");
        }
        std::vector<std::pair<std::string_view, Value>> fields{
            {"topic", atomic(std::move(topic))},
            {"partition", atomic(partition)},
            {"offset", atomic(offset)},
            {"timestamp_type", atomic(timestamp_type)},
            {"headers", headers(std::move(header_values))},
        };
        if (timestamp.has_value()) { fields.emplace_back("timestamp", atomic(*timestamp)); }
        if (key.has_value()) { fields.emplace_back("key", atomic(std::move(*key))); }
        if (value.has_value()) { fields.emplace_back("value", atomic(std::move(*value))); }
        return bundle<KafkaRecord>(std::move(fields));
    }

    Value make_cursor(Str subscription_identity, Int assignment_generation, Str topic, Int partition, Int next_offset) {
        register_kafka_types();
        if (subscription_identity.empty() || topic.empty() || assignment_generation <= 0 || partition < 0 || next_offset < 0) {
            throw std::invalid_argument("Kafka cursors require live identity, generation, topic, partition, and offset");
        }
        return bundle<KafkaCursor>({
            {"subscription_identity", atomic(std::move(subscription_identity))},
            {"assignment_generation", atomic(assignment_generation)},
            {"topic", atomic(std::move(topic))},
            {"partition", atomic(partition)},
            {"next_offset", atomic(next_offset)},
        });
    }

    Value make_delivery_report(Str user_token, Int sequence, Str topic, KafkaDeliveryStatus status, std::optional<Int> partition,
                               std::optional<Int> offset, Int error_code, Bool retriable, Bool fatal, Str message) {
        register_kafka_types();
        std::vector<std::pair<std::string_view, Value>> fields{
            {"user_token", atomic(std::move(user_token))},
            {"sequence", atomic(sequence)},
            {"topic", atomic(std::move(topic))},
            {"status", atomic(status)},
            {"error_code", atomic(error_code)},
            {"retriable", atomic(retriable)},
            {"fatal", atomic(fatal)},
            {"message", atomic(std::move(message))},
        };
        if (partition.has_value()) { fields.emplace_back("partition", atomic(*partition)); }
        if (offset.has_value()) { fields.emplace_back("offset", atomic(*offset)); }
        return bundle<KafkaDeliveryReport>(std::move(fields));
    }

    Value make_event(KafkaSeverity severity, Str component, Str category, Str service_path, Str message, Int error_code,
                     Bool retriable, Bool fatal, Str subscription_identity, Str publisher_identity) {
        register_kafka_types();
        return bundle<KafkaEvent>({
            {"severity", atomic(severity)},
            {"component", atomic(std::move(component))},
            {"category", atomic(std::move(category))},
            {"error_code", atomic(error_code)},
            {"retriable", atomic(retriable)},
            {"fatal", atomic(fatal)},
            {"service_path", atomic(std::move(service_path))},
            {"subscription_identity", atomic(std::move(subscription_identity))},
            {"publisher_identity", atomic(std::move(publisher_identity))},
            {"message", atomic(std::move(message))},
        });
    }
}  // namespace hgraph::kafka
