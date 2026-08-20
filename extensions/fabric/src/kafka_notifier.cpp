#include <hgraph/fabric/kafka_notifier.h>

#include <hgraph/fabric/metadata_codec.h>
#include <hgraph/fabric/types.h>
#include <hgraph/fabric/value_builders.h>

#include <hgraph/kafka/value_builders.h>
#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/runtime/executor.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_node.h>

#include <algorithm>
#include <compare>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <string>
#include <utility>

namespace hgraph::fabric::detail
{
    namespace
    {
        struct IdLess
        {
            using is_transparent = void;

            [[nodiscard]] bool operator()(std::string_view lhs,
                                          std::string_view rhs) const noexcept
            {
                return canonical_data_id_less(lhs, rhs);
            }
        };

        struct DeliveryState
        {
            std::mutex                 mutex{};
            NotificationDeliveryResult result{};
        };

        struct PartitionIdentity
        {
            Str subscription_identity{};
            Int assignment_generation{};
            Str topic{};
            Int partition{};

            friend auto operator<=>(const PartitionIdentity &,
                                    const PartitionIdentity &) = default;
        };

        struct Receipt
        {
            std::uint64_t receipt{};
            Int           next_offset{};
        };

        struct PartitionProgress
        {
            Int                            greatest_seen_next_offset{-1};
            Int                            greatest_queued_next_offset{-1};
            std::map<Str, Receipt, IdLess> outstanding{};
        };
    }  // namespace

    class KafkaNotifierBridge;

    struct KafkaNotifierBridgeHandle
    {
        std::shared_ptr<KafkaNotifierBridge> value{};

        friend bool operator==(const KafkaNotifierBridgeHandle &,
                               const KafkaNotifierBridgeHandle &) noexcept = default;
        friend std::strong_ordering
        operator<=>(const KafkaNotifierBridgeHandle &lhs,
                    const KafkaNotifierBridgeHandle &rhs) noexcept
        {
            return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
                   reinterpret_cast<std::uintptr_t>(rhs.value.get());
        }
    };

    inline std::ostream &operator<<(std::ostream &stream,
                                    const KafkaNotifierBridgeHandle &value)
    {
        return stream << "KafkaNotifierBridgeHandle(" << value.value.get() << ')';
    }

    struct KafkaNotificationSubscriber
    {
        struct PendingNotice
        {
            RevisionId          revision{};
            RevisionNotification notification{};
        };

        explicit KafkaNotificationSubscriber(
            std::weak_ptr<KafkaNotifierBridge> configured_owner)
            : owner(std::move(configured_owner))
        {
        }

        mutable std::mutex mutex{};
        std::weak_ptr<KafkaNotifierBridge> owner{};
        bool closed{};
        std::deque<Str> order{};
        std::map<Str, PendingNotice, IdLess> pending{};
        NotificationSubscriptionStatus transport{};
        std::function<void()> waker{};
    };

    class KafkaNotifierBridge final
        : public std::enable_shared_from_this<KafkaNotifierBridge>
    {
      public:
        explicit KafkaNotifierBridge(KafkaNotifierConfig configured)
            : config_(std::move(configured))
        {
        }

        ~KafkaNotifierBridge()
        {
            const std::scoped_lock lock{mutex_};
            fail_deliveries_locked("fabric Kafka notifier was destroyed");
        }

        [[nodiscard]] NotificationSubscription subscribe();
        [[nodiscard]] NotificationDelivery publish(RevisionNotification notification);

        void prepare_subscription()
        {
            const std::scoped_lock lock{mutex_};
            if (!subscriber_)
            {
                subscriber_ = std::make_shared<KafkaNotificationSubscriber>(
                    weak_from_this());
            }
        }

        void attach_publish_source(AsyncNodeWakeSender sender)
        {
            std::function<void()> wake;
            {
                const std::scoped_lock lock{mutex_};
                publish_sender_ = std::move(sender);
                publish_active_ = true;
                if (!outbound_.empty())
                {
                    wake = [sender = publish_sender_] { sender.wake(); };
                }
            }
            if (wake) { wake(); }
        }

        void detach_publish_source() noexcept
        {
            const std::scoped_lock lock{mutex_};
            publish_active_ = false;
            publish_sender_ = {};
            fail_deliveries_locked("fabric Kafka publish source stopped");
            outbound_.clear();
        }

        void attach_commit_source(AsyncNodeWakeSender sender)
        {
            std::function<void()> wake;
            {
                const std::scoped_lock lock{mutex_};
                commit_sender_ = std::move(sender);
                commit_active_ = true;
                if (!commits_.empty())
                {
                    wake = [sender = commit_sender_] { sender.wake(); };
                }
            }
            if (wake) { wake(); }
        }

        void detach_commit_source() noexcept
        {
            const std::scoped_lock lock{mutex_};
            commit_active_ = false;
            commit_sender_ = {};
            commits_.clear();
        }

        [[nodiscard]] std::optional<Value> pop_publish()
        {
            const std::scoped_lock lock{mutex_};
            if (outbound_.empty()) { return std::nullopt; }
            Value value = std::move(outbound_.front());
            outbound_.pop_front();
            return value;
        }

        [[nodiscard]] std::size_t pending_publish() const noexcept
        {
            const std::scoped_lock lock{mutex_};
            return outbound_.size();
        }

        [[nodiscard]] std::optional<Value> pop_commit()
        {
            const std::scoped_lock lock{mutex_};
            if (commits_.empty()) { return std::nullopt; }
            auto item = commits_.begin();
            Value value = std::move(item->second);
            commits_.erase(item);
            return value;
        }

        [[nodiscard]] std::size_t pending_commit() const noexcept
        {
            const std::scoped_lock lock{mutex_};
            return commits_.size();
        }

        void delivery(ValueView report)
        {
            const auto fields = report.as_bundle();
            const Str token = fields.at("user_token").checked_as<Str>();
            std::shared_ptr<DeliveryState> state;
            {
                const std::scoped_lock lock{mutex_};
                const auto found = deliveries_.find(token);
                if (found == deliveries_.end()) { return; }
                state = std::move(found->second);
                deliveries_.erase(found);
            }
            const auto status =
                fields.at("status").checked_as<kafka::KafkaDeliveryStatus>();
            NotificationDeliveryResult result;
            result.status = status == kafka::KafkaDeliveryStatus::Delivered
                                ? NotificationDeliveryStatus::Delivered
                                : NotificationDeliveryStatus::Failed;
            result.message = fields.at("message").has_value()
                                 ? fields.at("message").checked_as<Str>()
                                 : Str{};
            if (result.status == NotificationDeliveryStatus::Failed &&
                result.message.empty())
            {
                result.message = "Kafka delivery status " +
                                 std::string{kafka::detail::enum_name(status)};
            }
            const std::scoped_lock lock{state->mutex};
            state->result = std::move(result);
        }

        void ingest(ValueView record, ValueView cursor)
        {
            const auto record_fields = record.as_bundle();
            const auto cursor_fields = cursor.as_bundle();
            if (!record_fields.at("key").has_value() ||
                !record_fields.at("value").has_value())
            {
                fail_subscription(
                    "fabric Kafka notice requires non-null key and value");
                return;
            }
            const Str data_id =
                record_fields.at("key").checked_as<Bytes>().data;
            const Bytes payload =
                record_fields.at("value").checked_as<Bytes>();
            try
            {
                require_data_id(data_id);
                persistence::store::ObjectBytes encoded(payload.data.size());
                std::ranges::transform(payload.data, encoded.begin(),
                                       [](char value) {
                                           return static_cast<std::byte>(value);
                                       });
                const DataRevisionInput decoded =
                    data_revision_input(decode_revision(encoded).view());
                if (decoded.data_id != data_id)
                {
                    throw std::invalid_argument(
                        "Kafka record key does not match revision data id");
                }

                std::shared_ptr<KafkaNotificationSubscriber> subscriber;
                RevisionNotification notification{
                    .data_id = data_id,
                    .revision = std::move(encoded),
                };
                bool stale{};
                {
                    const std::scoped_lock lock{mutex_};
                    const PartitionIdentity identity{
                        cursor_fields.at("subscription_identity").checked_as<Str>(),
                        cursor_fields.at("assignment_generation").checked_as<Int>(),
                        cursor_fields.at("topic").checked_as<Str>(),
                        cursor_fields.at("partition").checked_as<Int>(),
                    };
                    const Int next_offset =
                        cursor_fields.at("next_offset").checked_as<Int>();
                    subscriber = subscriber_;
                    if (!subscriber)
                    {
                        subscriber = std::make_shared<KafkaNotificationSubscriber>(
                            weak_from_this());
                        subscriber_ = subscriber;
                    }
                    if (!partitions_.contains(identity) &&
                        partitions_.size() >= static_cast<std::size_t>(
                                                  config_.commit_partition_limit))
                    {
                        throw std::runtime_error(
                            "fabric Kafka commit partition limit exceeded");
                    }
                    auto &partition = partitions_[identity];
                    partition.greatest_seen_next_offset = std::max(
                        partition.greatest_seen_next_offset, next_offset);
                    const auto newest = newest_revision_.find(data_id);
                    if (newest != newest_revision_.end() &&
                        decoded.revision < newest->second)
                    {
                        stale = true;
                    }
                    else
                    {
                        if (newest == newest_revision_.end() &&
                            newest_revision_.size() >=
                                static_cast<std::size_t>(
                                    config_.pending_data_id_limit))
                        {
                            throw std::runtime_error(
                                "fabric Kafka pending data-id limit exceeded");
                        }
                        if (const auto prior =
                                partition.outstanding.find(data_id);
                            prior != partition.outstanding.end())
                        {
                            receipts_.erase(prior->second.receipt);
                        }
                        newest_revision_[data_id] = decoded.revision;
                        notification.receipt = next_receipt_++;
                        partition.outstanding[data_id] =
                            Receipt{notification.receipt, next_offset};
                        receipts_[notification.receipt] =
                            std::pair{identity, data_id};
                    }
                    recompute_commit_locked(identity, partition);
                }
                if (stale) { return; }

                std::function<void()> wake;
                std::optional<RevisionNotification> acknowledge_after_unlock;
                {
                    const std::scoped_lock lock{subscriber->mutex};
                    if (subscriber->closed)
                    {
                        acknowledge_after_unlock = notification;
                    }
                    else
                    {
                        auto [entry, inserted] = subscriber->pending.try_emplace(
                            data_id,
                            KafkaNotificationSubscriber::PendingNotice{
                                decoded.revision, notification});
                        if (inserted)
                        {
                            subscriber->order.push_back(data_id);
                            wake = subscriber->waker;
                        }
                        else if (decoded.revision >= entry->second.revision)
                        {
                            acknowledge_after_unlock =
                                std::move(entry->second.notification);
                            entry->second = {
                                decoded.revision, std::move(notification)};
                        }
                        else
                        {
                            acknowledge_after_unlock = std::move(notification);
                        }
                    }
                }
                if (acknowledge_after_unlock.has_value())
                {
                    acknowledge(*acknowledge_after_unlock);
                }
                if (wake) { wake(); }
            }
            catch (const std::exception &error)
            {
                fail_subscription(error.what());
            }
        }

        void transition(kafka::KafkaSubscriptionState state)
        {
            const NotificationSubscriptionState mapped = [&] {
                switch (state)
                {
                    case kafka::KafkaSubscriptionState::Starting:
                        return NotificationSubscriptionState::Starting;
                    case kafka::KafkaSubscriptionState::Recovering:
                        return NotificationSubscriptionState::Recovering;
                    case kafka::KafkaSubscriptionState::Live:
                    case kafka::KafkaSubscriptionState::BoundedComplete:
                        return NotificationSubscriptionState::Live;
                    case kafka::KafkaSubscriptionState::Retrying:
                        return NotificationSubscriptionState::Retrying;
                    case kafka::KafkaSubscriptionState::Stopped:
                        return NotificationSubscriptionState::Stopped;
                    case kafka::KafkaSubscriptionState::Failed:
                        return NotificationSubscriptionState::Failed;
                }
                return NotificationSubscriptionState::Failed;
            }();
            if (mapped == NotificationSubscriptionState::Starting ||
                mapped == NotificationSubscriptionState::Recovering ||
                mapped == NotificationSubscriptionState::Retrying)
            {
                // Assignment generations invalidate old cursor identities. A
                // recovered consumer replays from the last durable commit,
                // while LiveStrategy reconciles persistence on the next Live
                // generation before accepting normal wake processing.
                reset_ingress_tracking();
            }
            if (mapped == NotificationSubscriptionState::Failed)
            {
                fail_subscription("fabric Kafka subscription failed");
                return;
            }
            update_status(mapped, {});
        }

        void event(ValueView event)
        {
            const auto fields = event.as_bundle();
            const bool fatal = fields.at("fatal").checked_as<Bool>();
            const auto severity =
                fields.at("severity").checked_as<kafka::KafkaSeverity>();
            if (fatal || severity == kafka::KafkaSeverity::Fatal)
            {
                fail_subscription(fields.at("message").checked_as<Str>());
            }
        }

        void fail(std::string_view message) { fail_subscription(message); }

        void acknowledge(const RevisionNotification &notification)
        {
            if (notification.receipt == 0) { return; }
            const std::scoped_lock lock{mutex_};
            const auto receipt = receipts_.find(notification.receipt);
            if (receipt == receipts_.end()) { return; }
            const PartitionIdentity identity = receipt->second.first;
            const Str data_id = receipt->second.second;
            receipts_.erase(receipt);
            const auto progress = partitions_.find(identity);
            if (progress == partitions_.end()) { return; }
            const auto outstanding = progress->second.outstanding.find(data_id);
            if (outstanding != progress->second.outstanding.end() &&
                outstanding->second.receipt == notification.receipt)
            {
                progress->second.outstanding.erase(outstanding);
                newest_revision_.erase(data_id);
            }
            recompute_commit_locked(identity, progress->second);
        }

        void close_subscriber(KafkaNotificationSubscriber *subscriber) noexcept
        {
            {
                const std::scoped_lock lock{subscriber->mutex};
                if (subscriber->closed) { return; }
                subscriber->closed = true;
                subscriber->pending.clear();
                subscriber->order.clear();
                subscriber->waker = {};
                subscriber->transport.state =
                    NotificationSubscriptionState::Stopped;
                ++subscriber->transport.generation;
            }
            const std::scoped_lock lock{mutex_};
            if (const auto &current = subscriber_;
                current && current.get() == subscriber)
            {
                subscriber_.reset();
                subscriber_claimed_ = false;
                partitions_.clear();
                receipts_.clear();
                commits_.clear();
                newest_revision_.clear();
            }
        }

      private:
        void reset_ingress_tracking()
        {
            std::shared_ptr<KafkaNotificationSubscriber> subscriber;
            {
                const std::scoped_lock lock{mutex_};
                subscriber = subscriber_;
                partitions_.clear();
                receipts_.clear();
                commits_.clear();
                newest_revision_.clear();
            }
            if (!subscriber) { return; }
            const std::scoped_lock lock{subscriber->mutex};
            if (subscriber->closed) { return; }
            subscriber->pending.clear();
            subscriber->order.clear();
        }

        void update_status(NotificationSubscriptionState state, Str message)
        {
            std::shared_ptr<KafkaNotificationSubscriber> subscriber;
            {
                const std::scoped_lock lock{mutex_};
                if (!fatal_message_.empty())
                {
                    state = NotificationSubscriptionState::Failed;
                    message = fatal_message_;
                }
                subscriber = subscriber_;
            }
            if (!subscriber) { return; }
            std::function<void()> wake;
            {
                const std::scoped_lock lock{subscriber->mutex};
                if (subscriber->closed) { return; }
                if (subscriber->transport.state != state ||
                    subscriber->transport.message != message)
                {
                    subscriber->transport = {
                        state, subscriber->transport.generation + 1,
                        std::move(message)};
                    wake = subscriber->waker;
                }
            }
            if (wake) { wake(); }
        }

        void fail_subscription(std::string_view message)
        {
            {
                const std::scoped_lock lock{mutex_};
                if (fatal_message_.empty()) { fatal_message_ = Str{message}; }
            }
            update_status(NotificationSubscriptionState::Failed, Str{message});
        }

        void fail_deliveries_locked(std::string_view message) noexcept
        {
            for (auto &[token, state] : deliveries_)
            {
                static_cast<void>(token);
                const std::scoped_lock state_lock{state->mutex};
                state->result = {NotificationDeliveryStatus::Failed,
                                 Str{message}};
            }
            deliveries_.clear();
        }

        void recompute_commit_locked(const PartitionIdentity &identity,
                                     PartitionProgress &progress)
        {
            Int next_offset = progress.greatest_seen_next_offset;
            if (!progress.outstanding.empty())
            {
                next_offset = std::ranges::min_element(
                                  progress.outstanding, {},
                                  [](const auto &item) {
                                      return item.second.next_offset;
                                  })
                                  ->second.next_offset -
                              1;
            }
            if (next_offset <= progress.greatest_queued_next_offset ||
                next_offset < 0)
            {
                return;
            }
            commits_[identity] = kafka::make_cursor(
                identity.subscription_identity,
                identity.assignment_generation, identity.topic,
                identity.partition, next_offset);
            progress.greatest_queued_next_offset = next_offset;
            if (commit_active_ && commit_sender_.valid())
            {
                commit_sender_.wake();
            }
        }

        KafkaNotifierConfig config_;
        mutable std::mutex mutex_{};
        std::shared_ptr<KafkaNotificationSubscriber> subscriber_{};
        std::deque<Value> outbound_{};
        std::map<Str, std::shared_ptr<DeliveryState>> deliveries_{};
        std::map<PartitionIdentity, PartitionProgress> partitions_{};
        std::map<std::uint64_t, std::pair<PartitionIdentity, Str>> receipts_{};
        std::map<PartitionIdentity, Value> commits_{};
        std::map<Str, RevisionId, IdLess> newest_revision_{};
        Str fatal_message_{};
        std::uint64_t next_delivery_{};
        std::uint64_t next_receipt_{1};
        AsyncNodeWakeSender publish_sender_{};
        AsyncNodeWakeSender commit_sender_{};
        bool publish_active_{};
        bool commit_active_{};
        bool subscriber_claimed_{};
    };
}  // namespace hgraph::fabric::detail

namespace std
{
    template <>
    struct hash<hgraph::fabric::detail::KafkaNotifierBridgeHandle>
    {
        size_t operator()(
            const hgraph::fabric::detail::KafkaNotifierBridgeHandle &value) const noexcept
        {
            return hash<const void *>{}(value.value.get());
        }
    };
}  // namespace std

namespace hgraph::static_schema_detail
{
    template <>
    struct scalar_name<fabric::detail::KafkaNotifierBridgeHandle>
    {
        static constexpr std::string_view value{
            "hgraph.fabric.internal::KafkaNotifierBridgeHandle"};
    };
}  // namespace hgraph::static_schema_detail

namespace hgraph::fabric
{
    namespace
    {
        using detail::DeliveryState;
        using detail::KafkaNotificationSubscriber;
        using detail::KafkaNotifierBridge;
        using detail::KafkaNotifierBridgeHandle;

        [[nodiscard]] const NotificationDeliveryOps &delivery_ops() noexcept
        {
            static const NotificationDeliveryOps ops{
                [](void *context) {
                    auto &state = *static_cast<DeliveryState *>(context);
                    const std::scoped_lock lock{state.mutex};
                    return state.result;
                },
            };
            return ops;
        }

        [[nodiscard]] const NotificationSubscriptionOps &
        subscription_ops() noexcept
        {
            static const NotificationSubscriptionOps ops{
                [](void *context) -> std::optional<RevisionNotification> {
                    auto &subscriber =
                        *static_cast<KafkaNotificationSubscriber *>(context);
                    const std::scoped_lock lock{subscriber.mutex};
                    if (subscriber.closed || subscriber.order.empty())
                    {
                        return std::nullopt;
                    }
                    Str data_id = std::move(subscriber.order.front());
                    subscriber.order.pop_front();
                    auto found = subscriber.pending.find(data_id);
                    RevisionNotification result =
                        std::move(found->second.notification);
                    subscriber.pending.erase(found);
                    return result;
                },
                [](void *context) noexcept -> std::size_t {
                    if (context == nullptr) { return 0; }
                    auto &subscriber =
                        *static_cast<KafkaNotificationSubscriber *>(context);
                    const std::scoped_lock lock{subscriber.mutex};
                    return subscriber.closed ? 0 : subscriber.pending.size();
                },
                [](void *context) {
                    if (context == nullptr)
                    {
                        return NotificationSubscriptionStatus{
                            NotificationSubscriptionState::Stopped, 0, {}};
                    }
                    auto &subscriber =
                        *static_cast<KafkaNotificationSubscriber *>(context);
                    const std::scoped_lock lock{subscriber.mutex};
                    return subscriber.transport;
                },
                [](void *context,
                   const RevisionNotification &notification) {
                    if (context == nullptr) { return; }
                    auto &subscriber =
                        *static_cast<KafkaNotificationSubscriber *>(context);
                    if (const auto owner = subscriber.owner.lock())
                    {
                        owner->acknowledge(notification);
                    }
                },
                [](void *context, std::function<void()> waker) {
                    if (context == nullptr) { return; }
                    auto &subscriber =
                        *static_cast<KafkaNotificationSubscriber *>(context);
                    std::function<void()> wake;
                    {
                        const std::scoped_lock lock{subscriber.mutex};
                        if (subscriber.closed) { return; }
                        subscriber.waker = std::move(waker);
                        if (!subscriber.pending.empty() ||
                            subscriber.transport.generation != 0)
                        {
                            wake = subscriber.waker;
                        }
                    }
                    if (wake) { wake(); }
                },
                [](void *context) noexcept {
                    if (context == nullptr) { return; }
                    auto &subscriber =
                        *static_cast<KafkaNotificationSubscriber *>(context);
                    if (const auto owner = subscriber.owner.lock())
                    {
                        owner->close_subscriber(&subscriber);
                    }
                },
            };
            return ops;
        }

        [[nodiscard]] const NotifierOps &notifier_ops() noexcept
        {
            static const NotifierOps ops{
                [](void *context) {
                    return static_cast<KafkaNotifierBridge *>(context)->subscribe();
                },
                [](void *context, RevisionNotification notification) {
                    return static_cast<KafkaNotifierBridge *>(context)->publish(
                        std::move(notification));
                },
            };
            return ops;
        }

        struct KafkaPublishQueueSource
        {
            static constexpr auto name =
                "hgraph.fabric.kafka.publish_queue";

            static void start(Scalar<"bridge", KafkaNotifierBridgeHandle> bridge,
                              AsyncNodeWakeSender sender)
            {
                bridge.value().value->attach_publish_source(std::move(sender));
            }

            static void eval(
                Scalar<"bridge", KafkaNotifierBridgeHandle> bridge,
                SingleShotScheduler scheduler,
                Out<TS<kafka::KafkaProduceRecord>> out)
            {
                if (auto value = bridge.value().value->pop_publish())
                {
                    out.apply(value->view());
                }
                if (bridge.value().value->pending_publish() != 0)
                {
                    scheduler.schedule(MIN_TD);
                }
            }

            static void stop(Scalar<"bridge", KafkaNotifierBridgeHandle> bridge)
            {
                bridge.value().value->detach_publish_source();
            }
        };

        struct KafkaDeliverySink
        {
            static constexpr auto name = "hgraph.fabric.kafka.delivery";

            static void eval(
                In<"report", TS<kafka::KafkaDeliveryReport>,
                   InputValidity::Unchecked> report,
                Scalar<"bridge", KafkaNotifierBridgeHandle> bridge)
            {
                if (report.valid() && report.modified())
                {
                    bridge.value().value->delivery(report.base().value());
                }
            }
        };

        struct KafkaCommitQueueSource
        {
            static constexpr auto name = "hgraph.fabric.kafka.commit_queue";

            static void start(Scalar<"bridge", KafkaNotifierBridgeHandle> bridge,
                              AsyncNodeWakeSender sender)
            {
                bridge.value().value->attach_commit_source(std::move(sender));
            }

            static void eval(Scalar<"bridge", KafkaNotifierBridgeHandle> bridge,
                             SingleShotScheduler scheduler,
                             Out<TS<kafka::KafkaCursor>> out)
            {
                if (auto value = bridge.value().value->pop_commit())
                {
                    out.apply(value->view());
                }
                if (bridge.value().value->pending_commit() != 0)
                {
                    scheduler.schedule(MIN_TD);
                }
            }

            static void stop(Scalar<"bridge", KafkaNotifierBridgeHandle> bridge)
            {
                bridge.value().value->detach_commit_source();
            }
        };

        struct KafkaSubscriptionSink
        {
            static constexpr auto name = "hgraph.fabric.kafka.subscription";

            static void eval(
                In<"subscription", kafka::KafkaSubscriptionOutput,
                   InputValidity::Unchecked> subscription,
                Scalar<"bridge", KafkaNotifierBridgeHandle> bridge)
            {
                const auto state = subscription.template field<"state">();
                if (state.valid() && state.modified())
                {
                    bridge.value().value->transition(state.value());
                }
                const auto record = subscription.template field<"record">();
                if (!record.valid() || !record.modified()) { return; }
                const auto cursor = subscription.template field<"cursor">();
                if (!cursor.valid() || !cursor.modified())
                {
                    bridge.value().value->fail(
                        "fabric Kafka record arrived without its cursor");
                    return;
                }
                bridge.value().value->ingest(record.base().value(),
                                             cursor.base().value());
            }
        };

        struct KafkaEventSink
        {
            static constexpr auto name = "hgraph.fabric.kafka.event";

            static void eval(
                In<"event", TS<kafka::KafkaEvent>, InputValidity::Unchecked> event,
                Scalar<"bridge", KafkaNotifierBridgeHandle> bridge)
            {
                if (event.valid() && event.modified())
                {
                    bridge.value().value->event(event.base().value());
                }
            }
        };
    }  // namespace

    NotificationSubscription KafkaNotifierBridge::subscribe()
    {
        std::shared_ptr<KafkaNotificationSubscriber> subscriber;
        Str fatal_message;
        {
            const std::scoped_lock lock{mutex_};
            if (subscriber_claimed_)
            {
                throw std::logic_error(
                    "fabric Kafka notifier supports one live ingress coordinator");
            }
            if (!subscriber_)
            {
                subscriber_ = std::make_shared<KafkaNotificationSubscriber>(
                    weak_from_this());
            }
            subscriber = subscriber_;
            fatal_message = fatal_message_;
            subscriber_claimed_ = true;
        }
        if (!fatal_message.empty())
        {
            const std::scoped_lock lock{subscriber->mutex};
            subscriber->transport = {
                NotificationSubscriptionState::Failed, 1,
                std::move(fatal_message)};
        }
        return NotificationSubscription{std::move(subscriber),
                                        subscription_ops()};
    }

    NotificationDelivery KafkaNotifierBridge::publish(
        RevisionNotification notification)
    {
        auto delivery = std::make_shared<DeliveryState>();
        AsyncNodeWakeSender sender;
        {
            const std::scoped_lock lock{mutex_};
            if (deliveries_.size() >= static_cast<std::size_t>(
                                          config_.outbound_record_limit))
            {
                delivery->result = {
                    NotificationDeliveryStatus::Failed,
                    "fabric Kafka outbound record limit exceeded"};
                return NotificationDelivery{std::move(delivery), delivery_ops()};
            }
            const Str token = "fabric-" + std::to_string(next_delivery_++);
            std::string payload(notification.revision.size(), '\0');
            std::ranges::transform(notification.revision, payload.begin(),
                                   [](std::byte value) {
                                       return static_cast<char>(value);
                                   });
            outbound_.push_back(kafka::make_produce_record(
                Bytes{std::move(payload)}, Bytes{notification.data_id}, {},
                std::nullopt, std::nullopt, token));
            deliveries_[token] = delivery;
            if (publish_active_) { sender = publish_sender_; }
        }
        if (sender.valid()) { sender.wake(); }
        return NotificationDelivery{std::move(delivery), delivery_ops()};
    }

    void require_kafka_fabric_profile(ValueView service_config)
    {
        if (service_config.schema() !=
            scalar_descriptor<kafka::KafkaServiceConfig>::value_meta())
        {
            throw std::invalid_argument(
                "fabric Kafka notifier requires KafkaServiceConfig");
        }
        const auto fields = service_config.as_bundle();
        const auto consumer = fields.at("consumer_defaults").as_bundle();
        const auto producer = fields.at("producer").as_bundle();
        if (!producer.at("idempotent").checked_as<Bool>())
        {
            throw std::invalid_argument(
                "fabric Kafka notifier requires an idempotent producer");
        }
        const Str acknowledgements =
            producer.at("acknowledgements").checked_as<Str>();
        if (acknowledgements != "all" && acknowledgements != "-1")
        {
            throw std::invalid_argument(
                "fabric Kafka notifier requires acknowledgements=all or -1");
        }
        if (consumer.at("inbound_overflow")
                .checked_as<kafka::KafkaOverflowAction>() ==
            kafka::KafkaOverflowAction::Drop)
        {
            throw std::invalid_argument(
                "fabric Kafka notifier forbids dropping inbound records");
        }
        if (producer.at("overflow").checked_as<kafka::KafkaOverflowAction>() ==
                kafka::KafkaOverflowAction::Drop ||
            producer.at("stage_overflow")
                    .checked_as<kafka::KafkaOverflowAction>() ==
                kafka::KafkaOverflowAction::Drop)
        {
            throw std::invalid_argument(
                "fabric Kafka notifier forbids dropping outbound records");
        }
    }

    Notifier wire_kafka_notifier(Wiring &wiring,
                                 service::ServicePath service_path,
                                 KafkaNotifierConfig config)
    {
        if (config.topic.empty())
        {
            throw std::invalid_argument(
                "fabric Kafka notifier requires a topic");
        }
        if (config.group_id.empty())
        {
            throw std::invalid_argument(
                "fabric Kafka notifier requires a group id");
        }
        if (config.pending_data_id_limit <= 0 ||
            config.outbound_record_limit <= 0 ||
            config.commit_partition_limit <= 0)
        {
            throw std::invalid_argument(
                "fabric Kafka notifier queue limits must be positive");
        }
        if (config.sharing_identity.empty())
        {
            config.sharing_identity = "fabric:" + config.topic;
        }

        auto bridge = std::make_shared<KafkaNotifierBridge>(config);
        bridge->prepare_subscription();
        KafkaNotifierBridgeHandle handle{bridge};

        auto publish_source = wire<KafkaPublishQueueSource>(wiring, handle)
                                  .as<TS<kafka::KafkaProduceRecord>>();
        auto delivery = kafka::publish(
            wiring, service_path,
            kafka::publish_request(wiring, config.topic, publish_source));
        wire<KafkaDeliverySink>(wiring, delivery, handle);

        auto subscription_key =
            kafka::subscription_key()
                .topics({config.topic})
                .group_id(config.group_id)
                .assignment_mode(kafka::KafkaAssignmentMode::Independent)
                .start(kafka::make_start_position(
                    kafka::KafkaStartPositionKind::Committed,
                    kafka::KafkaOffsetFallback::Earliest))
                .stop(kafka::make_stop_position(
                    kafka::KafkaStopPositionKind::GraphLifetime))
                .commit_mode(kafka::KafkaCommitMode::Explicit)
                .recovery_clock(kafka::KafkaRecoveryClock::Arrival)
                .merge_policy(kafka::KafkaMergePolicy::Partition)
                .sharing_identity(config.sharing_identity)
                .build();
        auto key = wire<stdlib::const_, TS<kafka::KafkaSubscriptionKey>>(
            wiring, std::move(subscription_key));
        auto subscription = kafka::subscribe(wiring, service_path, key);
        wire<KafkaSubscriptionSink>(wiring, subscription, handle);

        auto commit_source = wire<KafkaCommitQueueSource>(wiring, handle)
                                 .as<TS<kafka::KafkaCursor>>();
        kafka::commit(wiring, service_path, commit_source);
        wire<KafkaEventSink>(wiring, kafka::events(wiring, service_path), handle);

        return Notifier{std::move(bridge), notifier_ops()};
    }

    Notifier register_kafka_notifier(Wiring &wiring,
                                     service::ServicePath service_path,
                                     Value kafka_service_config,
                                     KafkaNotifierConfig config)
    {
        require_kafka_fabric_profile(kafka_service_config.view());
        kafka::register_service(wiring, service_path,
                                std::move(kafka_service_config));
        return wire_kafka_notifier(wiring, std::move(service_path),
                                   std::move(config));
    }
}  // namespace hgraph::fabric
