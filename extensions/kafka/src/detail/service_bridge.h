#ifndef HGRAPH_KAFKA_DETAIL_SERVICE_BRIDGE_H
#define HGRAPH_KAFKA_DETAIL_SERVICE_BRIDGE_H

#include <hgraph/kafka/service.h>

#include <hgraph/runtime/executor.h>
#include <hgraph/runtime/push_source_node.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/value_builder.h>

#include <algorithm>
#include <array>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <span>
#include <stdexcept>
#include <tuple>
#include <typeindex>
#include <utility>

namespace hgraph::kafka::detail {
using KafkaSubscriptionEnvelope =
    Bundle<"hgraph.kafka.internal::KafkaSubscriptionEnvelope",
           Field<"subscription_key", KafkaSubscriptionKey>,
           Field<"record", KafkaRecord>, Field<"cursor", KafkaCursor>,
           Field<"state", KafkaSubscriptionState>,
           Field<"evaluation_time", DateTime>, Field<"removed", Bool>>;

using KafkaDeliveryEnvelope =
    Bundle<"hgraph.kafka.internal::KafkaDeliveryEnvelope",
           Field<"request_id", Int>, Field<"report", KafkaDeliveryReport>>;

using KafkaEventEnvelope =
    Bundle<"hgraph.kafka.internal::KafkaEventEnvelope",
           Field<"event", KafkaEvent>, Field<"stop_graph", Bool>>;

enum class OutputChannel : std::size_t {
  Subscription,
  Delivery,
  Event,
  Count,
};

struct OutputLimits {
  std::size_t records{};
  std::size_t bytes{};
};

class ServiceBridge {
public:
  struct QueuedOutput {
    Value value{};
    std::optional<DateTime> evaluation_time{};
    bool recovery_blocked{};
  };

  ServiceBridge(OutputLimits subscription, OutputLimits delivery,
                OutputLimits event, OutputLimits subscription_control = {},
                OutputLimits delivery_control = {},
                OutputLimits event_control = {})
      : channels_{
            Channel{
                .limits = subscription,
                .control_limits = subscription_control,
            },
            Channel{
                .limits = delivery,
                .control_limits = delivery_control,
            },
            Channel{
                .limits = event,
                .control_limits = event_control,
            },
        } {
    for (const auto &channel : channels_) {
      if (channel.limits.records == 0 || channel.limits.bytes == 0) {
        throw std::invalid_argument("Kafka bridge limits must be positive");
      }
    }
  }

  void attach(OutputChannel channel, PushSourceSender sender) {
    PushSourceSender wake;
    Int generation{};
    {
      std::lock_guard lock{mutex_};
      auto &state = at(channel);
      state.sender = std::move(sender);
      if (accepting_ && !state.values.empty() && !state.wake_outstanding) {
        state.wake_outstanding = true;
        generation = ++state.generation;
        wake = state.sender;
      }
    }
    static_cast<void>(wake.try_send(generation));
  }

  /** Start queue admission. Real-time services require all queue-owned push
   *  sources to be attached; deterministic simulation drains the same bounded
   *  queues through ordinary scheduled nodes and therefore has no sender. */
  void start(bool require_push_senders = true) {
    std::lock_guard lock{mutex_};
    if (accepting_) {
      throw std::logic_error("Kafka service bridge started twice");
    }
    if (require_push_senders) {
      for (const auto &channel : channels_) {
        if (!channel.sender.valid()) {
          throw std::logic_error(
              "Kafka service bridge started before its push sources");
        }
      }
    }
    accepting_ = true;
  }

  void begin_record_time_recovery(std::size_t participants) {
    if (participants == 0) {
      return;
    }
    std::lock_guard lock{mutex_};
    auto &state = at(OutputChannel::Subscription);
    const auto add_saturated = [](std::size_t value,
                                  std::size_t increment) noexcept {
      constexpr auto maximum = std::numeric_limits<std::size_t>::max();
      return increment > maximum - value ? maximum : value + increment;
    };
    const auto multiply_saturated = [](std::size_t value,
                                       std::size_t multiplier) noexcept {
      constexpr auto maximum = std::numeric_limits<std::size_t>::max();
      return value != 0 && multiplier > maximum / value ? maximum
                                                        : value * multiplier;
    };
    if (record_time_recoveries_pending_ == 0) {
      state.recovery_limits = OutputLimits{
          add_saturated(state.recovery_records,
                        multiply_saturated(state.limits.records, participants)),
          add_saturated(state.recovery_bytes,
                        multiply_saturated(state.limits.bytes, participants)),
      };
    } else {
      state.recovery_limits.records =
          add_saturated(state.recovery_limits.records,
                        multiply_saturated(state.limits.records, participants));
      state.recovery_limits.bytes =
          add_saturated(state.recovery_limits.bytes,
                        multiply_saturated(state.limits.bytes, participants));
    }
    if (record_time_recovery_controls_pending_ == 0) {
      state.recovery_control_limits = OutputLimits{
          add_saturated(
              state.control_records,
              multiply_saturated(state.control_limits.records, participants)),
          add_saturated(
              state.control_bytes,
              multiply_saturated(state.control_limits.bytes, participants)),
      };
    } else {
      state.recovery_control_limits.records = add_saturated(
          state.recovery_control_limits.records,
          multiply_saturated(state.control_limits.records, participants));
      state.recovery_control_limits.bytes = add_saturated(
          state.recovery_control_limits.bytes,
          multiply_saturated(state.control_limits.bytes, participants));
    }
    record_time_recoveries_pending_ =
        add_saturated(record_time_recoveries_pending_, participants);
    record_time_recovery_controls_pending_ =
        add_saturated(record_time_recovery_controls_pending_, participants);
  }

  void complete_record_time_recovery() {
    PushSourceSender wake;
    Int generation{};
    {
      std::lock_guard lock{mutex_};
      if (record_time_recoveries_pending_ == 0) {
        throw std::logic_error(
            "Kafka record-time recovery participant completed twice");
      }
      --record_time_recoveries_pending_;
      if (record_time_recoveries_pending_ != 0) {
        return;
      }
      auto &state = at(OutputChannel::Subscription);
      state.recovery_limits = {};
      if (accepting_ && !state.values.empty()) {
        state.wake_outstanding = true;
        generation = ++state.generation;
        wake = state.sender;
      }
    }
    static_cast<void>(wake.try_send(generation));
  }

  void finish_record_time_recovery() {
    std::lock_guard lock{mutex_};
    if (record_time_recovery_controls_pending_ == 0) {
      throw std::logic_error(
          "Kafka record-time recovery participant finished twice");
    }
    --record_time_recovery_controls_pending_;
    if (record_time_recovery_controls_pending_ == 0) {
      at(OutputChannel::Subscription).recovery_control_limits = {};
    }
  }

  void stop() noexcept {
    std::lock_guard lock{mutex_};
    accepting_ = false;
    for (auto &channel : channels_) {
      channel.values.clear();
      channel.retained_bytes = 0;
      channel.payload_records = 0;
      channel.payload_bytes = 0;
      channel.control_records = 0;
      channel.control_bytes = 0;
      channel.reserved_records = 0;
      channel.reserved_bytes = 0;
      channel.recovery_records = 0;
      channel.recovery_bytes = 0;
      channel.recovery_limits = {};
      channel.recovery_control_limits = {};
      channel.wake_outstanding = false;
      channel.sender = PushSourceSender{};
    }
    record_time_recoveries_pending_ = 0;
    record_time_recovery_controls_pending_ = 0;
    subscription_delivered_ = {};
  }

  void on_subscription_delivered(std::function<void(Value)> callback) {
    std::lock_guard lock{mutex_};
    subscription_delivered_ = std::move(callback);
  }

  void subscription_delivered(Value cursor) {
    std::function<void(Value)> callback;
    {
      std::lock_guard lock{mutex_};
      callback = subscription_delivered_;
    }
    if (callback) {
      callback(std::move(cursor));
    }
  }

  [[nodiscard]] bool push(OutputChannel channel, Value value,
                          std::size_t retained_bytes) {
    return push_impl(channel, std::move(value), retained_bytes, false, false);
  }

  [[nodiscard]] bool push_recovery(OutputChannel channel, Value value,
                                   std::size_t retained_bytes) {
    return push_impl(channel, std::move(value), retained_bytes, false, true);
  }

  [[nodiscard]] bool push_control(OutputChannel channel, Value value,
                                  std::size_t retained_bytes) {
    return push_impl(channel, std::move(value), retained_bytes, true, false);
  }

  [[nodiscard]] std::optional<QueuedOutput> pop(OutputChannel channel) {
    std::optional<QueuedOutput> result;
    {
      std::lock_guard lock{mutex_};
      auto &state = at(channel);
      if (state.values.empty()) {
        state.wake_outstanding = false;
        return std::nullopt;
      }
      if (state.values.front().recovery &&
          record_time_recoveries_pending_ != 0) {
        return std::nullopt;
      }

      QueuedValue item = std::move(state.values.front());
      state.values.pop_front();
      state.retained_bytes -= item.retained_bytes;
      if (item.control) {
        --state.control_records;
        state.control_bytes -= item.retained_bytes;
      } else {
        --state.payload_records;
        state.payload_bytes -= item.retained_bytes;
      }
      if (item.recovery) {
        --state.recovery_records;
        state.recovery_bytes -= item.retained_bytes;
      }
      result.emplace(
          QueuedOutput{std::move(item.value), item.evaluation_time, false});

      if (state.values.empty()) {
        state.wake_outstanding = false;
      }
    }
    return result;
  }

  [[nodiscard]] std::optional<QueuedOutput> peek(OutputChannel channel) const {
    std::lock_guard lock{mutex_};
    const auto &state = at(channel);
    if (state.values.empty()) {
      return std::nullopt;
    }
    const auto &item = state.values.front();
    return QueuedOutput{item.value.clone(), item.evaluation_time,
                        item.recovery && record_time_recoveries_pending_ != 0};
  }

  [[nodiscard]] std::size_t pending(OutputChannel channel) const {
    std::lock_guard lock{mutex_};
    return at(channel).values.size();
  }

  [[nodiscard]] std::size_t payload_pending(OutputChannel channel) const {
    std::lock_guard lock{mutex_};
    return at(channel).payload_records;
  }

  [[nodiscard]] std::size_t retained_bytes(OutputChannel channel) const {
    std::lock_guard lock{mutex_};
    return at(channel).retained_bytes;
  }

  [[nodiscard]] std::size_t
  payload_retained_bytes(OutputChannel channel) const {
    std::lock_guard lock{mutex_};
    return at(channel).payload_bytes;
  }

  void add_capacity(OutputChannel channel, OutputLimits payload_capacity,
                    OutputLimits control_capacity = {}) {
    std::lock_guard lock{mutex_};
    auto &state = at(channel);
    const auto add_saturated = [](std::size_t value,
                                  std::size_t increment) noexcept {
      constexpr auto maximum = std::numeric_limits<std::size_t>::max();
      return increment > maximum - value ? maximum : value + increment;
    };
    state.limits.records =
        add_saturated(state.limits.records, payload_capacity.records);
    state.limits.bytes =
        add_saturated(state.limits.bytes, payload_capacity.bytes);
    state.control_limits.records =
        add_saturated(state.control_limits.records, control_capacity.records);
    state.control_limits.bytes =
        add_saturated(state.control_limits.bytes, control_capacity.bytes);
  }

  void discard_subscription(const ValueView &key) {
    std::lock_guard lock{mutex_};
    auto &state = at(OutputChannel::Subscription);
    auto item = state.values.begin();
    while (item != state.values.end()) {
      const auto envelope = item->value.view().as_bundle();
      if (!envelope.at("subscription_key").equals(key)) {
        ++item;
        continue;
      }
      state.retained_bytes -= item->retained_bytes;
      if (item->control) {
        --state.control_records;
        state.control_bytes -= item->retained_bytes;
      } else {
        --state.payload_records;
        state.payload_bytes -= item->retained_bytes;
      }
      if (item->recovery) {
        --state.recovery_records;
        state.recovery_bytes -= item->retained_bytes;
      }
      item = state.values.erase(item);
    }
    if (state.values.empty()) {
      // The outstanding conflated wake may already have been
      // consumed by this graph cycle.  Let the next surviving
      // subscription value publish a fresh wake instead of
      // inheriting liveness from discarded work.
      state.wake_outstanding = false;
    }
  }

  [[nodiscard]] bool erase_subscription(Value key) {
    discard_subscription(key.view());
    BundleBuilder envelope{ValuePlanFactory::instance().type_for(
        scalar_descriptor<KafkaSubscriptionEnvelope>::value_meta())};
    envelope.set("subscription_key", std::move(key));
    envelope.set("removed", Value{Bool{true}});
    return push_control(OutputChannel::Subscription, envelope.build(), 512);
  }

  [[nodiscard]] bool can_accept(OutputChannel channel,
                                std::size_t retained_bytes) const {
    std::lock_guard lock{mutex_};
    const auto &state = at(channel);
    return accepting_ &&
           state.payload_records + state.reserved_records <
               state.limits.records &&
           retained_bytes <=
               state.limits.bytes -
                   std::min(state.payload_bytes + state.reserved_bytes,
                            state.limits.bytes);
  }

  [[nodiscard]] bool can_accept_recovery(OutputChannel channel,
                                         std::size_t retained_bytes) const {
    std::lock_guard lock{mutex_};
    const auto &state = at(channel);
    return accepting_ && record_time_recoveries_pending_ != 0 &&
           state.recovery_records < state.recovery_limits.records &&
           retained_bytes <=
               state.recovery_limits.bytes -
                   std::min(state.recovery_bytes, state.recovery_limits.bytes);
  }

  [[nodiscard]] bool reserve(OutputChannel channel,
                             std::size_t retained_bytes) {
    std::lock_guard lock{mutex_};
    auto &state = at(channel);
    if (!accepting_ ||
        state.payload_records + state.reserved_records >=
            state.limits.records ||
        retained_bytes > state.limits.bytes - std::min(state.payload_bytes +
                                                           state.reserved_bytes,
                                                       state.limits.bytes)) {
      return false;
    }
    ++state.reserved_records;
    state.reserved_bytes += retained_bytes;
    return true;
  }

  [[nodiscard]] bool push_reserved(OutputChannel channel, Value value,
                                   std::size_t retained_bytes,
                                   std::size_t reserved_bytes) {
    PushSourceSender wake;
    Int generation{};
    {
      std::lock_guard lock{mutex_};
      auto &state = at(channel);
      if (state.reserved_records == 0 ||
          state.reserved_bytes < reserved_bytes) {
        throw std::logic_error("Kafka bridge output reservation is not live");
      }
      if (retained_bytes > reserved_bytes) {
        throw std::logic_error("Kafka bridge output exceeded its reservation");
      }
      --state.reserved_records;
      state.reserved_bytes -= reserved_bytes;
      if (!accepting_) {
        return false;
      }
      QueuedValue item{std::move(value), retained_bytes, false};
      item.evaluation_time = queued_evaluation_time(channel, item.value);
      inherit_subscription_schedule(channel, state, item);
      const auto position = insertion_point(channel, state, item);
      const bool became_head = position == state.values.begin();
      state.values.insert(position, std::move(item));
      ++state.payload_records;
      state.payload_bytes += retained_bytes;
      state.retained_bytes += retained_bytes;
      if (!state.wake_outstanding || became_head) {
        state.wake_outstanding = true;
        generation = ++state.generation;
        wake = state.sender;
      }
    }
    static_cast<void>(wake.try_send(generation));
    return true;
  }

  void release_reservation(OutputChannel channel,
                           std::size_t reserved_bytes) noexcept {
    std::lock_guard lock{mutex_};
    auto &state = at(channel);
    if (state.reserved_records == 0 || state.reserved_bytes < reserved_bytes) {
      return;
    }
    --state.reserved_records;
    state.reserved_bytes -= reserved_bytes;
  }

private:
  [[nodiscard]] bool push_impl(OutputChannel channel, Value value,
                               std::size_t retained_bytes, bool control,
                               bool recovery) {
    PushSourceSender wake;
    Int generation{};
    {
      std::lock_guard lock{mutex_};
      auto &state = at(channel);
      const auto limits = control && record_time_recovery_controls_pending_ != 0
                              ? state.recovery_control_limits
                              : (control ? state.control_limits : state.limits);
      const auto records =
          control ? state.control_records : state.payload_records;
      const auto bytes = control ? state.control_bytes : state.payload_bytes;
      const bool recovery_full =
          recovery &&
          (record_time_recoveries_pending_ == 0 ||
           state.recovery_records >= state.recovery_limits.records ||
           retained_bytes >
               state.recovery_limits.bytes -
                   std::min(state.recovery_bytes, state.recovery_limits.bytes));
      const bool ordinary_full =
          !recovery &&
          (limits.records == 0 || limits.bytes == 0 ||
           records + (control ? 0 : state.reserved_records) >= limits.records ||
           retained_bytes >
               limits.bytes -
                   std::min(bytes + (control ? 0 : state.reserved_bytes),
                            limits.bytes));
      if (!accepting_ || recovery_full || ordinary_full) {
        return false;
      }
      state.retained_bytes += retained_bytes;
      if (control) {
        ++state.control_records;
        state.control_bytes += retained_bytes;
      } else {
        ++state.payload_records;
        state.payload_bytes += retained_bytes;
      }
      if (recovery) {
        ++state.recovery_records;
        state.recovery_bytes += retained_bytes;
      }
      QueuedValue item{std::move(value), retained_bytes, control, recovery};
      item.evaluation_time = queued_evaluation_time(channel, item.value);
      inherit_subscription_schedule(channel, state, item);
      const auto position = insertion_point(channel, state, item);
      const bool became_head = position == state.values.begin();
      state.values.insert(position, std::move(item));
      if (!state.wake_outstanding || became_head) {
        state.wake_outstanding = true;
        generation = ++state.generation;
        wake = state.sender;
      }
    }
    static_cast<void>(wake.try_send(generation));
    return true;
  }
  struct QueuedValue {
    Value value{};
    std::size_t retained_bytes{};
    bool control{};
    bool recovery{};
    std::optional<DateTime> evaluation_time{};
  };

  struct Channel {
    OutputLimits limits{};
    OutputLimits control_limits{};
    std::deque<QueuedValue> values{};
    std::size_t retained_bytes{};
    std::size_t payload_records{};
    std::size_t payload_bytes{};
    std::size_t control_records{};
    std::size_t control_bytes{};
    std::size_t reserved_records{};
    std::size_t reserved_bytes{};
    std::size_t recovery_records{};
    std::size_t recovery_bytes{};
    OutputLimits recovery_limits{};
    OutputLimits recovery_control_limits{};
    PushSourceSender sender{};
    Int generation{};
    bool wake_outstanding{};
  };

  [[nodiscard]] static std::optional<DateTime>
  queued_evaluation_time(OutputChannel channel, const Value &value) {
    if (channel != OutputChannel::Subscription) {
      return std::nullopt;
    }
    const auto field = value.view().as_bundle().at("evaluation_time");
    return field.data() != nullptr
               ? std::optional<DateTime>{field.checked_as<DateTime>()}
               : std::nullopt;
  }

  [[nodiscard]] static bool scheduled_before(const QueuedValue &lhs,
                                             const QueuedValue &rhs) noexcept {
    if (!lhs.evaluation_time.has_value()) {
      return rhs.evaluation_time.has_value();
    }
    return rhs.evaluation_time.has_value() &&
           *lhs.evaluation_time < *rhs.evaluation_time;
  }

  static void inherit_subscription_schedule(OutputChannel channel,
                                            const Channel &state,
                                            QueuedValue &item) {
    if (channel != OutputChannel::Subscription ||
        item.evaluation_time.has_value()) {
      return;
    }
    const auto key = item.value.view().as_bundle().at("subscription_key");
    const auto predecessor =
        std::find_if(state.values.rbegin(), state.values.rend(),
                     [&](const QueuedValue &queued) {
                       return queued.value.view()
                           .as_bundle()
                           .at("subscription_key")
                           .equals(key);
                     });
    if (predecessor != state.values.rend() &&
        predecessor->evaluation_time.has_value()) {
      // A real-time consumer may resume polling while its timestamped
      // recovery tail is still waiting on graph time. Keep later lifecycle
      // and live values behind that per-subscription tail without delaying
      // unrelated untimed subscriptions.
      item.evaluation_time = *predecessor->evaluation_time + MIN_TD;
    }
  }

  [[nodiscard]] static std::deque<QueuedValue>::iterator
  insertion_point(OutputChannel channel, Channel &state,
                  const QueuedValue &item) {
    if (channel != OutputChannel::Subscription) {
      return state.values.end();
    }
    // Recovery sessions preload independently, so enqueue order is not replay
    // order. Untimed lifecycle/live values remain FIFO at the front; recovered
    // values form one stable stream ordered by their graph evaluation time.
    return std::upper_bound(state.values.begin(), state.values.end(), item,
                            scheduled_before);
  }

  [[nodiscard]] Channel &at(OutputChannel channel) {
    return channels_.at(static_cast<std::size_t>(channel));
  }

  [[nodiscard]] const Channel &at(OutputChannel channel) const {
    return channels_.at(static_cast<std::size_t>(channel));
  }

  mutable std::mutex mutex_{};
  std::array<Channel, static_cast<std::size_t>(OutputChannel::Count)> channels_;
  bool accepting_{};
  std::size_t record_time_recoveries_pending_{};
  std::size_t record_time_recovery_controls_pending_{};
  std::function<void(Value)> subscription_delivered_{};
};

struct ServiceBridgeHandle {
  std::shared_ptr<ServiceBridge> value{};

  friend bool operator==(const ServiceBridgeHandle &,
                         const ServiceBridgeHandle &) noexcept = default;
  friend std::strong_ordering
  operator<=>(const ServiceBridgeHandle &lhs,
              const ServiceBridgeHandle &rhs) noexcept {
    return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
           reinterpret_cast<std::uintptr_t>(rhs.value.get());
  }
};

inline std::ostream &operator<<(std::ostream &stream,
                                const ServiceBridgeHandle &value) {
  return stream << "ServiceBridgeHandle(" << value.value.get() << ')';
}

struct SubscriptionSignalTag {};
struct DeliverySignalTag {};
struct EventSignalTag {};

template <typename Tag>
[[nodiscard]] Port<TS<Int>> signal_source(Wiring &w, ServiceBridgeHandle bridge,
                                          OutputChannel channel) {
  const auto *schema = ts_type<TS<Int>>();
  return Port<TS<Int>>{
      w,
      w.add_unique_node(
          std::type_index(typeid(Tag)),
          make_push_source_node(
              *schema,
              make_push_source_conflating_policy(*schema->delta_value_schema),
              [bridge = std::move(bridge), channel](PushSourceSender sender) {
                bridge.value->attach(channel, std::move(sender));
              }),
          std::span<const WiringPortRef>{}, Value{})};
}

struct SubscriptionDrainNode {
  static constexpr auto name = "kafka_subscription_drain";

  static void
  eval(In<"signal", TS<Int>>, Scalar<"bridge", ServiceBridgeHandle> bridge,
       SingleShotScheduler scheduler,
       Out<TSD<KafkaSubscriptionKey, KafkaSubscriptionOutput>> out) {
    auto next = bridge.value().value->peek(OutputChannel::Subscription);
    if (!next.has_value()) {
      return;
    }
    if (next->recovery_blocked) {
      return;
    }
    if (next->evaluation_time.has_value() &&
        *next->evaluation_time > scheduler.now()) {
      scheduler.schedule(*next->evaluation_time);
      return;
    }
    const auto batch_time = next->evaluation_time;
    auto mutation = out.begin_mutation(out.evaluation_time());
    while (true) {
      auto queued = bridge.value().value->pop(OutputChannel::Subscription);
      if (!queued.has_value()) {
        return;
      }

      const auto fields = queued->value.view().as_bundle();
      const auto removed = fields.at("removed");
      if (removed.data() != nullptr && removed.checked_as<Bool>()) {
        static_cast<void>(mutation.erase(fields.at("subscription_key")));
      } else {
        const auto value_binding = ValuePlanFactory::instance().type_for(
            schema_descriptor<KafkaSubscriptionOutput>::ts_meta()
                ->value_schema);
        BundleBuilder value{value_binding};
        const auto record = fields.at("record");
        const auto cursor = fields.at("cursor");
        const auto state = fields.at("state");
        if (record.data() != nullptr) {
          value.set("record", record.clone());
        }
        if (cursor.data() != nullptr) {
          value.set("cursor", cursor.clone());
        }
        if (state.data() != nullptr) {
          value.set("state", state.clone());
        }
        Value update = value.build();
        mutation.set(fields.at("subscription_key"), update.view());
        if (cursor.data() != nullptr) {
          bridge.value().value->subscription_delivered(cursor.clone());
        }
      }

      next = bridge.value().value->peek(OutputChannel::Subscription);
      if (!next.has_value()) {
        return;
      }
      const auto next_time = next->evaluation_time;
      if (batch_time.has_value() && next_time == batch_time) {
        // Independent subscriptions may legitimately tick at the same Kafka
        // timestamp. Apply their distinct TSD keys in one graph mutation.
        continue;
      }
      if (next_time.has_value() && *next_time > scheduler.now()) {
        scheduler.schedule(*next_time);
      } else {
        scheduler.schedule(MIN_TD);
      }
      return;
    }
  }
};

struct DeliveryDrainNode {
  static constexpr auto name = "kafka_delivery_drain";

  static void eval(In<"signal", TS<Int>>,
                   Scalar<"bridge", ServiceBridgeHandle> bridge,
                   SingleShotScheduler scheduler,
                   Out<TSD<Int, TS<KafkaDeliveryReport>>> out) {
    auto envelope = bridge.value().value->pop(OutputChannel::Delivery);
    if (!envelope.has_value()) {
      return;
    }
    const auto fields = envelope->value.view().as_bundle();
    auto mutation = out.begin_mutation(out.evaluation_time());
    mutation.set(fields.at("request_id"), fields.at("report"));
    if (bridge.value().value->pending(OutputChannel::Delivery) != 0) {
      scheduler.schedule(MIN_TD);
    }
  }
};

struct EventDrainNode {
  static constexpr auto name = "kafka_event_drain";

  static void eval(In<"signal", TS<Int>>,
                   Scalar<"bridge", ServiceBridgeHandle> bridge,
                   EngineControlView engine, SingleShotScheduler scheduler,
                   Out<TS<KafkaEvent>> out) {
    auto envelope = bridge.value().value->pop(OutputChannel::Event);
    if (!envelope.has_value()) {
      return;
    }
    const auto fields = envelope->value.view().as_bundle();
    out.apply(fields.at("event"));
    if (fields.at("stop_graph").checked_as<Bool>()) {
      engine.request_stop();
    }
    if (bridge.value().value->pending(OutputChannel::Event) != 0) {
      scheduler.schedule(MIN_TD);
    }
  }
};

struct ServiceOutputs {
  Port<TSD<KafkaSubscriptionKey, KafkaSubscriptionOutput>> subscriptions;
  Port<TSD<Int, TS<KafkaDeliveryReport>>> deliveries;
  Port<TS<KafkaEvent>> events;
};

[[nodiscard]] inline ServiceOutputs
wire_service_outputs(Wiring &w, ServiceBridgeHandle bridge) {
  auto subscription_signal = signal_source<SubscriptionSignalTag>(
      w, bridge, OutputChannel::Subscription);
  auto delivery_signal =
      signal_source<DeliverySignalTag>(w, bridge, OutputChannel::Delivery);
  auto event_signal =
      signal_source<EventSignalTag>(w, bridge, OutputChannel::Event);

  return ServiceOutputs{
      wire<SubscriptionDrainNode>(w, subscription_signal, bridge)
          .template as<TSD<KafkaSubscriptionKey, KafkaSubscriptionOutput>>(),
      wire<DeliveryDrainNode>(w, delivery_signal, bridge)
          .template as<TSD<Int, TS<KafkaDeliveryReport>>>(),
      wire<EventDrainNode>(w, event_signal, bridge)
          .template as<TS<KafkaEvent>>(),
  };
}
} // namespace hgraph::kafka::detail

namespace std {
template <> struct hash<hgraph::kafka::detail::ServiceBridgeHandle> {
  size_t operator()(
      const hgraph::kafka::detail::ServiceBridgeHandle &value) const noexcept {
    return hash<const void *>{}(value.value.get());
  }
};
} // namespace std

namespace hgraph::static_schema_detail {
template <> struct scalar_name<kafka::detail::ServiceBridgeHandle> {
  static constexpr std::string_view value{
      "hgraph.kafka.internal::ServiceBridgeHandle"};
};
} // namespace hgraph::static_schema_detail

#endif // HGRAPH_KAFKA_DETAIL_SERVICE_BRIDGE_H
