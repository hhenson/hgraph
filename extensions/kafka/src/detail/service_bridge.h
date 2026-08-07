#ifndef HGRAPH_KAFKA_DETAIL_SERVICE_BRIDGE_H
#define HGRAPH_KAFKA_DETAIL_SERVICE_BRIDGE_H

#include <hgraph/kafka/service.h>

#include <hgraph/runtime/executor.h>
#include <hgraph/runtime/push_source_node.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/value_builder.h>

#include <array>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
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
    if (wake.valid()) {
      wake.send(generation);
    }
  }

  void start() {
    std::lock_guard lock{mutex_};
    if (accepting_) {
      throw std::logic_error("Kafka service bridge started twice");
    }
    for (const auto &channel : channels_) {
      if (!channel.sender.valid()) {
        throw std::logic_error(
            "Kafka service bridge started before its push sources");
      }
    }
    accepting_ = true;
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
      channel.wake_outstanding = false;
      channel.sender = PushSourceSender{};
    }
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
    return push_impl(channel, std::move(value), retained_bytes, false);
  }

  [[nodiscard]] bool push_control(OutputChannel channel, Value value,
                                  std::size_t retained_bytes) {
    return push_impl(channel, std::move(value), retained_bytes, true);
  }

  [[nodiscard]] std::optional<Value> pop(OutputChannel channel) {
    std::optional<Value> result;
    {
      std::lock_guard lock{mutex_};
      auto &state = at(channel);
      if (state.values.empty()) {
        state.wake_outstanding = false;
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
      result.emplace(std::move(item.value));

      if (state.values.empty()) {
        state.wake_outstanding = false;
      }
    }
    return result;
  }

  [[nodiscard]] std::optional<Value> peek(OutputChannel channel) const {
    std::lock_guard lock{mutex_};
    const auto &state = at(channel);
    if (state.values.empty()) {
      return std::nullopt;
    }
    return state.values.front().value.clone();
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
      state.values.push_back(
          QueuedValue{std::move(value), retained_bytes, false});
      --state.reserved_records;
      state.reserved_bytes -= reserved_bytes;
      if (!accepting_) {
        state.values.pop_back();
        return false;
      }
      ++state.payload_records;
      state.payload_bytes += retained_bytes;
      state.retained_bytes += retained_bytes;
      if (!state.wake_outstanding) {
        state.wake_outstanding = true;
        generation = ++state.generation;
        wake = state.sender;
      }
    }
    if (wake.valid()) {
      wake.send(generation);
    }
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
                               std::size_t retained_bytes, bool control) {
    PushSourceSender wake;
    Int generation{};
    {
      std::lock_guard lock{mutex_};
      auto &state = at(channel);
      const auto limits = control ? state.control_limits : state.limits;
      const auto records =
          control ? state.control_records : state.payload_records;
      const auto bytes = control ? state.control_bytes : state.payload_bytes;
      if (!accepting_ || limits.records == 0 || limits.bytes == 0 ||
          records + (control ? 0 : state.reserved_records) >= limits.records ||
          retained_bytes >
              limits.bytes -
                  std::min(bytes + (control ? 0 : state.reserved_bytes),
                           limits.bytes)) {
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
      state.values.push_back(
          QueuedValue{std::move(value), retained_bytes, control});
      if (!state.wake_outstanding) {
        state.wake_outstanding = true;
        generation = ++state.generation;
        wake = state.sender;
      }
    }
    if (wake.valid()) {
      wake.send(generation);
    }
    return true;
  }
  struct QueuedValue {
    Value value{};
    std::size_t retained_bytes{};
    bool control{};
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
    PushSourceSender sender{};
    Int generation{};
    bool wake_outstanding{};
  };

  [[nodiscard]] Channel &at(OutputChannel channel) {
    return channels_.at(static_cast<std::size_t>(channel));
  }

  [[nodiscard]] const Channel &at(OutputChannel channel) const {
    return channels_.at(static_cast<std::size_t>(channel));
  }

  mutable std::mutex mutex_{};
  std::array<Channel, static_cast<std::size_t>(OutputChannel::Count)> channels_;
  bool accepting_{};
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
          make_simulation_capable_push_source_node(
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
    const auto next_fields = next->view().as_bundle();
    const auto evaluation_time = next_fields.at("evaluation_time");
    if (evaluation_time.data() != nullptr) {
      const DateTime requested = evaluation_time.checked_as<DateTime>();
      if (requested > scheduler.now()) {
        scheduler.schedule(requested);
        return;
      }
    }
    auto envelope = bridge.value().value->pop(OutputChannel::Subscription);
    if (!envelope.has_value()) {
      return;
    }

    const auto fields = envelope->view().as_bundle();
    const auto removed = fields.at("removed");
    if (removed.data() != nullptr && removed.checked_as<Bool>()) {
      auto mutation = out.begin_mutation(out.evaluation_time());
      static_cast<void>(mutation.erase(fields.at("subscription_key")));
      if (bridge.value().value->pending(OutputChannel::Subscription) != 0) {
        scheduler.schedule(MIN_TD);
      }
      return;
    }
    const auto value_binding = ValuePlanFactory::instance().type_for(
        schema_descriptor<KafkaSubscriptionOutput>::ts_meta()->value_schema);
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

    auto mutation = out.begin_mutation(out.evaluation_time());
    mutation.set(fields.at("subscription_key"), update.view());
    if (cursor.data() != nullptr) {
      bridge.value().value->subscription_delivered(cursor.clone());
    }
    if (bridge.value().value->pending(OutputChannel::Subscription) != 0) {
      scheduler.schedule(MIN_TD);
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
    const auto fields = envelope->view().as_bundle();
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
    const auto fields = envelope->view().as_bundle();
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
