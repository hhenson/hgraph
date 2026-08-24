#ifndef HGRAPH_KAFKA_DETAIL_SERVICE_TRANSPORT_H
#define HGRAPH_KAFKA_DETAIL_SERVICE_TRANSPORT_H

#include <hgraph/kafka/service.h>

#include <hgraph/runtime/push_source_node.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/value_builder.h>

#include <algorithm>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <ranges>
#include <span>
#include <string_view>
#include <typeindex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace hgraph::kafka::detail {

/** The one ordered transport stream crossing from Kafka-owned threads into
 *  the graph. The envelope is internal; public services expose projections. */
enum class KafkaTransportEventKind : std::int64_t {
  Subscription,
  Delivery,
  Event,
  RecoveryBarrier,
};

} // namespace hgraph::kafka::detail

namespace hgraph::static_schema_detail {
template <> struct scalar_name<kafka::detail::KafkaTransportEventKind> {
  static constexpr std::string_view value{
      "hgraph.kafka.internal::KafkaTransportEventKind"};
};
} // namespace hgraph::static_schema_detail

namespace hgraph::kafka::detail {

using KafkaTransportEvent =
    Bundle<"hgraph.kafka.internal::KafkaTransportEvent",
           Field<"kind", KafkaTransportEventKind>,
           Field<"subscription_key", KafkaSubscriptionKey>,
           Field<"record", KafkaRecord>, Field<"cursor", KafkaCursor>,
           Field<"state", KafkaSubscriptionState>,
           Field<"evaluation_time", DateTime>, Field<"removed", Bool>,
           Field<"recovery", Bool>, Field<"request_id", Int>,
           Field<"report", KafkaDeliveryReport>, Field<"event", KafkaEvent>,
           Field<"stop_graph", Bool>>;
using KafkaTransportEventBatch = HomogeneousTuple<KafkaTransportEvent>;

struct KafkaTransportBindings {
  ValueTypeRef event{};
  ValueTypeRef batch{};
  ValueTypeRef subscription_output{};
};

struct KafkaTransportBindingsHandle {
  std::shared_ptr<const KafkaTransportBindings> value{};

  friend bool
  operator==(const KafkaTransportBindingsHandle &,
             const KafkaTransportBindingsHandle &) noexcept = default;
  friend std::strong_ordering
  operator<=>(const KafkaTransportBindingsHandle &lhs,
              const KafkaTransportBindingsHandle &rhs) noexcept {
    return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
           reinterpret_cast<std::uintptr_t>(rhs.value.get());
  }
};

inline std::ostream &operator<<(std::ostream &stream,
                                const KafkaTransportBindingsHandle &value) {
  return stream << "KafkaTransportBindingsHandle(" << value.value.get() << ')';
}

[[nodiscard]] inline KafkaTransportBindingsHandle make_transport_bindings() {
  auto &factory = ValuePlanFactory::instance();
  return KafkaTransportBindingsHandle{
      std::make_shared<const KafkaTransportBindings>(KafkaTransportBindings{
          factory.type_for(
              scalar_descriptor<KafkaTransportEvent>::value_meta()),
          factory.type_for(
              scalar_descriptor<KafkaTransportEventBatch>::value_meta()),
          factory.type_for(schema_descriptor<KafkaSubscriptionOutput>::ts_meta()
                               ->value_schema),
      })};
}

class SubscriptionEventSchedule {
public:
  explicit SubscriptionEventSchedule(KafkaTransportBindings bindings)
      : bindings_{std::move(bindings)} {}

  void push(Value value) {
    const auto recovery = value.view().as_bundle().at("recovery");
    if (recovery.data() != nullptr && recovery.checked_as<Bool>()) {
      recovery_blocked_ = true;
    }
    if (!evaluation_time(value.view()).has_value()) {
      const auto fields = value.view().as_bundle();
      const auto key = fields.at("subscription_key");
      const auto predecessor = std::find_if(
          values_.rbegin(), values_.rend(), [&](const Value &item) {
            return item.view().as_bundle().at("subscription_key").equals(key);
          });
      if (predecessor != values_.rend()) {
        const auto predecessor_time = evaluation_time(predecessor->view());
        if (predecessor_time.has_value()) {
          value = with_evaluation_time(std::move(value),
                                       *predecessor_time + MIN_TD);
        }
      }
    }
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
  }

  [[nodiscard]] std::optional<DateTime> next_time() const {
    return values_.empty() ? std::nullopt
                           : evaluation_time(values_.front().view());
  }

  [[nodiscard]] Value pop() {
    Value value = std::move(values_.front());
    values_.pop_front();
    return value;
  }

  /** Detaches the next graph-visible cohort. Explicit recovery timestamps
   * remain atomic; live events without timestamps release at most one event
   * per subscription key so independent subscriptions can advance together.
   * Cost is O(N) for N queued subscription events. */
  [[nodiscard]] std::vector<Value> pop_ready_cohort() {
    std::vector<Value> result;
    if (values_.empty()) {
      return result;
    }
    const auto cohort_time = evaluation_time(values_.front().view());
    if (cohort_time.has_value()) {
      while (!values_.empty() &&
             evaluation_time(values_.front().view()) == cohort_time) {
        result.push_back(pop());
      }
      return result;
    }

    struct Hash {
      [[nodiscard]] std::size_t operator()(const Value &value) const {
        return value.hash();
      }
    };
    struct Equal {
      [[nodiscard]] bool operator()(const Value &lhs, const Value &rhs) const {
        return lhs.equals(rhs.view());
      }
    };
    std::unordered_set<Value, Hash, Equal> emitted;
    std::deque<Value> remaining;
    while (!values_.empty()) {
      Value value = pop();
      if (evaluation_time(value.view()).has_value()) {
        remaining.push_back(std::move(value));
        while (!values_.empty()) {
          remaining.push_back(pop());
        }
        break;
      }
      const auto key = value.view().as_bundle().at("subscription_key");
      if (emitted.emplace(key).second) {
        result.push_back(std::move(value));
      } else {
        remaining.push_back(std::move(value));
      }
    }
    values_.swap(remaining);
    return result;
  }

  [[nodiscard]] bool empty() const noexcept { return values_.empty(); }

  [[nodiscard]] bool recovery_blocked() const {
    if (!recovery_blocked_ || values_.empty()) {
      return false;
    }
    const auto recovery = values_.front().view().as_bundle().at("recovery");
    return recovery.data() != nullptr && recovery.checked_as<Bool>();
  }

  void release_recovery() noexcept { recovery_blocked_ = false; }

  [[nodiscard]] const ValueTypeRef &subscription_output_binding() const {
    return bindings_.subscription_output;
  }

private:
  [[nodiscard]] Value with_evaluation_time(Value value, DateTime time) const {
    BundleBuilder builder{bindings_.event};
    const auto fields = value.view().as_bundle();
    for (const auto name :
         {std::string_view{"kind"}, std::string_view{"subscription_key"},
          std::string_view{"record"}, std::string_view{"cursor"},
          std::string_view{"state"}, std::string_view{"removed"},
          std::string_view{"recovery"}, std::string_view{"request_id"},
          std::string_view{"report"}, std::string_view{"event"},
          std::string_view{"stop_graph"}}) {
      const auto field = fields.at(name);
      if (field.data() != nullptr) {
        builder.set(name, field.clone());
      }
    }
    builder.set("evaluation_time", Value{time});
    return builder.build();
  }

  [[nodiscard]] static std::optional<DateTime>
  evaluation_time(const ValueView &value) {
    const auto field = value.as_bundle().at("evaluation_time");
    return field.data() == nullptr
               ? std::nullopt
               : std::optional<DateTime>{field.checked_as<DateTime>()};
  }

  std::deque<Value> values_{};
  bool recovery_blocked_{};
  KafkaTransportBindings bindings_{};
};

struct SubscriptionEventScheduleHandle {
  std::shared_ptr<SubscriptionEventSchedule> value{};

  friend bool
  operator==(const SubscriptionEventScheduleHandle &,
             const SubscriptionEventScheduleHandle &) noexcept = default;
  friend std::strong_ordering
  operator<=>(const SubscriptionEventScheduleHandle &lhs,
              const SubscriptionEventScheduleHandle &rhs) noexcept {
    return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
           reinterpret_cast<std::uintptr_t>(rhs.value.get());
  }
};

/** Graph-thread spill used when one burst contains several delivery reports
 * for the same service request id. Distinct ids never enter this state. Drain
 * cost is O(P) for P pending ids; retained memory is O(C) for collisions. */
class DeliveryEventSchedule {
public:
  template <typename Apply>
  void drain(std::unordered_set<Int> &emitted, Apply &&apply) {
    for (auto entry = values_.begin(); entry != values_.end();) {
      auto &queue = entry->second;
      Value event = std::move(queue.front());
      queue.pop_front();
      apply(event.view());
      emitted.emplace(entry->first);
      if (queue.empty()) {
        entry = values_.erase(entry);
      } else {
        ++entry;
      }
    }
  }

  void defer(Int request_id, const ValueView &event) {
    values_[request_id].emplace_back(event);
  }

  [[nodiscard]] bool empty() const noexcept { return values_.empty(); }

private:
  std::unordered_map<Int, std::deque<Value>> values_{};
};

struct DeliveryEventScheduleHandle {
  std::shared_ptr<DeliveryEventSchedule> value{};

  friend bool
  operator==(const DeliveryEventScheduleHandle &,
             const DeliveryEventScheduleHandle &) noexcept = default;
  friend std::strong_ordering
  operator<=>(const DeliveryEventScheduleHandle &lhs,
              const DeliveryEventScheduleHandle &rhs) noexcept {
    return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
           reinterpret_cast<std::uintptr_t>(rhs.value.get());
  }
};

/** Graph-thread FIFO used only when a burst contains several scalar service
 * events. Cost is O(1) to emit and retain each event; memory is O(E) for E
 * events awaiting their own ticks. */
class KafkaEventSchedule {
public:
  void defer(const ValueView &event) { values_.emplace_back(event); }

  [[nodiscard]] std::optional<Value> pop() {
    if (values_.empty()) {
      return std::nullopt;
    }
    Value value = std::move(values_.front());
    values_.pop_front();
    return value;
  }

  [[nodiscard]] bool empty() const noexcept { return values_.empty(); }

private:
  std::deque<Value> values_{};
};

struct KafkaEventScheduleHandle {
  std::shared_ptr<KafkaEventSchedule> value{};

  friend bool operator==(const KafkaEventScheduleHandle &,
                         const KafkaEventScheduleHandle &) noexcept = default;
  friend std::strong_ordering
  operator<=>(const KafkaEventScheduleHandle &lhs,
              const KafkaEventScheduleHandle &rhs) noexcept {
    return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
           reinterpret_cast<std::uintptr_t>(rhs.value.get());
  }
};

inline std::ostream &operator<<(std::ostream &stream,
                                const SubscriptionEventScheduleHandle &value) {
  return stream << "SubscriptionEventScheduleHandle(" << value.value.get()
                << ')';
}

inline std::ostream &operator<<(std::ostream &stream,
                                const DeliveryEventScheduleHandle &value) {
  return stream << "DeliveryEventScheduleHandle(" << value.value.get() << ')';
}

inline std::ostream &operator<<(std::ostream &stream,
                                const KafkaEventScheduleHandle &value) {
  return stream << "KafkaEventScheduleHandle(" << value.value.get() << ')';
}

} // namespace hgraph::kafka::detail

namespace std {
template <> struct hash<hgraph::kafka::detail::KafkaTransportBindingsHandle> {
  size_t operator()(const hgraph::kafka::detail::KafkaTransportBindingsHandle
                        &value) const noexcept {
    return hash<const void *>{}(value.value.get());
  }
};

template <>
struct hash<hgraph::kafka::detail::SubscriptionEventScheduleHandle> {
  size_t operator()(const hgraph::kafka::detail::SubscriptionEventScheduleHandle
                        &value) const noexcept {
    return hash<const void *>{}(value.value.get());
  }
};

template <> struct hash<hgraph::kafka::detail::DeliveryEventScheduleHandle> {
  size_t operator()(const hgraph::kafka::detail::DeliveryEventScheduleHandle
                        &value) const noexcept {
    return hash<const void *>{}(value.value.get());
  }
};

template <> struct hash<hgraph::kafka::detail::KafkaEventScheduleHandle> {
  size_t operator()(const hgraph::kafka::detail::KafkaEventScheduleHandle
                        &value) const noexcept {
    return hash<const void *>{}(value.value.get());
  }
};
} // namespace std

namespace hgraph::static_schema_detail {
template <> struct scalar_name<kafka::detail::KafkaTransportBindingsHandle> {
  static constexpr std::string_view value{
      "hgraph.kafka.internal::KafkaTransportBindingsHandle"};
};

template <> struct scalar_name<kafka::detail::SubscriptionEventScheduleHandle> {
  static constexpr std::string_view value{
      "hgraph.kafka.internal::SubscriptionEventScheduleHandle"};
};

template <> struct scalar_name<kafka::detail::DeliveryEventScheduleHandle> {
  static constexpr std::string_view value{
      "hgraph.kafka.internal::DeliveryEventScheduleHandle"};
};

template <> struct scalar_name<kafka::detail::KafkaEventScheduleHandle> {
  static constexpr std::string_view value{
      "hgraph.kafka.internal::KafkaEventScheduleHandle"};
};
} // namespace hgraph::static_schema_detail

namespace hgraph::kafka::detail {

template <typename T> [[nodiscard]] inline Value transport_atomic(T value) {
  static_cast<void>(scalar_descriptor<T>::value_meta());
  return Value{std::move(value)};
}

[[nodiscard]] inline Value
transport_event(const KafkaTransportBindings &bindings,
                KafkaTransportEventKind kind,
                std::vector<std::pair<std::string_view, Value>> fields) {
  BundleBuilder builder{bindings.event};
  builder.set("kind", transport_atomic(kind));
  for (auto &[name, field] : fields) {
    if (field.has_value()) {
      builder.set(name, std::move(field));
    }
  }
  return builder.build();
}

[[nodiscard]] inline Value subscription_transport_event(
    const KafkaTransportBindings &bindings, Value key,
    std::optional<Value> record, std::optional<Value> cursor,
    KafkaSubscriptionState state,
    std::optional<DateTime> evaluation_time = std::nullopt,
    Bool recovery = false) {
  std::vector<std::pair<std::string_view, Value>> fields;
  fields.emplace_back("subscription_key", std::move(key));
  if (record.has_value()) {
    fields.emplace_back("record", std::move(*record));
  }
  if (cursor.has_value()) {
    fields.emplace_back("cursor", std::move(*cursor));
  }
  fields.emplace_back("state", transport_atomic(state));
  if (recovery) {
    fields.emplace_back("recovery", transport_atomic(recovery));
  }
  if (evaluation_time.has_value()) {
    fields.emplace_back("evaluation_time", transport_atomic(*evaluation_time));
  }
  return transport_event(bindings, KafkaTransportEventKind::Subscription,
                         std::move(fields));
}

[[nodiscard]] inline Value
recovery_barrier_transport_event(const KafkaTransportBindings &bindings) {
  return transport_event(bindings, KafkaTransportEventKind::RecoveryBarrier,
                         {});
}

[[nodiscard]] inline Value
subscription_removed_transport_event(const KafkaTransportBindings &bindings,
                                     Value key) {
  return transport_event(bindings, KafkaTransportEventKind::Subscription,
                         {{"subscription_key", std::move(key)},
                          {"removed", transport_atomic(Bool{true})}});
}

[[nodiscard]] inline Value
delivery_transport_event(const KafkaTransportBindings &bindings, Int request_id,
                         Value report) {
  return transport_event(bindings, KafkaTransportEventKind::Delivery,
                         {{"request_id", transport_atomic(request_id)},
                          {"report", std::move(report)}});
}

[[nodiscard]] inline Value
service_transport_event(const KafkaTransportBindings &bindings, Value event,
                        Bool stop_graph = false) {
  return transport_event(bindings, KafkaTransportEventKind::Event,
                         {{"event", std::move(event)},
                          {"stop_graph", transport_atomic(stop_graph)}});
}

/** Runtime demultiplexing cannot be expressed as a wiring-time graph because
 * the envelope kind and recovery timestamp arrive in each burst. This compute
 * node retains the ordered subscription schedule and emits one event per
 * independent subscription key in a live cycle, while explicit same-time
 * recovery cohorts remain atomic. Insertion and live cohort selection are
 * O(n) in queued subscription events; projection is O(B) for emitted cohort
 * size B and retained memory is O(n). State is ephemeral ingress state and is
 * discarded at stop. */
struct SubscriptionProjectionNode {
  static constexpr auto name = "kafka_subscription_projection";

  static void start(Scalar<"bindings", KafkaTransportBindingsHandle> bindings,
                    State<SubscriptionEventScheduleHandle> state) {
    state.set(SubscriptionEventScheduleHandle{
        std::make_shared<SubscriptionEventSchedule>(*bindings.value().value)});
  }

  static void
  eval(In<"transport", TS<KafkaTransportEventBatch>> transport,
       Scalar<"bindings", KafkaTransportBindingsHandle>,
       State<SubscriptionEventScheduleHandle> state,
       SingleShotScheduler scheduler,
       Out<TSD<KafkaSubscriptionKey, KafkaSubscriptionOutput>> out) {
    auto schedule = state.get().value;
    if (transport.modified()) {
      for (const auto event : transport.base().value().as_list()) {
        const auto fields = event.as_bundle();
        const auto kind =
            fields.at("kind").checked_as<KafkaTransportEventKind>();
        if (kind == KafkaTransportEventKind::Subscription) {
          schedule->push(event.clone());
        } else if (kind == KafkaTransportEventKind::RecoveryBarrier) {
          schedule->release_recovery();
        }
      }
    }

    if (schedule->recovery_blocked()) {
      return;
    }
    if (schedule->empty()) {
      return;
    }

    const auto next_time = schedule->next_time();
    if (next_time.has_value() && *next_time > scheduler.now()) {
      scheduler.schedule(*next_time);
      return;
    }
    const auto schedule_following = [&] {
      const auto following_time = schedule->next_time();
      if (following_time.has_value() && *following_time > scheduler.now()) {
        scheduler.schedule(*following_time);
      } else if (!schedule->empty()) {
        scheduler.schedule(MIN_TD);
      }
    };

    auto mutation = out.begin_mutation(out.evaluation_time());
    const auto apply_event = [&](const Value &event) {
      const auto fields = event.view().as_bundle();
      const auto removed = fields.at("removed");
      if (removed.data() != nullptr && removed.checked_as<Bool>()) {
        static_cast<void>(mutation.erase(fields.at("subscription_key")));
        return;
      }

      BundleBuilder update{schedule->subscription_output_binding()};
      for (const auto name :
           {std::string_view{"record"}, std::string_view{"cursor"},
            std::string_view{"state"}}) {
        const auto field = fields.at(name);
        if (field.data() != nullptr) {
          update.set(name, field.clone());
        }
      }
      const Value value = update.build();
      mutation.set(fields.at("subscription_key"), value.view());
    };

    for (const auto &event : schedule->pop_ready_cohort()) {
      apply_event(event);
    }
    schedule_following();
  }
};

/** Runtime projection for delivery envelopes. Distinct request ids tick
 * together; same-id collisions retain FIFO order. Cost is O(B + P) per tick
 * for burst size B and P pending ids; retained memory is O(C) for collisions.
 * State is ephemeral ingress state and is discarded at stop. */
struct DeliveryProjectionNode {
  static constexpr auto name = "kafka_delivery_projection";

  static void start(State<DeliveryEventScheduleHandle> state) {
    state.set(
        DeliveryEventScheduleHandle{std::make_shared<DeliveryEventSchedule>()});
  }

  static void eval(In<"transport", TS<KafkaTransportEventBatch>> transport,
                   State<DeliveryEventScheduleHandle> state,
                   SingleShotScheduler scheduler,
                   Out<TSD<Int, TS<KafkaDeliveryReport>>> out) {
    auto schedule = state.get().value;
    auto mutation = out.begin_mutation(out.evaluation_time());
    std::unordered_set<Int> emitted;
    const auto apply_event = [&](const ValueView &event) {
      const auto fields = event.as_bundle();
      mutation.set(fields.at("request_id"), fields.at("report"));
    };
    schedule->drain(emitted, apply_event);
    if (transport.modified()) {
      for (const auto event : transport.base().value().as_list()) {
        const auto fields = event.as_bundle();
        if (fields.at("kind").checked_as<KafkaTransportEventKind>() !=
            KafkaTransportEventKind::Delivery) {
          continue;
        }
        const Int request_id = fields.at("request_id").checked_as<Int>();
        if (emitted.emplace(request_id).second) {
          apply_event(event);
        } else {
          schedule->defer(request_id, event);
        }
      }
    }
    if (!schedule->empty()) {
      scheduler.schedule(MIN_TD);
    }
  }
};

/** Runtime projection for scalar service-event output. A burst is scanned in
 * O(B), the first event is emitted, and remaining events are retained in FIFO
 * order using O(E) memory until their individual ticks. */
struct EventProjectionNode {
  static constexpr auto name = "kafka_event_projection";

  static void start(State<KafkaEventScheduleHandle> state) {
    state.set(KafkaEventScheduleHandle{std::make_shared<KafkaEventSchedule>()});
  }

  static void eval(In<"transport", TS<KafkaTransportEventBatch>> transport,
                   State<KafkaEventScheduleHandle> state,
                   SingleShotScheduler scheduler, EngineControlView engine,
                   Out<TS<KafkaEvent>> out) {
    auto schedule = state.get().value;
    bool emitted = false;
    const auto apply_event = [&](const ValueView &event) {
      const auto fields = event.as_bundle();
      out.apply(fields.at("event"));
      const auto stop_graph = fields.at("stop_graph");
      if (stop_graph.data() != nullptr && stop_graph.checked_as<Bool>()) {
        engine.request_stop();
      }
      emitted = true;
    };
    if (auto pending = schedule->pop()) {
      apply_event(pending->view());
    }
    if (transport.modified()) {
      for (const auto event : transport.base().value().as_list()) {
        const auto fields = event.as_bundle();
        if (fields.at("kind").checked_as<KafkaTransportEventKind>() !=
            KafkaTransportEventKind::Event) {
          continue;
        }
        if (emitted) {
          schedule->defer(event);
        } else {
          apply_event(event);
        }
      }
    }
    if (!schedule->empty()) {
      scheduler.schedule(MIN_TD);
    }
  }
};

/** Simulation projection applies a same-time batch as one keyed mutation so
 *  the graph observes an atomic replay cohort. It retains no replay history;
 *  cost is O(B) per tick for batch size B. */
struct SimulationSubscriptionProjectionNode {
  static constexpr auto name = "kafka_simulation_subscription_projection";

  static void start(Scalar<"bindings", KafkaTransportBindingsHandle> bindings,
                    State<SubscriptionEventScheduleHandle> state) {
    state.set(SubscriptionEventScheduleHandle{
        std::make_shared<SubscriptionEventSchedule>(*bindings.value().value)});
  }

  static void
  eval(In<"transport", TS<KafkaTransportEventBatch>> transport,
       Scalar<"bindings", KafkaTransportBindingsHandle>,
       State<SubscriptionEventScheduleHandle> state,
       Out<TSD<KafkaSubscriptionKey, KafkaSubscriptionOutput>> out) {
    const auto values = transport.base().value().as_list();
    const bool has_subscription =
        std::ranges::any_of(values, [](const ValueView &value) {
          return value.as_bundle()
                     .at("kind")
                     .checked_as<KafkaTransportEventKind>() ==
                 KafkaTransportEventKind::Subscription;
        });
    if (!has_subscription) {
      return;
    }
    auto mutation = out.begin_mutation(out.evaluation_time());
    for (const auto value : values) {
      const auto fields = value.as_bundle();
      if (fields.at("kind").checked_as<KafkaTransportEventKind>() !=
          KafkaTransportEventKind::Subscription) {
        continue;
      }
      const auto removed = fields.at("removed");
      if (removed.data() != nullptr && removed.checked_as<Bool>()) {
        static_cast<void>(mutation.erase(fields.at("subscription_key")));
        continue;
      }
      BundleBuilder update{state.get().value->subscription_output_binding()};
      for (const auto name :
           {std::string_view{"record"}, std::string_view{"cursor"},
            std::string_view{"state"}}) {
        const auto field = fields.at(name);
        if (field.data() != nullptr) {
          update.set(name, field.clone());
        }
      }
      const Value projected = update.build();
      mutation.set(fields.at("subscription_key"), projected.view());
    }
  }
};

/** Stateless simulation delivery projection. It scans one replay batch and
 *  emits its delivery reports in one keyed mutation: O(B) per batch. */
struct SimulationDeliveryProjectionNode {
  static constexpr auto name = "kafka_simulation_delivery_projection";

  static void eval(In<"transport", TS<KafkaTransportEventBatch>> transport,
                   Out<TSD<Int, TS<KafkaDeliveryReport>>> out) {
    const auto values = transport.base().value().as_list();
    const bool has_delivery =
        std::ranges::any_of(values, [](const ValueView &value) {
          return value.as_bundle()
                     .at("kind")
                     .checked_as<KafkaTransportEventKind>() ==
                 KafkaTransportEventKind::Delivery;
        });
    if (!has_delivery) {
      return;
    }
    auto mutation = out.begin_mutation(out.evaluation_time());
    for (const auto value : values) {
      const auto fields = value.as_bundle();
      if (fields.at("kind").checked_as<KafkaTransportEventKind>() ==
          KafkaTransportEventKind::Delivery) {
        mutation.set(fields.at("request_id"), fields.at("report"));
      }
    }
  }
};

/** Stateless simulation service-event projection. It scans at most one replay
 *  batch, emits the first event, and applies stop policy on the graph thread:
 *  O(B) worst-case per batch. */
struct SimulationEventProjectionNode {
  static constexpr auto name = "kafka_simulation_event_projection";

  static void eval(In<"transport", TS<KafkaTransportEventBatch>> transport,
                   EngineControlView engine, Out<TS<KafkaEvent>> out) {
    for (const auto value : transport.base().value().as_list()) {
      const auto fields = value.as_bundle();
      if (fields.at("kind").checked_as<KafkaTransportEventKind>() !=
          KafkaTransportEventKind::Event) {
        continue;
      }
      out.apply(fields.at("event"));
      const auto stop_graph = fields.at("stop_graph");
      if (stop_graph.data() != nullptr && stop_graph.checked_as<Bool>()) {
        engine.request_stop();
      }
      return;
    }
  }
};

struct ServiceOutputs {
  Port<TSD<KafkaSubscriptionKey, KafkaSubscriptionOutput>> subscriptions;
  Port<TSD<Int, TS<KafkaDeliveryReport>>> deliveries;
  Port<TS<KafkaEvent>> events;
};

[[nodiscard]] inline ServiceOutputs
wire_service_outputs(Wiring &w, Port<TS<KafkaTransportEventBatch>> transport,
                     KafkaTransportBindingsHandle bindings) {
  return ServiceOutputs{
      wire<SubscriptionProjectionNode>(w, transport, bindings)
          .template as<TSD<KafkaSubscriptionKey, KafkaSubscriptionOutput>>(),
      wire<DeliveryProjectionNode>(w, transport)
          .template as<TSD<Int, TS<KafkaDeliveryReport>>>(),
      wire<EventProjectionNode>(w, transport).template as<TS<KafkaEvent>>(),
  };
}

[[nodiscard]] inline ServiceOutputs
wire_simulation_service_outputs(Wiring &w,
                                Port<TS<KafkaTransportEventBatch>> transport,
                                KafkaTransportBindingsHandle bindings) {
  return ServiceOutputs{
      wire<SimulationSubscriptionProjectionNode>(w, transport, bindings)
          .template as<TSD<KafkaSubscriptionKey, KafkaSubscriptionOutput>>(),
      wire<SimulationDeliveryProjectionNode>(w, transport)
          .template as<TSD<Int, TS<KafkaDeliveryReport>>>(),
      wire<SimulationEventProjectionNode>(w, transport)
          .template as<TS<KafkaEvent>>(),
  };
}

template <typename Tag>
[[nodiscard]] Port<TS<KafkaTransportEventBatch>>
wire_transport_source(Wiring &w, PushSourceStartViewCallback on_start,
                      std::function<void(const NodeView &)> on_stop) {
  const auto *schema = ts_type<TS<KafkaTransportEventBatch>>();
  PushSourceNodeExtension extension{
      .on_start = std::move(on_start),
      .on_stop = std::move(on_stop),
  };
  return Port<TS<KafkaTransportEventBatch>>{
      w, w.add_unique_node(std::type_index(typeid(Tag)),
                           make_push_source_node_with_view(
                               *schema, make_push_source_burst_policy(*schema),
                               std::move(extension)),
                           std::span<const WiringPortRef>{}, Value{})};
}

} // namespace hgraph::kafka::detail

#endif // HGRAPH_KAFKA_DETAIL_SERVICE_TRANSPORT_H
