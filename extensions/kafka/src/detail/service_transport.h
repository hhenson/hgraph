#ifndef HGRAPH_KAFKA_DETAIL_SERVICE_TRANSPORT_H
#define HGRAPH_KAFKA_DETAIL_SERVICE_TRANSPORT_H

#include <hgraph/kafka/service.h>

#include <hgraph/lib/std/operators/container.h>
#include <hgraph/lib/std/operators/higher_order.h>
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
#include <span>
#include <string_view>
#include <typeindex>
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
using KafkaTransportStreams = TSB<
    "hgraph.kafka.internal::KafkaTransportStreams",
    Field<"subscriptions", TSD<KafkaSubscriptionKey, TS<KafkaTransportEvent>>>,
    Field<"deliveries", TSD<Int, TS<KafkaTransportEvent>>>,
    Field<"events", TS<KafkaTransportEvent>>>;

struct KafkaTransportBindings {
  ValueTypeRef event{};
  ValueTypeRef batch{};
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
      })};
}

struct OwnedValueHash {
  using is_transparent = void;

  [[nodiscard]] std::size_t operator()(const Value &value) const {
    return value.hash();
  }

  [[nodiscard]] std::size_t operator()(const ValueView &value) const {
    return value.hash();
  }
};

struct OwnedValueEqual {
  using is_transparent = void;

  [[nodiscard]] bool operator()(const Value &lhs, const Value &rhs) const {
    return lhs.equals(rhs.view());
  }

  [[nodiscard]] bool operator()(const Value &lhs, const ValueView &rhs) const {
    return lhs.equals(rhs);
  }

  [[nodiscard]] bool operator()(const ValueView &lhs, const Value &rhs) const {
    return rhs.equals(lhs);
  }
};

struct KafkaTransportEmitState {
  ValueTypeRef event_binding{};
  std::deque<Value> subscriptions{};
  std::deque<Value> deliveries{};
  std::deque<Value> events{};
  std::unordered_set<Value, OwnedValueHash, OwnedValueEqual> emitted_keys{};
  bool recovery_blocked{};
};

} // namespace hgraph::kafka::detail

namespace std {
template <> struct hash<hgraph::kafka::detail::KafkaTransportBindingsHandle> {
  size_t operator()(const hgraph::kafka::detail::KafkaTransportBindingsHandle
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

template <> struct scalar_name<kafka::detail::KafkaTransportEmitState> {
  static constexpr std::string_view value{
      "hgraph.kafka.internal::KafkaTransportEmitState"};
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

/** Splits one Kafka burst into graph-visible service lanes. Runtime envelope
 * kinds cannot be selected at wiring time, so this is the one Kafka-specific
 * emit primitive. Independent subscription keys and delivery request ids are
 * emitted together as keyed deltas; a repeated key is retained for the next
 * engine cycle so its event order remains observable. Scalar service events
 * remain FIFO and release one per cycle. Timestamped recovery events release
 * when due and remain blocked until their recovery barrier arrives.
 *
 * Normal live classification and draining are O(B + Q), where B is the
 * incoming burst and Q is retained work examined for the current lanes.
 * Sorted timestamp recovery insertion is O(Bs * Qs) worst-case for Bs new and
 * Qs retained subscription events. Retained memory is O(Q). State is
 * ephemeral ingress sequencing state and is discarded at stop. */
struct KafkaTransportEmitNode {
  static constexpr auto name = "kafka_transport_emit";

  static void start(Scalar<"bindings", KafkaTransportBindingsHandle> bindings,
                    State<KafkaTransportEmitState> state) {
    state.modify().event_binding = bindings.value().value->event;
  }

  static void
  eval(In<"transport", TS<KafkaTransportEventBatch>, InputValidity::Unchecked>
           transport,
       Scalar<"bindings", KafkaTransportBindingsHandle>,
       State<KafkaTransportEmitState> state, SingleShotScheduler scheduler,
       Out<KafkaTransportStreams> out) {
    auto &current = state.modify();
    const auto evaluation_time =
        [](const ValueView &value) -> std::optional<DateTime> {
      const auto field = value.as_bundle().at("evaluation_time");
      return field.data() == nullptr
                 ? std::nullopt
                 : std::optional<DateTime>{field.checked_as<DateTime>()};
    };
    const auto with_evaluation_time = [&](const ValueView &value,
                                          DateTime time) {
      BundleBuilder builder{current.event_binding};
      const auto fields = value.as_bundle();
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
    };

    if (transport.modified() && transport.valid()) {
      const auto incoming = transport.base().value().as_list();
      for (std::size_t index = 0; index != incoming.size(); ++index) {
        const auto value = incoming.at(index);
        const auto fields = value.as_bundle();
        switch (fields.at("kind").checked_as<KafkaTransportEventKind>()) {
        case KafkaTransportEventKind::Subscription: {
          Value event = value.clone();
          auto event_time = evaluation_time(value);
          const auto recovery = fields.at("recovery");
          if (recovery.data() != nullptr && recovery.checked_as<Bool>()) {
            current.recovery_blocked = true;
          }
          // The ordered queue stores all live values before timestamped
          // recovery. Avoid a predecessor scan on the normal live hot path
          // when its tail proves that no timestamped value is retained.
          const bool has_timestamped_tail =
              !current.subscriptions.empty() &&
              evaluation_time(current.subscriptions.back().view()).has_value();
          if (!event_time.has_value() && has_timestamped_tail) {
            const auto key = fields.at("subscription_key");
            const auto predecessor = std::find_if(
                current.subscriptions.rbegin(), current.subscriptions.rend(),
                [&](const Value &candidate) {
                  return candidate.view()
                      .as_bundle()
                      .at("subscription_key")
                      .equals(key);
                });
            if (predecessor != current.subscriptions.rend()) {
              const auto predecessor_time =
                  evaluation_time(predecessor->view());
              if (predecessor_time.has_value()) {
                event = with_evaluation_time(event.view(),
                                             *predecessor_time + MIN_TD);
                event_time = *predecessor_time + MIN_TD;
              }
            }
          }
          if (!event_time.has_value() && !has_timestamped_tail) {
            current.subscriptions.push_back(std::move(event));
            break;
          }
          const auto position = std::upper_bound(
              current.subscriptions.begin(), current.subscriptions.end(), event,
              [&](const Value &lhs, const Value &rhs) {
                const auto lhs_time = evaluation_time(lhs.view());
                const auto rhs_time = evaluation_time(rhs.view());
                if (!lhs_time.has_value()) {
                  return rhs_time.has_value();
                }
                return rhs_time.has_value() && *lhs_time < *rhs_time;
              });
          current.subscriptions.insert(position, std::move(event));
          break;
        }
        case KafkaTransportEventKind::Delivery:
          current.deliveries.emplace_back(value);
          break;
        case KafkaTransportEventKind::Event:
          current.events.emplace_back(value);
          break;
        case KafkaTransportEventKind::RecoveryBarrier:
          current.recovery_blocked = false;
          break;
        }
      }
    }

    const auto subscription_is_blocked = [&] {
      if (!current.recovery_blocked || current.subscriptions.empty()) {
        return false;
      }
      const auto recovery =
          current.subscriptions.front().view().as_bundle().at("recovery");
      return recovery.data() != nullptr && recovery.checked_as<Bool>();
    };

    if (!current.subscriptions.empty() && !subscription_is_blocked()) {
      const auto cohort_time =
          evaluation_time(current.subscriptions.front().view());
      if (!cohort_time.has_value() || *cohort_time <= scheduler.now()) {
        auto subscriptions = out.template field<"subscriptions">();
        auto mutation =
            subscriptions.begin_mutation(subscriptions.evaluation_time());
        auto &emitted = current.emitted_keys;
        emitted.clear();
        std::deque<Value> remaining;
        while (!current.subscriptions.empty()) {
          Value event = std::move(current.subscriptions.front());
          current.subscriptions.pop_front();
          const auto event_time = evaluation_time(event.view());
          const bool outside_cohort = cohort_time.has_value()
                                          ? event_time != cohort_time
                                          : event_time.has_value();
          if (outside_cohort) {
            remaining.push_back(std::move(event));
            while (!current.subscriptions.empty()) {
              remaining.push_back(std::move(current.subscriptions.front()));
              current.subscriptions.pop_front();
            }
            break;
          }
          const auto fields = event.view().as_bundle();
          const auto key = fields.at("subscription_key");
          if (!emitted.emplace(key).second) {
            remaining.push_back(std::move(event));
            continue;
          }
          const auto removed = fields.at("removed");
          if (removed.data() != nullptr && removed.checked_as<Bool>()) {
            static_cast<void>(mutation.erase(key));
          } else {
            mutation.set(key, event.view());
          }
        }
        current.subscriptions.swap(remaining);
      }
    }

    if (!current.deliveries.empty()) {
      auto deliveries = out.template field<"deliveries">();
      auto mutation = deliveries.begin_mutation(deliveries.evaluation_time());
      auto &emitted = current.emitted_keys;
      emitted.clear();
      std::deque<Value> remaining;
      while (!current.deliveries.empty()) {
        Value event = std::move(current.deliveries.front());
        current.deliveries.pop_front();
        const auto fields = event.view().as_bundle();
        const auto key = fields.at("request_id");
        if (!emitted.emplace(key).second) {
          remaining.push_back(std::move(event));
          continue;
        }
        mutation.set(key, event.view());
      }
      current.deliveries.swap(remaining);
    }

    if (!current.events.empty()) {
      Value event = std::move(current.events.front());
      current.events.pop_front();
      out.template field<"events">().apply(event.view());
    }

    bool schedule_immediately =
        !current.deliveries.empty() || !current.events.empty();
    std::optional<DateTime> subscription_time;
    if (!current.subscriptions.empty() && !subscription_is_blocked()) {
      subscription_time = evaluation_time(current.subscriptions.front().view());
      schedule_immediately = schedule_immediately ||
                             !subscription_time.has_value() ||
                             *subscription_time <= scheduler.now();
    }
    if (schedule_immediately) {
      scheduler.schedule(MIN_TD);
    } else if (subscription_time.has_value()) {
      scheduler.schedule(*subscription_time);
    }
  }
};

/** Projects one subscription event. The custom emit and map_ own sequencing
 * and keyed lifetime; this node retains no private state and costs O(1). */
struct SubscriptionProjectionNode {
  static constexpr auto name = "kafka_subscription_projection";

  static void eval(In<"event", TS<KafkaTransportEvent>> event,
                   Out<KafkaSubscriptionOutput> out) {
    const auto fields = event.base().value().as_bundle();
    const auto record = fields.at("record");
    if (record.data() != nullptr) {
      out.template field<"record">().apply(record);
    }
    const auto cursor = fields.at("cursor");
    if (cursor.data() != nullptr) {
      out.template field<"cursor">().apply(cursor);
    }
    const auto state = fields.at("state");
    if (state.data() != nullptr) {
      out.template field<"state">().apply(state);
    }
  }
};

struct SubscriptionProjectionGraph {
  static constexpr auto name = "kafka_subscription_projection_graph";

  static Port<KafkaSubscriptionOutput>
  compose(Wiring &w, NamedPort<"key", TS<KafkaSubscriptionKey>>,
          Port<TS<KafkaTransportEvent>> event) {
    return wire<SubscriptionProjectionNode>(w, event)
        .template as<KafkaSubscriptionOutput>();
  }
};

/** Projects one delivery report; the custom emit and map_ own sequencing. */
struct DeliveryProjectionNode {
  static constexpr auto name = "kafka_delivery_projection";

  static void eval(In<"event", TS<KafkaTransportEvent>> event,
                   Out<TS<KafkaDeliveryReport>> out) {
    out.apply(event.base().value().as_bundle().at("report"));
  }
};

struct DeliveryProjectionGraph {
  static constexpr auto name = "kafka_delivery_projection_graph";

  static Port<TS<KafkaDeliveryReport>>
  compose(Wiring &w, NamedPort<"key", TS<Int>>,
          Port<TS<KafkaTransportEvent>> event) {
    return wire<DeliveryProjectionNode>(w, event)
        .template as<TS<KafkaDeliveryReport>>();
  }
};

/** Projects one scalar service event and applies stop policy on graph. */
struct EventProjectionNode {
  static constexpr auto name = "kafka_event_projection";

  static void eval(In<"event", TS<KafkaTransportEvent>> event,
                   EngineControlView engine, Out<TS<KafkaEvent>> out) {
    const auto fields = event.base().value().as_bundle();
    out.apply(fields.at("event"));
    const auto stop_graph = fields.at("stop_graph");
    if (stop_graph.data() != nullptr && stop_graph.checked_as<Bool>()) {
      engine.request_stop();
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
  auto streams = wire<KafkaTransportEmitNode>(w, transport, bindings)
                     .template as<KafkaTransportStreams>();
  auto subscriptions = wire<stdlib::getattr_,
                            TSD<KafkaSubscriptionKey, TS<KafkaTransportEvent>>>(
      w, streams, Str{"subscriptions"});
  auto deliveries = wire<stdlib::getattr_, TSD<Int, TS<KafkaTransportEvent>>>(
      w, streams, Str{"deliveries"});
  auto events = wire<stdlib::getattr_, TS<KafkaTransportEvent>>(w, streams,
                                                                Str{"events"});
  return ServiceOutputs{
      wire<stdlib::map_, TSD<KafkaSubscriptionKey, KafkaSubscriptionOutput>>(
          w, fn<SubscriptionProjectionGraph>(), subscriptions)
          .template as<TSD<KafkaSubscriptionKey, KafkaSubscriptionOutput>>(),
      wire<stdlib::map_, TSD<Int, TS<KafkaDeliveryReport>>>(
          w, fn<DeliveryProjectionGraph>(), deliveries)
          .template as<TSD<Int, TS<KafkaDeliveryReport>>>(),
      wire<EventProjectionNode>(w, events).template as<TS<KafkaEvent>>(),
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
