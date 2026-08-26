#ifndef HGRAPH_KAFKA_DETAIL_SERVICE_TRANSPORT_H
#define HGRAPH_KAFKA_DETAIL_SERVICE_TRANSPORT_H

#include <hgraph/kafka/service.h>

#include <hgraph/lib/std/operators/container.h>
#include <hgraph/lib/std/operators/higher_order.h>
#include <hgraph/runtime/push_source_node.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/types/value/value_hash.h>

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

struct KafkaTransportEmitState {
  ValueTypeRef event_binding{};
  std::deque<Value> pending{};
  // Per-evaluation output-occupancy scratch. It is cleared before dequeueing
  // and never retains transport work; keeping its allocation avoids rebuilding
  // the hash table on every engine cycle until outputs expose this directly.
  std::unordered_set<Value, ValueHash, ValueEqual> occupied_keys{};
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

/** Splits one ordered Kafka burst into graph-visible service lanes. Runtime
 * envelope kinds cannot be selected at wiring time, so this is the one
 * Kafka-specific emit primitive. It walks one pending queue in order and
 * delegates events until the next event would write a second value to an
 * already occupied output in the same engine cycle. That event remains at the
 * queue front and resumes on the next cycle. Timestamped recovery events
 * release when due and remain blocked until their recovery barrier arrives.
 *
 * Normal live append and draining are O(B + D), where B is the incoming burst
 * and D is the prefix delegated this cycle. Sorted timestamp insertion and a
 * live subscription's recovery-tail lookup are O(Bt * Q) worst-case for Bt
 * timestamp-related events and Q retained events. Retained memory is O(Q).
 * State is ephemeral ingress sequencing state and is discarded at stop. */
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
        const auto kind =
            fields.at("kind").checked_as<KafkaTransportEventKind>();
        if (kind == KafkaTransportEventKind::RecoveryBarrier) {
          current.recovery_blocked = false;
          continue;
        }

        Value event = value.clone();
        auto event_time = evaluation_time(value);
        if (kind == KafkaTransportEventKind::Subscription) {
          const auto recovery = fields.at("recovery");
          if (recovery.data() != nullptr && recovery.checked_as<Bool>()) {
            current.recovery_blocked = true;
          }
          // Untimed live values normally append in O(1). If timestamped work
          // remains, a live value for the same subscription follows its
          // recovery tail on the next engine time.
          const bool has_timestamped_tail =
              !current.pending.empty() &&
              evaluation_time(current.pending.back().view()).has_value();
          if (!event_time.has_value() && has_timestamped_tail) {
            const auto key = fields.at("subscription_key");
            const auto predecessor = std::find_if(
                current.pending.rbegin(), current.pending.rend(),
                [&](const Value &candidate) {
                  const auto candidate_fields = candidate.view().as_bundle();
                  return candidate_fields.at("kind")
                                 .checked_as<KafkaTransportEventKind>() ==
                             KafkaTransportEventKind::Subscription &&
                         candidate_fields.at("subscription_key").equals(key);
                });
            if (predecessor != current.pending.rend()) {
              const auto predecessor_time =
                  evaluation_time(predecessor->view());
              if (predecessor_time.has_value()) {
                event = with_evaluation_time(event.view(),
                                             *predecessor_time + MIN_TD);
                event_time = *predecessor_time + MIN_TD;
              }
            }
          }
        }

        const bool has_timestamped_tail =
            !current.pending.empty() &&
            evaluation_time(current.pending.back().view()).has_value();
        if (!event_time.has_value() && !has_timestamped_tail) {
          current.pending.push_back(std::move(event));
          continue;
        }
        const auto position = std::upper_bound(
            current.pending.begin(), current.pending.end(), event,
            [&](const Value &lhs, const Value &rhs) {
              const auto lhs_time = evaluation_time(lhs.view());
              const auto rhs_time = evaluation_time(rhs.view());
              if (!lhs_time.has_value()) {
                return rhs_time.has_value();
              }
              return rhs_time.has_value() && *lhs_time < *rhs_time;
            });
        current.pending.insert(position, std::move(event));
      }
    }

    auto subscriptions = out.template field<"subscriptions">();
    std::optional<TSDDataMutationView> subscription_mutation;
    auto deliveries = out.template field<"deliveries">();
    std::optional<TSDDataMutationView> delivery_mutation;
    auto events = out.template field<"events">();
    auto &occupied = current.occupied_keys;
    occupied.clear();
    bool event_emitted{};

    while (!current.pending.empty()) {
      const auto event = current.pending.front().view();
      const auto fields = event.as_bundle();
      const auto kind = fields.at("kind").checked_as<KafkaTransportEventKind>();

      if (kind == KafkaTransportEventKind::Subscription) {
        const auto recovery = fields.at("recovery");
        if (current.recovery_blocked && recovery.data() != nullptr &&
            recovery.checked_as<Bool>()) {
          break;
        }
        const auto event_time = evaluation_time(event);
        if (event_time.has_value() && *event_time > scheduler.now()) {
          scheduler.schedule(*event_time);
          break;
        }
        const auto key = fields.at("subscription_key");
        if (!occupied.emplace(key).second) {
          scheduler.schedule(MIN_TD);
          break;
        }
        const auto removed = fields.at("removed");
        if (!subscription_mutation.has_value()) {
          subscription_mutation.emplace(
              subscriptions.begin_mutation(subscriptions.evaluation_time()));
        }
        if (removed.data() != nullptr && removed.checked_as<Bool>()) {
          static_cast<void>(subscription_mutation->erase(key));
        } else {
          subscription_mutation->set(key, event);
        }
      } else if (kind == KafkaTransportEventKind::Delivery) {
        const auto key = fields.at("request_id");
        if (!occupied.emplace(key).second) {
          scheduler.schedule(MIN_TD);
          break;
        }
        if (!delivery_mutation.has_value()) {
          delivery_mutation.emplace(
              deliveries.begin_mutation(deliveries.evaluation_time()));
        }
        delivery_mutation->set(key, event);
      } else if (kind == KafkaTransportEventKind::Event) {
        if (event_emitted) {
          scheduler.schedule(MIN_TD);
          break;
        }
        events.apply(event);
        event_emitted = true;
      }
      current.pending.pop_front();
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
