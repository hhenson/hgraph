#ifndef HGRAPH_WEB_DETAIL_SERVICE_TRANSPORT_H
#define HGRAPH_WEB_DETAIL_SERVICE_TRANSPORT_H

#include <hgraph/web/service.h>

#include "stream_model.h"

#include <hgraph/lib/std/operators/container.h>
#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/operators/higher_order.h>
#include <hgraph/runtime/executor.h>
#include <hgraph/runtime/push_source_node.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <array>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <ostream>
#include <span>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <typeindex>
#include <unordered_map>
#include <utility>
#include <vector>

namespace hgraph::web::detail {

enum class ServerChannel : std::size_t {
  Request,
  WsIngress,
  RespondDelivery,
  WsSendDelivery,
  Event,
  Stats,
  Count,
};

enum class ClientChannel : std::size_t {
  Response,
  WsIngress,
  SendDelivery,
  Event,
  Stats,
  Count,
};

[[nodiscard]] constexpr std::size_t index(ServerChannel channel) noexcept {
  return static_cast<std::size_t>(channel);
}

[[nodiscard]] constexpr std::size_t index(ClientChannel channel) noexcept {
  return static_cast<std::size_t>(channel);
}

struct OutputLimits {
  std::size_t records{};
  std::size_t bytes{};
};

/** One active subscription key and the graph evaluation that created it.
 * External tasks copy this generation into routed events so a graph can
 * reject work queued for an earlier remove/re-add lifetime. */
struct SubscriptionBinding {
  Value key{};
  DateTime generation{MIN_ST};
};

inline constexpr std::size_t kControlLaneBytes = 1024 * 1024;

struct WatermarkConfig {
  OutputLimits high{};
  OutputLimits low{};
  std::function<void(bool paused)> callback{};
};

/**
 * Domain byte and reservation accounting for bounded burst push sources.
 *
 * This object owns no Values and is not a second transport queue. Each core
 * push source owns its channel's cross-thread record storage, queue admission,
 * and wake-up. The web runtime uses this graph-scoped budget only to reserve
 * payload bytes before socket reads, retain per-channel watermarks, and
 * guarantee control headroom until the admitted burst enters graph processing.
 */
template <std::size_t ChannelCount> class AdmissionBudget {
public:
  static constexpr std::size_t channel_count = ChannelCount;

  explicit AdmissionBudget(
      std::array<OutputLimits, ChannelCount> limits,
      std::array<OutputLimits, ChannelCount> control_limits = {}) {
    for (std::size_t channel = 0; channel != ChannelCount; ++channel) {
      if (limits[channel].records == 0 || limits[channel].bytes == 0) {
        throw std::invalid_argument("Web transport limits must be positive");
      }
      channels_[channel].limits = limits[channel];
      channels_[channel].control_limits = control_limits[channel];
      if (control_limits[channel].records >
          std::numeric_limits<std::size_t>::max() - limits[channel].records) {
        throw std::overflow_error(
            "Web transport channel record capacity overflowed");
      }
      max_pending_[channel] =
          limits[channel].records + control_limits[channel].records;
    }
  }

  void start() {
    std::lock_guard lock{mutex_};
    if (accepting_) {
      throw std::logic_error("Web transport budget started twice");
    }
    accepting_ = true;
  }

  void stop() noexcept {
    std::lock_guard lock{mutex_};
    accepting_ = false;
    for (auto &channel : channels_) {
      channel.payload_records = 0;
      channel.payload_bytes = 0;
      channel.control_records = 0;
      channel.control_bytes = 0;
      channel.reserved_records = 0;
      channel.reserved_bytes = 0;
      channel.watermark = WatermarkConfig{};
      channel.paused = false;
      channel.delivered_paused = false;
    }
  }

  [[nodiscard]] std::size_t max_pending(std::size_t channel) const {
    return max_pending_.at(channel);
  }

  void set_watermark(std::size_t channel, WatermarkConfig config) {
    std::lock_guard lock{mutex_};
    auto &state = at(channel);
    state.watermark = std::move(config);
    state.paused = false;
    state.delivered_paused = false;
  }

  [[nodiscard]] bool reserve(std::size_t channel, std::size_t bytes) {
    std::lock_guard lock{mutex_};
    auto &state = at(channel);
    if (!accepting_ ||
        state.payload_records + state.reserved_records >=
            state.limits.records ||
        bytes > state.limits.bytes -
                    std::min(state.payload_bytes + state.reserved_bytes,
                             state.limits.bytes)) {
      return false;
    }
    ++state.reserved_records;
    state.reserved_bytes += bytes;
    return true;
  }

  [[nodiscard]] bool grow_reservation(std::size_t channel,
                                      std::size_t additional_bytes) {
    std::lock_guard lock{mutex_};
    auto &state = at(channel);
    if (!accepting_ || state.reserved_records == 0 ||
        additional_bytes >
            state.limits.bytes -
                std::min(state.payload_bytes + state.reserved_bytes,
                         state.limits.bytes)) {
      return false;
    }
    state.reserved_bytes += additional_bytes;
    return true;
  }

  void release_reservation(std::size_t channel,
                           std::size_t reserved_bytes) noexcept {
    std::lock_guard lock{mutex_};
    auto &state = at(channel);
    if (state.reserved_records == 0 || state.reserved_bytes < reserved_bytes) {
      return;
    }
    --state.reserved_records;
    state.reserved_bytes -= reserved_bytes;
  }

  [[nodiscard]] bool admit(std::size_t channel, std::size_t retained_bytes,
                           bool control) {
    bool notify = false;
    {
      std::lock_guard lock{mutex_};
      auto &state = at(channel);
      const auto limits = control ? state.control_limits : state.limits;
      auto &records = control ? state.control_records : state.payload_records;
      auto &bytes = control ? state.control_bytes : state.payload_bytes;
      if (!accepting_ || limits.records == 0 || limits.bytes == 0 ||
          records >= limits.records ||
          retained_bytes > limits.bytes - std::min(bytes, limits.bytes)) {
        return false;
      }
      ++records;
      bytes += retained_bytes;
      notify = update_pause_locked(state);
    }
    if (notify) {
      deliver_watermark(channel);
    }
    return true;
  }

  [[nodiscard]] bool admit_reserved(std::size_t channel,
                                    std::size_t retained_bytes,
                                    std::size_t reserved_bytes) {
    bool notify = false;
    {
      std::lock_guard lock{mutex_};
      auto &state = at(channel);
      if (!accepting_) {
        return false;
      }
      if (state.reserved_records == 0 ||
          state.reserved_bytes < reserved_bytes) {
        throw std::logic_error("Web transport reservation is not live");
      }
      if (retained_bytes > reserved_bytes) {
        throw std::logic_error("Web transport output exceeded its reservation");
      }
      --state.reserved_records;
      state.reserved_bytes -= reserved_bytes;
      ++state.payload_records;
      state.payload_bytes += retained_bytes;
      notify = update_pause_locked(state);
    }
    if (notify) {
      deliver_watermark(channel);
    }
    return true;
  }

  void release(std::size_t channel, std::size_t retained_bytes,
               bool control) noexcept {
    bool notify = false;
    {
      std::lock_guard lock{mutex_};
      auto &state = at(channel);
      auto &records = control ? state.control_records : state.payload_records;
      auto &bytes = control ? state.control_bytes : state.payload_bytes;
      if (records == 0) {
        return;
      }
      --records;
      bytes -= std::min(bytes, retained_bytes);
      if (!control && state.paused && state.watermark.callback &&
          state.payload_records <= state.watermark.low.records &&
          state.payload_bytes <= state.watermark.low.bytes) {
        state.paused = false;
        notify = true;
      }
    }
    if (notify) {
      deliver_watermark(channel);
    }
  }

  [[nodiscard]] std::size_t payload_pending(std::size_t channel) const {
    std::lock_guard lock{mutex_};
    return at(channel).payload_records;
  }

  [[nodiscard]] std::size_t payload_retained_bytes(std::size_t channel) const {
    std::lock_guard lock{mutex_};
    return at(channel).payload_bytes;
  }

private:
  struct Channel {
    OutputLimits limits{};
    OutputLimits control_limits{};
    std::size_t payload_records{};
    std::size_t payload_bytes{};
    std::size_t control_records{};
    std::size_t control_bytes{};
    std::size_t reserved_records{};
    std::size_t reserved_bytes{};
    WatermarkConfig watermark{};
    bool paused{};
    bool delivered_paused{};
  };

  [[nodiscard]] static bool update_pause_locked(Channel &state) {
    if (!state.paused && state.watermark.callback &&
        (state.payload_records >= state.watermark.high.records ||
         state.payload_bytes >= state.watermark.high.bytes)) {
      state.paused = true;
      return true;
    }
    return false;
  }

  void deliver_watermark(std::size_t channel) {
    std::lock_guard delivery{watermark_delivery_mutex_};
    std::function<void(bool)> callback;
    bool desired{};
    {
      std::lock_guard lock{mutex_};
      auto &state = at(channel);
      if (!state.watermark.callback || state.delivered_paused == state.paused) {
        return;
      }
      desired = state.paused;
      state.delivered_paused = desired;
      callback = state.watermark.callback;
    }
    callback(desired);
  }

  [[nodiscard]] Channel &at(std::size_t channel) {
    return channels_.at(channel);
  }

  [[nodiscard]] const Channel &at(std::size_t channel) const {
    return channels_.at(channel);
  }

  mutable std::mutex mutex_{};
  std::mutex watermark_delivery_mutex_{};
  std::array<Channel, ChannelCount> channels_{};
  std::array<std::size_t, ChannelCount> max_pending_{};
  bool accepting_{};
};

using ServerAdmission = AdmissionBudget<index(ServerChannel::Count)>;
using ClientAdmission = AdmissionBudget<index(ClientChannel::Count)>;

template <typename Budget> struct AdmissionHandle {
  std::shared_ptr<Budget> value{};

  friend bool operator==(const AdmissionHandle &,
                         const AdmissionHandle &) noexcept = default;
  friend std::strong_ordering operator<=>(const AdmissionHandle &lhs,
                                          const AdmissionHandle &rhs) noexcept {
    return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
           reinterpret_cast<std::uintptr_t>(rhs.value.get());
  }
};

using ServerAdmissionHandle = AdmissionHandle<ServerAdmission>;
using ClientAdmissionHandle = AdmissionHandle<ClientAdmission>;

inline std::ostream &operator<<(std::ostream &stream,
                                const ServerAdmissionHandle &value) {
  return stream << "ServerAdmissionHandle(" << value.value.get() << ')';
}

inline std::ostream &operator<<(std::ostream &stream,
                                const ClientAdmissionHandle &value) {
  return stream << "ClientAdmissionHandle(" << value.value.get() << ')';
}

[[nodiscard]] inline OutputLimits limits_from(const ValueView &config,
                                              std::string_view records,
                                              std::string_view bytes) {
  const auto fields = config.as_bundle();
  return OutputLimits{
      static_cast<std::size_t>(fields.at(records).checked_as<Int>()),
      static_cast<std::size_t>(fields.at(bytes).checked_as<Int>()),
  };
}

[[nodiscard]] inline ServerAdmissionHandle
make_server_admission(const Value &config) {
  const auto view = config.view();
  const auto ingress =
      limits_from(view, "ingress_record_limit", "ingress_byte_limit");
  const auto ws_ingress =
      limits_from(view, "ws_ingress_record_limit", "ws_ingress_byte_limit");
  const auto outbound =
      limits_from(view, "outbound_message_limit", "outbound_byte_limit");
  const OutputLimits control{1024, kControlLaneBytes};
  return ServerAdmissionHandle{std::make_shared<ServerAdmission>(
      std::array<OutputLimits, index(ServerChannel::Count)>{
          ingress, ws_ingress, outbound, outbound,
          OutputLimits{1024, 1024 * 1024}, OutputLimits{1, 1024 * 1024}},
      std::array<OutputLimits, index(ServerChannel::Count)>{
          control, control, control, control, OutputLimits{1, 64 * 1024},
          OutputLimits{}})};
}

[[nodiscard]] inline ClientAdmissionHandle
make_client_admission(const Value &config) {
  const auto view = config.view();
  const auto ingress =
      limits_from(view, "ingress_record_limit", "ingress_byte_limit");
  const auto ws_ingress =
      limits_from(view, "ws_ingress_record_limit", "ws_ingress_byte_limit");
  const auto outbound =
      limits_from(view, "outbound_record_limit", "outbound_byte_limit");
  const OutputLimits control{1024, kControlLaneBytes};
  return ClientAdmissionHandle{std::make_shared<ClientAdmission>(
      std::array<OutputLimits, index(ClientChannel::Count)>{
          ingress, ws_ingress, outbound, OutputLimits{1024, 1024 * 1024},
          OutputLimits{1, 1024 * 1024}},
      std::array<OutputLimits, index(ClientChannel::Count)>{
          control, control, control, OutputLimits{1, 64 * 1024},
          OutputLimits{}})};
}

struct WebTransportBindings {
  ValueTypeRef event{};
  ValueTypeRef event_kind{};
  ValueTypeRef integer{};
  ValueTypeRef boolean{};
};

struct WebTransportBindingsHandle {
  std::shared_ptr<const WebTransportBindings> value{};

  friend bool operator==(const WebTransportBindingsHandle &,
                         const WebTransportBindingsHandle &) noexcept = default;
  friend std::strong_ordering
  operator<=>(const WebTransportBindingsHandle &lhs,
              const WebTransportBindingsHandle &rhs) noexcept {
    return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
           reinterpret_cast<std::uintptr_t>(rhs.value.get());
  }
};

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

/** Wiring-fixed value bindings used by the one graph-side grouping primitive.
 * The node performs only O(B) transient classification. Standard collect,
 * map_, and emit nodes own all state used to unroll same-key collisions. */
struct WebKeyedBatchBindings {
  ValueTypeRef key{};
  ValueTypeRef event{};
  ValueTypeRef event_batch{};
  ValueTypeRef sequenced_batch{};
  ValueTypeRef batch_map{};

  friend bool operator==(const WebKeyedBatchBindings &,
                         const WebKeyedBatchBindings &) noexcept = default;
};

inline std::ostream &operator<<(std::ostream &stream,
                                const WebTransportBindingsHandle &value) {
  return stream << "WebTransportBindingsHandle(" << value.value.get() << ')';
}

inline std::ostream &operator<<(std::ostream &stream,
                                const WebKeyedBatchBindings &value) {
  return stream << "WebKeyedBatchBindings(" << value.batch_map.schema() << ')';
}

[[nodiscard]] inline WebTransportBindingsHandle make_transport_bindings() {
  auto &factory = ValuePlanFactory::instance();
  return WebTransportBindingsHandle{
      std::make_shared<const WebTransportBindings>(WebTransportBindings{
          factory.type_for(scalar_descriptor<WebTransportEvent>::value_meta()),
          factory.type_for(
              scalar_descriptor<WebTransportEventKind>::value_meta()),
          factory.type_for(scalar_descriptor<Int>::value_meta()),
          factory.type_for(scalar_descriptor<Bool>::value_meta()),
      })};
}

[[nodiscard]] inline Value transport_scalar(const ValueTypeRef &binding,
                                            const void *source) {
  return Value{binding, source};
}

[[nodiscard]] inline Value transport_event(const WebTransportBindings &bindings,
                                           WebTransportEventKind kind,
                                           std::string_view payload_name,
                                           Value payload, std::size_t channel,
                                           std::size_t retained_bytes,
                                           bool control) {
  BundleBuilder builder{bindings.event};
  builder.set("kind", transport_scalar(bindings.event_kind, &kind));
  builder.set(payload_name, std::move(payload));
  const Int channel_value = static_cast<Int>(channel);
  const Int byte_value = static_cast<Int>(retained_bytes);
  const Bool control_value = control;
  builder.set("channel", transport_scalar(bindings.integer, &channel_value));
  builder.set("retained_bytes",
              transport_scalar(bindings.integer, &byte_value));
  builder.set("control", transport_scalar(bindings.boolean, &control_value));
  return builder.build();
}

template <typename Budget> class TransportOutput {
public:
  using Senders = std::array<PushSourceSender, Budget::channel_count>;

  TransportOutput(Senders senders, AdmissionHandle<Budget> admission,
                  WebTransportBindingsHandle bindings)
      : senders_{std::move(senders)}, admission_{std::move(admission)},
        bindings_{std::move(bindings)} {}

  [[nodiscard]] bool send(WebTransportEventKind kind,
                          std::string_view payload_name, Value payload,
                          std::size_t channel, std::size_t retained_bytes,
                          bool control = false) const {
    if (!admission_.value->admit(channel, retained_bytes, control)) {
      return false;
    }
    auto rollback = make_scope_exit<true>(
        [&] { admission_.value->release(channel, retained_bytes, control); });
    Value event =
        transport_event(*bindings_.value, kind, payload_name,
                        std::move(payload), channel, retained_bytes, control);
    if (!senders_.at(channel).try_send(std::move(event))) {
      return false;
    }
    rollback.release();
    return true;
  }

  [[nodiscard]] bool send_reserved(WebTransportEventKind kind,
                                   std::string_view payload_name, Value payload,
                                   std::size_t channel,
                                   std::size_t retained_bytes,
                                   std::size_t reserved_bytes) const {
    if (!admission_.value->admit_reserved(channel, retained_bytes,
                                          reserved_bytes)) {
      return false;
    }
    auto rollback = make_scope_exit<true>(
        [&] { admission_.value->release(channel, retained_bytes, false); });
    Value event =
        transport_event(*bindings_.value, kind, payload_name,
                        std::move(payload), channel, retained_bytes, false);
    if (!senders_.at(channel).try_send(std::move(event))) {
      return false;
    }
    rollback.release();
    return true;
  }

private:
  Senders senders_{};
  AdmissionHandle<Budget> admission_{};
  WebTransportBindingsHandle bindings_{};
};

using ServerTransportOutput = TransportOutput<ServerAdmission>;
using ClientTransportOutput = TransportOutput<ClientAdmission>;
using ServerTransportPorts = std::array<Port<TS<WebTransportEventBatch>>,
                                        ServerAdmission::channel_count>;
using ClientTransportPorts = std::array<Port<TS<WebTransportEventBatch>>,
                                        ClientAdmission::channel_count>;

/** Materialises a generation for each active subscription lifetime. The
 * generation is graph data derived from the key-set delta and retained in the
 * TSD output; no private loopback state is required. Cost is O(A + R) per
 * modified key tick and O(K) retained graph output for K active keys. */
template <typename Key> struct SubscriptionGenerationNode {
  static constexpr auto name = "web_subscription_generation";

  static void eval(In<"keys", TSS<Key>, InputValidity::Unchecked> keys,
                   Out<TSD<Key, TS<DateTime>>> out) {
    if (!keys.modified()) {
      return;
    }
    const auto &delta = static_cast<const TSSInputView &>(keys);
    auto mutation = out.begin_mutation(out.evaluation_time());
    for (const auto key : delta.added()) {
      const Value generation{out.evaluation_time()};
      mutation.set(key, generation.view());
    }
    // Removal wins if a malformed delta names one key in both collections.
    for (const auto key : delta.removed()) {
      static_cast<void>(mutation.erase(key));
    }
  }
};

/** Releases the domain admission for a burst once the standard push source has
 * handed it to graph processing. Protocol completion remains a later sink-node
 * concern. Cost is O(B) per burst and retained memory is O(1). */
template <typename Handle> struct ReleaseTransportBatchSink {
  static constexpr auto name = "web_release_transport_batch";

  static void eval(In<"transport", TS<WebTransportEventBatch>> transport,
                   Scalar<"admission", Handle> admission) {
    for (const auto event : transport.base().value().as_list()) {
      const auto fields = event.as_bundle();
      admission.value().value->release(
          static_cast<std::size_t>(fields.at("channel").checked_as<Int>()),
          static_cast<std::size_t>(
              fields.at("retained_bytes").checked_as<Int>()),
          fields.at("control").checked_as<Bool>());
    }
  }
};

/** Groups one burst by its public TSD key. This is the only web-specific
 * collection primitive: it performs O(B) average work and O(B) transient
 * memory per burst. Standard collect retains the latest per-key batch and a
 * mapped standard emit supplies FIFO unrolling independently for every key. */
template <typename Key, fixed_string PayloadField, fixed_string KeyField>
struct GroupTransportBatchNode {
  static constexpr auto name = "web_group_transport_batch";

  static void start(State<WebKeyedBatchBindings> bindings) {
    const auto key = value_type_for_active_realization(
        scalar_descriptor<Key>::value_meta());
    const auto event = value_type_for_active_realization(
        scalar_descriptor<WebTransportEvent>::value_meta());
    const auto *event_batch_meta =
        scalar_descriptor<WebTransportEventBatch>::value_meta();
    const auto event_batch = compact_list_type(event, *event_batch_meta);
    const auto sequenced_batch = value_type_for_active_realization(
        scalar_descriptor<SequencedWebTransportBatch>::value_meta());
    bindings.set(WebKeyedBatchBindings{
        .key = key,
        .event = event,
        .event_batch = event_batch,
        .sequenced_batch = sequenced_batch,
        .batch_map = compact_map_type(key, sequenced_batch),
    });
  }

  static void eval(In<"transport", TS<WebTransportEventBatch>> transport,
                   State<WebKeyedBatchBindings> bindings,
                   Out<TS<KeyedWebTransportBatches<Key>>> out) {
    using EventsByKey = std::unordered_map<Value, std::vector<std::size_t>,
                                           OwnedValueHash, OwnedValueEqual>;
    EventsByKey grouped;
    const auto events = transport.base().value().as_list();
    grouped.reserve(events.size());
    for (std::size_t index = 0; index != events.size(); ++index) {
      const auto event = events.at(index);
      const auto envelope =
          event.as_bundle().at(PayloadField.sv()).as_bundle();
      const auto key = envelope.at(KeyField.sv());
      auto entry = grouped.find(key);
      if (entry == grouped.end()) {
        entry = grouped.try_emplace(Value{key}).first;
      }
      entry->second.push_back(index);
    }

    const auto &resolved = bindings.ref();
    const auto *event_batch_meta =
        scalar_descriptor<WebTransportEventBatch>::value_meta();
    MapBuilder batches{resolved.key, resolved.sequenced_batch};
    for (const auto &[key, grouped_events] : grouped) {
      ListBuilder event_batch{resolved.event, *event_batch_meta};
      for (const auto index : grouped_events) {
        event_batch.push_back(events.at(index));
      }
      ListStorage event_storage = event_batch.build_storage();
      Value event_value{resolved.event_batch, &event_storage};
      BundleBuilder batch{resolved.sequenced_batch};
      batch.set("sequence", Value{out.evaluation_time()});
      batch.set("events", std::move(event_value));
      batches.set_item(key.view(), batch.build().view());
    }
    MapStorage batch_storage = batches.build_storage();
    out.apply(ValueView{resolved.batch_map, &batch_storage});
  }
};

template <typename Key, typename GroupNode>
[[nodiscard]] Port<TSD<Key, TS<SequencedWebTransportBatch>>>
wire_keyed_transport_batches(Wiring &w,
                             Port<TS<WebTransportEventBatch>> transport) {
  auto grouped = wire<GroupNode>(w, transport)
                     .template as<TS<KeyedWebTransportBatches<Key>>>();
  return wire<stdlib::collect,
              TSD<Key, TS<SequencedWebTransportBatch>>>(w, grouped);
}

[[nodiscard]] inline Port<TS<WebTransportEvent>>
wire_unrolled_transport_event(Wiring &w,
                              Port<TS<SequencedWebTransportBatch>> batch) {
  auto events = wire<stdlib::getattr_, TS<WebTransportEventBatch>>(
      w, batch, Str{"events"});
  return wire<stdlib::emit>(w, events).template as<TS<WebTransportEvent>>();
}

using ServerRequestBatchNode =
    GroupTransportBatchNode<WebRoute, "request", "route">;
using ServerWsBatchNode =
    GroupTransportBatchNode<WebRoute, "server_ws", "route">;
using DeliveryBatchNode =
    GroupTransportBatchNode<Int, "delivery", "request_id">;
using ClientResponseBatchNode =
    GroupTransportBatchNode<Int, "response", "request_id">;
using ClientWsBatchNode =
    GroupTransportBatchNode<WsClientKey, "client_ws", "key">;

/** Projects one request event. Route lifetime and same-route FIFO ownership
 * are supplied by map_ and emit respectively, so this node retains no state. */
struct ServerRequestProjectionNode {
  static constexpr auto name = "web_server_request_projection";
  static constexpr bool schedule_on_start = true;

  static void eval(
      In<"event", TS<WebTransportEvent>, InputValidity::Unchecked> event,
      In<"generation", TS<DateTime>, InputValidity::Unchecked> generation,
      Out<WebRouteOutput> out) {
    auto state = out.template field<"state">();
    if (!state.valid()) {
      state.set(WebRouteState::Serving);
    }
    if (!event.modified() || !event.valid() || !generation.valid()) {
      return;
    }
    const auto envelope =
        event.base().value().as_bundle().at("request").as_bundle();
    const auto event_generation = envelope.at("generation");
    if (event_generation.data() == nullptr ||
        event_generation.checked_as<DateTime>() != generation.value()) {
      return;
    }
    const auto request = envelope.at("request");
    if (request.data() != nullptr) {
      out.template field<"request">().apply(request);
    }
  }
};

struct ServerRequestProjectionGraph {
  static constexpr auto name = "web_server_request_projection_graph";

  static Port<WebRouteOutput>
  compose(Wiring &w, NamedPort<"key", TS<WebRoute>>,
          Port<TS<SequencedWebTransportBatch>> batch,
          Port<TS<DateTime>> generation) {
    return wire<ServerRequestProjectionNode>(
               w, wire_unrolled_transport_event(w, batch), generation)
        .template as<WebRouteOutput>();
  }
};

/** Projects one server WebSocket event. Cost is O(1) per event. */
struct ServerWsProjectionNode {
  static constexpr auto name = "web_server_ws_projection";

  static void eval(In<"event", TS<WebTransportEvent>> event,
                   In<"generation", TS<DateTime>, InputValidity::Unchecked>
                       generation,
                   Out<WsRouteOutput> out) {
    if (!generation.valid()) {
      return;
    }
    const auto envelope =
        event.base().value().as_bundle().at("server_ws").as_bundle();
    const auto event_generation = envelope.at("generation");
    if (event_generation.data() == nullptr ||
        event_generation.checked_as<DateTime>() != generation.value()) {
      return;
    }
    const auto event_value = envelope.at("event");
    if (event_value.data() != nullptr) {
      out.template field<"event">().apply(event_value);
    }
    const auto frame = envelope.at("frame");
    if (frame.data() != nullptr) {
      out.template field<"frame">().apply(frame);
    }
  }
};

struct ServerWsProjectionGraph {
  static constexpr auto name = "web_server_ws_projection_graph";

  static Port<WsRouteOutput>
  compose(Wiring &w, NamedPort<"key", TS<WebRoute>>,
          Port<TS<SequencedWebTransportBatch>> batch,
          Port<TS<DateTime>> generation) {
    return wire<ServerWsProjectionNode>(
               w, wire_unrolled_transport_event(w, batch), generation)
        .template as<WsRouteOutput>();
  }
};

/** Projects one delivery report. Cost and retained memory are O(1). */
struct DeliveryProjectionNode {
  static constexpr auto name = "web_delivery_projection";

  static void eval(In<"event", TS<WebTransportEvent>> event,
                   Out<TS<WebDeliveryReport>> out) {
    out.apply(event.base()
                  .value()
                  .as_bundle()
                  .at("delivery")
                  .as_bundle()
                  .at("report"));
  }
};

struct DeliveryProjectionGraph {
  static constexpr auto name = "web_delivery_projection_graph";

  static Port<TS<WebDeliveryReport>>
  compose(Wiring &w, NamedPort<"key", TS<Int>>,
          Port<TS<SequencedWebTransportBatch>> batch) {
    return wire<DeliveryProjectionNode>(
               w, wire_unrolled_transport_event(w, batch))
        .template as<TS<WebDeliveryReport>>();
  }
};

/** Projects one graph service event and applies stop policy on graph. */
struct EventProjectionNode {
  static constexpr auto name = "web_event_projection";

  static void eval(In<"event", TS<WebTransportEvent>> event,
                   EngineControlView engine, Out<TS<WebEvent>> out) {
    const auto envelope =
        event.base().value().as_bundle().at("event").as_bundle();
    out.apply(envelope.at("event"));
    if (envelope.at("stop_graph").checked_as<Bool>()) {
      engine.request_stop();
    }
  }
};

/** Projects the selected latest statistics event. */
template <typename Stats> struct StatsProjectionNode {
  static constexpr auto name = "web_stats_projection";

  static void eval(In<"event", TS<WebTransportEvent>> event,
                   Out<TS<Stats>> out) {
    const auto fields = event.base().value().as_bundle();
    if constexpr (std::is_same_v<Stats, WebServerStats>) {
      out.apply(fields.at("server_stats"));
    } else {
      out.apply(fields.at("client_stats"));
    }
  }
};

/** Projects one HTTP client result. Cost and retained memory are O(1). */
struct ClientResponseProjectionNode {
  static constexpr auto name = "web_client_response_projection";

  static void eval(In<"event", TS<WebTransportEvent>> event,
                   Out<HttpCallResult> out) {
    const auto envelope =
        event.base().value().as_bundle().at("response").as_bundle();
    const auto response = envelope.at("response");
    if (response.data() != nullptr) {
      out.template field<"response">().apply(response);
    }
    const auto failure = envelope.at("failure");
    if (failure.data() != nullptr) {
      out.template field<"failure">().apply(failure);
    }
  }
};

struct ClientResponseProjectionGraph {
  static constexpr auto name = "web_client_response_projection_graph";

  static Port<HttpCallResult>
  compose(Wiring &w, NamedPort<"key", TS<Int>>,
          Port<TS<SequencedWebTransportBatch>> batch) {
    return wire<ClientResponseProjectionNode>(
               w, wire_unrolled_transport_event(w, batch))
        .template as<HttpCallResult>();
  }
};

/** Projects one WebSocket client event. Cost is O(1) per event. */
struct ClientWsProjectionNode {
  static constexpr auto name = "web_client_ws_projection";

  static void eval(In<"event", TS<WebTransportEvent>> event,
                   In<"generation", TS<DateTime>, InputValidity::Unchecked>
                       generation,
                   Out<WsClientOutput> out) {
    if (!generation.valid()) {
      return;
    }
    const auto envelope =
        event.base().value().as_bundle().at("client_ws").as_bundle();
    const auto event_generation = envelope.at("generation");
    if (event_generation.data() == nullptr ||
        event_generation.checked_as<DateTime>() != generation.value()) {
      return;
    }
    const auto event_value = envelope.at("event");
    if (event_value.data() != nullptr) {
      out.template field<"event">().apply(event_value);
    }
    const auto frame = envelope.at("frame");
    if (frame.data() != nullptr) {
      out.template field<"frame">().apply(frame);
    }
  }
};

struct ClientWsProjectionGraph {
  static constexpr auto name = "web_client_ws_projection_graph";

  static Port<WsClientOutput>
  compose(Wiring &w, NamedPort<"key", TS<WsClientKey>>,
          Port<TS<SequencedWebTransportBatch>> batch,
          Port<TS<DateTime>> generation) {
    return wire<ClientWsProjectionNode>(
               w, wire_unrolled_transport_event(w, batch), generation)
        .template as<WsClientOutput>();
  }
};

struct ServerOutputs {
  Port<TSD<WebRoute, WebRouteOutput>> requests;
  Port<TSD<WebRoute, WsRouteOutput>> ws;
  Port<TSD<Int, TS<WebDeliveryReport>>> respond_reports;
  Port<TSD<Int, TS<WebDeliveryReport>>> ws_send_reports;
  Port<TS<WebEvent>> events;
  Port<TS<WebServerStats>> stats;
};

[[nodiscard]] inline ServerOutputs wire_server_outputs(
    Wiring &w, const ServerTransportPorts &transport,
    Port<TSS<WebRoute>> http_routes, Port<TSS<WebRoute>> ws_routes,
    Port<TSD<WebRoute, TS<DateTime>>> http_generations,
    Port<TSD<WebRoute, TS<DateTime>>> ws_generations,
    ServerAdmissionHandle admission, WebTransportBindingsHandle bindings) {
  static_cast<void>(bindings);
  for (const auto &channel : transport) {
    static_cast<void>(
        wire<ReleaseTransportBatchSink<ServerAdmissionHandle>>(w, channel,
                                                               admission));
  }

  auto requests = wire_keyed_transport_batches<WebRoute,
                                                ServerRequestBatchNode>(
      w, transport[index(ServerChannel::Request)]);
  auto ws = wire_keyed_transport_batches<WebRoute, ServerWsBatchNode>(
      w, transport[index(ServerChannel::WsIngress)]);
  auto respond_deliveries =
      wire_keyed_transport_batches<Int, DeliveryBatchNode>(
          w, transport[index(ServerChannel::RespondDelivery)]);
  auto ws_send_deliveries =
      wire_keyed_transport_batches<Int, DeliveryBatchNode>(
          w, transport[index(ServerChannel::WsSendDelivery)]);

  auto event = wire<stdlib::emit>(w, transport[index(ServerChannel::Event)])
                   .template as<TS<WebTransportEvent>>();
  auto latest_stats = wire<stdlib::getitem_>(
                          w, transport[index(ServerChannel::Stats)],
                          wire<stdlib::const_, TS<Int>>(w, Int{-1}))
          .template as<TS<WebTransportEvent>>();

  return ServerOutputs{
      wire<stdlib::map_, TSD<WebRoute, WebRouteOutput>>(
          w, fn<ServerRequestProjectionGraph>(), requests, http_generations,
          arg<"__keys__">(http_routes))
          .template as<TSD<WebRoute, WebRouteOutput>>(),
      wire<stdlib::map_, TSD<WebRoute, WsRouteOutput>>(
          w, fn<ServerWsProjectionGraph>(), ws, ws_generations,
          arg<"__keys__">(ws_routes))
          .template as<TSD<WebRoute, WsRouteOutput>>(),
      wire<stdlib::map_, TSD<Int, TS<WebDeliveryReport>>>(
          w, fn<DeliveryProjectionGraph>(), respond_deliveries)
          .template as<TSD<Int, TS<WebDeliveryReport>>>(),
      wire<stdlib::map_, TSD<Int, TS<WebDeliveryReport>>>(
          w, fn<DeliveryProjectionGraph>(), ws_send_deliveries)
          .template as<TSD<Int, TS<WebDeliveryReport>>>(),
      wire<EventProjectionNode>(w, event).template as<TS<WebEvent>>(),
      wire<StatsProjectionNode<WebServerStats>>(w, latest_stats)
          .template as<TS<WebServerStats>>(),
  };
}

struct ClientOutputs {
  Port<TSD<Int, HttpCallResult>> responses;
  Port<TSD<WsClientKey, WsClientOutput>> ws;
  Port<TSD<Int, TS<WebDeliveryReport>>> send_reports;
  Port<TS<WebEvent>> events;
  Port<TS<WebClientStats>> stats;
};

[[nodiscard]] inline ClientOutputs
wire_client_outputs(Wiring &w, const ClientTransportPorts &transport,
                    Port<TSS<WsClientKey>> ws_keys,
                    Port<TSD<WsClientKey, TS<DateTime>>> ws_generations,
                    ClientAdmissionHandle admission,
                    WebTransportBindingsHandle bindings) {
  static_cast<void>(bindings);
  for (const auto &channel : transport) {
    static_cast<void>(
        wire<ReleaseTransportBatchSink<ClientAdmissionHandle>>(w, channel,
                                                               admission));
  }

  auto responses = wire_keyed_transport_batches<Int, ClientResponseBatchNode>(
      w, transport[index(ClientChannel::Response)]);
  auto ws = wire_keyed_transport_batches<WsClientKey, ClientWsBatchNode>(
      w, transport[index(ClientChannel::WsIngress)]);
  auto send_deliveries = wire_keyed_transport_batches<Int, DeliveryBatchNode>(
      w, transport[index(ClientChannel::SendDelivery)]);
  auto event = wire<stdlib::emit>(w, transport[index(ClientChannel::Event)])
                   .template as<TS<WebTransportEvent>>();
  auto latest_stats = wire<stdlib::getitem_>(
                          w, transport[index(ClientChannel::Stats)],
                          wire<stdlib::const_, TS<Int>>(w, Int{-1}))
          .template as<TS<WebTransportEvent>>();

  return ClientOutputs{
      wire<stdlib::map_, TSD<Int, HttpCallResult>>(
          w, fn<ClientResponseProjectionGraph>(), responses)
          .template as<TSD<Int, HttpCallResult>>(),
      wire<stdlib::map_, TSD<WsClientKey, WsClientOutput>>(
          w, fn<ClientWsProjectionGraph>(), ws, ws_generations,
          arg<"__keys__">(ws_keys))
          .template as<TSD<WsClientKey, WsClientOutput>>(),
      wire<stdlib::map_, TSD<Int, TS<WebDeliveryReport>>>(
          w, fn<DeliveryProjectionGraph>(), send_deliveries)
          .template as<TSD<Int, TS<WebDeliveryReport>>>(),
      wire<EventProjectionNode>(w, event).template as<TS<WebEvent>>(),
      wire<StatsProjectionNode<WebClientStats>>(w, latest_stats)
          .template as<TS<WebClientStats>>(),
  };
}

template <typename Tag, std::size_t Channel>
struct TransportSourceChannelTag {};

template <typename Budget> class TransportSourceGroup {
public:
  using Senders = typename TransportOutput<Budget>::Senders;
  using Start = std::function<void(Senders, const NodeView &, DateTime)>;

  TransportSourceGroup(Start on_start,
                       std::function<void(const NodeView &)> on_stop)
      : on_start_{std::move(on_start)}, on_stop_{std::move(on_stop)} {}

  void start(std::size_t channel, PushSourceSender sender, const NodeView &node,
             DateTime evaluation_time) {
    senders_.at(channel) = std::move(sender);
    if (channel + 1 != Budget::channel_count) {
      return;
    }
    for (const auto &installed : senders_) {
      if (!installed.valid()) {
        throw std::logic_error(
            "Web transport push sources did not start in channel order");
      }
    }
    on_start_(std::move(senders_), node, evaluation_time);
  }

  void stop(std::size_t channel, const NodeView &node) {
    auto clear = make_scope_exit([&] { senders_.at(channel) = {}; });
    if (channel + 1 == Budget::channel_count && on_stop_) {
      on_stop_(node);
    }
  }

private:
  Senders senders_{};
  Start on_start_{};
  std::function<void(const NodeView &)> on_stop_{};
};

template <typename Tag, typename Budget, std::size_t... Channel>
[[nodiscard]]
std::array<Port<TS<WebTransportEventBatch>>, Budget::channel_count>
wire_transport_sources_impl(
    Wiring &w, const AdmissionHandle<Budget> &admission,
    const std::shared_ptr<TransportSourceGroup<Budget>> &group,
    std::index_sequence<Channel...>) {
  const auto *schema = ts_type<TS<WebTransportEventBatch>>();
  return {Port<TS<WebTransportEventBatch>>{
      w, w.add_unique_node(
             std::type_index(typeid(TransportSourceChannelTag<Tag, Channel>)),
             make_push_source_node_with_view(
                 *schema,
                 make_push_source_burst_policy(
                     *schema, admission.value->max_pending(Channel)),
                 PushSourceNodeExtension{
                     .on_start =
                         [group](PushSourceSender sender, const NodeView &node,
                                 DateTime evaluation_time) {
                           group->start(Channel, std::move(sender), node,
                                        evaluation_time);
                         },
                     .on_stop =
                         [group](const NodeView &node) {
                           group->stop(Channel, node);
                         },
                 }),
             std::span<const WiringPortRef>{}, Value{})}...};
}

template <typename Tag, typename Budget>
[[nodiscard]]
std::array<Port<TS<WebTransportEventBatch>>, Budget::channel_count>
wire_transport_sources(Wiring &w, AdmissionHandle<Budget> admission,
                       typename TransportSourceGroup<Budget>::Start on_start,
                       std::function<void(const NodeView &)> on_stop) {
  auto group = std::make_shared<TransportSourceGroup<Budget>>(
      std::move(on_start), std::move(on_stop));
  return wire_transport_sources_impl<Tag>(
      w, admission, group, std::make_index_sequence<Budget::channel_count>{});
}

} // namespace hgraph::web::detail

namespace std {
template <> struct hash<hgraph::web::detail::ServerAdmissionHandle> {
  size_t operator()(
      const hgraph::web::detail::ServerAdmissionHandle &value) const noexcept {
    return hash<const void *>{}(value.value.get());
  }
};

template <> struct hash<hgraph::web::detail::ClientAdmissionHandle> {
  size_t operator()(
      const hgraph::web::detail::ClientAdmissionHandle &value) const noexcept {
    return hash<const void *>{}(value.value.get());
  }
};

template <> struct hash<hgraph::web::detail::WebTransportBindingsHandle> {
  size_t operator()(const hgraph::web::detail::WebTransportBindingsHandle
                        &value) const noexcept {
    return hash<const void *>{}(value.value.get());
  }
};

template <> struct hash<hgraph::web::detail::WebKeyedBatchBindings> {
  size_t operator()(const hgraph::web::detail::WebKeyedBatchBindings
                        &value) const noexcept {
    size_t result = hash<hgraph::ValueTypeRef>{}(value.key);
    result ^= hash<hgraph::ValueTypeRef>{}(value.event) + 0x9e3779b9U +
              (result << 6U) + (result >> 2U);
    result ^= hash<hgraph::ValueTypeRef>{}(value.batch_map) + 0x9e3779b9U +
              (result << 6U) + (result >> 2U);
    return result;
  }
};
} // namespace std

namespace hgraph::static_schema_detail {
template <> struct scalar_name<web::detail::ServerAdmissionHandle> {
  static constexpr std::string_view value{
      "hgraph.web.internal::ServerAdmissionHandle"};
};

template <> struct scalar_name<web::detail::ClientAdmissionHandle> {
  static constexpr std::string_view value{
      "hgraph.web.internal::ClientAdmissionHandle"};
};

template <> struct scalar_name<web::detail::WebTransportBindingsHandle> {
  static constexpr std::string_view value{
      "hgraph.web.internal::WebTransportBindingsHandle"};
};

template <> struct scalar_name<web::detail::WebKeyedBatchBindings> {
  static constexpr std::string_view value{
      "hgraph.web.internal::WebKeyedBatchBindings"};
};
} // namespace hgraph::static_schema_detail

#endif // HGRAPH_WEB_DETAIL_SERVICE_TRANSPORT_H
