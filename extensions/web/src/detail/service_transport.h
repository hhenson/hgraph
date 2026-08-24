#ifndef HGRAPH_WEB_DETAIL_SERVICE_TRANSPORT_H
#define HGRAPH_WEB_DETAIL_SERVICE_TRANSPORT_H

#include <hgraph/web/service.h>

#include "stream_model.h"

#include <hgraph/runtime/executor.h>
#include <hgraph/runtime/push_source_node.h>
#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/value_builder.h>
#include <hgraph/util/scope.h>

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
#include <string_view>
#include <type_traits>
#include <typeindex>
#include <unordered_map>
#include <unordered_set>
#include <utility>

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
 * payload bytes before socket reads, account for graph-side same-key spill,
 * retain per-channel watermarks, and guarantee control headroom.
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
  ValueTypeRef route_state{};
  ValueTypeRef server_route_output{};
  ValueTypeRef server_ws_output{};
  ValueTypeRef client_call_result{};
  ValueTypeRef client_ws_output{};
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

/** Graph-thread-only overflow retained after a burst contains more than one
 * event for the same public TSD key, plus scalar events awaiting their own
 * ticks. Distinct keys never enter this state. Keyed drain is O(P) per tick
 * for P pending keys; scalar drain is O(1). Retained memory is O(C + S) for C
 * same-key collisions and S scalar events. */
class WebProjectionSchedule {
public:
  using EmittedKeys =
      std::unordered_set<Value, OwnedValueHash, OwnedValueEqual>;

  template <typename Apply>
  void drain_keyed(EmittedKeys &emitted, Apply &&apply) {
    for (auto entry = keyed_.begin(); entry != keyed_.end();) {
      auto &queue = entry->second;
      bool did_emit = false;
      while (!queue.empty() && !did_emit) {
        Value event = std::move(queue.front());
        queue.pop_front();
        did_emit = apply(event.view());
      }
      if (did_emit) {
        emitted.emplace(entry->first.clone());
      }
      if (queue.empty()) {
        entry = keyed_.erase(entry);
      } else {
        ++entry;
      }
    }
  }

  [[nodiscard]] static bool contains(const EmittedKeys &emitted,
                                     const ValueView &key) {
    return emitted.contains(key);
  }

  static void mark(EmittedKeys &emitted, const ValueView &key) {
    emitted.emplace(key);
  }

  void defer(const ValueView &key, const ValueView &event) {
    auto entry = keyed_.find(key);
    if (entry == keyed_.end()) {
      entry = keyed_.try_emplace(Value{key}).first;
    }
    entry->second.emplace_back(event);
  }

  [[nodiscard]] bool keyed_empty() const noexcept { return keyed_.empty(); }

  void defer_scalar(const ValueView &event) { scalar_.emplace_back(event); }

  [[nodiscard]] std::optional<Value> pop_scalar() {
    if (scalar_.empty()) {
      return std::nullopt;
    }
    Value event = std::move(scalar_.front());
    scalar_.pop_front();
    return event;
  }

  [[nodiscard]] bool scalar_empty() const noexcept { return scalar_.empty(); }

private:
  std::unordered_map<Value, std::deque<Value>, OwnedValueHash, OwnedValueEqual>
      keyed_{};
  std::deque<Value> scalar_{};
};

struct WebProjectionScheduleHandle {
  std::shared_ptr<WebProjectionSchedule> value{};

  friend bool
  operator==(const WebProjectionScheduleHandle &,
             const WebProjectionScheduleHandle &) noexcept = default;
  friend std::strong_ordering
  operator<=>(const WebProjectionScheduleHandle &lhs,
              const WebProjectionScheduleHandle &rhs) noexcept {
    return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
           reinterpret_cast<std::uintptr_t>(rhs.value.get());
  }
};

inline std::ostream &operator<<(std::ostream &stream,
                                const WebTransportBindingsHandle &value) {
  return stream << "WebTransportBindingsHandle(" << value.value.get() << ')';
}

inline std::ostream &operator<<(std::ostream &stream,
                                const WebProjectionScheduleHandle &value) {
  return stream << "WebProjectionScheduleHandle(" << value.value.get() << ')';
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
          factory.type_for(scalar_descriptor<WebRouteState>::value_meta()),
          factory.type_for(
              schema_descriptor<WebRouteOutput>::ts_meta()->value_schema),
          factory.type_for(
              schema_descriptor<WsRouteOutput>::ts_meta()->value_schema),
          factory.type_for(
              schema_descriptor<HttpCallResult>::ts_meta()->value_schema),
          factory.type_for(
              schema_descriptor<WsClientOutput>::ts_meta()->value_schema),
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

[[nodiscard]] inline bool generation_matches(const TSDInputView &generations,
                                             const ValueView &key,
                                             const ValueView &generation) {
  if (generation.data() == nullptr) {
    return false;
  }
  const auto current = generations.at(key);
  return current.valid() && current.value().checked_as<DateTime>() ==
                                generation.checked_as<DateTime>();
}

/** Releases one transport event after its public output has been applied or
 * the event has been discarded. Deferred same-key collisions remain admitted,
 * so the configured web record/byte limits also bound graph-side spill.
 * Cost is O(1) per consumed event. */
template <typename Handle>
void release_transport_event(const Handle &admission, const ValueView &event) {
  const auto fields = event.as_bundle();
  admission.value->release(
      static_cast<std::size_t>(fields.at("channel").checked_as<Int>()),
      static_cast<std::size_t>(fields.at("retained_bytes").checked_as<Int>()),
      fields.at("control").checked_as<Bool>());
}

/** Projects HTTP route lifecycle and request bursts. Distinct routes tick
 * together; same-route collisions retain FIFO order over following cycles.
 * Cost is O(A + R + B + P) per tick for route delta sizes A/R, burst size B,
 * and P pending route keys. Retained memory is O(C) for deferred collisions. */
struct ServerRequestProjectionNode {
  static constexpr auto name = "web_server_request_projection";

  static void start(State<WebProjectionScheduleHandle> state) {
    state.set(
        WebProjectionScheduleHandle{std::make_shared<WebProjectionSchedule>()});
  }

  static void
  eval(In<"transport", TS<WebTransportEventBatch>, InputValidity::Unchecked>
           transport,
       In<"routes", TSS<WebRoute>, InputValidity::Unchecked> routes,
       In<"generations", TSD<WebRoute, TS<DateTime>>, InputValidity::Unchecked>
           generations,
       Scalar<"admission", ServerAdmissionHandle> admission,
       Scalar<"bindings", WebTransportBindingsHandle> bindings,
       State<WebProjectionScheduleHandle> state, SingleShotScheduler scheduler,
       Out<TSD<WebRoute, WebRouteOutput>> out) {
    const bool has_batch = transport.modified();
    auto schedule = state.get().value;
    const auto &route_delta = static_cast<const TSSInputView &>(routes);
    const auto removed = route_delta.removed();
    if (!has_batch && !routes.modified() && schedule->keyed_empty()) {
      return;
    }
    auto mutation = out.begin_mutation(out.evaluation_time());
    WebProjectionSchedule::EmittedKeys emitted;
    const auto apply_envelope = [&](const auto &envelope) {
      if (generation_matches(static_cast<const TSDInputView &>(generations),
                             envelope.at("route"), envelope.at("generation"))) {
        BundleBuilder value{bindings.value().value->server_route_output};
        for (const auto name :
             {std::string_view{"request"}, std::string_view{"state"}}) {
          const auto field = envelope.at(name);
          if (field.data() != nullptr) {
            value.set(name, field.clone());
          }
        }
        const Value update = value.build();
        mutation.set(envelope.at("route"), update.view());
        return true;
      }
      return false;
    };
    schedule->drain_keyed(emitted, [&](const ValueView &event) {
      const bool applied =
          apply_envelope(event.as_bundle().at("request").as_bundle());
      release_transport_event(admission.value(), event);
      return applied;
    });
    if (has_batch) {
      for (const auto event : transport.base().value().as_list()) {
        const auto envelope = event.as_bundle().at("request").as_bundle();
        const auto key = envelope.at("route");
        if (WebProjectionSchedule::contains(emitted, key)) {
          schedule->defer(key, event);
        } else {
          if (apply_envelope(envelope)) {
            WebProjectionSchedule::mark(emitted, key);
          }
          release_transport_event(admission.value(), event);
        }
      }
    }
    if (!schedule->keyed_empty()) {
      scheduler.schedule(MIN_TD);
    }
    if (routes.modified()) {
      for (const auto route : route_delta.added()) {
        const WebRouteState state = WebRouteState::Serving;
        BundleBuilder value{bindings.value().value->server_route_output};
        value.set("state", transport_scalar(bindings.value().value->route_state,
                                            &state));
        const Value update = value.build();
        mutation.set(route, update.view());
      }
      // Push sources run before ordinary nodes. If the last admitted event and
      // a graph-side route removal meet in one cycle, the later removal wins.
      for (const auto route : removed) {
        static_cast<void>(mutation.erase(route));
      }
    }
  }
};

/** Projects WebSocket ingress and route removal. Distinct routes tick
 * together; same-route collisions retain FIFO order. Cost is O(R + B + P)
 * per tick for R removals, burst size B, and P pending route keys. Retained
 * memory is O(C) for deferred collisions. */
struct ServerWsProjectionNode {
  static constexpr auto name = "web_server_ws_projection";

  static void start(State<WebProjectionScheduleHandle> state) {
    state.set(
        WebProjectionScheduleHandle{std::make_shared<WebProjectionSchedule>()});
  }

  static void
  eval(In<"transport", TS<WebTransportEventBatch>, InputValidity::Unchecked>
           transport,
       In<"routes", TSS<WebRoute>, InputValidity::Unchecked> routes,
       In<"generations", TSD<WebRoute, TS<DateTime>>, InputValidity::Unchecked>
           generations,
       Scalar<"admission", ServerAdmissionHandle> admission,
       Scalar<"bindings", WebTransportBindingsHandle> bindings,
       State<WebProjectionScheduleHandle> state, SingleShotScheduler scheduler,
       Out<TSD<WebRoute, WsRouteOutput>> out) {
    const bool has_batch = transport.modified();
    auto schedule = state.get().value;
    const auto &route_delta = static_cast<const TSSInputView &>(routes);
    const auto removed = route_delta.removed();
    if (!has_batch && schedule->keyed_empty() &&
        (!routes.modified() || removed.begin() == removed.end())) {
      return;
    }
    auto mutation = out.begin_mutation(out.evaluation_time());
    WebProjectionSchedule::EmittedKeys emitted;
    const auto apply_envelope = [&](const auto &envelope) {
      if (generation_matches(static_cast<const TSDInputView &>(generations),
                             envelope.at("route"), envelope.at("generation"))) {
        BundleBuilder value{bindings.value().value->server_ws_output};
        for (const auto name :
             {std::string_view{"event"}, std::string_view{"frame"}}) {
          const auto field = envelope.at(name);
          if (field.data() != nullptr) {
            value.set(name, field.clone());
          }
        }
        const Value update = value.build();
        mutation.set(envelope.at("route"), update.view());
        return true;
      }
      return false;
    };
    schedule->drain_keyed(emitted, [&](const ValueView &event) {
      const bool applied =
          apply_envelope(event.as_bundle().at("server_ws").as_bundle());
      release_transport_event(admission.value(), event);
      return applied;
    });
    if (has_batch) {
      for (const auto event : transport.base().value().as_list()) {
        const auto envelope = event.as_bundle().at("server_ws").as_bundle();
        const auto key = envelope.at("route");
        if (WebProjectionSchedule::contains(emitted, key)) {
          schedule->defer(key, event);
        } else {
          if (apply_envelope(envelope)) {
            WebProjectionSchedule::mark(emitted, key);
          }
          release_transport_event(admission.value(), event);
        }
      }
    }
    if (!schedule->keyed_empty()) {
      scheduler.schedule(MIN_TD);
    }
    if (routes.modified()) {
      for (const auto route : removed) {
        static_cast<void>(mutation.erase(route));
      }
    }
  }
};

/** Projects delivery-report bursts. Distinct request ids tick together;
 * duplicate ids retain FIFO order. Cost is O(B + P) per tick for burst size B
 * and P pending ids. Retained memory is O(C) for duplicate-id collisions. */
template <typename Admission> struct DeliveryProjectionNode {
  static constexpr auto name = "web_delivery_projection";

  static void start(State<WebProjectionScheduleHandle> state) {
    state.set(
        WebProjectionScheduleHandle{std::make_shared<WebProjectionSchedule>()});
  }

  static void eval(In<"transport", TS<WebTransportEventBatch>> transport,
                   Scalar<"admission", Admission> admission,
                   State<WebProjectionScheduleHandle> state,
                   SingleShotScheduler scheduler,
                   Out<TSD<Int, TS<WebDeliveryReport>>> out) {
    auto schedule = state.get().value;
    auto mutation = out.begin_mutation(out.evaluation_time());
    WebProjectionSchedule::EmittedKeys emitted;
    const auto apply_event = [&](const ValueView &event) {
      const auto envelope = event.as_bundle().at("delivery").as_bundle();
      mutation.set(envelope.at("request_id"), envelope.at("report"));
      return true;
    };
    schedule->drain_keyed(emitted, [&](const ValueView &event) {
      const bool applied = apply_event(event);
      release_transport_event(admission.value(), event);
      return applied;
    });
    if (transport.modified()) {
      for (const auto event : transport.base().value().as_list()) {
        const auto key =
            event.as_bundle().at("delivery").as_bundle().at("request_id");
        if (WebProjectionSchedule::contains(emitted, key)) {
          schedule->defer(key, event);
        } else {
          static_cast<void>(apply_event(event));
          WebProjectionSchedule::mark(emitted, key);
          release_transport_event(admission.value(), event);
        }
      }
    }
    if (!schedule->keyed_empty()) {
      scheduler.schedule(MIN_TD);
    }
  }
};

/** Projects service-event bursts and applies stop policy on graph. Scalar
 * output requires FIFO unrolling: O(B) to admit a burst, O(1) to emit, and
 * O(S) retained memory for S pending events. */
template <typename Admission> struct EventProjectionNode {
  static constexpr auto name = "web_event_projection";

  static void start(State<WebProjectionScheduleHandle> state) {
    state.set(
        WebProjectionScheduleHandle{std::make_shared<WebProjectionSchedule>()});
  }

  static void eval(In<"transport", TS<WebTransportEventBatch>> transport,
                   Scalar<"admission", Admission> admission,
                   State<WebProjectionScheduleHandle> state,
                   SingleShotScheduler scheduler, EngineControlView engine,
                   Out<TS<WebEvent>> out) {
    auto schedule = state.get().value;
    bool emitted = false;
    const auto apply_event = [&](const ValueView &event) {
      const auto envelope = event.as_bundle().at("event").as_bundle();
      out.apply(envelope.at("event"));
      if (envelope.at("stop_graph").checked_as<Bool>()) {
        engine.request_stop();
      }
      emitted = true;
    };
    if (auto pending = schedule->pop_scalar()) {
      apply_event(pending->view());
      release_transport_event(admission.value(), pending->view());
    }
    if (transport.modified()) {
      for (const auto event : transport.base().value().as_list()) {
        if (emitted) {
          schedule->defer_scalar(event);
        } else {
          apply_event(event);
          release_transport_event(admission.value(), event);
        }
      }
    }
    if (!schedule->scalar_empty()) {
      scheduler.schedule(MIN_TD);
    }
  }
};

/** Projects the latest statistics sample in a burst. Statistics describe
 * current state rather than an event-accurate stream, so older samples are
 * superseded. Cost is O(1) per tick and retained memory is O(1). */
template <typename Stats, typename Admission> struct StatsProjectionNode {
  static constexpr auto name = "web_stats_projection";

  static void eval(In<"transport", TS<WebTransportEventBatch>> transport,
                   Scalar<"admission", Admission> admission,
                   Out<TS<Stats>> out) {
    const auto events = transport.base().value().as_list();
    const auto fields = events.at(events.size() - 1).as_bundle();
    if constexpr (std::is_same_v<Stats, WebServerStats>) {
      out.apply(fields.at("server_stats"));
    } else {
      out.apply(fields.at("client_stats"));
    }
    for (const auto event : events) {
      release_transport_event(admission.value(), event);
    }
  }
};

/** Projects HTTP client result bursts. Distinct request ids tick together;
 * duplicate ids retain FIFO order. Cost is O(B + P) per tick for burst size B
 * and P pending ids. Retained memory is O(C) for duplicate-id collisions. */
struct ClientResponseProjectionNode {
  static constexpr auto name = "web_client_response_projection";

  static void start(State<WebProjectionScheduleHandle> state) {
    state.set(
        WebProjectionScheduleHandle{std::make_shared<WebProjectionSchedule>()});
  }

  static void eval(In<"transport", TS<WebTransportEventBatch>> transport,
                   Scalar<"admission", ClientAdmissionHandle> admission,
                   Scalar<"bindings", WebTransportBindingsHandle> bindings,
                   State<WebProjectionScheduleHandle> state,
                   SingleShotScheduler scheduler,
                   Out<TSD<Int, HttpCallResult>> out) {
    auto schedule = state.get().value;
    auto mutation = out.begin_mutation(out.evaluation_time());
    WebProjectionSchedule::EmittedKeys emitted;
    const auto apply_event = [&](const ValueView &event) {
      const auto envelope = event.as_bundle().at("response").as_bundle();
      BundleBuilder value{bindings.value().value->client_call_result};
      for (const auto name :
           {std::string_view{"response"}, std::string_view{"failure"}}) {
        const auto field = envelope.at(name);
        if (field.data() != nullptr) {
          value.set(name, field.clone());
        }
      }
      const Value update = value.build();
      mutation.set(envelope.at("request_id"), update.view());
      return true;
    };
    schedule->drain_keyed(emitted, [&](const ValueView &event) {
      const bool applied = apply_event(event);
      release_transport_event(admission.value(), event);
      return applied;
    });
    if (transport.modified()) {
      for (const auto event : transport.base().value().as_list()) {
        const auto key =
            event.as_bundle().at("response").as_bundle().at("request_id");
        if (WebProjectionSchedule::contains(emitted, key)) {
          schedule->defer(key, event);
        } else {
          static_cast<void>(apply_event(event));
          WebProjectionSchedule::mark(emitted, key);
          release_transport_event(admission.value(), event);
        }
      }
    }
    if (!schedule->keyed_empty()) {
      scheduler.schedule(MIN_TD);
    }
  }
};

/** Projects WebSocket client ingress and key removal. Distinct subscription
 * keys tick together; same-key collisions retain FIFO order. Cost is
 * O(R + B + P) per tick for R removals, burst size B, and P pending keys.
 * Retained memory is O(C) for deferred collisions. */
struct ClientWsProjectionNode {
  static constexpr auto name = "web_client_ws_projection";

  static void start(State<WebProjectionScheduleHandle> state) {
    state.set(
        WebProjectionScheduleHandle{std::make_shared<WebProjectionSchedule>()});
  }

  static void
  eval(In<"transport", TS<WebTransportEventBatch>, InputValidity::Unchecked>
           transport,
       In<"keys", TSS<WsClientKey>, InputValidity::Unchecked> keys,
       In<"generations", TSD<WsClientKey, TS<DateTime>>,
          InputValidity::Unchecked>
           generations,
       Scalar<"admission", ClientAdmissionHandle> admission,
       Scalar<"bindings", WebTransportBindingsHandle> bindings,
       State<WebProjectionScheduleHandle> state, SingleShotScheduler scheduler,
       Out<TSD<WsClientKey, WsClientOutput>> out) {
    const bool has_batch = transport.modified();
    auto schedule = state.get().value;
    const auto &key_delta = static_cast<const TSSInputView &>(keys);
    const auto removed = key_delta.removed();
    if (!has_batch && schedule->keyed_empty() &&
        (!keys.modified() || removed.begin() == removed.end())) {
      return;
    }
    auto mutation = out.begin_mutation(out.evaluation_time());
    WebProjectionSchedule::EmittedKeys emitted;
    const auto apply_envelope = [&](const auto &envelope) {
      if (generation_matches(static_cast<const TSDInputView &>(generations),
                             envelope.at("key"), envelope.at("generation"))) {
        BundleBuilder value{bindings.value().value->client_ws_output};
        for (const auto name :
             {std::string_view{"event"}, std::string_view{"frame"}}) {
          const auto field = envelope.at(name);
          if (field.data() != nullptr) {
            value.set(name, field.clone());
          }
        }
        const Value update = value.build();
        mutation.set(envelope.at("key"), update.view());
        return true;
      }
      return false;
    };
    schedule->drain_keyed(emitted, [&](const ValueView &event) {
      const bool applied =
          apply_envelope(event.as_bundle().at("client_ws").as_bundle());
      release_transport_event(admission.value(), event);
      return applied;
    });
    if (has_batch) {
      for (const auto event : transport.base().value().as_list()) {
        const auto envelope = event.as_bundle().at("client_ws").as_bundle();
        const auto key = envelope.at("key");
        if (WebProjectionSchedule::contains(emitted, key)) {
          schedule->defer(key, event);
        } else {
          if (apply_envelope(envelope)) {
            WebProjectionSchedule::mark(emitted, key);
          }
          release_transport_event(admission.value(), event);
        }
      }
    }
    if (!schedule->keyed_empty()) {
      scheduler.schedule(MIN_TD);
    }
    if (keys.modified()) {
      for (const auto key : removed) {
        static_cast<void>(mutation.erase(key));
      }
    }
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
  return ServerOutputs{
      wire<ServerRequestProjectionNode>(
          w, transport[index(ServerChannel::Request)], http_routes,
          http_generations, admission, bindings)
          .template as<TSD<WebRoute, WebRouteOutput>>(),
      wire<ServerWsProjectionNode>(
          w, transport[index(ServerChannel::WsIngress)], ws_routes,
          ws_generations, admission, bindings)
          .template as<TSD<WebRoute, WsRouteOutput>>(),
      wire<DeliveryProjectionNode<ServerAdmissionHandle>>(
          w, transport[index(ServerChannel::RespondDelivery)], admission)
          .template as<TSD<Int, TS<WebDeliveryReport>>>(),
      wire<DeliveryProjectionNode<ServerAdmissionHandle>>(
          w, transport[index(ServerChannel::WsSendDelivery)], admission)
          .template as<TSD<Int, TS<WebDeliveryReport>>>(),
      wire<EventProjectionNode<ServerAdmissionHandle>>(
          w, transport[index(ServerChannel::Event)], admission)
          .template as<TS<WebEvent>>(),
      wire<StatsProjectionNode<WebServerStats, ServerAdmissionHandle>>(
          w, transport[index(ServerChannel::Stats)], admission)
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
  return ClientOutputs{
      wire<ClientResponseProjectionNode>(
          w, transport[index(ClientChannel::Response)], admission, bindings)
          .template as<TSD<Int, HttpCallResult>>(),
      wire<ClientWsProjectionNode>(w,
                                   transport[index(ClientChannel::WsIngress)],
                                   ws_keys, ws_generations, admission, bindings)
          .template as<TSD<WsClientKey, WsClientOutput>>(),
      wire<DeliveryProjectionNode<ClientAdmissionHandle>>(
          w, transport[index(ClientChannel::SendDelivery)], admission)
          .template as<TSD<Int, TS<WebDeliveryReport>>>(),
      wire<EventProjectionNode<ClientAdmissionHandle>>(
          w, transport[index(ClientChannel::Event)], admission)
          .template as<TS<WebEvent>>(),
      wire<StatsProjectionNode<WebClientStats, ClientAdmissionHandle>>(
          w, transport[index(ClientChannel::Stats)], admission)
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

template <> struct hash<hgraph::web::detail::WebProjectionScheduleHandle> {
  size_t operator()(const hgraph::web::detail::WebProjectionScheduleHandle
                        &value) const noexcept {
    return hash<const void *>{}(value.value.get());
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

template <> struct scalar_name<web::detail::WebProjectionScheduleHandle> {
  static constexpr std::string_view value{
      "hgraph.web.internal::WebProjectionScheduleHandle"};
};
} // namespace hgraph::static_schema_detail

#endif // HGRAPH_WEB_DETAIL_SERVICE_TRANSPORT_H
