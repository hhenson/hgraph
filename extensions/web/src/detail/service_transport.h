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
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <ostream>
#include <span>
#include <stdexcept>
#include <string_view>
#include <typeindex>
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

inline constexpr std::size_t kControlLaneBytes = 1024 * 1024;

struct WatermarkConfig {
  OutputLimits high{};
  OutputLimits low{};
  std::function<void(bool paused)> callback{};
};

/**
 * Domain byte and reservation accounting for one standard push-source queue.
 *
 * This object owns no Values and is not a second transport queue. The core
 * push source owns record storage, admission and wake-up. The web runtime uses
 * this graph-scoped budget only to reserve payload bytes before socket reads,
 * retain the existing per-channel watermarks, and guarantee control headroom.
 */
template <std::size_t ChannelCount> class AdmissionBudget {
public:
  explicit AdmissionBudget(
      std::array<OutputLimits, ChannelCount> limits,
      std::array<OutputLimits, ChannelCount> control_limits = {}) {
    for (std::size_t channel = 0; channel != ChannelCount; ++channel) {
      if (limits[channel].records == 0 || limits[channel].bytes == 0) {
        throw std::invalid_argument("Web transport limits must be positive");
      }
      channels_[channel].limits = limits[channel];
      channels_[channel].control_limits = control_limits[channel];
      for (const std::size_t records :
           {limits[channel].records, control_limits[channel].records}) {
        if (records > std::numeric_limits<std::size_t>::max() - max_pending_) {
          throw std::overflow_error(
              "Aggregate web transport record capacity overflowed");
        }
        max_pending_ += records;
      }
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

  [[nodiscard]] std::size_t max_pending() const noexcept {
    return max_pending_;
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
        state.payload_records + state.reserved_records >= state.limits.records ||
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
  std::size_t max_pending_{};
  bool accepting_{};
};

using ServerAdmission = AdmissionBudget<index(ServerChannel::Count)>;
using ClientAdmission = AdmissionBudget<index(ClientChannel::Count)>;

template <typename Budget> struct AdmissionHandle {
  std::shared_ptr<Budget> value{};

  friend bool operator==(const AdmissionHandle &,
                         const AdmissionHandle &) noexcept = default;
  friend std::strong_ordering
  operator<=>(const AdmissionHandle &lhs,
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
  const auto ws_ingress = limits_from(
      view, "ws_ingress_record_limit", "ws_ingress_byte_limit");
  const auto outbound = limits_from(
      view, "outbound_message_limit", "outbound_byte_limit");
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
  const auto ws_ingress = limits_from(
      view, "ws_ingress_record_limit", "ws_ingress_byte_limit");
  const auto outbound = limits_from(
      view, "outbound_record_limit", "outbound_byte_limit");
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

inline std::ostream &operator<<(std::ostream &stream,
                                const WebTransportBindingsHandle &value) {
  return stream << "WebTransportBindingsHandle(" << value.value.get() << ')';
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

[[nodiscard]] inline Value transport_event(
    const WebTransportBindings &bindings, WebTransportEventKind kind,
    std::string_view payload_name, Value payload, std::size_t channel,
    std::size_t retained_bytes, bool control) {
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
  TransportOutput(PushSourceSender sender, AdmissionHandle<Budget> admission,
                  WebTransportBindingsHandle bindings)
      : sender_{std::move(sender)}, admission_{std::move(admission)},
        bindings_{std::move(bindings)} {}

  [[nodiscard]] bool send(WebTransportEventKind kind,
                          std::string_view payload_name, Value payload,
                          std::size_t channel, std::size_t retained_bytes,
                          bool control = false) const {
    if (!admission_.value->admit(channel, retained_bytes, control)) {
      return false;
    }
    auto rollback = make_scope_exit<true>([&] {
      admission_.value->release(channel, retained_bytes, control);
    });
    Value event = transport_event(*bindings_.value, kind, payload_name,
                                  std::move(payload), channel, retained_bytes,
                                  control);
    if (!sender_.try_send(std::move(event))) {
      return false;
    }
    rollback.release();
    return true;
  }

  [[nodiscard]] bool send_reserved(
      WebTransportEventKind kind, std::string_view payload_name, Value payload,
      std::size_t channel, std::size_t retained_bytes,
      std::size_t reserved_bytes) const {
    if (!admission_.value->admit_reserved(channel, retained_bytes,
                                          reserved_bytes)) {
      return false;
    }
    auto rollback = make_scope_exit<true>([&] {
      admission_.value->release(channel, retained_bytes, false);
    });
    Value event = transport_event(*bindings_.value, kind, payload_name,
                                  std::move(payload), channel, retained_bytes,
                                  false);
    if (!sender_.try_send(std::move(event))) {
      return false;
    }
    rollback.release();
    return true;
  }

private:
  PushSourceSender sender_{};
  AdmissionHandle<Budget> admission_{};
  WebTransportBindingsHandle bindings_{};
};

using ServerTransportOutput = TransportOutput<ServerAdmission>;
using ClientTransportOutput = TransportOutput<ClientAdmission>;

/** Releases domain record/byte accounting once the graph dequeues an event.
 * Cost is O(1) per transport tick. */
template <typename Handle> struct AdmissionReleaseSink {
  static constexpr auto name = "web_transport_admission_release";

  static void eval(In<"transport", TS<WebTransportEvent>> transport,
                   Scalar<"admission", Handle> admission) {
    const auto fields = transport.base().value().as_bundle();
    admission.value().value->release(
        static_cast<std::size_t>(fields.at("channel").checked_as<Int>()),
        static_cast<std::size_t>(
            fields.at("retained_bytes").checked_as<Int>()),
        fields.at("control").checked_as<Bool>());
  }
};

/** Projects HTTP route lifecycle and request events. Cost is O(A + R) for
 * route additions/removals plus O(1) for one transport event. */
struct ServerRequestProjectionNode {
  static constexpr auto name = "web_server_request_projection";

  static void eval(
      In<"transport", TS<WebTransportEvent>, InputValidity::Unchecked>
          transport,
      In<"routes", TSS<WebRoute>, InputValidity::Unchecked> routes,
      Scalar<"bindings", WebTransportBindingsHandle> bindings,
      Out<TSD<WebRoute, WebRouteOutput>> out) {
    const bool has_event =
        transport.modified() &&
        transport.base().value().as_bundle().at("kind").checked_as<
            WebTransportEventKind>() == WebTransportEventKind::ServerRequest;
    const auto &route_delta = static_cast<const TSSInputView &>(routes);
    const auto removed = route_delta.removed();
    if (!has_event && !routes.modified()) {
      return;
    }
    auto mutation = out.begin_mutation(out.evaluation_time());
    if (has_event) {
      const auto envelope = transport.base()
                                .value()
                                .as_bundle()
                                .at("request")
                                .as_bundle();
      BundleBuilder value{bindings.value().value->server_route_output};
      for (const auto name : {std::string_view{"request"},
                              std::string_view{"state"}}) {
        const auto field = envelope.at(name);
        if (field.data() != nullptr) {
          value.set(name, field.clone());
        }
      }
      const Value update = value.build();
      mutation.set(envelope.at("route"), update.view());
    }
    if (routes.modified()) {
      for (const auto route : route_delta.added()) {
        const WebRouteState state = WebRouteState::Serving;
        BundleBuilder value{bindings.value().value->server_route_output};
        value.set("state", transport_scalar(
                               bindings.value().value->route_state, &state));
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

/** Projects WebSocket ingress and route removal. Cost is O(R) for removed
 * routes plus O(1) for one transport event. */
struct ServerWsProjectionNode {
  static constexpr auto name = "web_server_ws_projection";

  static void eval(
      In<"transport", TS<WebTransportEvent>, InputValidity::Unchecked>
          transport,
      In<"routes", TSS<WebRoute>, InputValidity::Unchecked> routes,
      Scalar<"bindings", WebTransportBindingsHandle> bindings,
      Out<TSD<WebRoute, WsRouteOutput>> out) {
    const bool has_event =
        transport.modified() &&
        transport.base().value().as_bundle().at("kind").checked_as<
            WebTransportEventKind>() == WebTransportEventKind::ServerWsIngress;
    const auto &route_delta = static_cast<const TSSInputView &>(routes);
    const auto removed = route_delta.removed();
    if (!has_event && (!routes.modified() || removed.begin() == removed.end())) {
      return;
    }
    auto mutation = out.begin_mutation(out.evaluation_time());
    if (has_event) {
      const auto envelope = transport.base()
                                .value()
                                .as_bundle()
                                .at("server_ws")
                                .as_bundle();
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
    }
    if (routes.modified()) {
      for (const auto route : removed) {
        static_cast<void>(mutation.erase(route));
      }
    }
  }
};

/** Projects one delivery-report event. Cost is O(1) per transport tick. */
template <WebTransportEventKind Kind> struct DeliveryProjectionNode {
  static constexpr auto name = "web_delivery_projection";

  static void eval(In<"transport", TS<WebTransportEvent>> transport,
                   Out<TSD<Int, TS<WebDeliveryReport>>> out) {
    const auto fields = transport.base().value().as_bundle();
    if (fields.at("kind").checked_as<WebTransportEventKind>() != Kind) {
      return;
    }
    const auto envelope = fields.at("delivery").as_bundle();
    auto mutation = out.begin_mutation(out.evaluation_time());
    mutation.set(envelope.at("request_id"), envelope.at("report"));
  }
};

/** Projects one service event and applies its stop policy on graph. Cost is
 * O(1) per transport tick. */
template <WebTransportEventKind Kind> struct EventProjectionNode {
  static constexpr auto name = "web_event_projection";

  static void eval(In<"transport", TS<WebTransportEvent>> transport,
                   EngineControlView engine, Out<TS<WebEvent>> out) {
    const auto fields = transport.base().value().as_bundle();
    if (fields.at("kind").checked_as<WebTransportEventKind>() != Kind) {
      return;
    }
    const auto envelope = fields.at("event").as_bundle();
    out.apply(envelope.at("event"));
    if (envelope.at("stop_graph").checked_as<Bool>()) {
      engine.request_stop();
    }
  }
};

/** Projects one statistics sample. Cost is O(1) per transport tick. */
template <WebTransportEventKind Kind, typename Stats>
struct StatsProjectionNode {
  static constexpr auto name = "web_stats_projection";

  static void eval(In<"transport", TS<WebTransportEvent>> transport,
                   Out<TS<Stats>> out) {
    const auto fields = transport.base().value().as_bundle();
    if (fields.at("kind").checked_as<WebTransportEventKind>() != Kind) {
      return;
    }
    if constexpr (Kind == WebTransportEventKind::ServerStats) {
      out.apply(fields.at("server_stats"));
    } else {
      out.apply(fields.at("client_stats"));
    }
  }
};

/** Projects one HTTP client result. Cost is O(1) per transport tick. */
struct ClientResponseProjectionNode {
  static constexpr auto name = "web_client_response_projection";

  static void eval(In<"transport", TS<WebTransportEvent>> transport,
                   Scalar<"bindings", WebTransportBindingsHandle> bindings,
                   Out<TSD<Int, HttpCallResult>> out) {
    const auto fields = transport.base().value().as_bundle();
    if (fields.at("kind").checked_as<WebTransportEventKind>() !=
        WebTransportEventKind::ClientResponse) {
      return;
    }
    const auto envelope = fields.at("response").as_bundle();
    BundleBuilder value{bindings.value().value->client_call_result};
    for (const auto name :
         {std::string_view{"response"}, std::string_view{"failure"}}) {
      const auto field = envelope.at(name);
      if (field.data() != nullptr) {
        value.set(name, field.clone());
      }
    }
    const Value update = value.build();
    auto mutation = out.begin_mutation(out.evaluation_time());
    mutation.set(envelope.at("request_id"), update.view());
  }
};

/** Projects WebSocket client ingress and key removal. Cost is O(R) for
 * removed keys plus O(1) for one transport event. */
struct ClientWsProjectionNode {
  static constexpr auto name = "web_client_ws_projection";

  static void eval(
      In<"transport", TS<WebTransportEvent>, InputValidity::Unchecked>
          transport,
      In<"keys", TSS<WsClientKey>, InputValidity::Unchecked> keys,
      Scalar<"bindings", WebTransportBindingsHandle> bindings,
      Out<TSD<WsClientKey, WsClientOutput>> out) {
    const bool has_event =
        transport.modified() &&
        transport.base().value().as_bundle().at("kind").checked_as<
            WebTransportEventKind>() == WebTransportEventKind::ClientWsIngress;
    const auto &key_delta = static_cast<const TSSInputView &>(keys);
    const auto removed = key_delta.removed();
    if (!has_event && (!keys.modified() || removed.begin() == removed.end())) {
      return;
    }
    auto mutation = out.begin_mutation(out.evaluation_time());
    if (has_event) {
      const auto envelope = transport.base()
                                .value()
                                .as_bundle()
                                .at("client_ws")
                                .as_bundle();
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
    Wiring &w, Port<TS<WebTransportEvent>> transport,
    Port<TSS<WebRoute>> http_routes, Port<TSS<WebRoute>> ws_routes,
    ServerAdmissionHandle admission, WebTransportBindingsHandle bindings) {
  static_cast<void>(
      wire<AdmissionReleaseSink<ServerAdmissionHandle>>(w, transport,
                                                        admission));
  return ServerOutputs{
      wire<ServerRequestProjectionNode>(w, transport, http_routes, bindings)
          .template as<TSD<WebRoute, WebRouteOutput>>(),
      wire<ServerWsProjectionNode>(w, transport, ws_routes, bindings)
          .template as<TSD<WebRoute, WsRouteOutput>>(),
      wire<DeliveryProjectionNode<
          WebTransportEventKind::ServerRespondDelivery>>(w, transport)
          .template as<TSD<Int, TS<WebDeliveryReport>>>(),
      wire<DeliveryProjectionNode<
          WebTransportEventKind::ServerWsSendDelivery>>(w, transport)
          .template as<TSD<Int, TS<WebDeliveryReport>>>(),
      wire<EventProjectionNode<WebTransportEventKind::ServerEvent>>(w,
                                                                    transport)
          .template as<TS<WebEvent>>(),
      wire<StatsProjectionNode<WebTransportEventKind::ServerStats,
                               WebServerStats>>(w, transport)
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

[[nodiscard]] inline ClientOutputs wire_client_outputs(
    Wiring &w, Port<TS<WebTransportEvent>> transport,
    Port<TSS<WsClientKey>> ws_keys, ClientAdmissionHandle admission,
    WebTransportBindingsHandle bindings) {
  static_cast<void>(
      wire<AdmissionReleaseSink<ClientAdmissionHandle>>(w, transport,
                                                        admission));
  return ClientOutputs{
      wire<ClientResponseProjectionNode>(w, transport, bindings)
          .template as<TSD<Int, HttpCallResult>>(),
      wire<ClientWsProjectionNode>(w, transport, ws_keys, bindings)
          .template as<TSD<WsClientKey, WsClientOutput>>(),
      wire<DeliveryProjectionNode<WebTransportEventKind::ClientSendDelivery>>(
          w, transport)
          .template as<TSD<Int, TS<WebDeliveryReport>>>(),
      wire<EventProjectionNode<WebTransportEventKind::ClientEvent>>(w,
                                                                    transport)
          .template as<TS<WebEvent>>(),
      wire<StatsProjectionNode<WebTransportEventKind::ClientStats,
                               WebClientStats>>(w, transport)
          .template as<TS<WebClientStats>>(),
  };
}

template <typename Tag>
[[nodiscard]] Port<TS<WebTransportEvent>> wire_transport_source(
    Wiring &w, std::size_t max_pending, PushSourceStartViewCallback on_start,
    std::function<void(const NodeView &)> on_stop) {
  const auto *schema = ts_type<TS<WebTransportEvent>>();
  PushSourceNodeExtension extension{
      .on_start = std::move(on_start),
      .on_stop = std::move(on_stop),
  };
  return Port<TS<WebTransportEvent>>{
      w, w.add_unique_node(
             std::type_index(typeid(Tag)),
             make_push_source_node_with_view(
                 *schema, make_push_source_queue_policy(*schema, max_pending),
                 std::move(extension)),
             std::span<const WiringPortRef>{}, Value{})};
}

} // namespace hgraph::web::detail

namespace std {
template <>
struct hash<hgraph::web::detail::ServerAdmissionHandle> {
  size_t operator()(
      const hgraph::web::detail::ServerAdmissionHandle &value) const noexcept {
    return hash<const void *>{}(value.value.get());
  }
};

template <>
struct hash<hgraph::web::detail::ClientAdmissionHandle> {
  size_t operator()(
      const hgraph::web::detail::ClientAdmissionHandle &value) const noexcept {
    return hash<const void *>{}(value.value.get());
  }
};

template <>
struct hash<hgraph::web::detail::WebTransportBindingsHandle> {
  size_t operator()(const hgraph::web::detail::WebTransportBindingsHandle
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
} // namespace hgraph::static_schema_detail

#endif // HGRAPH_WEB_DETAIL_SERVICE_TRANSPORT_H
