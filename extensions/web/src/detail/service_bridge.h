#ifndef HGRAPH_WEB_DETAIL_SERVICE_BRIDGE_H
#define HGRAPH_WEB_DETAIL_SERVICE_BRIDGE_H

#include <hgraph/web/service.h>

#include "stream_model.h"

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
#include <string_view>
#include <typeindex>
#include <utility>
#include <vector>

namespace hgraph::web::detail {

// The kafka service-bridge contract without record-time recovery: web
// transports have no replay tier, so every queued value is untimed and the
// queues are strict FIFO (RFC 0024, runtime architecture).
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

struct WatermarkConfig {
  OutputLimits high{};
  OutputLimits low{};
  std::function<void(bool paused)> callback{};
};

template <std::size_t ChannelCount> class BoundedBridge {
public:
  explicit BoundedBridge(std::array<OutputLimits, ChannelCount> limits,
                         std::array<OutputLimits, ChannelCount> control_limits =
                             std::array<OutputLimits, ChannelCount>{}) {
    for (std::size_t channel = 0; channel != ChannelCount; ++channel) {
      if (limits[channel].records == 0 || limits[channel].bytes == 0) {
        throw std::invalid_argument("Web bridge limits must be positive");
      }
      channels_[channel].limits = limits[channel];
      channels_[channel].control_limits = control_limits[channel];
    }
  }

  void attach(std::size_t channel, PushSourceSender sender) {
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
      throw std::logic_error("Web service bridge started twice");
    }
    for (const auto &channel : channels_) {
      if (!channel.sender.valid()) {
        throw std::logic_error(
            "Web service bridge started before its push sources");
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
      channel.watermark = WatermarkConfig{};
      channel.paused = false;
    }
  }

  void set_watermark(std::size_t channel, WatermarkConfig config) {
    std::lock_guard lock{mutex_};
    auto &state = at(channel);
    state.watermark = std::move(config);
    state.paused = false;
  }

  [[nodiscard]] bool push(std::size_t channel, Value value,
                          std::size_t retained_bytes) {
    return push_impl(channel, std::move(value), retained_bytes, false);
  }

  [[nodiscard]] bool push_control(std::size_t channel, Value value,
                                  std::size_t retained_bytes) {
    return push_impl(channel, std::move(value), retained_bytes, true);
  }

  /** Self-superseding channels (statistics) evict their oldest value instead
   * of rejecting; the only channel permitted drop-oldest overflow. */
  void push_latest(std::size_t channel, Value value,
                   std::size_t retained_bytes) {
    PushSourceSender wake;
    Int generation{};
    {
      std::lock_guard lock{mutex_};
      auto &state = at(channel);
      if (!accepting_) {
        return;
      }
      while (!state.values.empty() &&
             (state.payload_records >= state.limits.records ||
              retained_bytes >
                  state.limits.bytes -
                      std::min(state.payload_bytes, state.limits.bytes))) {
        remove_accounting(state, state.values.front());
        state.values.pop_front();
      }
      if (state.payload_records >= state.limits.records ||
          retained_bytes > state.limits.bytes) {
        return;
      }
      const bool became_head = state.values.empty();
      state.values.push_back(QueuedValue{std::move(value), retained_bytes,
                                         false});
      ++state.payload_records;
      state.payload_bytes += retained_bytes;
      state.retained_bytes += retained_bytes;
      if (!state.wake_outstanding || became_head) {
        state.wake_outstanding = true;
        generation = ++state.generation;
        wake = state.sender;
      }
    }
    if (wake.valid()) {
      wake.send(generation);
    }
  }

  [[nodiscard]] std::optional<Value> pop(std::size_t channel) {
    std::optional<Value> result;
    std::function<void(bool)> resume;
    {
      std::lock_guard lock{mutex_};
      auto &state = at(channel);
      if (state.values.empty()) {
        state.wake_outstanding = false;
        return std::nullopt;
      }
      QueuedValue item = std::move(state.values.front());
      state.values.pop_front();
      remove_accounting(state, item);
      result.emplace(std::move(item.value));
      if (state.values.empty()) {
        state.wake_outstanding = false;
      }
      if (state.paused && state.watermark.callback &&
          state.payload_records <= state.watermark.low.records &&
          state.payload_bytes <= state.watermark.low.bytes) {
        state.paused = false;
        resume = state.watermark.callback;
      }
    }
    if (resume) {
      resume(false);
    }
    return result;
  }

  [[nodiscard]] std::size_t pending(std::size_t channel) const {
    std::lock_guard lock{mutex_};
    return at(channel).values.size();
  }

  [[nodiscard]] std::size_t payload_pending(std::size_t channel) const {
    std::lock_guard lock{mutex_};
    return at(channel).payload_records;
  }

  [[nodiscard]] std::size_t retained_bytes(std::size_t channel) const {
    std::lock_guard lock{mutex_};
    return at(channel).retained_bytes;
  }

  [[nodiscard]] std::size_t payload_retained_bytes(std::size_t channel) const {
    std::lock_guard lock{mutex_};
    return at(channel).payload_bytes;
  }

  [[nodiscard]] bool can_accept(std::size_t channel,
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

  [[nodiscard]] bool reserve(std::size_t channel, std::size_t retained_bytes) {
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

  [[nodiscard]] bool push_reserved(std::size_t channel, Value value,
                                   std::size_t retained_bytes,
                                   std::size_t reserved_bytes) {
    PushSourceSender wake;
    Int generation{};
    std::function<void(bool)> pause;
    {
      std::lock_guard lock{mutex_};
      auto &state = at(channel);
      if (state.reserved_records == 0 ||
          state.reserved_bytes < reserved_bytes) {
        throw std::logic_error("Web bridge output reservation is not live");
      }
      if (retained_bytes > reserved_bytes) {
        throw std::logic_error("Web bridge output exceeded its reservation");
      }
      --state.reserved_records;
      state.reserved_bytes -= reserved_bytes;
      if (!accepting_) {
        return false;
      }
      const bool became_head = state.values.empty();
      state.values.push_back(
          QueuedValue{std::move(value), retained_bytes, false});
      ++state.payload_records;
      state.payload_bytes += retained_bytes;
      state.retained_bytes += retained_bytes;
      if (!state.wake_outstanding || became_head) {
        state.wake_outstanding = true;
        generation = ++state.generation;
        wake = state.sender;
      }
      // Reserved pushes count toward the same payload watermark as plain
      // pushes; without this a reservation-fed channel would never engage
      // pause/resume.
      if (!state.paused && state.watermark.callback &&
          (state.payload_records >= state.watermark.high.records ||
           state.payload_bytes >= state.watermark.high.bytes)) {
        state.paused = true;
        pause = state.watermark.callback;
      }
    }
    if (wake.valid()) {
      wake.send(generation);
    }
    if (pause) {
      pause(true);
    }
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

  /** Drop every queued value whose bundle field `key_field` equals `key`
   * (a route or connection key being torn down). */
  void discard_matching(std::size_t channel, std::string_view key_field,
                        const ValueView &key) {
    std::lock_guard lock{mutex_};
    auto &state = at(channel);
    auto item = state.values.begin();
    while (item != state.values.end()) {
      const auto envelope = item->value.view().as_bundle();
      if (!envelope.at(key_field).equals(key)) {
        ++item;
        continue;
      }
      remove_accounting(state, *item);
      item = state.values.erase(item);
    }
    if (state.values.empty()) {
      // The outstanding conflated wake may already have been consumed by
      // this graph cycle.  Let the next surviving value publish a fresh
      // wake instead of inheriting liveness from discarded work.
      state.wake_outstanding = false;
    }
  }

private:
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
    WatermarkConfig watermark{};
    bool paused{};
  };

  static void remove_accounting(Channel &state,
                                const QueuedValue &item) noexcept {
    state.retained_bytes -= item.retained_bytes;
    if (item.control) {
      --state.control_records;
      state.control_bytes -= item.retained_bytes;
    } else {
      --state.payload_records;
      state.payload_bytes -= item.retained_bytes;
    }
  }

  [[nodiscard]] bool push_impl(std::size_t channel, Value value,
                               std::size_t retained_bytes, bool control) {
    PushSourceSender wake;
    Int generation{};
    std::function<void(bool)> pause;
    {
      std::lock_guard lock{mutex_};
      auto &state = at(channel);
      const auto limits = control ? state.control_limits : state.limits;
      const auto records =
          control ? state.control_records : state.payload_records;
      const auto bytes = control ? state.control_bytes : state.payload_bytes;
      const bool full =
          limits.records == 0 || limits.bytes == 0 ||
          records + (control ? 0 : state.reserved_records) >= limits.records ||
          retained_bytes >
              limits.bytes - std::min(bytes + (control ? 0 : state.reserved_bytes),
                                      limits.bytes);
      if (!accepting_ || full) {
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
      const bool became_head = state.values.empty();
      state.values.push_back(
          QueuedValue{std::move(value), retained_bytes, control});
      if (!state.wake_outstanding || became_head) {
        state.wake_outstanding = true;
        generation = ++state.generation;
        wake = state.sender;
      }
      if (!control && !state.paused && state.watermark.callback &&
          (state.payload_records >= state.watermark.high.records ||
           state.payload_bytes >= state.watermark.high.bytes)) {
        state.paused = true;
        pause = state.watermark.callback;
      }
    }
    if (wake.valid()) {
      wake.send(generation);
    }
    if (pause) {
      pause(true);
    }
    return true;
  }

  [[nodiscard]] Channel &at(std::size_t channel) {
    return channels_.at(channel);
  }

  [[nodiscard]] const Channel &at(std::size_t channel) const {
    return channels_.at(channel);
  }

  mutable std::mutex mutex_{};
  std::array<Channel, ChannelCount> channels_{};
  bool accepting_{};
};

using ServerBridge = BoundedBridge<index(ServerChannel::Count)>;
using ClientBridge = BoundedBridge<index(ClientChannel::Count)>;

/** Discard queued work for a key and enqueue the control record erasing it
 * from the keyed output (route removal, WS client key removal). */
template <typename Envelope, typename Bridge>
[[nodiscard]] bool erase_keyed(Bridge &bridge, std::size_t channel,
                               std::string_view key_field, Value key) {
  bridge.discard_matching(channel, key_field, key.view());
  BundleBuilder envelope{ValuePlanFactory::instance().type_for(
      scalar_descriptor<Envelope>::value_meta())};
  envelope.set(key_field, std::move(key));
  envelope.set("removed", Value{Bool{true}});
  return bridge.push_control(channel, envelope.build(), 512);
}

struct ServerBridgeHandle {
  std::shared_ptr<ServerBridge> value{};

  friend bool operator==(const ServerBridgeHandle &,
                         const ServerBridgeHandle &) noexcept = default;
  friend std::strong_ordering
  operator<=>(const ServerBridgeHandle &lhs,
              const ServerBridgeHandle &rhs) noexcept {
    return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
           reinterpret_cast<std::uintptr_t>(rhs.value.get());
  }
};

inline std::ostream &operator<<(std::ostream &stream,
                                const ServerBridgeHandle &value) {
  return stream << "ServerBridgeHandle(" << value.value.get() << ')';
}

struct ClientBridgeHandle {
  std::shared_ptr<ClientBridge> value{};

  friend bool operator==(const ClientBridgeHandle &,
                         const ClientBridgeHandle &) noexcept = default;
  friend std::strong_ordering
  operator<=>(const ClientBridgeHandle &lhs,
              const ClientBridgeHandle &rhs) noexcept {
    return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
           reinterpret_cast<std::uintptr_t>(rhs.value.get());
  }
};

inline std::ostream &operator<<(std::ostream &stream,
                                const ClientBridgeHandle &value) {
  return stream << "ClientBridgeHandle(" << value.value.get() << ')';
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

// Channel capacities come from the validated configuration; the event and
// stats channels are small fixed-size side streams, and every payload
// channel carries a control lane sized so route/key removals and rejection
// reports always fit (RFC 0024, flow control).
[[nodiscard]] inline ServerBridgeHandle make_server_bridge(const Value &config) {
  const auto view = config.view();
  const auto ingress = limits_from(view, "ingress_record_limit", "ingress_byte_limit");
  const auto ws_ingress =
      limits_from(view, "ws_ingress_record_limit", "ws_ingress_byte_limit");
  const auto outbound =
      limits_from(view, "outbound_message_limit", "outbound_byte_limit");
  const OutputLimits control{1024, 1024 * 1024};
  return ServerBridgeHandle{std::make_shared<ServerBridge>(
      std::array<OutputLimits, index(ServerChannel::Count)>{
          ingress, ws_ingress, outbound, outbound, OutputLimits{1024, 1024 * 1024},
          OutputLimits{16, 1024 * 1024}},
      std::array<OutputLimits, index(ServerChannel::Count)>{
          control, control, control, control, OutputLimits{1, 64 * 1024},
          OutputLimits{}})};
}

[[nodiscard]] inline ClientBridgeHandle make_client_bridge(const Value &config) {
  const auto view = config.view();
  const auto ingress = limits_from(view, "ingress_record_limit", "ingress_byte_limit");
  const auto ws_ingress =
      limits_from(view, "ws_ingress_record_limit", "ws_ingress_byte_limit");
  const auto outbound =
      limits_from(view, "outbound_record_limit", "outbound_byte_limit");
  const OutputLimits control{1024, 1024 * 1024};
  return ClientBridgeHandle{std::make_shared<ClientBridge>(
      std::array<OutputLimits, index(ClientChannel::Count)>{
          ingress, ws_ingress, outbound, OutputLimits{1024, 1024 * 1024},
          OutputLimits{16, 1024 * 1024}},
      std::array<OutputLimits, index(ClientChannel::Count)>{
          control, control, control, OutputLimits{1, 64 * 1024},
          OutputLimits{}})};
}

template <typename Tag, typename Handle>
[[nodiscard]] Port<TS<Int>> signal_source(Wiring &w, Handle bridge,
                                          std::size_t channel) {
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

// One payload-free conflating wake per channel: shared wakes would couple
// unrelated channels' scheduling (RFC 0024).
struct RequestSignalTag {};
struct WsIngressSignalTag {};
struct RespondDeliverySignalTag {};
struct WsSendDeliverySignalTag {};
struct ServerEventSignalTag {};
struct ServerStatsSignalTag {};
struct ResponseSignalTag {};
struct ClientWsSignalTag {};
struct ClientSendDeliverySignalTag {};
struct ClientEventSignalTag {};
struct ClientStatsSignalTag {};

struct RequestDrainNode {
  static constexpr auto name = "web_request_drain";

  static void eval(In<"signal", TS<Int>>,
                   Scalar<"bridge", ServerBridgeHandle> bridge,
                   SingleShotScheduler scheduler,
                   Out<TSD<WebRoute, WebRouteOutput>> out) {
    auto queued = bridge.value().value->pop(index(ServerChannel::Request));
    if (!queued.has_value()) {
      return;
    }
    const auto fields = queued->view().as_bundle();
    auto mutation = out.begin_mutation(out.evaluation_time());
    const auto removed = fields.at("removed");
    if (removed.data() != nullptr && removed.checked_as<Bool>()) {
      static_cast<void>(mutation.erase(fields.at("route")));
    } else {
      const auto value_binding = ValuePlanFactory::instance().type_for(
          schema_descriptor<WebRouteOutput>::ts_meta()->value_schema);
      BundleBuilder value{value_binding};
      const auto request = fields.at("request");
      const auto state = fields.at("state");
      if (request.data() != nullptr) {
        value.set("request", request.clone());
      }
      if (state.data() != nullptr) {
        value.set("state", state.clone());
      }
      Value update = value.build();
      mutation.set(fields.at("route"), update.view());
    }
    // Requests are untimed FIFO: one envelope per engine cycle, later work
    // paced at MIN_TD exactly like kafka's live subscription drain.
    if (bridge.value().value->pending(index(ServerChannel::Request)) != 0) {
      scheduler.schedule(MIN_TD);
    }
  }
};

struct WsIngressDrainNode {
  static constexpr auto name = "web_ws_ingress_drain";

  static void eval(In<"signal", TS<Int>>,
                   Scalar<"bridge", ServerBridgeHandle> bridge,
                   SingleShotScheduler scheduler,
                   Out<TSD<WebRoute, WsRouteOutput>> out) {
    auto queued = bridge.value().value->pop(index(ServerChannel::WsIngress));
    if (!queued.has_value()) {
      return;
    }
    const auto fields = queued->view().as_bundle();
    auto mutation = out.begin_mutation(out.evaluation_time());
    const auto removed = fields.at("removed");
    if (removed.data() != nullptr && removed.checked_as<Bool>()) {
      static_cast<void>(mutation.erase(fields.at("route")));
    } else {
      const auto value_binding = ValuePlanFactory::instance().type_for(
          schema_descriptor<WsRouteOutput>::ts_meta()->value_schema);
      BundleBuilder value{value_binding};
      const auto event = fields.at("event");
      const auto frame = fields.at("frame");
      if (event.data() != nullptr) {
        value.set("event", event.clone());
      }
      if (frame.data() != nullptr) {
        value.set("frame", frame.clone());
      }
      Value update = value.build();
      mutation.set(fields.at("route"), update.view());
    }
    if (bridge.value().value->pending(index(ServerChannel::WsIngress)) != 0) {
      scheduler.schedule(MIN_TD);
    }
  }
};

template <typename Handle>
void drain_delivery(const Handle &bridge, std::size_t channel,
                    SingleShotScheduler &scheduler,
                    Out<TSD<Int, TS<WebDeliveryReport>>> &out) {
  auto envelope = bridge.value->pop(channel);
  if (!envelope.has_value()) {
    return;
  }
  const auto fields = envelope->view().as_bundle();
  auto mutation = out.begin_mutation(out.evaluation_time());
  mutation.set(fields.at("request_id"), fields.at("report"));
  if (bridge.value->pending(channel) != 0) {
    scheduler.schedule(MIN_TD);
  }
}

struct RespondDeliveryDrainNode {
  static constexpr auto name = "web_respond_delivery_drain";

  static void eval(In<"signal", TS<Int>>,
                   Scalar<"bridge", ServerBridgeHandle> bridge,
                   SingleShotScheduler scheduler,
                   Out<TSD<Int, TS<WebDeliveryReport>>> out) {
    drain_delivery(bridge.value(), index(ServerChannel::RespondDelivery),
                   scheduler, out);
  }
};

struct WsSendDeliveryDrainNode {
  static constexpr auto name = "web_ws_send_delivery_drain";

  static void eval(In<"signal", TS<Int>>,
                   Scalar<"bridge", ServerBridgeHandle> bridge,
                   SingleShotScheduler scheduler,
                   Out<TSD<Int, TS<WebDeliveryReport>>> out) {
    drain_delivery(bridge.value(), index(ServerChannel::WsSendDelivery),
                   scheduler, out);
  }
};

template <typename Handle>
void drain_event(const Handle &bridge, std::size_t channel,
                 EngineControlView &engine, SingleShotScheduler &scheduler,
                 Out<TS<WebEvent>> &out) {
  auto envelope = bridge.value->pop(channel);
  if (!envelope.has_value()) {
    return;
  }
  const auto fields = envelope->view().as_bundle();
  out.apply(fields.at("event"));
  if (fields.at("stop_graph").template checked_as<Bool>()) {
    engine.request_stop();
  }
  if (bridge.value->pending(channel) != 0) {
    scheduler.schedule(MIN_TD);
  }
}

struct ServerEventDrainNode {
  static constexpr auto name = "web_server_event_drain";

  static void eval(In<"signal", TS<Int>>,
                   Scalar<"bridge", ServerBridgeHandle> bridge,
                   EngineControlView engine, SingleShotScheduler scheduler,
                   Out<TS<WebEvent>> out) {
    drain_event(bridge.value(), index(ServerChannel::Event), engine, scheduler,
                out);
  }
};

struct ClientEventDrainNode {
  static constexpr auto name = "web_client_event_drain";

  static void eval(In<"signal", TS<Int>>,
                   Scalar<"bridge", ClientBridgeHandle> bridge,
                   EngineControlView engine, SingleShotScheduler scheduler,
                   Out<TS<WebEvent>> out) {
    drain_event(bridge.value(), index(ClientChannel::Event), engine, scheduler,
                out);
  }
};

struct ServerStatsDrainNode {
  static constexpr auto name = "web_server_stats_drain";

  static void eval(In<"signal", TS<Int>>,
                   Scalar<"bridge", ServerBridgeHandle> bridge,
                   SingleShotScheduler scheduler, Out<TS<WebServerStats>> out) {
    auto stats = bridge.value().value->pop(index(ServerChannel::Stats));
    if (!stats.has_value()) {
      return;
    }
    out.apply(stats->view());
    if (bridge.value().value->pending(index(ServerChannel::Stats)) != 0) {
      scheduler.schedule(MIN_TD);
    }
  }
};

struct ClientStatsDrainNode {
  static constexpr auto name = "web_client_stats_drain";

  static void eval(In<"signal", TS<Int>>,
                   Scalar<"bridge", ClientBridgeHandle> bridge,
                   SingleShotScheduler scheduler, Out<TS<WebClientStats>> out) {
    auto stats = bridge.value().value->pop(index(ClientChannel::Stats));
    if (!stats.has_value()) {
      return;
    }
    out.apply(stats->view());
    if (bridge.value().value->pending(index(ClientChannel::Stats)) != 0) {
      scheduler.schedule(MIN_TD);
    }
  }
};

struct ResponseDrainNode {
  static constexpr auto name = "web_response_drain";

  static void eval(In<"signal", TS<Int>>,
                   Scalar<"bridge", ClientBridgeHandle> bridge,
                   SingleShotScheduler scheduler,
                   Out<TSD<Int, HttpCallResult>> out) {
    auto queued = bridge.value().value->pop(index(ClientChannel::Response));
    if (!queued.has_value()) {
      return;
    }
    const auto fields = queued->view().as_bundle();
    const auto value_binding = ValuePlanFactory::instance().type_for(
        schema_descriptor<HttpCallResult>::ts_meta()->value_schema);
    BundleBuilder value{value_binding};
    const auto response = fields.at("response");
    const auto failure = fields.at("failure");
    if (response.data() != nullptr) {
      value.set("response", response.clone());
    }
    if (failure.data() != nullptr) {
      value.set("failure", failure.clone());
    }
    Value update = value.build();
    auto mutation = out.begin_mutation(out.evaluation_time());
    mutation.set(fields.at("request_id"), update.view());
    if (bridge.value().value->pending(index(ClientChannel::Response)) != 0) {
      scheduler.schedule(MIN_TD);
    }
  }
};

struct ClientWsDrainNode {
  static constexpr auto name = "web_client_ws_drain";

  static void eval(In<"signal", TS<Int>>,
                   Scalar<"bridge", ClientBridgeHandle> bridge,
                   SingleShotScheduler scheduler,
                   Out<TSD<WsClientKey, WsClientOutput>> out) {
    auto queued = bridge.value().value->pop(index(ClientChannel::WsIngress));
    if (!queued.has_value()) {
      return;
    }
    const auto fields = queued->view().as_bundle();
    auto mutation = out.begin_mutation(out.evaluation_time());
    const auto removed = fields.at("removed");
    if (removed.data() != nullptr && removed.checked_as<Bool>()) {
      static_cast<void>(mutation.erase(fields.at("key")));
    } else {
      const auto value_binding = ValuePlanFactory::instance().type_for(
          schema_descriptor<WsClientOutput>::ts_meta()->value_schema);
      BundleBuilder value{value_binding};
      const auto event = fields.at("event");
      const auto frame = fields.at("frame");
      if (event.data() != nullptr) {
        value.set("event", event.clone());
      }
      if (frame.data() != nullptr) {
        value.set("frame", frame.clone());
      }
      Value update = value.build();
      mutation.set(fields.at("key"), update.view());
    }
    if (bridge.value().value->pending(index(ClientChannel::WsIngress)) != 0) {
      scheduler.schedule(MIN_TD);
    }
  }
};

struct ClientSendDeliveryDrainNode {
  static constexpr auto name = "web_client_send_delivery_drain";

  static void eval(In<"signal", TS<Int>>,
                   Scalar<"bridge", ClientBridgeHandle> bridge,
                   SingleShotScheduler scheduler,
                   Out<TSD<Int, TS<WebDeliveryReport>>> out) {
    drain_delivery(bridge.value(), index(ClientChannel::SendDelivery),
                   scheduler, out);
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

[[nodiscard]] inline ServerOutputs
wire_server_outputs(Wiring &w, ServerBridgeHandle bridge) {
  auto request_signal = signal_source<RequestSignalTag>(
      w, bridge, index(ServerChannel::Request));
  auto ws_signal = signal_source<WsIngressSignalTag>(
      w, bridge, index(ServerChannel::WsIngress));
  auto respond_signal = signal_source<RespondDeliverySignalTag>(
      w, bridge, index(ServerChannel::RespondDelivery));
  auto ws_send_signal = signal_source<WsSendDeliverySignalTag>(
      w, bridge, index(ServerChannel::WsSendDelivery));
  auto event_signal = signal_source<ServerEventSignalTag>(
      w, bridge, index(ServerChannel::Event));
  auto stats_signal = signal_source<ServerStatsSignalTag>(
      w, bridge, index(ServerChannel::Stats));

  return ServerOutputs{
      wire<RequestDrainNode>(w, request_signal, bridge)
          .template as<TSD<WebRoute, WebRouteOutput>>(),
      wire<WsIngressDrainNode>(w, ws_signal, bridge)
          .template as<TSD<WebRoute, WsRouteOutput>>(),
      wire<RespondDeliveryDrainNode>(w, respond_signal, bridge)
          .template as<TSD<Int, TS<WebDeliveryReport>>>(),
      wire<WsSendDeliveryDrainNode>(w, ws_send_signal, bridge)
          .template as<TSD<Int, TS<WebDeliveryReport>>>(),
      wire<ServerEventDrainNode>(w, event_signal, bridge)
          .template as<TS<WebEvent>>(),
      wire<ServerStatsDrainNode>(w, stats_signal, bridge)
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
wire_client_outputs(Wiring &w, ClientBridgeHandle bridge) {
  auto response_signal = signal_source<ResponseSignalTag>(
      w, bridge, index(ClientChannel::Response));
  auto ws_signal = signal_source<ClientWsSignalTag>(
      w, bridge, index(ClientChannel::WsIngress));
  auto send_signal = signal_source<ClientSendDeliverySignalTag>(
      w, bridge, index(ClientChannel::SendDelivery));
  auto event_signal = signal_source<ClientEventSignalTag>(
      w, bridge, index(ClientChannel::Event));
  auto stats_signal = signal_source<ClientStatsSignalTag>(
      w, bridge, index(ClientChannel::Stats));

  return ClientOutputs{
      wire<ResponseDrainNode>(w, response_signal, bridge)
          .template as<TSD<Int, HttpCallResult>>(),
      wire<ClientWsDrainNode>(w, ws_signal, bridge)
          .template as<TSD<WsClientKey, WsClientOutput>>(),
      wire<ClientSendDeliveryDrainNode>(w, send_signal, bridge)
          .template as<TSD<Int, TS<WebDeliveryReport>>>(),
      wire<ClientEventDrainNode>(w, event_signal, bridge)
          .template as<TS<WebEvent>>(),
      wire<ClientStatsDrainNode>(w, stats_signal, bridge)
          .template as<TS<WebClientStats>>(),
  };
}
} // namespace hgraph::web::detail

namespace std {
template <> struct hash<hgraph::web::detail::ServerBridgeHandle> {
  size_t operator()(
      const hgraph::web::detail::ServerBridgeHandle &value) const noexcept {
    return hash<const void *>{}(value.value.get());
  }
};

template <> struct hash<hgraph::web::detail::ClientBridgeHandle> {
  size_t operator()(
      const hgraph::web::detail::ClientBridgeHandle &value) const noexcept {
    return hash<const void *>{}(value.value.get());
  }
};
} // namespace std

namespace hgraph::static_schema_detail {
template <> struct scalar_name<web::detail::ServerBridgeHandle> {
  static constexpr std::string_view value{
      "hgraph.web.internal::ServerBridgeHandle"};
};

template <> struct scalar_name<web::detail::ClientBridgeHandle> {
  static constexpr std::string_view value{
      "hgraph.web.internal::ClientBridgeHandle"};
};
} // namespace hgraph::static_schema_detail

#endif // HGRAPH_WEB_DETAIL_SERVICE_BRIDGE_H
