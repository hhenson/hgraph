#include <hgraph/kafka/service.h>
#include <hgraph/kafka/testing/fake_broker.h>
#include <hgraph/kafka/testing/mock_cluster.h>
#include <hgraph/kafka/value_builders.h>

#include <hgraph/lib/std/operators/container.h>
#include <hgraph/lib/std/operators/conversion.h>
#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/lib/testing/runtime_support.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <typeindex>
#include <utility>
#include <vector>

namespace {
using namespace hgraph;
using namespace hgraph::kafka;
using namespace hgraph::kafka::testing;
using namespace hgraph::testing;
using namespace std::chrono_literals;

void require(bool condition, std::string message) {
  if (!condition) {
    throw std::runtime_error(std::move(message));
  }
}

template <typename Fn> void require_invalid(Fn &&fn, std::string message) {
  bool rejected = false;
  try {
    std::forward<Fn>(fn)();
  } catch (const std::invalid_argument &) {
    rejected = true;
  }
  require(rejected, std::move(message));
}

template <typename Fn> void require_failure(Fn &&fn, std::string message) {
  bool rejected = false;
  try {
    std::forward<Fn>(fn)();
  } catch (const std::exception &) {
    rejected = true;
  }
  require(rejected, std::move(message));
}

template <typename Schema, typename Tag>
void capture_value(Wiring &w, Port<Schema> port, Value &observed,
                   std::size_t &count) {
  const auto *input_ts = ts_type<Schema>();
  const auto *input_schema = single_input_schema(*input_ts);
  const std::array inputs{port.erased()};
  w.add_unique_node(
      std::type_index(typeid(Tag)),
      recording_value_sink(*input_schema, *input_ts, observed, count),
      std::span<const WiringPortRef>{inputs}, Value{});
}

[[nodiscard]] GraphExecutorValue start_realtime(GraphBuilder builder,
                                                TimeDelta duration = TimeDelta{
                                                    5'000'000}) {
  const DateTime start = wall_now();
  GraphExecutorBuilder executor_builder;
  executor_builder.graph_builder(std::move(builder))
      .mode(GraphExecutorMode::RealTime)
      .start_time(start)
      .end_time(start + duration);
  return executor_builder.make_executor();
}

[[nodiscard]] Str bundle_string(const Value &value, std::string_view field) {
  return value.view().as_bundle().at(field).checked_as<Str>();
}

[[nodiscard]] bool present(ValueView value) noexcept {
  return value.data() != nullptr;
}

class SenderLatch {
public:
  void reset() {
    std::lock_guard lock{mutex_};
    sender_.reset();
  }

  void publish(PushSourceSender sender) {
    {
      std::lock_guard lock{mutex_};
      sender_ = std::move(sender);
    }
    changed_.notify_all();
  }

  [[nodiscard]] std::optional<PushSourceSender> await() {
    std::unique_lock lock{mutex_};
    if (!changed_.wait_for(lock, 2s, [&] { return sender_.has_value(); })) {
      return std::nullopt;
    }
    return sender_;
  }

private:
  std::mutex mutex_{};
  std::condition_variable changed_{};
  std::optional<PushSourceSender> sender_{};
};

class GenerationLatch {
public:
  void reset() {
    std::lock_guard lock{mutex_};
    generations_.clear();
  }

  void publish(Str topic, Int generation) {
    {
      std::lock_guard lock{mutex_};
      generations_.emplace_back(std::move(topic), generation);
    }
    changed_.notify_all();
  }

  [[nodiscard]] bool await(std::size_t count) {
    std::unique_lock lock{mutex_};
    return changed_.wait_for(lock, 5s,
                             [&] { return generations_.size() >= count; });
  }

  [[nodiscard]] std::vector<std::pair<Str, Int>> values() const {
    std::lock_guard lock{mutex_};
    return generations_;
  }

private:
  mutable std::mutex mutex_{};
  std::condition_variable changed_{};
  std::vector<std::pair<Str, Int>> generations_{};
};

class CountLatch {
public:
  void reset() {
    std::lock_guard lock{mutex_};
    count_ = 0;
  }

  void publish() {
    {
      std::lock_guard lock{mutex_};
      ++count_;
    }
    changed_.notify_all();
  }

  [[nodiscard]] bool await(std::size_t count) {
    std::unique_lock lock{mutex_};
    return changed_.wait_for(lock, 5s, [&] { return count_ >= count; });
  }

private:
  std::mutex mutex_{};
  std::condition_variable changed_{};
  std::size_t count_{};
};

inline SenderLatch subscription_key_sender{};
inline SenderLatch subscription_commit_sender{};
inline GenerationLatch subscription_generations{};
inline CountLatch stale_commit_events{};
inline CountLatch graph_lifetime_live{};
inline CountLatch record_time_recovery_started{};

inline Value service_config{};
inline Value subscription_key{};
inline Value consumed_record{};
inline Value cursor{};
inline Value produced_record{};
inline Value event_value{};
inline Value production_config{};
inline Value independent_subscription_key{};
inline Value secondary_subscription_key{};
inline Str production_topic{"native-out"};

inline FakeBrokerPtr subscription_broker{};
inline FakeBrokerPtr publish_broker{};
inline FakeBrokerPtr event_broker{};
inline FakeBrokerPtr engine_a_broker{};
inline FakeBrokerPtr engine_b_broker{};
inline FakeBrokerPtr multi_publish_broker{};
inline FakeBrokerPtr sharing_broker{};
inline FakeBrokerPtr backlog_broker{};

inline Value subscription_observed{};
inline Value delivery_observed{};
inline Value event_observed{};
inline Value engine_a_event{};
inline Value engine_b_event{};
inline Value production_delivery{};
inline Value multi_delivery_a{};
inline Value multi_delivery_b{};
inline Value production_record{};
inline Value production_cursor{};
inline Value first_commit_cursor{};
inline Value first_readded_cursor{};
inline std::vector<Int> bounded_offsets{};
inline std::vector<KafkaSubscriptionState> bounded_states{};
inline std::vector<Str> bounded_payloads{};
inline std::vector<DateTime> bounded_evaluation_times{};
inline std::vector<std::pair<Str, DateTime>> multi_bounded_records{};
inline std::vector<std::pair<Str, DateTime>> recovery_live_records{};
inline std::size_t multi_bounded_complete_count{};
inline std::size_t multi_bounded_failed_count{};
inline std::vector<Int> backlog_offsets{};
inline std::vector<Str> backlog_events{};
inline std::vector<Str> flow_control_events{};
inline bool flow_control_complete{};
inline Value bounded_event{};
inline std::size_t bounded_event_count{};
inline std::size_t subscription_count{};
inline std::size_t delivery_count{};
inline std::size_t event_count{};
inline std::size_t engine_a_count{};
inline std::size_t engine_b_count{};
inline std::size_t production_delivery_count{};
inline std::size_t multi_delivery_a_count{};
inline std::size_t multi_delivery_b_count{};

struct SubscriptionCaptureTag {};
struct DeliveryCaptureTag {};
struct EventCaptureTag {};
struct EngineACaptureTag {};
struct EngineBCaptureTag {};
struct ProductionDeliveryCaptureTag {};
struct BoundedEventCaptureTag {};

struct SubscriptionBacklogCapture {
  static constexpr auto name = "kafka_subscription_backlog_capture";

  static void
  eval(NodeView node,
       In<"subscription", KafkaSubscriptionOutput, InputValidity::Unchecked>
           subscription) {
    auto record = subscription.template field<"record">();
    if (!record.valid() || !record.modified()) {
      return;
    }
    backlog_offsets.push_back(
        record.base().value().as_bundle().at("offset").checked_as<Int>());
    if (backlog_offsets.size() == 3) {
      node.graph().executor().request_stop();
    }
  }
};

struct EventBacklogCapture {
  static constexpr auto name = "kafka_event_backlog_capture";

  static void eval(NodeView node, In<"event", TS<KafkaEvent>> event) {
    backlog_events.push_back(
        event.base().value().as_bundle().at("category").checked_as<Str>());
    if (backlog_events.size() == 3) {
      node.graph().executor().request_stop();
    }
  }
};

struct MultiDeliveryCaptureNode {
  static constexpr auto name = "kafka_multi_delivery_capture";

  static void
  eval(NodeView node,
       In<"first", TS<KafkaDeliveryReport>, InputValidity::Unchecked> first,
       In<"second", TS<KafkaDeliveryReport>, InputValidity::Unchecked> second) {
    if (first.valid() && first.modified()) {
      multi_delivery_a = first.base().value().clone();
      ++multi_delivery_a_count;
    }
    if (second.valid() && second.modified()) {
      multi_delivery_b = second.base().value().clone();
      ++multi_delivery_b_count;
    }
    if (multi_delivery_a_count != 0 && multi_delivery_b_count != 0) {
      node.graph().executor().request_stop();
    }
  }
};

struct SubscriptionGraph {
  static constexpr auto name = "kafka_subscription_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("primary");
    register_fake_service(w, path, service_config.clone(), subscription_broker);
    auto key = wire<stdlib::const_, TS<KafkaSubscriptionKey>>(
        w, subscription_key.clone());
    auto output = subscribe(w, path, key);
    capture_value<KafkaSubscriptionOutput, SubscriptionCaptureTag>(
        w, output, subscription_observed, subscription_count);
  }
};

struct PublishGraph {
  static constexpr auto name = "kafka_publish_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("primary");
    register_fake_service(w, path, service_config.clone(), publish_broker);
    auto record = wire<stdlib::const_, TS<KafkaProduceRecord>>(
        w, produced_record.clone());
    auto report = publish(w, path, publish_request(w, Str{"orders"}, record));
    capture_value<TS<KafkaDeliveryReport>, DeliveryCaptureTag>(
        w, report, delivery_observed, delivery_count);
  }
};

struct CommitAndEventGraph {
  static constexpr auto name = "kafka_commit_event_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("primary");
    register_fake_service(w, path, service_config.clone(), event_broker);
    auto commit_cursor =
        wire<stdlib::const_, TS<KafkaCursor>>(w, cursor.clone());
    commit(w, path, commit_cursor);
    capture_value<TS<KafkaEvent>, EventCaptureTag>(w, events(w, path),
                                                   event_observed, event_count);
  }
};

struct EngineAGraph {
  static constexpr auto name = "kafka_engine_a_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("shared-name");
    register_fake_service(w, path, service_config.clone(), engine_a_broker);
    capture_value<TS<KafkaEvent>, EngineACaptureTag>(
        w, events(w, path), engine_a_event, engine_a_count);
  }
};

struct EngineBGraph {
  static constexpr auto name = "kafka_engine_b_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("shared-name");
    register_fake_service(w, path, service_config.clone(), engine_b_broker);
    capture_value<TS<KafkaEvent>, EngineBCaptureTag>(
        w, events(w, path), engine_b_event, engine_b_count);
  }
};

struct ProductionPublishGraph {
  static constexpr auto name = "kafka_production_publish_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("production-publish");
    register_service(w, path, production_config.clone());
    auto record = wire<stdlib::const_, TS<KafkaProduceRecord>>(
        w, produced_record.clone());
    auto report =
        publish(w, path, publish_request(w, production_topic, record));
    capture_value<TS<KafkaDeliveryReport>, ProductionDeliveryCaptureTag>(
        w, report, production_delivery, production_delivery_count);
  }
};

struct ProductionCommitGraph {
  static constexpr auto name = "kafka_production_commit_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("production-commit");
    register_service(w, path, production_config.clone());
    auto commit_cursor =
        wire<stdlib::const_, TS<KafkaCursor>>(w, cursor.clone());
    commit(w, path, commit_cursor);
  }
};

struct MultiPublisherGraph {
  static constexpr auto name = "kafka_multi_publisher_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("multi-publish");
    register_fake_service(w, path, service_config.clone(),
                          multi_publish_broker);
    auto first = wire<stdlib::const_, TS<KafkaProduceRecord>>(
        w, produced_record.clone());
    auto second = wire<stdlib::const_, TS<KafkaProduceRecord>>(
        w, produced_record.clone());
    auto dynamic_topic = wire<stdlib::const_, TS<Str>>(w, Str{"orders"});
    auto first_report =
        publish(w, path, publish_request(w, Str{"orders"}, first));
    auto second_report =
        publish(w, path, publish_request(w, dynamic_topic, second));
    static_cast<void>(
        wire<MultiDeliveryCaptureNode>(w, first_report, second_report));
  }
};

struct ProductionSubscriptionCapture {
  static constexpr auto name = "kafka_production_subscription_capture";

  static void
  eval(NodeView node,
       In<"subscription", KafkaSubscriptionOutput, InputValidity::Unchecked>
           subscription) {
    auto record = subscription.template field<"record">();
    if (!record.valid() || !record.modified()) {
      return;
    }
    auto delivered_cursor = subscription.template field<"cursor">();
    if (!delivered_cursor.valid() || !delivered_cursor.modified()) {
      throw std::logic_error("Kafka record and cursor did not tick together");
    }
    production_record = record.base().value().clone();
    production_cursor = delivered_cursor.base().value().clone();
    node.graph().executor().request_stop();
  }
};

struct ProductionSubscriptionGraph {
  static constexpr auto name = "kafka_production_subscription_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("production-subscription");
    register_service(w, path, production_config.clone());
    auto key = wire<stdlib::const_, TS<KafkaSubscriptionKey>>(
        w, subscription_key.clone());
    static_cast<void>(
        wire<ProductionSubscriptionCapture>(w, subscribe(w, path, key)));
  }
};

struct GraphLifetimeSubscriptionCapture {
  static constexpr auto name = "kafka_graph_lifetime_subscription_capture";

  static void
  eval(NodeView node,
       In<"subscription", KafkaSubscriptionOutput, InputValidity::Unchecked>
           subscription) {
    auto state = subscription.template field<"state">();
    if (state.valid() && state.modified() &&
        state.value() == KafkaSubscriptionState::Live) {
      graph_lifetime_live.publish();
    }
    auto record = subscription.template field<"record">();
    if (!record.valid() || !record.modified()) {
      return;
    }
    production_record = record.base().value().clone();
    node.graph().executor().request_stop();
  }
};

struct GraphLifetimeSubscriptionGraph {
  static constexpr auto name = "kafka_graph_lifetime_subscription_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("graph-lifetime-subscription");
    register_service(w, path, production_config.clone());
    auto key = wire<stdlib::const_, TS<KafkaSubscriptionKey>>(
        w, subscription_key.clone());
    static_cast<void>(wire<GraphLifetimeSubscriptionCapture>(
        w, subscribe(w, path, key)));
  }
};

struct RecordTimeRecoveryLiveCapture {
  static constexpr auto name = "kafka_record_time_recovery_live_capture";

  static void
  eval(NodeView node,
       In<"subscription", KafkaSubscriptionOutput, InputValidity::Unchecked>
           subscription) {
    auto state = subscription.template field<"state">();
    if (state.valid() && state.modified() &&
        state.value() == KafkaSubscriptionState::Recovering) {
      record_time_recovery_started.publish();
    }
    auto record = subscription.template field<"record">();
    if (!record.valid() || !record.modified()) {
      return;
    }
    const auto fields = record.base().value().as_bundle();
    recovery_live_records.emplace_back(
        fields.at("value").checked_as<Bytes>().data,
        node.graph().evaluation_time());
    if (recovery_live_records.size() == 2) {
      node.graph().executor().request_stop();
    }
  }
};

struct RecordTimeRecoveryLiveGraph {
  static constexpr auto name = "kafka_record_time_recovery_live_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("record-time-recovery-live");
    register_service(w, path, production_config.clone());
    auto key = wire<stdlib::const_, TS<KafkaSubscriptionKey>>(
        w, subscription_key.clone());
    static_cast<void>(
        wire<RecordTimeRecoveryLiveCapture>(w, subscribe(w, path, key)));
  }
};

struct BoundedSubscriptionCapture {
  static constexpr auto name = "kafka_bounded_subscription_capture";

  static void
  eval(NodeView node,
       In<"subscription", KafkaSubscriptionOutput, InputValidity::Unchecked>
           subscription) {
    auto record = subscription.template field<"record">();
    if (record.valid() && record.modified()) {
      const auto fields = record.base().value().as_bundle();
      bounded_offsets.push_back(fields.at("offset").checked_as<Int>());
      if (present(fields.at("value"))) {
        bounded_payloads.push_back(fields.at("value").checked_as<Bytes>().data);
      }
      bounded_evaluation_times.push_back(node.graph().evaluation_time());
    }
    auto state = subscription.template field<"state">();
    if (state.valid() && state.modified()) {
      const auto value = state.value();
      bounded_states.push_back(value);
      if (value == KafkaSubscriptionState::BoundedComplete) {
        node.graph().executor().request_stop();
      }
    }
  }
};

struct BoundedSubscriptionGraph {
  static constexpr auto name = "kafka_bounded_subscription_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("production-bounded-subscription");
    register_service(w, path, production_config.clone());
    auto key = wire<stdlib::const_, TS<KafkaSubscriptionKey>>(
        w, subscription_key.clone());
    static_cast<void>(
        wire<BoundedSubscriptionCapture>(w, subscribe(w, path, key)));
    capture_value<TS<KafkaEvent>, BoundedEventCaptureTag>(
        w, events(w, path), bounded_event, bounded_event_count);
  }
};

struct MultiBoundedSubscriptionCapture {
  static constexpr auto name = "kafka_multi_bounded_subscription_capture";

  static void
  eval(NodeView node,
       In<"subscription", KafkaSubscriptionOutput, InputValidity::Unchecked>
           subscription) {
    auto record = subscription.template field<"record">();
    if (record.valid() && record.modified()) {
      const auto fields = record.base().value().as_bundle();
      if (present(fields.at("value"))) {
        const auto payload = fields.at("value").checked_as<Bytes>().data;
        bounded_payloads.push_back(payload);
        multi_bounded_records.emplace_back(payload,
                                           node.graph().evaluation_time());
      }
    }
    auto state = subscription.template field<"state">();
    if (!state.valid() || !state.modified()) {
      return;
    }
    if (state.value() == KafkaSubscriptionState::BoundedComplete) {
      ++multi_bounded_complete_count;
    } else if (state.value() == KafkaSubscriptionState::Failed) {
      ++multi_bounded_failed_count;
    }
    if (multi_bounded_complete_count == 2 ||
        (multi_bounded_complete_count == 1 &&
         multi_bounded_failed_count == 1)) {
      node.graph().executor().request_stop();
    }
  }
};

struct MultiBoundedSubscriptionGraph {
  static constexpr auto name = "kafka_multi_bounded_subscription_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("production-multi-bounded-subscription");
    register_service(w, path, production_config.clone());
    auto first_key = wire<stdlib::const_, TS<KafkaSubscriptionKey>>(
        w, subscription_key.clone());
    auto second_key = wire<stdlib::const_, TS<KafkaSubscriptionKey>>(
        w, secondary_subscription_key.clone());
    static_cast<void>(wire<MultiBoundedSubscriptionCapture>(
        w, subscribe(w, path, first_key)));
    static_cast<void>(wire<MultiBoundedSubscriptionCapture>(
        w, subscribe(w, path, second_key)));
  }
};

struct FlowControlCapture {
  static constexpr auto name = "kafka_flow_control_capture";

  static void
  eval(NodeView node,
       In<"subscription", KafkaSubscriptionOutput, InputValidity::Unchecked>
           subscription,
       In<"event", TS<KafkaEvent>, InputValidity::Unchecked> event) {
    auto record = subscription.template field<"record">();
    if (record.valid() && record.modified()) {
      const bool first_record = bounded_offsets.empty();
      bounded_offsets.push_back(
          record.base().value().as_bundle().at("offset").checked_as<Int>());
      if (first_record) {
        // Hold the graph consumer briefly while the broker owner fills the
        // two-record ingress channel. This makes the watermark transition a
        // constructed ordering property rather than a scheduler-speed race.
        std::this_thread::sleep_for(100ms);
      }
    }
    auto state = subscription.template field<"state">();
    if (state.valid() && state.modified() &&
        state.value() == KafkaSubscriptionState::BoundedComplete) {
      flow_control_complete = true;
    }
    if (event.valid() && event.modified()) {
      flow_control_events.push_back(
          event.base().value().as_bundle().at("category").checked_as<Str>());
    }
    if (flow_control_complete &&
        std::ranges::find(flow_control_events, Str{"paused"}) !=
            flow_control_events.end() &&
        std::ranges::find(flow_control_events, Str{"resumed"}) !=
            flow_control_events.end()) {
      node.graph().executor().request_stop();
    }
  }
};

struct FlowControlledSubscriptionGraph {
  static constexpr auto name = "kafka_flow_control_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("production-flow-control");
    register_service(w, path, production_config.clone());
    auto key = wire<stdlib::const_, TS<KafkaSubscriptionKey>>(
        w, subscription_key.clone());
    static_cast<void>(
        wire<FlowControlCapture>(w, subscribe(w, path, key), events(w, path)));
  }
};

struct BoundedSubscriptionCommitGraph {
  static constexpr auto name = "kafka_bounded_subscription_commit_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("production-bounded-subscription");
    register_service(w, path, production_config.clone());
    auto key = wire<stdlib::const_, TS<KafkaSubscriptionKey>>(
        w, subscription_key.clone());
    auto subscription = subscribe(w, path, key);
    auto cursor_port =
        wire<stdlib::getattr_, TS<KafkaCursor>>(w, subscription, Str{"cursor"});
    commit(w, path, cursor_port);
    static_cast<void>(wire<BoundedSubscriptionCapture>(w, subscription));
    capture_value<TS<KafkaEvent>, BoundedEventCaptureTag>(
        w, events(w, path), bounded_event, bounded_event_count);
  }
};

struct ReverseCommitCursors {
  static constexpr auto name = "kafka_reverse_commit_cursors";

  static void
  eval(In<"cursor", TS<KafkaCursor>, InputValidity::Unchecked> cursor,
       NodeScheduler scheduler, State<Int> phase, EngineControlView engine,
       Out<TS<KafkaCursor>> out) {
    if (cursor.valid() && cursor.modified()) {
      if (phase.get() == Int{0}) {
        first_commit_cursor = cursor.base().value().clone();
        phase.set(Int{1});
        return;
      }
      if (phase.get() == Int{1}) {
        out.apply(cursor.base().value());
        phase.set(Int{2});
        scheduler.schedule(TimeDelta{100'000});
        return;
      }
    }
    if (phase.get() == Int{2}) {
      out.apply(first_commit_cursor.view());
      phase.set(Int{3});
      scheduler.schedule(TimeDelta{100'000});
    } else if (phase.get() == Int{3}) {
      engine.request_stop();
    }
  }
};

struct MonotonicCommitGraph {
  static constexpr auto name = "kafka_monotonic_commit_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("production-monotonic-commit");
    register_service(w, path, production_config.clone());
    auto key = wire<stdlib::const_, TS<KafkaSubscriptionKey>>(
        w, subscription_key.clone());
    auto subscription = subscribe(w, path, key);
    auto cursor_port =
        wire<stdlib::getattr_, TS<KafkaCursor>>(w, subscription, Str{"cursor"});
    commit(w, path, wire<ReverseCommitCursors>(w, cursor_port));
  }
};

struct DynamicSubscriptionKeyTag {};
struct DynamicCommitCursorTag {};

struct ReaddedSubscriptionCapture {
  static constexpr auto name = "kafka_readded_subscription_capture";

  static void
  eval(In<"subscription", KafkaSubscriptionOutput, InputValidity::Unchecked>
           subscription) {
    auto record_port = subscription.template field<"record">();
    auto cursor_port = subscription.template field<"cursor">();
    if (!record_port.valid() || !record_port.modified() ||
        !cursor_port.valid() || !cursor_port.modified()) {
      return;
    }
    if (!first_readded_cursor.has_value()) {
      first_readded_cursor = cursor_port.base().value().clone();
    }
    subscription_generations.publish(
        record_port.base().value().as_bundle().at("topic").checked_as<Str>(),
        cursor_port.base()
            .value()
            .as_bundle()
            .at("assignment_generation")
            .checked_as<Int>());
  }
};

struct StaleCommitEventCapture {
  static constexpr auto name = "kafka_stale_commit_event_capture";

  static void
  eval(In<"event", TS<KafkaEvent>, InputValidity::Unchecked> event) {
    if (event.valid() && event.modified() &&
        event.base().value().as_bundle().at("category").checked_as<Str>() ==
            Str{"stale_commit"}) {
      stale_commit_events.publish();
    }
  }
};

struct ReaddedSubscriptionGraph {
  static constexpr auto name = "kafka_readded_subscription_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("production-readded-subscription");
    register_service(w, path, production_config.clone());
    const auto *schema = ts_type<TS<KafkaSubscriptionKey>>();
    Port<TS<KafkaSubscriptionKey>> key{
        w, w.add_unique_node(
               std::type_index(typeid(DynamicSubscriptionKeyTag)),
               make_push_source_node(*schema,
                                     [](PushSourceSender sender) {
                                       subscription_key_sender.publish(
                                           std::move(sender));
                                     }),
               std::span<const WiringPortRef>{}, Value{})};
    static_cast<void>(
        wire<ReaddedSubscriptionCapture>(w, subscribe(w, path, key)));
    Port<TS<KafkaCursor>> commit_cursor{
        w, w.add_unique_node(
               std::type_index(typeid(DynamicCommitCursorTag)),
               make_push_source_node(*ts_type<TS<KafkaCursor>>(),
                                     [](PushSourceSender sender) {
                                       subscription_commit_sender.publish(
                                           std::move(sender));
                                     }),
               std::span<const WiringPortRef>{}, Value{})};
    commit(w, path, commit_cursor);
    static_cast<void>(wire<StaleCommitEventCapture>(w, events(w, path)));
  }
};

struct SubscriptionSharingGraph {
  static constexpr auto name = "kafka_subscription_sharing_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("sharing");
    register_fake_service(w, path, service_config.clone(), sharing_broker);
    auto first = wire<stdlib::const_, TS<KafkaSubscriptionKey>>(
        w, subscription_key.clone());
    auto second = wire<stdlib::const_, TS<KafkaSubscriptionKey>>(
        w, subscription_key.clone());
    auto independent = wire<stdlib::const_, TS<KafkaSubscriptionKey>>(
        w, independent_subscription_key.clone());
    static_cast<void>(subscribe(w, path, first));
    static_cast<void>(subscribe(w, path, second));
    static_cast<void>(subscribe(w, path, independent));
  }
};

struct DuplicateRegistrationGraph {
  static constexpr auto name = "kafka_duplicate_registration_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("duplicate");
    register_fake_service(w, path, service_config.clone(), sharing_broker);
    register_fake_service(w, path, service_config.clone(), sharing_broker);
    static_cast<void>(events(w, path));
  }
};

struct SubscriptionBacklogGraph {
  static constexpr auto name = "kafka_subscription_backlog_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("backlog-subscription");
    register_fake_service(w, path, service_config.clone(), backlog_broker);
    auto key = wire<stdlib::const_, TS<KafkaSubscriptionKey>>(
        w, subscription_key.clone());
    static_cast<void>(
        wire<SubscriptionBacklogCapture>(w, subscribe(w, path, key)));
  }
};

struct EventBacklogGraph {
  static constexpr auto name = "kafka_event_backlog_test_graph";

  static void compose(Wiring &w) {
    const auto path = service::path("backlog-event");
    register_fake_service(w, path, service_config.clone(), backlog_broker);
    static_cast<void>(wire<EventBacklogCapture>(w, events(w, path)));
  }
};

void initialize_values() {
  register_kafka_types();
  service_config =
      make_service_config({Str{"localhost:9092"}}, Str{"native-test"});
  subscription_key = make_subscription_key(
      {Str{"orders"}}, Str{"risk"}, Str{"earliest"}, Str{"unbounded"},
      KafkaCommitMode::Explicit, Str{"orders-risk"});
  independent_subscription_key = make_subscription_key(
      {Str{"orders"}}, Str{"risk"}, Str{"earliest"}, Str{"unbounded"},
      KafkaCommitMode::Explicit, Str{"orders-risk-independent"});
  secondary_subscription_key = Value{};
  consumed_record = make_record(
      Str{"orders"}, Int{2}, Int{41}, Bytes{"payload"}, Bytes{"key"},
      {{Str{"trace"}, Bytes{"abc"}}, {Str{"trace"}, std::nullopt}});
  cursor =
      make_cursor(Str{"orders-risk"}, Int{3}, Str{"orders"}, Int{2}, Int{42});
  produced_record = make_produce_record(Bytes{"result"}, Bytes{"key"},
                                        {{Str{"trace"}, Bytes{"abc"}}},
                                        std::nullopt, Int{2}, Str{"token-7"});
  event_value =
      make_event(KafkaSeverity::Warning, Str{"consumer"}, Str{"rebalance"},
                 Str{"primary"}, Str{"assignment changed"});
}

void release_test_state() noexcept {
  subscription_key_sender.reset();
  subscription_commit_sender.reset();
  subscription_broker.reset();
  publish_broker.reset();
  event_broker.reset();
  engine_a_broker.reset();
  engine_b_broker.reset();
  multi_publish_broker.reset();
  sharing_broker.reset();
  backlog_broker.reset();

  for (Value *value : std::array{
           &service_config,        &subscription_key,
           &consumed_record,       &cursor,
           &produced_record,       &event_value,
           &production_config,     &independent_subscription_key,
           &secondary_subscription_key,
           &subscription_observed, &delivery_observed,
           &event_observed,        &engine_a_event,
           &engine_b_event,        &production_delivery,
           &multi_delivery_a,      &multi_delivery_b,
           &production_record,     &production_cursor,
           &first_commit_cursor,   &first_readded_cursor,
           &bounded_event,
       }) {
    *value = Value{};
  }
}

void test_public_value_validation_and_producer_configuration() {
  require_invalid([] { static_cast<void>(make_service_config({})); },
                  "an empty Kafka bootstrap-server list was accepted");
  require_invalid([] { static_cast<void>(make_service_config({Str{}})); },
                  "an empty Kafka bootstrap server was accepted");
  require_invalid(
      [] {
        static_cast<void>(hgraph::kafka::service_config()
                              .bootstrap_servers({Str{"localhost:9092"}})
                              .producer_acknowledgements(Str{"1"})
                              .build());
      },
      "idempotent Kafka producer accepted acknowledgements=1");
  require_invalid(
      [] {
        static_cast<void>(hgraph::kafka::service_config()
                              .bootstrap_servers({Str{"localhost:9092"}})
                              .producer_retries(Int{-1})
                              .build());
      },
      "negative Kafka producer retries were accepted");
  require_invalid(
      [] {
        static_cast<void>(make_start_position(KafkaStartPositionKind::Earliest,
                                              KafkaOffsetFallback::Earliest,
                                              wall_now()));
      },
      "a timestamp attached to an earliest start position was accepted");
  require_invalid(
      [] {
        static_cast<void>(make_stop_position(KafkaStopPositionKind::Offsets));
      },
      "an offset stop without offsets was accepted");
  require_invalid(
      [] {
        static_cast<void>(make_subscription_key(
            {}, Str{"group"},
            make_start_position(KafkaStartPositionKind::Earliest),
            make_stop_position(KafkaStopPositionKind::Snapshot)));
      },
      "a topic subscription without topics was accepted");
  require_invalid(
      [] {
        static_cast<void>(make_subscription_key(
            {Str{""}}, Str{"group"},
            make_start_position(KafkaStartPositionKind::Earliest),
            make_stop_position(KafkaStopPositionKind::Snapshot)));
      },
      "a Kafka subscription with an empty topic was accepted");
  require_invalid(
      [] {
        static_cast<void>(make_pattern_subscription_key(
            Str{"orders-.*"}, Str{"group"},
            make_start_position(KafkaStartPositionKind::Earliest),
            make_stop_position(KafkaStopPositionKind::Snapshot),
            KafkaCommitMode::Explicit, {}, Str{"read_uncommitted"},
            KafkaRecoveryClock::Arrival, KafkaMergePolicy::Partition,
            std::nullopt, KafkaAssignmentMode::Independent));
      },
      "independent Kafka assignment accepted a topic pattern");
  require_invalid(
      [] {
        static_cast<void>(make_produce_record(Bytes{"value"}, std::nullopt, {},
                                              std::nullopt, Int{-1}));
      },
      "a negative Kafka produce partition was accepted");
  require_invalid(
      [] {
        static_cast<void>(make_record(Str{}, Int{0}, Int{0}, Bytes{"value"}));
      },
      "a Kafka record without a topic was accepted");
  require_invalid(
      [] {
        static_cast<void>(make_cursor(Str{}, Int{0}, Str{}, Int{-1}, Int{-1}));
      },
      "an invalid Kafka cursor was accepted");

  const Value configured = hgraph::kafka::service_config()
                               .bootstrap_servers({Str{"localhost:9092"}})
                               .client_id(Str{"typed-producer"})
                               .idempotent_producer(false)
                               .producer_acknowledgements(Str{"1"})
                               .producer_retries(Int{7})
                               .producer_linger(17ms)
                               .producer_batch_record_limit(Int{23})
                               .build();
  const auto producer =
      configured.view().as_bundle().at("producer").as_bundle();
  require(!producer.at("idempotent").checked_as<Bool>(),
          "producer idempotence setting was not preserved");
  require(producer.at("acknowledgements").checked_as<Str>() == Str{"1"},
          "producer acknowledgement setting was not preserved");
  require(producer.at("retries").checked_as<Int>() == Int{7},
          "producer retries setting was not preserved");
  require(producer.at("linger_ms").checked_as<Int>() == Int{17},
          "producer linger setting was not preserved");
  require(producer.at("batch_record_limit").checked_as<Int>() == Int{23},
          "producer batch record setting was not preserved");

  production_config =
      hgraph::kafka::service_config()
          .bootstrap_servers({Str{"localhost:9092"}})
          .common_option(Str{"security.protocol"}, Str{"plaintext"})
          .producer_option(Str{"security.protocol"}, Str{"plaintext"})
          .build();
  require_invalid(
      [] { static_cast<void>(build_graph<ProductionPublishGraph>()); },
      "a Kafka option duplicated across common and producer config was "
      "accepted");
  production_config = hgraph::kafka::service_config()
                          .bootstrap_servers({Str{"localhost:9092"}})
                          .producer_option(Str{"acks"}, Str{"all"})
                          .build();
  require_invalid(
      [] { static_cast<void>(build_graph<ProductionPublishGraph>()); },
      "a Kafka option owned by the typed service contract was accepted as "
      "passthrough");
  production_config =
      make_service_config({Str{"localhost:9092"}}, Str{"native-test"});
}

void test_simulation_rejects_publish_and_commit_work() {
  production_config =
      make_service_config({Str{"localhost:9092"}}, Str{"simulation-rejection"});
  const DateTime start = wall_now();
  require_failure(
      [&] {
        static_cast<void>(run_graph(build_graph<ProductionPublishGraph>(),
                                    start, start + TimeDelta{1'000'000},
                                    GraphExecutorMode::Simulation));
      },
      "Kafka publish work was accepted by a simulation executor");
  require_failure(
      [&] {
        static_cast<void>(run_graph(build_graph<ProductionCommitGraph>(), start,
                                    start + TimeDelta{1'000'000},
                                    GraphExecutorMode::Simulation));
      },
      "Kafka commit work was accepted by a simulation executor");

  subscription_key =
      hgraph::kafka::subscription_key()
          .topics({Str{"simulation-contract"}})
          .group_id(Str{"simulation-contract"})
          .assignment_mode(KafkaAssignmentMode::Independent)
          .start(make_start_position(KafkaStartPositionKind::Earliest))
          .stop(make_stop_position(KafkaStopPositionKind::Unbounded))
          .recovery_clock(KafkaRecoveryClock::RecordTimestamp)
          .merge_policy(KafkaMergePolicy::TimestampTopicPartitionOffset)
          .sharing_identity(Str{"simulation-unbounded"})
          .build();
  require_failure(
      [&] {
        static_cast<void>(run_graph(build_graph<ProductionSubscriptionGraph>(),
                                    start, start + TimeDelta{1'000'000},
                                    GraphExecutorMode::Simulation));
      },
      "an unbounded Kafka subscription was accepted by a simulation executor");

  subscription_key =
      hgraph::kafka::subscription_key()
          .topics({Str{"simulation-contract"}})
          .group_id(Str{"simulation-contract"})
          .assignment_mode(KafkaAssignmentMode::Independent)
          .start(make_start_position(KafkaStartPositionKind::Earliest))
          .stop(make_stop_position(KafkaStopPositionKind::Snapshot))
          .recovery_clock(KafkaRecoveryClock::RecordTimestamp)
          .merge_policy(KafkaMergePolicy::Partition)
          .sharing_identity(Str{"simulation-nondeterministic"})
          .build();
  require_failure(
      [&] {
        static_cast<void>(run_graph(build_graph<ProductionSubscriptionGraph>(),
                                    start, start + TimeDelta{1'000'000},
                                    GraphExecutorMode::Simulation));
      },
      "nondeterministically merged Kafka recovery was accepted by a simulation "
      "executor");
  initialize_values();
}

void test_subscription_boundary() {
  subscription_broker = std::make_shared<FakeBroker>();
  subscription_observed = Value{};
  subscription_count = 0;

  auto executor = start_realtime(build_graph<SubscriptionGraph>());
  auto view = executor.view();
  AsyncGraphExecutorRun runner{view};

  require(subscription_broker->wait_until_attached(2s),
          "subscription service did not attach");
  require(subscription_broker->wait_for_subscription_updates(1, 2s),
          "subscription key did not reach the transport sink");
  subscription_broker->emit_subscription(
      subscription_key.clone(), consumed_record.clone(), cursor.clone());
  runner.join();

  require(subscription_count == 1,
          "subscription output did not tick exactly once");
  const auto output = subscription_observed.view().as_bundle();
  require(output.at("record").as_bundle().at("offset").checked_as<Int>() ==
              Int{41},
          "subscription record offset was not preserved");
  require(output.at("cursor").as_bundle().at("next_offset").checked_as<Int>() ==
              Int{42},
          "subscription cursor was not delivered with the record");
  require(output.at("state").checked_as<KafkaSubscriptionState>() ==
              KafkaSubscriptionState::Live,
          "subscription state was not delivered");
  require(subscription_broker->attach_count() == 1,
          "service materialized more than once");
  require(subscription_broker->wait_until_detached(2s),
          "subscription service did not detach");
}

void test_publish_boundary() {
  publish_broker = std::make_shared<FakeBroker>();
  delivery_observed = Value{};
  delivery_count = 0;

  auto executor = start_realtime(build_graph<PublishGraph>());
  auto view = executor.view();
  AsyncGraphExecutorRun runner{view};

  require(publish_broker->wait_for_publications(1, 2s),
          "publish request did not reach sink");
  runner.join();

  const auto publications = publish_broker->published_records();
  require(publications.size() == 1, "unexpected publish count");
  require(publications.front().topic == Str{"orders"},
          "static topic was not preserved");
  require(bundle_string(publications.front().record, "user_token") ==
              Str{"token-7"},
          "publish record token was not preserved");
  require(delivery_count == 1, "delivery report did not tick exactly once");
  require(bundle_string(delivery_observed, "user_token") == Str{"token-7"},
          "delivery report was not correlated to the published record");
  require(delivery_observed.view()
                  .as_bundle()
                  .at("status")
                  .checked_as<KafkaDeliveryStatus>() ==
              KafkaDeliveryStatus::Delivered,
          "delivery status was not preserved");
  require(publish_broker->wait_until_detached(2s),
          "publish service did not detach");
}

void test_multiple_publishers_and_dynamic_topics() {
  multi_publish_broker = std::make_shared<FakeBroker>();
  multi_delivery_a = Value{};
  multi_delivery_b = Value{};
  multi_delivery_a_count = 0;
  multi_delivery_b_count = 0;

  auto executor = start_realtime(build_graph<MultiPublisherGraph>());
  auto view = executor.view();
  AsyncGraphExecutorRun runner{view};

  require(multi_publish_broker->wait_for_publications(2, 2s),
          "multiple Kafka publishers did not reach the shared service");
  runner.join();

  const auto publications = multi_publish_broker->published_records();
  require(publications.size() == 2,
          "shared Kafka service rejected a second publisher");
  require(std::ranges::all_of(
              publications,
              [](const auto &item) { return item.topic == Str{"orders"}; }),
          "static and dynamic Kafka topic wiring disagreed");
  require(multi_delivery_a_count == 1 && multi_delivery_b_count == 1,
          "delivery reports were not isolated by publisher request id (first=" +
              std::to_string(multi_delivery_a_count) +
              ", second=" + std::to_string(multi_delivery_b_count) +
              ", request_ids=" + std::to_string(publications[0].request_id) +
              "," + std::to_string(publications[1].request_id) + ")");
  require(
      multi_publish_broker->attach_count() == 1,
      "multiple publishers materialized more than one service implementation");
}

void test_subscription_sharing_is_explicit_and_duplicate_registration_fails() {
  sharing_broker = std::make_shared<FakeBroker>();
  auto executor = start_realtime(build_graph<SubscriptionSharingGraph>());
  auto view = executor.view();
  AsyncGraphExecutorRun runner{view};

  require(sharing_broker->wait_for_subscription_updates(1, 2s),
          "shared Kafka subscription keys did not reach the service "
          "implementation");
  view.request_stop();
  runner.join();

  std::size_t additions{};
  for (const auto &delta : sharing_broker->subscription_deltas()) {
    additions += delta.view().as_bundle().at("added").as_set().size();
  }
  require(additions == 2, "identical subscriptions did not share or an "
                          "explicit sharing identity was ignored");
  require(
      sharing_broker->attach_count() == 1,
      "subscription clients materialized more than one service implementation");

  bool duplicate_rejected = false;
  try {
    static_cast<void>(build_graph<DuplicateRegistrationGraph>());
  } catch (const std::exception &) {
    duplicate_rejected = true;
  }
  require(duplicate_rejected,
          "duplicate Kafka service registration at one path was accepted");
}

void test_push_backlogs_drain_one_value_per_graph_cycle() {
  backlog_broker = std::make_shared<FakeBroker>();
  backlog_offsets.clear();
  auto subscription_executor =
      start_realtime(build_graph<SubscriptionBacklogGraph>());
  auto subscription_view = subscription_executor.view();
  AsyncGraphExecutorRun subscription_runner{subscription_view};
  require(backlog_broker->wait_for_subscription_updates(1, 2s),
          "backlog subscription did not start");
  for (Int offset = 0; offset < 3; ++offset) {
    backlog_broker->emit_subscription(
        subscription_key.clone(),
        make_record(Str{"orders"}, Int{0}, offset, Bytes{"value"}),
        make_cursor(Str{"orders-risk"}, Int{1}, Str{"orders"}, Int{0},
                    offset + 1));
  }
  subscription_runner.join();
  require(backlog_offsets == std::vector<Int>{Int{0}, Int{1}, Int{2}},
          "subscription backlog was conflated, reordered, or lost");

  backlog_broker = std::make_shared<FakeBroker>();
  backlog_events.clear();
  auto event_executor = start_realtime(build_graph<EventBacklogGraph>());
  auto event_view = event_executor.view();
  AsyncGraphExecutorRun event_runner{event_view};
  require(backlog_broker->wait_until_attached(2s),
          "backlog event service did not start");
  for (Int index = 0; index < 3; ++index) {
    backlog_broker->emit_event(make_event(KafkaSeverity::Info, Str{"consumer"},
                                          Str{"event-"} + std::to_string(index),
                                          Str{"backlog-event"}, Str{}));
  }
  event_runner.join();
  require(backlog_events ==
              std::vector<Str>{Str{"event-0"}, Str{"event-1"}, Str{"event-2"}},
          "event backlog was conflated, reordered, or lost");
}

void test_service_can_start_and_stop_repeatedly() {
  event_broker = std::make_shared<FakeBroker>();
  for (int run = 0; run < 2; ++run) {
    auto executor = start_realtime(build_graph<CommitAndEventGraph>());
    auto view = executor.view();
    AsyncGraphExecutorRun runner{view};
    require(event_broker->wait_until_attached(2s),
            "restarted service did not attach");
    view.request_stop();
    runner.join();
    require(event_broker->wait_until_detached(2s),
            "restarted service did not detach");
  }
  require(event_broker->attach_count() == 2,
          "service did not materialize once per graph run");
}

void test_ingress_is_bounded_before_the_graph_drains() {
  service_config =
      make_service_config({Str{"localhost:9092"}}, Str{"bounded-test"}, true,
                          Int{1}, Int{1024}, Int{1}, Int{1024});
  subscription_broker = std::make_shared<FakeBroker>();
  subscription_observed = Value{};
  subscription_count = 0;

  const DateTime start = wall_now();
  auto executor = start_realtime(build_graph<SubscriptionGraph>());
  auto graph = executor.view().graph();
  graph.start(start);
  require(subscription_broker->wait_until_attached(2s),
          "bounded service did not attach");

  subscription_broker->emit_subscription(
      subscription_key.clone(), consumed_record.clone(), cursor.clone());
  bool overflowed = false;
  try {
    subscription_broker->emit_subscription(
        subscription_key.clone(), consumed_record.clone(), cursor.clone());
  } catch (const std::overflow_error &) {
    overflowed = true;
  }
  graph.stop(start);

  require(overflowed,
          "bounded ingress silently accepted work beyond its record limit");
  require(subscription_broker->wait_until_detached(2s),
          "bounded service did not detach");
  service_config =
      make_service_config({Str{"localhost:9092"}}, Str{"native-test"});
}

void test_librdkafka_ingress_pauses_and_resumes_at_watermarks() {
  MockCluster cluster;
  cluster.create_topic(Str{"typed-flow-control"});
  for (Int offset = 0; offset < 8; ++offset) {
    cluster.seed_record(Str{"typed-flow-control"},
                        Bytes{"record-" + std::to_string(offset)});
  }
  production_config = hgraph::kafka::service_config()
                          .bootstrap_servers({cluster.bootstrap_servers()})
                          .client_id(Str{"typed-flow-control-subscriber"})
                          .ingress_limits(2, 64 * 1024)
                          .build();
  subscription_key = make_subscription_key(
      {Str{"typed-flow-control"}}, Str{"typed-flow-control-group"},
      make_start_position(KafkaStartPositionKind::Earliest),
      make_stop_position(KafkaStopPositionKind::Snapshot),
      KafkaCommitMode::None, Str{"typed-flow-control-subscription"});
  bounded_offsets.clear();
  flow_control_events.clear();
  flow_control_complete = false;

  auto executor = start_realtime(build_graph<FlowControlledSubscriptionGraph>(),
                                 TimeDelta{20'000'000});
  auto view = executor.view();
  AsyncGraphExecutorRun runner{view};
  runner.join();

  require(flow_control_complete,
          "flow-controlled Kafka subscription did not complete");
  require(bounded_offsets == std::vector<Int>{Int{0}, Int{1}, Int{2}, Int{3},
                                              Int{4}, Int{5}, Int{6}, Int{7}},
          "flow control changed per-partition record order");
  require(std::ranges::find(flow_control_events, Str{"paused"}) !=
              flow_control_events.end(),
          "Kafka consumer did not report its high-watermark pause");
  require(std::ranges::find(flow_control_events, Str{"resumed"}) !=
              flow_control_events.end(),
          "Kafka consumer did not report its low-watermark resume");
  initialize_values();
}

void test_commit_and_event_boundaries() {
  event_broker = std::make_shared<FakeBroker>();
  event_observed = Value{};
  event_count = 0;

  auto executor = start_realtime(build_graph<CommitAndEventGraph>());
  auto view = executor.view();
  AsyncGraphExecutorRun runner{view};

  require(event_broker->wait_until_attached(2s),
          "event service did not attach");
  require(event_broker->wait_for_commits(1, 2s),
          "commit did not reach transport sink");
  event_broker->emit_event(event_value.clone());
  runner.join();

  const auto commits = event_broker->committed_cursors();
  require(commits.size() == 1, "unexpected commit count");
  require(
      commits.front().view().as_bundle().at("next_offset").checked_as<Int>() ==
          Int{42},
      "commit cursor was not preserved");
  require(event_count == 1, "event stream did not tick exactly once");
  require(bundle_string(event_observed, "category") == Str{"rebalance"},
          "typed event did not cross the push boundary");
  require(event_broker->wait_until_detached(2s),
          "event service did not detach");
}

void test_concurrent_engines_are_independent() {
  engine_a_broker = std::make_shared<FakeBroker>();
  engine_b_broker = std::make_shared<FakeBroker>();
  engine_a_event = Value{};
  engine_b_event = Value{};
  engine_a_count = 0;
  engine_b_count = 0;

  auto executor_a = start_realtime(build_graph<EngineAGraph>());
  auto executor_b = start_realtime(build_graph<EngineBGraph>());
  auto view_a = executor_a.view();
  auto view_b = executor_b.view();
  AsyncGraphExecutorRun runner_a{view_a};
  AsyncGraphExecutorRun runner_b{view_b};

  require(engine_a_broker->wait_until_attached(2s), "engine A did not attach");
  require(engine_b_broker->wait_until_attached(2s), "engine B did not attach");
  engine_a_broker->emit_event(make_event(KafkaSeverity::Info, Str{"producer"},
                                         Str{"engine-a"}, Str{"shared-name"},
                                         Str{"a"}));
  engine_b_broker->emit_event(make_event(KafkaSeverity::Info, Str{"producer"},
                                         Str{"engine-b"}, Str{"shared-name"},
                                         Str{"b"}));

  runner_a.join();
  runner_b.join();

  require(engine_a_count == 1 && engine_b_count == 1,
          "concurrent event streams did not tick");
  require(bundle_string(engine_a_event, "category") == Str{"engine-a"},
          "engine A crossed streams");
  require(bundle_string(engine_b_event, "category") == Str{"engine-b"},
          "engine B crossed streams");
  require(engine_a_broker->attach_count() == 1,
          "engine A materialized more than once");
  require(engine_b_broker->attach_count() == 1,
          "engine B materialized more than once");
  require(engine_a_broker->wait_until_detached(2s), "engine A did not detach");
  require(engine_b_broker->wait_until_detached(2s), "engine B did not detach");
}

void test_librdkafka_publish_path() {
  MockCluster cluster;
  cluster.create_topic(Str{"native-out"}, 3);
  production_config = make_service_config({cluster.bootstrap_servers()},
                                          Str{"native-publisher"});
  production_delivery = Value{};
  production_delivery_count = 0;

  auto executor = start_realtime(build_graph<ProductionPublishGraph>());
  auto view = executor.view();
  AsyncGraphExecutorRun runner{view};
  runner.join();

  require(production_delivery_count == 1,
          "librdkafka delivery report did not tick");
  const auto delivery_fields = production_delivery.view().as_bundle();
  const auto status =
      delivery_fields.at("status").checked_as<KafkaDeliveryStatus>();
  require(
      status == KafkaDeliveryStatus::Delivered,
      "mock broker did not report delivery: " +
          delivery_fields.at("message").checked_as<Str>() + " (" +
          std::to_string(delivery_fields.at("error_code").checked_as<Int>()) +
          ")");
  require(bundle_string(production_delivery, "topic") == Str{"native-out"},
          "librdkafka delivery report lost the topic");
  require(bundle_string(production_delivery, "user_token") == Str{"token-7"},
          "librdkafka delivery report lost correlation");
}

void test_librdkafka_delivery_failures_are_typed() {
  const auto run_case = [](MockProduceError injected,
                           KafkaDeliveryStatus expected_status,
                           Bool expected_retriable, Str topic) {
    MockCluster cluster;
    cluster.create_topic(topic);
    cluster.fail_next_produce(injected);
    production_config =
        hgraph::kafka::service_config()
            .bootstrap_servers({cluster.bootstrap_servers()})
            .client_id(Str{"native-delivery-failure"})
            .idempotent_producer(false)
            .producer_retries(0)
            .producer_option(Str{"message.timeout.ms"}, Str{"1000"})
            .build();
    production_topic = topic;
    produced_record =
        make_produce_record(Bytes{"failure"}, std::nullopt, {}, std::nullopt,
                            std::nullopt, Str{"typed-failure"});
    production_delivery = Value{};
    production_delivery_count = 0;

    auto executor = start_realtime(build_graph<ProductionPublishGraph>());
    auto view = executor.view();
    AsyncGraphExecutorRun runner{view};
    runner.join();

    require(production_delivery_count == 1,
            "failed Kafka delivery did not produce exactly one report");
    const auto report = production_delivery.view().as_bundle();
    require(report.at("status").checked_as<KafkaDeliveryStatus>() ==
                expected_status,
            "failed Kafka delivery used the wrong typed status");
    require(report.at("retriable").checked_as<Bool>() == expected_retriable,
            "failed Kafka delivery used the wrong retriable flag");
    require(report.at("error_code").checked_as<Int>() != 0,
            "failed Kafka delivery omitted the broker error code");
  };

  run_case(MockProduceError::Retriable, KafkaDeliveryStatus::RetriableFailure,
           true, Str{"native-retriable-failure"});
  run_case(MockProduceError::Permanent, KafkaDeliveryStatus::PermanentFailure,
           false, Str{"native-permanent-failure"});
  production_topic = Str{"native-out"};
  initialize_values();
}

void test_librdkafka_subscription_path() {
  MockCluster cluster;
  cluster.create_topic(Str{"native-in"});
  cluster.seed_record(
      Str{"native-in"}, std::nullopt, Bytes{""},
      {{Str{"duplicate"}, Bytes{"one"}}, {Str{"duplicate"}, std::nullopt}}, 0);

  production_config = make_service_config({cluster.bootstrap_servers()},
                                          Str{"native-subscriber"});
  subscription_key = make_subscription_key(
      {Str{"native-in"}}, Str{"native-subscription-group"}, Str{"earliest"},
      Str{"snapshot"}, KafkaCommitMode::Explicit, Str{"native-subscription"});
  production_record = Value{};
  production_cursor = Value{};

  auto executor = start_realtime(build_graph<ProductionSubscriptionGraph>());
  auto view = executor.view();
  AsyncGraphExecutorRun runner{view};
  runner.join();

  const auto record = production_record.view().as_bundle();
  require(record.at("topic").checked_as<Str>() == Str{"native-in"},
          "consumed topic was lost");
  require(record.at("offset").checked_as<Int>() == Int{0},
          "consumed offset was lost");
  require(!present(record.at("value")),
          "Kafka tombstone became an empty byte string");
  require(present(record.at("key")) &&
              record.at("key").checked_as<Bytes>().data.empty(),
          "empty Kafka key became null");
  const auto headers = record.at("headers").as_list();
  require(headers.size() == 2, "duplicate Kafka headers were collapsed");
  require(headers.at(0).as_bundle().at("name").checked_as<Str>() ==
                  Str{"duplicate"} &&
              headers.at(1).as_bundle().at("name").checked_as<Str>() ==
                  Str{"duplicate"},
          "Kafka header order was not preserved");
  require(production_cursor.view()
                  .as_bundle()
                  .at("next_offset")
                  .checked_as<Int>() == Int{1},
          "cursor did not expose the next commit position");
}

void test_graph_lifetime_stop_remains_live_in_real_time() {
  MockCluster cluster;
  cluster.create_topic(Str{"graph-lifetime-live"});
  production_config = hgraph::kafka::service_config()
                          .bootstrap_servers({cluster.bootstrap_servers()})
                          .client_id(Str{"graph-lifetime-live-subscriber"})
                          .build();
  subscription_key =
      hgraph::kafka::subscription_key()
          .topics({Str{"graph-lifetime-live"}})
          .group_id(Str{"graph-lifetime-live-group"})
          .assignment_mode(KafkaAssignmentMode::Independent)
          .start(make_start_position(KafkaStartPositionKind::Latest))
          .stop(make_stop_position(KafkaStopPositionKind::GraphLifetime))
          .commit_mode(KafkaCommitMode::None)
          .recovery_clock(KafkaRecoveryClock::Arrival)
          .merge_policy(KafkaMergePolicy::Partition)
          .sharing_identity(Str{"graph-lifetime-live-subscription"})
          .build();
  production_record = Value{};
  graph_lifetime_live.reset();

  auto executor =
      start_realtime(build_graph<GraphLifetimeSubscriptionGraph>());
  auto view = executor.view();
  AsyncGraphExecutorRun runner{view};
  require(graph_lifetime_live.await(1),
          "graph-lifetime subscription did not enter its live phase");
  cluster.seed_record(Str{"graph-lifetime-live"}, Bytes{"live"});
  runner.join();

  require(production_record.view()
              .as_bundle()
              .at("value")
              .checked_as<Bytes>()
              .data == "live",
          "graph-lifetime subscription stopped at the real-time snapshot");
  initialize_values();
}

void test_permanent_consumer_failure_stops_the_graph() {
  MockCluster cluster;
  cluster.create_topic(Str{"native-consumer-failure"});
  cluster.fail_next_fetch(MockConsumeError::Permanent, 10);

  production_config =
      hgraph::kafka::service_config()
          .bootstrap_servers({cluster.bootstrap_servers()})
          .client_id(Str{"native-consumer-failure"})
          .consumer_failure_policy(KafkaFailurePolicy::StopGraph)
          .build();
  subscription_key = make_subscription_key(
      {Str{"native-consumer-failure"}}, Str{"native-consumer-failure-group"},
      Str{"earliest"}, Str{"unbounded"}, KafkaCommitMode::None,
      Str{"native-consumer-failure"});
  bounded_event = Value{};
  bounded_event_count = 0;

  const auto started = std::chrono::steady_clock::now();
  auto executor = start_realtime(build_graph<BoundedSubscriptionGraph>());
  auto view = executor.view();
  AsyncGraphExecutorRun runner{view};
  runner.join();

  require(std::chrono::steady_clock::now() - started < 2s,
          "permanent Kafka consumer failure did not stop the graph");
  require(bounded_event_count != 0,
          "permanent Kafka consumer failure did not publish an event");
  require(bundle_string(bounded_event, "category") == Str{"poll"},
          "permanent Kafka consumer failure used the wrong event category");
  require(bounded_event.view()
              .as_bundle()
              .at("fatal")
              .checked_as<Bool>(),
          "permanent Kafka consumer failure was not marked fatal");
}

void test_typed_explicit_partition_boundaries() {
  MockCluster cluster;
  cluster.create_topic(Str{"typed-explicit"});
  cluster.seed_record(Str{"typed-explicit"}, Bytes{"zero"});
  cluster.seed_record(Str{"typed-explicit"}, Bytes{"one"});
  cluster.seed_record(Str{"typed-explicit"}, Bytes{"two"});

  production_config = make_service_config({cluster.bootstrap_servers()},
                                          Str{"typed-explicit-subscriber"});
  subscription_key = make_partition_subscription_key(
      {{Str{"typed-explicit"}, Int{0}}}, Str{"typed-explicit-group"},
      make_start_position(KafkaStartPositionKind::Offsets,
                          KafkaOffsetFallback::Fail, std::nullopt,
                          {{Str{"typed-explicit"}, Int{0}, Int{1}}}),
      make_stop_position(KafkaStopPositionKind::Offsets, std::nullopt,
                         {{Str{"typed-explicit"}, Int{0}, Int{3}}}),
      KafkaCommitMode::Explicit, Str{"typed-explicit-subscription"});
  bounded_offsets.clear();
  bounded_states.clear();
  bounded_payloads.clear();
  bounded_evaluation_times.clear();
  bounded_event = Value{};
  bounded_event_count = 0;

  auto executor = start_realtime(build_graph<BoundedSubscriptionGraph>());
  auto view = executor.view();
  AsyncGraphExecutorRun runner{view};
  runner.join();

  require(
      bounded_offsets == std::vector<Int>{Int{1}, Int{2}},
      "explicit Kafka start/stop offsets did not bound the selected partition");
  require(std::find(bounded_states.begin(), bounded_states.end(),
                    KafkaSubscriptionState::BoundedComplete) !=
              bounded_states.end(),
          "bounded explicit subscription did not report completion");
}

void test_latest_snapshot_is_empty() {
  MockCluster cluster;
  cluster.create_topic(Str{"typed-latest"});
  cluster.seed_record(Str{"typed-latest"}, Bytes{"history"});

  production_config = make_service_config({cluster.bootstrap_servers()},
                                          Str{"typed-latest-subscriber"});
  subscription_key = make_subscription_key(
      {Str{"typed-latest"}}, Str{"typed-latest-group"},
      make_start_position(KafkaStartPositionKind::Latest),
      make_stop_position(KafkaStopPositionKind::Snapshot),
      KafkaCommitMode::Explicit, Str{"typed-latest-subscription"});
  bounded_offsets.clear();
  bounded_states.clear();

  auto executor = start_realtime(build_graph<BoundedSubscriptionGraph>());
  auto view = executor.view();
  AsyncGraphExecutorRun runner{view};
  runner.join();

  require(bounded_offsets.empty(),
          "latest-to-snapshot subscription replayed history");
  require(std::find(bounded_states.begin(), bounded_states.end(),
                    KafkaSubscriptionState::BoundedComplete) !=
              bounded_states.end(),
          "empty latest snapshot did not report bounded completion (states=" +
              std::to_string(bounded_states.size()) +
              ", records=" + std::to_string(bounded_offsets.size()) + ")");
}

void run_bounded_subscription(std::string_view context = {}) {
  bounded_offsets.clear();
  bounded_states.clear();
  bounded_payloads.clear();
  bounded_evaluation_times.clear();
  bounded_event = Value{};
  bounded_event_count = 0;
  auto executor = start_realtime(build_graph<BoundedSubscriptionGraph>());
  auto view = executor.view();
  AsyncGraphExecutorRun runner{view};
  runner.join();
  require(
      std::find(bounded_states.begin(), bounded_states.end(),
                KafkaSubscriptionState::BoundedComplete) !=
          bounded_states.end(),
      (context.empty() ? Str{} : Str{context} + ": ") +
          "bounded Kafka subscription did not report completion" +
          (bounded_event.has_value()
               ? ": " + bundle_string(bounded_event, "category") + ": " +
                     bundle_string(bounded_event, "message")
               : ": states=" + std::to_string(bounded_states.size()) +
                     ", records=" + std::to_string(bounded_offsets.size())));
}

void test_timestamp_and_graph_start_positions() {
  {
    MockCluster cluster;
    cluster.create_topic(Str{"typed-timestamp"});
    const DateTime first = wall_now();
    const DateTime second = first + TimeDelta{1'000'000};
    cluster.seed_record(Str{"typed-timestamp"}, Bytes{"before"}, std::nullopt,
                        {}, 0, first);
    cluster.seed_record(Str{"typed-timestamp"}, Bytes{"after"}, std::nullopt,
                        {}, 0, second);
    production_config = make_service_config({cluster.bootstrap_servers()},
                                            Str{"typed-timestamp-subscriber"});
    subscription_key = make_partition_subscription_key(
        {{Str{"typed-timestamp"}, Int{0}}}, Str{"typed-timestamp-group"},
        make_start_position(KafkaStartPositionKind::Timestamp,
                            KafkaOffsetFallback::Earliest,
                            first + TimeDelta{500'000}),
        make_stop_position(KafkaStopPositionKind::Snapshot),
        KafkaCommitMode::Explicit, Str{"typed-timestamp-subscription"});
    run_bounded_subscription();
    require(bounded_offsets == std::vector<Int>{Int{0}, Int{1}},
            "unresolved timestamp start did not apply its explicit earliest "
            "fallback");
  }

  {
    MockCluster cluster;
    cluster.create_topic(Str{"typed-graph-start"});
    const DateTime now = wall_now();
    cluster.seed_record(Str{"typed-graph-start"}, Bytes{"before"}, std::nullopt,
                        {}, 0, now - TimeDelta{1'000'000});
    cluster.seed_record(Str{"typed-graph-start"}, Bytes{"after"}, std::nullopt,
                        {}, 0, now + TimeDelta{5'000'000});
    production_config = make_service_config(
        {cluster.bootstrap_servers()}, Str{"typed-graph-start-subscriber"});
    subscription_key = make_partition_subscription_key(
        {{Str{"typed-graph-start"}, Int{0}}}, Str{"typed-graph-start-group"},
        make_start_position(KafkaStartPositionKind::GraphStartTime,
                            KafkaOffsetFallback::Latest),
        make_stop_position(KafkaStopPositionKind::Snapshot),
        KafkaCommitMode::Explicit, Str{"typed-graph-start-subscription"});
    run_bounded_subscription();
    require(bounded_offsets.empty(), "unresolved graph-start position did not "
                                     "apply its explicit latest fallback");
  }
}

void test_subscription_removal_and_readd_uses_a_fresh_assignment_generation() {
  MockCluster cluster;
  cluster.create_topic(Str{"readd-a"});
  cluster.create_topic(Str{"readd-b"});
  cluster.seed_record(Str{"readd-a"}, Bytes{"a"});
  cluster.seed_record(Str{"readd-b"}, Bytes{"b"});
  production_config = make_service_config({cluster.bootstrap_servers()},
                                          Str{"readded-subscription"});

  const auto make_key = [](Str topic, Str identity) {
    return hgraph::kafka::subscription_key()
        .topics({topic})
        .group_id(Str{"readded-subscription-group"})
        .assignment_mode(KafkaAssignmentMode::Independent)
        .start(make_start_position(KafkaStartPositionKind::Earliest))
        .stop(make_stop_position(KafkaStopPositionKind::Snapshot))
        .commit_mode(KafkaCommitMode::Explicit)
        .sharing_identity(std::move(identity))
        .build();
  };
  Value first = make_key(Str{"readd-a"}, Str{"readd-a"});
  Value second = make_key(Str{"readd-b"}, Str{"readd-b"});

  subscription_key_sender.reset();
  subscription_commit_sender.reset();
  subscription_generations.reset();
  stale_commit_events.reset();
  first_readded_cursor = Value{};
  auto executor = start_realtime(build_graph<ReaddedSubscriptionGraph>(),
                                 TimeDelta{20'000'000});
  auto view = executor.view();
  AsyncGraphExecutorRun runner{view};
  auto sender = subscription_key_sender.await();
  auto commit_sender = subscription_commit_sender.await();
  require(sender.has_value(),
          "dynamic subscription-key push source did not start");
  require(commit_sender.has_value(),
          "dynamic Kafka commit push source did not start");

  sender->send(first.clone());
  require(subscription_generations.await(1),
          "first subscription assignment did not produce a cursor");
  sender->send(second.clone());
  require(subscription_generations.await(2),
          "replacement subscription assignment did not produce a cursor");
  sender->send(first.clone());
  require(subscription_generations.await(3),
          "re-added subscription assignment did not produce a cursor");
  commit_sender->send(first_readded_cursor.clone());
  require(stale_commit_events.await(1), "a cursor from the removed assignment "
                                        "was not rejected with a typed event");

  view.request_stop();
  runner.join();
  sender.reset();
  commit_sender.reset();
  subscription_key_sender.reset();
  subscription_commit_sender.reset();

  const auto generations = subscription_generations.values();
  require(
      generations.size() >= 3 && generations[0].first == Str{"readd-a"} &&
          generations[1].first == Str{"readd-b"} &&
          generations[2].first == Str{"readd-a"} &&
          generations[0].second != generations[2].second,
      "a re-added subscription reused the former session's cursor generation "
      "(" +
          (generations.empty() ? Str{}
                               : generations[0].first + ':' +
                                     std::to_string(generations[0].second)) +
          "," +
          (generations.size() < 2 ? Str{}
                                  : generations[1].first + ':' +
                                        std::to_string(generations[1].second)) +
          "," +
          (generations.size() < 3 ? Str{}
                                  : generations[2].first + ':' +
                                        std::to_string(generations[2].second)) +
          ")");
}

void test_pattern_committed_fallback_and_key_filter() {
  MockCluster cluster;
  cluster.create_topic(Str{"typed-pattern-records"});
  cluster.seed_record(Str{"typed-pattern-records"}, Bytes{"ignored"},
                      Bytes{"other"});
  cluster.seed_record(Str{"typed-pattern-records"}, Bytes{"selected"},
                      Bytes{"selected"});
  production_config = make_service_config({cluster.bootstrap_servers()},
                                          Str{"typed-pattern-subscriber"});
  subscription_key = make_pattern_subscription_key(
      Str{"typed-pattern-.*"}, Str{"typed-pattern-group"},
      make_start_position(KafkaStartPositionKind::Committed,
                          KafkaOffsetFallback::Earliest),
      make_stop_position(KafkaStopPositionKind::Snapshot),
      KafkaCommitMode::Explicit, Str{"typed-pattern-subscription"},
      Str{"read_uncommitted"}, KafkaRecoveryClock::Arrival,
      KafkaMergePolicy::Partition, Bytes{"selected"});
  run_bounded_subscription();
  require(bounded_offsets == std::vector<Int>{Int{1}},
          "pattern selection, committed fallback, or exact key filtering was "
          "not preserved");
}

void test_independent_assignment_reads_every_partition() {
  MockCluster cluster;
  cluster.create_topic(Str{"typed-independent"}, 2);
  cluster.seed_record(Str{"typed-independent"}, Bytes{"partition-0"},
                      std::nullopt, {}, 0);
  cluster.seed_record(Str{"typed-independent"}, Bytes{"partition-1"},
                      std::nullopt, {}, 1);
  production_config = hgraph::kafka::service_config()
                          .bootstrap_servers({cluster.bootstrap_servers()})
                          .client_id(Str{"typed-independent-subscriber"})
                          .build();
  subscription_key =
      hgraph::kafka::subscription_key()
          .topics({Str{"typed-independent"}})
          .group_id(Str{"typed-independent-group"})
          .assignment_mode(KafkaAssignmentMode::Independent)
          .start(make_start_position(KafkaStartPositionKind::Earliest))
          .stop(make_stop_position(KafkaStopPositionKind::Snapshot))
          .sharing_identity(Str{"typed-independent-subscription"})
          .build();
  run_bounded_subscription();

  std::ranges::sort(bounded_payloads);
  require(bounded_payloads ==
              std::vector<Str>{Str{"partition-0"}, Str{"partition-1"}},
          "independent assignment did not read every selected topic partition");
}

void test_commit_modes_and_monotonic_explicit_commits() {
  {
    MockCluster cluster;
    cluster.create_topic(Str{"typed-graph-delivery"});
    cluster.seed_record(Str{"typed-graph-delivery"}, Bytes{"delivered"});
    production_config = make_service_config(
        {cluster.bootstrap_servers()}, Str{"typed-graph-delivery-subscriber"});
    subscription_key = make_subscription_key(
        {Str{"typed-graph-delivery"}}, Str{"typed-graph-delivery-group"},
        make_start_position(KafkaStartPositionKind::Earliest),
        make_stop_position(KafkaStopPositionKind::Snapshot),
        KafkaCommitMode::OnGraphDelivery,
        Str{"typed-graph-delivery-subscription"});
    run_bounded_subscription("OnGraphDelivery source");
    require(bounded_payloads == std::vector<Str>{Str{"delivered"}},
            "OnGraphDelivery subscription did not reach the graph");

    subscription_key =
        hgraph::kafka::subscription_key()
            .topics({Str{"typed-graph-delivery"}})
            .group_id(Str{"typed-graph-delivery-group"})
            .assignment_mode(KafkaAssignmentMode::Independent)
            .start(make_start_position(KafkaStartPositionKind::Committed,
                                       KafkaOffsetFallback::Earliest))
            .stop(make_stop_position(KafkaStopPositionKind::Snapshot))
            .commit_mode(KafkaCommitMode::Explicit)
            .sharing_identity(Str{"typed-graph-delivery-verification"})
            .build();
    run_bounded_subscription("OnGraphDelivery committed verification");
    require(
        bounded_payloads.empty(),
        "OnGraphDelivery did not commit the cursor published into the graph");
  }

  {
    MockCluster cluster;
    cluster.create_topic(Str{"typed-no-commit"});
    cluster.seed_record(Str{"typed-no-commit"}, Bytes{"uncommitted"});
    production_config = make_service_config({cluster.bootstrap_servers()},
                                            Str{"typed-no-commit-subscriber"});
    subscription_key = make_subscription_key(
        {Str{"typed-no-commit"}}, Str{"typed-no-commit-group"},
        make_start_position(KafkaStartPositionKind::Earliest),
        make_stop_position(KafkaStopPositionKind::Snapshot),
        KafkaCommitMode::None, Str{"typed-no-commit-subscription"});
    run_bounded_subscription("None source");

    subscription_key =
        hgraph::kafka::subscription_key()
            .topics({Str{"typed-no-commit"}})
            .group_id(Str{"typed-no-commit-group"})
            .assignment_mode(KafkaAssignmentMode::Independent)
            .start(make_start_position(KafkaStartPositionKind::Committed,
                                       KafkaOffsetFallback::Earliest))
            .stop(make_stop_position(KafkaStopPositionKind::Snapshot))
            .commit_mode(KafkaCommitMode::Explicit)
            .sharing_identity(Str{"typed-no-commit-verification"})
            .build();
    run_bounded_subscription("None committed verification");
    require(bounded_payloads == std::vector<Str>{Str{"uncommitted"}},
            "None commit mode advanced a managed Kafka position");
  }

  {
    MockCluster cluster;
    cluster.create_topic(Str{"typed-monotonic-commit"});
    cluster.seed_record(Str{"typed-monotonic-commit"}, Bytes{"zero"});
    cluster.seed_record(Str{"typed-monotonic-commit"}, Bytes{"one"});
    production_config = make_service_config({cluster.bootstrap_servers()},
                                            Str{"typed-monotonic-subscriber"});
    subscription_key = make_subscription_key(
        {Str{"typed-monotonic-commit"}}, Str{"typed-monotonic-group"},
        make_start_position(KafkaStartPositionKind::Earliest),
        make_stop_position(KafkaStopPositionKind::Snapshot),
        KafkaCommitMode::Explicit, Str{"typed-monotonic-subscription"});
    first_commit_cursor = Value{};

    auto executor = start_realtime(build_graph<MonotonicCommitGraph>(),
                                   TimeDelta{20'000'000});
    auto view = executor.view();
    AsyncGraphExecutorRun runner{view};
    runner.join();
    require(first_commit_cursor.has_value(),
            "explicit commit test did not observe the first Kafka cursor");

    subscription_key =
        hgraph::kafka::subscription_key()
            .topics({Str{"typed-monotonic-commit"}})
            .group_id(Str{"typed-monotonic-group"})
            .assignment_mode(KafkaAssignmentMode::Independent)
            .start(make_start_position(KafkaStartPositionKind::Committed,
                                       KafkaOffsetFallback::Fail))
            .stop(make_stop_position(KafkaStopPositionKind::Snapshot))
            .commit_mode(KafkaCommitMode::Explicit)
            .sharing_identity(Str{"typed-monotonic-verification"})
            .build();
    run_bounded_subscription("monotonic committed verification");
    require(
        bounded_payloads.empty(),
        "a lower explicit cursor moved the committed Kafka position backwards");
  }
}

void test_record_time_recovery_is_deterministically_merged() {
  MockCluster cluster;
  cluster.create_topic(Str{"typed-record-time"}, 2);
  const DateTime base = wall_now() + TimeDelta{1'000'000};
  cluster.seed_record(Str{"typed-record-time"}, Bytes{"late"}, std::nullopt, {},
                      0, base + TimeDelta{1'000'000});
  cluster.seed_record(Str{"typed-record-time"}, Bytes{"early"}, std::nullopt,
                      {}, 1, base);

  production_config = hgraph::kafka::service_config()
                          .bootstrap_servers({cluster.bootstrap_servers()})
                          .client_id(Str{"typed-record-time-subscriber"})
                          .build();
  subscription_key =
      hgraph::kafka::subscription_key()
          .partitions({
              {Str{"typed-record-time"}, Int{0}},
              {Str{"typed-record-time"}, Int{1}},
          })
          .group_id(Str{"typed-record-time-group"})
          .start(make_start_position(KafkaStartPositionKind::Earliest))
          .stop(make_stop_position(KafkaStopPositionKind::Snapshot))
          .recovery_clock(KafkaRecoveryClock::RecordTimestamp)
          .merge_policy(KafkaMergePolicy::TimestampTopicPartitionOffset)
          .sharing_identity(Str{"typed-record-time-subscription"})
          .build();
  run_bounded_subscription();

  require(bounded_payloads == std::vector<Str>{Str{"early"}, Str{"late"}},
          "record-time recovery did not apply deterministic "
          "timestamp/topic/partition/offset ordering");
  require(
      bounded_evaluation_times.size() == 2 &&
          bounded_evaluation_times[0] < bounded_evaluation_times[1],
      "record-time recovery did not emit records on increasing graph times");
}

void test_record_time_recovery_hands_off_before_live_records() {
  MockCluster cluster;
  cluster.create_topic(Str{"typed-record-time-live"});
  const DateTime history_time{
      std::chrono::duration_cast<TimeDelta>(
          std::chrono::duration_cast<std::chrono::milliseconds>(
              wall_now().time_since_epoch())) +
      TimeDelta{2'000'000}};
  cluster.seed_record(Str{"typed-record-time-live"}, Bytes{"history"},
                      std::nullopt, {}, 0, history_time);

  production_config = hgraph::kafka::service_config()
                          .bootstrap_servers({cluster.bootstrap_servers()})
                          .client_id(Str{"typed-record-time-live-subscriber"})
                          .build();
  subscription_key =
      hgraph::kafka::subscription_key()
          .partitions({{Str{"typed-record-time-live"}, Int{0}}})
          .group_id(Str{"typed-record-time-live-group"})
          .start(make_start_position(KafkaStartPositionKind::Earliest))
          .stop(make_stop_position(KafkaStopPositionKind::Unbounded))
          .commit_mode(KafkaCommitMode::None)
          .recovery_clock(KafkaRecoveryClock::RecordTimestamp)
          .merge_policy(KafkaMergePolicy::TimestampTopicPartitionOffset)
          .sharing_identity(Str{"typed-record-time-live-subscription"})
          .build();
  record_time_recovery_started.reset();
  recovery_live_records.clear();

  auto executor = start_realtime(build_graph<RecordTimeRecoveryLiveGraph>(),
                                 TimeDelta{8'000'000});
  auto view = executor.view();
  AsyncGraphExecutorRun runner{view};
  require(record_time_recovery_started.await(1),
          "record-time subscription did not enter recovery");
  cluster.seed_record(Str{"typed-record-time-live"}, Bytes{"live"});
  runner.join();

  require(recovery_live_records.size() == 2,
          "record-time handoff did not deliver history and live records");
  require(recovery_live_records[0].first == Str{"history"} &&
              recovery_live_records[1].first == Str{"live"},
          "a live record overtook timestamped recovery");
  require(recovery_live_records[0].second == history_time,
          "record-time history did not retain its Kafka timestamp");
  require(recovery_live_records[1].second > recovery_live_records[0].second,
          "the live handoff did not follow the timestamped recovery tail");
}

void test_record_time_recovery_waits_for_independent_subscriptions() {
  MockCluster cluster;
  cluster.create_topic(Str{"typed-recovery-cohort-early"});
  cluster.create_topic(Str{"typed-recovery-cohort-late"});
  const DateTime early_time{
      std::chrono::duration_cast<TimeDelta>(
          std::chrono::duration_cast<std::chrono::milliseconds>(
              wall_now().time_since_epoch())) +
      TimeDelta{1'000'000}};
  const DateTime late_time = early_time + TimeDelta{500'000};
  const DateTime second_early_time = early_time + TimeDelta{1'000'000};
  const DateTime second_late_time = early_time + TimeDelta{1'500'000};
  // Seed the later stream first. Consumer sessions recover independently, so
  // broker/poll timing must not let it enter the graph before the earlier
  // stream has established the same recovery cohort's complete merge set.
  cluster.seed_record(Str{"typed-recovery-cohort-late"}, Bytes{"late-1"},
                      std::nullopt, {}, 0, late_time);
  cluster.seed_record(Str{"typed-recovery-cohort-late"}, Bytes{"late-2"},
                      std::nullopt, {}, 0, second_late_time);
  cluster.seed_record(Str{"typed-recovery-cohort-early"}, Bytes{"early-1"},
                      std::nullopt, {}, 0, early_time);
  cluster.seed_record(Str{"typed-recovery-cohort-early"}, Bytes{"early-2"},
                      std::nullopt, {}, 0, second_early_time);

  production_config = hgraph::kafka::service_config()
                          .bootstrap_servers({cluster.bootstrap_servers()})
                          .client_id(Str{"typed-recovery-cohort"})
                          .ingress_limits(Int{2}, Int{4096})
                          .build();
  subscription_key =
      hgraph::kafka::subscription_key()
          .partitions({{Str{"typed-recovery-cohort-early"}, Int{0}}})
          .group_id(Str{"typed-recovery-cohort-early"})
          .start(make_start_position(KafkaStartPositionKind::Earliest))
          .stop(make_stop_position(KafkaStopPositionKind::Snapshot))
          .recovery_clock(KafkaRecoveryClock::RecordTimestamp)
          .merge_policy(KafkaMergePolicy::TimestampTopicPartitionOffset)
          .sharing_identity(Str{"typed-recovery-cohort-early"})
          .build();
  secondary_subscription_key =
      hgraph::kafka::subscription_key()
          .partitions({{Str{"typed-recovery-cohort-late"}, Int{0}}})
          .group_id(Str{"typed-recovery-cohort-late"})
          .start(make_start_position(KafkaStartPositionKind::Earliest))
          .stop(make_stop_position(KafkaStopPositionKind::Snapshot))
          .recovery_clock(KafkaRecoveryClock::RecordTimestamp)
          .merge_policy(KafkaMergePolicy::TimestampTopicPartitionOffset)
          .sharing_identity(Str{"typed-recovery-cohort-late"})
          .build();
  bounded_payloads.clear();
  multi_bounded_records.clear();
  multi_bounded_complete_count = 0;
  multi_bounded_failed_count = 0;

  auto executor = start_realtime(build_graph<MultiBoundedSubscriptionGraph>(),
                                 TimeDelta{5'000'000});
  auto view = executor.view();
  AsyncGraphExecutorRun runner{view};
  runner.join();

  Str observed_order;
  for (const auto &payload : bounded_payloads) {
    observed_order += observed_order.empty() ? payload : Str{", "} + payload;
  }
  require(bounded_payloads == std::vector<Str>{Str{"early-1"}, Str{"late-1"},
                                               Str{"early-2"}, Str{"late-2"}},
          "an independently recovered topic was replayed before the complete "
          "record-time cohort was available (observed: " +
              observed_order + ")");
  require(multi_bounded_records ==
              std::vector<std::pair<Str, DateTime>>{
                  {Str{"early-1"}, early_time},
                  {Str{"late-1"}, late_time},
                  {Str{"early-2"}, second_early_time},
                  {Str{"late-2"}, second_late_time}},
          "independent recovery streams did not retain their globally merged "
          "Kafka event times");
  require(multi_bounded_complete_count == 2,
          "the record-time recovery cohort did not complete both streams "
          "(completed=" +
              std::to_string(multi_bounded_complete_count) + ")");
}

void test_record_time_recovery_releases_a_failed_participant() {
  MockCluster cluster;
  cluster.create_topic(Str{"typed-recovery-failure-a"});
  cluster.create_topic(Str{"typed-recovery-failure-b"});
  const DateTime record_time{
      std::chrono::duration_cast<TimeDelta>(
          std::chrono::duration_cast<std::chrono::milliseconds>(
              wall_now().time_since_epoch())) +
      TimeDelta{1'000'000}};
  cluster.seed_record(Str{"typed-recovery-failure-a"}, Bytes{"a"}, std::nullopt,
                      {}, 0, record_time);

  production_config = hgraph::kafka::service_config()
                          .bootstrap_servers({cluster.bootstrap_servers()})
                          .client_id(Str{"typed-recovery-failure"})
                          .ingress_limits(Int{2}, Int{4096})
                          .build();
  subscription_key =
      hgraph::kafka::subscription_key()
          .partitions({{Str{"typed-recovery-failure-a"}, Int{0}}})
          .group_id(Str{"typed-recovery-failure-a"})
          .start(make_start_position(KafkaStartPositionKind::Earliest))
          .stop(make_stop_position(KafkaStopPositionKind::Snapshot))
          .recovery_clock(KafkaRecoveryClock::RecordTimestamp)
          .merge_policy(KafkaMergePolicy::TimestampTopicPartitionOffset)
          .sharing_identity(Str{"typed-recovery-failure-a"})
          .build();
  secondary_subscription_key =
      hgraph::kafka::subscription_key()
          .partitions({{Str{"typed-recovery-failure-b"}, Int{99}}})
          .group_id(Str{"typed-recovery-failure-b"})
          .start(make_start_position(KafkaStartPositionKind::Earliest))
          .stop(make_stop_position(KafkaStopPositionKind::Snapshot))
          .recovery_clock(KafkaRecoveryClock::RecordTimestamp)
          .merge_policy(KafkaMergePolicy::TimestampTopicPartitionOffset)
          .sharing_identity(Str{"typed-recovery-failure-b"})
          .build();
  bounded_payloads.clear();
  multi_bounded_records.clear();
  multi_bounded_complete_count = 0;
  multi_bounded_failed_count = 0;

  auto executor = start_realtime(build_graph<MultiBoundedSubscriptionGraph>(),
                                 TimeDelta{5'000'000});
  auto view = executor.view();
  AsyncGraphExecutorRun runner{view};
  runner.join();

  require(bounded_payloads.size() == 1,
          "the healthy recovery participant remained blocked after its peer "
          "failed (records=" +
              std::to_string(bounded_payloads.size()) +
              ", complete=" + std::to_string(multi_bounded_complete_count) +
              ", failed=" + std::to_string(multi_bounded_failed_count) + ")");
  require(multi_bounded_complete_count == 1 && multi_bounded_failed_count == 1,
          "the failed recovery participant did not release its cohort");
}

void test_graph_lifetime_stop_is_bounded_in_simulation() {
  MockCluster cluster;
  cluster.create_topic(Str{"simulation-record-time"});
  const DateTime graph_start{std::chrono::duration_cast<TimeDelta>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          wall_now().time_since_epoch()))};
  const DateTime record_time = graph_start + TimeDelta{1'000'000};
  cluster.seed_record(Str{"simulation-record-time"},
                      Bytes{"simulation-history"}, std::nullopt, {}, 0,
                      record_time);

  production_config = hgraph::kafka::service_config()
                          .bootstrap_servers({cluster.bootstrap_servers()})
                          .client_id(Str{"simulation-record-time-subscriber"})
                          .build();
  subscription_key =
      hgraph::kafka::subscription_key()
          .partitions({{Str{"simulation-record-time"}, Int{0}}})
          .group_id(Str{"simulation-record-time-group"})
          .start(make_start_position(KafkaStartPositionKind::Earliest))
          .stop(make_stop_position(KafkaStopPositionKind::GraphLifetime))
          .recovery_clock(KafkaRecoveryClock::RecordTimestamp)
          .merge_policy(KafkaMergePolicy::TimestampTopicPartitionOffset)
          .sharing_identity(Str{"simulation-record-time-subscription"})
          .build();
  bounded_offsets.clear();
  bounded_states.clear();
  bounded_payloads.clear();
  bounded_evaluation_times.clear();
  bounded_event = Value{};
  bounded_event_count = 0;

  static_cast<void>(run_graph(build_graph<BoundedSubscriptionGraph>(),
                              graph_start, record_time + TimeDelta{1'000'000},
                              GraphExecutorMode::Simulation));

  require(
      bounded_payloads == std::vector<Str>{Str{"simulation-history"}},
      "graph-lifetime simulation completed before record-time history became available");
  require(
      bounded_evaluation_times == std::vector<DateTime>{record_time},
      "graph-lifetime simulation did not use Kafka record time as the historical graph time");
}

void test_multiple_simulation_subscriptions_replay_at_record_time() {
  MockCluster cluster;
  cluster.create_topic(Str{"simulation-preload-a"});
  cluster.create_topic(Str{"simulation-preload-b"});
  const DateTime graph_start{std::chrono::duration_cast<TimeDelta>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          wall_now().time_since_epoch()))};
  const DateTime record_time = graph_start + TimeDelta{1'000'000};
  std::vector<Str> expected_payloads;
  for (Int offset = 0; offset < 3; ++offset) {
    const Str first = Str{"first-"} + std::to_string(offset);
    const Str second = Str{"second-"} + std::to_string(offset);
    cluster.seed_record(Str{"simulation-preload-a"}, Bytes{first}, std::nullopt,
                        {}, 0, record_time + offset * MIN_TD);
    cluster.seed_record(Str{"simulation-preload-b"}, Bytes{second},
                        std::nullopt, {}, 0, record_time + offset * MIN_TD);
    expected_payloads.push_back(first);
    expected_payloads.push_back(second);
  }

  production_config = hgraph::kafka::service_config()
                          .bootstrap_servers({cluster.bootstrap_servers()})
                          .client_id(Str{"simulation-multi-preload"})
                          .ingress_limits(Int{3}, Int{4096})
                          .build();
  subscription_key =
      hgraph::kafka::subscription_key()
          .partitions({{Str{"simulation-preload-a"}, Int{0}}})
          .group_id(Str{"simulation-preload-a"})
          .start(make_start_position(KafkaStartPositionKind::Earliest))
          .stop(make_stop_position(KafkaStopPositionKind::Snapshot))
          .recovery_clock(KafkaRecoveryClock::RecordTimestamp)
          .merge_policy(KafkaMergePolicy::TimestampTopicPartitionOffset)
          .sharing_identity(Str{"simulation-preload-a"})
          .build();
  secondary_subscription_key =
      hgraph::kafka::subscription_key()
          .partitions({{Str{"simulation-preload-b"}, Int{0}}})
          .group_id(Str{"simulation-preload-b"})
          .start(make_start_position(KafkaStartPositionKind::Earliest))
          .stop(make_stop_position(KafkaStopPositionKind::Snapshot))
          .recovery_clock(KafkaRecoveryClock::RecordTimestamp)
          .merge_policy(KafkaMergePolicy::TimestampTopicPartitionOffset)
          .sharing_identity(Str{"simulation-preload-b"})
          .build();
  bounded_payloads.clear();
  multi_bounded_records.clear();
  multi_bounded_complete_count = 0;
  multi_bounded_failed_count = 0;

  static_cast<void>(run_graph(build_graph<MultiBoundedSubscriptionGraph>(),
                              graph_start, record_time + TimeDelta{1'000'000},
                              GraphExecutorMode::Simulation));

  std::ranges::sort(bounded_payloads);
  std::ranges::sort(expected_payloads);
  require(bounded_payloads == expected_payloads,
          "multiple simulation subscriptions did not preload before the "
          "graph began draining their shared ingress queue");
  std::ranges::sort(multi_bounded_records, {},
                    &decltype(multi_bounded_records)::value_type::first);
  require(multi_bounded_records.size() == 6,
          "multiple simulation subscriptions did not replay every "
          "timestamped record");
  for (const auto &[payload, evaluation_time] : multi_bounded_records) {
    const auto separator = payload.find('-');
    require(separator != Str::npos,
            "timestamp replay test produced an unexpected payload");
    const auto offset =
        static_cast<Int>(std::stoll(payload.substr(separator + 1)));
    require(evaluation_time == record_time + offset * MIN_TD,
            "multiple simulation subscriptions replayed '" + payload +
                "' at drain time instead of its Kafka record timestamp");
  }
  require(multi_bounded_complete_count == 2,
          "simulation did not complete both bounded subscriptions");
}

void test_real_broker_publish_subscribe_and_commit_round_trip() {
  const char *bootstrap_value =
      std::getenv("HGRAPH_KAFKA_INTEGRATION_BOOTSTRAP");
  const char *topic_value = std::getenv("HGRAPH_KAFKA_INTEGRATION_TOPIC");
  if (bootstrap_value == nullptr && topic_value == nullptr) {
    return;
  }
  require(
      bootstrap_value != nullptr && topic_value != nullptr,
      "real Kafka integration requires both HGRAPH_KAFKA_INTEGRATION_BOOTSTRAP "
      "and HGRAPH_KAFKA_INTEGRATION_TOPIC");

  const Str bootstrap{bootstrap_value};
  const Str topic{topic_value};
  const Str group = Str{"hgraph-kafka-integration-"} + topic;
  production_topic = topic;
  produced_record = make_produce_record(
      Bytes{"real-broker-value"}, Bytes{"real-broker-key"},
      {{Str{"duplicate"}, Bytes{"one"}}, {Str{"duplicate"}, std::nullopt}},
      std::nullopt, Int{0}, Str{"real-broker-token"});
  production_config = hgraph::kafka::service_config()
                          .bootstrap_servers({bootstrap})
                          .client_id(Str{"hgraph-kafka-real-broker"})
                          .build();
  production_delivery = Value{};
  production_delivery_count = 0;

  auto publish_executor = start_realtime(build_graph<ProductionPublishGraph>());
  auto publish_view = publish_executor.view();
  AsyncGraphExecutorRun publish_runner{publish_view};
  publish_runner.join();
  require(production_delivery_count == 1,
          "real Kafka broker did not return a delivery report");
  require(production_delivery.view()
                  .as_bundle()
                  .at("status")
                  .checked_as<KafkaDeliveryStatus>() ==
              KafkaDeliveryStatus::Delivered,
          "real Kafka broker rejected the produced record");

  subscription_key =
      hgraph::kafka::subscription_key()
          .topics({topic})
          .group_id(group)
          .start(make_start_position(KafkaStartPositionKind::Earliest))
          .stop(make_stop_position(KafkaStopPositionKind::Snapshot))
          .commit_mode(KafkaCommitMode::Explicit)
          .sharing_identity(group)
          .build();
  bounded_offsets.clear();
  bounded_states.clear();
  bounded_payloads.clear();
  bounded_evaluation_times.clear();
  bounded_event = Value{};
  bounded_event_count = 0;
  auto commit_executor = start_realtime(
      build_graph<BoundedSubscriptionCommitGraph>(), TimeDelta{20'000'000});
  auto commit_view = commit_executor.view();
  AsyncGraphExecutorRun commit_runner{commit_view};
  commit_runner.join();
  require(bounded_payloads == std::vector<Str>{Str{"real-broker-value"}},
          "real Kafka broker publish/subscribe round trip changed or lost the "
          "payload (records=" +
              std::to_string(bounded_payloads.size()) +
              (bounded_payloads.empty()
                   ? Str{}
                   : Str{", first='"} + bounded_payloads.front() + "'") +
              ")");

  subscription_key =
      hgraph::kafka::subscription_key()
          .topics({topic})
          .group_id(group)
          .start(make_start_position(KafkaStartPositionKind::Committed,
                                     KafkaOffsetFallback::Fail))
          .stop(make_stop_position(KafkaStopPositionKind::Snapshot))
          .commit_mode(KafkaCommitMode::Explicit)
          .sharing_identity(group)
          .build();
  bounded_offsets.clear();
  bounded_states.clear();
  bounded_payloads.clear();
  bounded_evaluation_times.clear();
  bounded_event = Value{};
  bounded_event_count = 0;
  auto verify_executor = start_realtime(build_graph<BoundedSubscriptionGraph>(),
                                        TimeDelta{20'000'000});
  auto verify_view = verify_executor.view();
  AsyncGraphExecutorRun verify_runner{verify_view};
  verify_runner.join();
  require(
      std::ranges::find(bounded_states,
                        KafkaSubscriptionState::BoundedComplete) !=
          bounded_states.end(),
      "real Kafka committed subscription did not reach its snapshot boundary");
  require(bounded_payloads.empty(), "real Kafka explicit commit was not used "
                                    "as the next subscription start position");

  production_topic = Str{"native-out"};
  initialize_values();
}
} // namespace

int main() {
  try {
    hgraph::stdlib::register_standard_operators();
    const auto release_state = hgraph::make_scope_exit(release_test_state);
    initialize_values();
    test_public_value_validation_and_producer_configuration();
    test_simulation_rejects_publish_and_commit_work();
    test_subscription_boundary();
    test_publish_boundary();
    test_multiple_publishers_and_dynamic_topics();
    test_subscription_sharing_is_explicit_and_duplicate_registration_fails();
    test_push_backlogs_drain_one_value_per_graph_cycle();
    test_service_can_start_and_stop_repeatedly();
    test_subscription_removal_and_readd_uses_a_fresh_assignment_generation();
    test_ingress_is_bounded_before_the_graph_drains();
    test_librdkafka_ingress_pauses_and_resumes_at_watermarks();
    test_commit_and_event_boundaries();
    test_concurrent_engines_are_independent();
    test_librdkafka_publish_path();
    test_librdkafka_delivery_failures_are_typed();
    test_librdkafka_subscription_path();
    test_graph_lifetime_stop_remains_live_in_real_time();
    test_permanent_consumer_failure_stops_the_graph();
    test_typed_explicit_partition_boundaries();
    test_latest_snapshot_is_empty();
    test_timestamp_and_graph_start_positions();
    test_pattern_committed_fallback_and_key_filter();
    test_independent_assignment_reads_every_partition();
    test_commit_modes_and_monotonic_explicit_commits();
    test_record_time_recovery_is_deterministically_merged();
    test_record_time_recovery_hands_off_before_live_records();
    test_record_time_recovery_waits_for_independent_subscriptions();
    test_record_time_recovery_releases_a_failed_participant();
    test_graph_lifetime_stop_is_bounded_in_simulation();
    test_multiple_simulation_subscriptions_replay_at_record_time();
    test_real_broker_publish_subscribe_and_commit_round_trip();
    std::cout << "hgraph-kafka service tests passed\n";
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "hgraph-kafka service test failed: " << error.what() << '\n';
    return 1;
  }
}
