#include <hgraph/kafka/testing/fake_broker.h>

#include "detail/service_transport.h"

#include <hgraph/types/static_node.h>
#include <hgraph/util/scope.h>

#include <chrono>
#include <compare>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <ostream>
#include <stdexcept>
#include <tuple>
#include <utility>

namespace hgraph::kafka::testing::detail {
struct FakeBrokerHandle {
  FakeBrokerPtr value{};

  friend bool operator==(const FakeBrokerHandle &,
                         const FakeBrokerHandle &) noexcept = default;
  friend std::strong_ordering
  operator<=>(const FakeBrokerHandle &lhs,
              const FakeBrokerHandle &rhs) noexcept {
    return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
           reinterpret_cast<std::uintptr_t>(rhs.value.get());
  }
};

class FakeRuntime {
public:
  FakeRuntime(FakeBrokerPtr broker, PushSourceSender sender,
              ::hgraph::kafka::detail::KafkaTransportBindingsHandle bindings)
      : broker_{std::move(broker)}, sender_{std::move(sender)},
        bindings_{std::move(bindings)} {
    if (!broker_ || !sender_.valid() || !bindings_.value) {
      throw std::invalid_argument(
          "Kafka fake service requires a broker and transport sender");
    }
  }
  ~FakeRuntime() { stop(); }

  void start();
  void stop() noexcept;
  void subscriptions(Value delta);
  void publish(Int request_id, Str topic, Value record);
  void commit(Value cursor);

private:
  FakeBrokerPtr broker_{};
  PushSourceSender sender_{};
  ::hgraph::kafka::detail::KafkaTransportBindingsHandle bindings_{};
  bool attached_{false};
};

struct FakeRuntimeHandle {
  std::shared_ptr<FakeRuntime> value{};

  friend bool operator==(const FakeRuntimeHandle &,
                         const FakeRuntimeHandle &) noexcept = default;
  friend std::strong_ordering
  operator<=>(const FakeRuntimeHandle &lhs,
              const FakeRuntimeHandle &rhs) noexcept {
    return reinterpret_cast<std::uintptr_t>(lhs.value.get()) <=>
           reinterpret_cast<std::uintptr_t>(rhs.value.get());
  }
};

inline std::ostream &operator<<(std::ostream &stream,
                                const FakeBrokerHandle &value) {
  return stream << "FakeBrokerHandle(" << value.value.get() << ')';
}

inline std::ostream &operator<<(std::ostream &stream,
                                const FakeRuntimeHandle &value) {
  return stream << "FakeRuntimeHandle(" << value.value.get() << ')';
}
} // namespace hgraph::kafka::testing::detail

namespace std {
template <> struct hash<hgraph::kafka::testing::detail::FakeBrokerHandle> {
  size_t operator()(const hgraph::kafka::testing::detail::FakeBrokerHandle
                        &value) const noexcept {
    return hash<const void *>{}(value.value.get());
  }
};

template <> struct hash<hgraph::kafka::testing::detail::FakeRuntimeHandle> {
  size_t operator()(const hgraph::kafka::testing::detail::FakeRuntimeHandle
                        &value) const noexcept {
    return hash<const void *>{}(value.value.get());
  }
};
} // namespace std

namespace hgraph::static_schema_detail {
template <> struct scalar_name<kafka::testing::detail::FakeBrokerHandle> {
  static constexpr std::string_view value{
      "hgraph.kafka.testing::FakeBrokerHandle"};
};

template <> struct scalar_name<kafka::testing::detail::FakeRuntimeHandle> {
  static constexpr std::string_view value{
      "hgraph.kafka.testing::FakeRuntimeHandle"};
};
} // namespace hgraph::static_schema_detail

namespace hgraph::kafka::testing {
namespace {
[[nodiscard]] Str optional_string_field(const Value &record,
                                        std::string_view name) {
  const auto field = record.view().as_bundle().at(name);
  return field.data() != nullptr ? field.checked_as<Str>() : Str{};
}

[[nodiscard]] Value subscription_envelope(
    Value key, Value record, Value cursor, KafkaSubscriptionState state,
    const ::hgraph::kafka::detail::KafkaTransportBindings &bindings) {
  return ::hgraph::kafka::detail::subscription_transport_event(
      bindings, std::move(key), std::move(record), std::move(cursor), state);
}

[[nodiscard]] Value delivery_envelope(
    Int request_id, Value report,
    const ::hgraph::kafka::detail::KafkaTransportBindings &bindings) {
  return ::hgraph::kafka::detail::delivery_transport_event(bindings, request_id,
                                                           std::move(report));
}

[[nodiscard]] Value event_envelope(
    Value event,
    const ::hgraph::kafka::detail::KafkaTransportBindings &bindings) {
  return ::hgraph::kafka::detail::service_transport_event(bindings,
                                                          std::move(event));
}
} // namespace

struct FakeBroker::Impl {
  mutable std::mutex mutex{};
  mutable std::condition_variable changed{};
  PushSourceSender sender{};
  ::hgraph::kafka::detail::KafkaTransportBindingsHandle bindings{};
  std::size_t attaches{};
  Int sequence{};
  std::vector<Value> subscription_updates{};
  std::vector<FakePublishedRecord> publications{};
  std::vector<Value> commits{};
};

struct detail::FakeRuntimeAccess {
  static void
  attach(FakeBroker &broker, PushSourceSender sender,
         ::hgraph::kafka::detail::KafkaTransportBindingsHandle bindings) {
    {
      std::lock_guard lock{broker.impl_->mutex};
      broker.impl_->sender = std::move(sender);
      broker.impl_->bindings = std::move(bindings);
      ++broker.impl_->attaches;
    }
    broker.impl_->changed.notify_all();
  }

  static void detach(FakeBroker &broker) noexcept {
    {
      std::lock_guard lock{broker.impl_->mutex};
      broker.impl_->sender = PushSourceSender{};
      broker.impl_->bindings = {};
    }
    broker.impl_->changed.notify_all();
  }

  static void subscriptions(FakeBroker &broker, Value delta) {
    {
      std::lock_guard lock{broker.impl_->mutex};
      const auto fields = delta.view().as_bundle();
      const auto removed = fields.at("removed").as_set();
      for (const auto key : removed) {
        if (!broker.impl_->sender.try_send(
                ::hgraph::kafka::detail::subscription_removed_transport_event(
                    *broker.impl_->bindings.value, key.clone()))) {
          throw std::runtime_error(
              "Kafka fake graph stopped before subscription removal");
        }
      }
      broker.impl_->subscription_updates.push_back(std::move(delta));
    }
    broker.impl_->changed.notify_all();
  }

  static void publish(FakeBroker &broker, Int request_id, Str topic,
                      Value record) {
    PushSourceSender sender;
    ::hgraph::kafka::detail::KafkaTransportBindingsHandle bindings;
    Int sequence{};
    Str user_token = optional_string_field(record, "user_token");
    {
      std::lock_guard lock{broker.impl_->mutex};
      if (!broker.impl_->sender.valid()) {
        throw std::logic_error(
            "Kafka fake broker is not attached to a running graph");
      }
      sequence = ++broker.impl_->sequence;
      broker.impl_->publications.push_back(
          FakePublishedRecord{request_id, topic, record.clone()});
      sender = broker.impl_->sender;
      bindings = broker.impl_->bindings;
    }
    broker.impl_->changed.notify_all();

    Value report =
        make_delivery_report(std::move(user_token), sequence, std::move(topic),
                             KafkaDeliveryStatus::Delivered, Int{0}, sequence);
    if (!sender.send_blocking(delivery_envelope(request_id, std::move(report),
                                                *bindings.value))) {
      throw std::runtime_error("Kafka fake graph stopped before delivery");
    }
  }

  static void commit(FakeBroker &broker, Value cursor) {
    {
      std::lock_guard lock{broker.impl_->mutex};
      broker.impl_->commits.push_back(std::move(cursor));
    }
    broker.impl_->changed.notify_all();
  }
};

FakeBroker::FakeBroker() : impl_{std::make_unique<Impl>()} {}
FakeBroker::~FakeBroker() = default;

bool FakeBroker::wait_until_attached(std::chrono::milliseconds timeout) const {
  std::unique_lock lock{impl_->mutex};
  return impl_->changed.wait_for(lock, timeout,
                                 [&] { return impl_->sender.valid(); });
}

bool FakeBroker::wait_until_detached(std::chrono::milliseconds timeout) const {
  std::unique_lock lock{impl_->mutex};
  return impl_->changed.wait_for(lock, timeout,
                                 [&] { return !impl_->sender.valid(); });
}

bool FakeBroker::wait_for_subscription_updates(
    std::size_t count, std::chrono::milliseconds timeout) const {
  std::unique_lock lock{impl_->mutex};
  return impl_->changed.wait_for(lock, timeout, [&] {
    return impl_->subscription_updates.size() >= count;
  });
}

bool FakeBroker::wait_for_publications(
    std::size_t count, std::chrono::milliseconds timeout) const {
  std::unique_lock lock{impl_->mutex};
  return impl_->changed.wait_for(
      lock, timeout, [&] { return impl_->publications.size() >= count; });
}

bool FakeBroker::wait_for_commits(std::size_t count,
                                  std::chrono::milliseconds timeout) const {
  std::unique_lock lock{impl_->mutex};
  return impl_->changed.wait_for(
      lock, timeout, [&] { return impl_->commits.size() >= count; });
}

std::size_t FakeBroker::attach_count() const {
  std::lock_guard lock{impl_->mutex};
  return impl_->attaches;
}

std::vector<Value> FakeBroker::subscription_deltas() const {
  std::lock_guard lock{impl_->mutex};
  return impl_->subscription_updates;
}

std::vector<FakePublishedRecord> FakeBroker::published_records() const {
  std::lock_guard lock{impl_->mutex};
  return impl_->publications;
}

std::vector<Value> FakeBroker::committed_cursors() const {
  std::lock_guard lock{impl_->mutex};
  return impl_->commits;
}

void FakeBroker::emit_subscription(Value subscription_key, Value record,
                                   Value cursor, KafkaSubscriptionState state) {
  if (subscription_key.schema() !=
          scalar_descriptor<KafkaSubscriptionKey>::value_meta() ||
      record.schema() != scalar_descriptor<KafkaRecord>::value_meta() ||
      cursor.schema() != scalar_descriptor<KafkaCursor>::value_meta()) {
    throw std::invalid_argument(
        "Kafka fake subscription payload has the wrong schema");
  }

  {
    std::lock_guard lock{impl_->mutex};
    if (!impl_->sender.valid()) {
      throw std::logic_error(
          "Kafka fake broker is not attached to a running graph");
    }
    // The fake broker and subscription-removal sink serialize through this
    // mutex before entering the same unbounded FIFO sender.
    if (!impl_->sender.try_send(subscription_envelope(
            std::move(subscription_key), std::move(record), std::move(cursor),
            state, *impl_->bindings.value))) {
      throw std::runtime_error("Kafka fake graph stopped before subscription");
    }
  }
}

void FakeBroker::emit_event(Value event) {
  if (event.schema() != scalar_descriptor<KafkaEvent>::value_meta()) {
    throw std::invalid_argument("Kafka fake event has the wrong schema");
  }
  PushSourceSender sender;
  ::hgraph::kafka::detail::KafkaTransportBindingsHandle bindings;
  {
    std::lock_guard lock{impl_->mutex};
    if (!impl_->sender.valid()) {
      throw std::logic_error(
          "Kafka fake broker is not attached to a running graph");
    }
    sender = impl_->sender;
    bindings = impl_->bindings;
  }
  if (!sender.send_blocking(
          event_envelope(std::move(event), *bindings.value))) {
    throw std::runtime_error("Kafka fake graph stopped before event");
  }
}

void detail::FakeRuntime::start() {
  FakeRuntimeAccess::attach(*broker_, sender_, bindings_);
  attached_ = true;
}

void detail::FakeRuntime::stop() noexcept {
  if (!attached_) {
    return;
  }
  FakeRuntimeAccess::detach(*broker_);
  attached_ = false;
}

void detail::FakeRuntime::subscriptions(Value delta) {
  FakeRuntimeAccess::subscriptions(*broker_, std::move(delta));
}

void detail::FakeRuntime::publish(Int request_id, Str topic, Value record) {
  FakeRuntimeAccess::publish(*broker_, request_id, std::move(topic),
                             std::move(record));
}

void detail::FakeRuntime::commit(Value cursor) {
  FakeRuntimeAccess::commit(*broker_, std::move(cursor));
}

namespace {
[[nodiscard]] std::shared_ptr<detail::FakeRuntime>
fake_runtime(GlobalStateView global_state,
             Scalar<"runtime_key", Str> runtime_key) {
  const auto stored = global_state.get(runtime_key.value());
  if (!stored.valid()) {
    throw std::logic_error("Kafka fake command evaluated before runtime start");
  }
  auto runtime = stored.checked_as<detail::FakeRuntimeHandle>().value;
  if (!runtime) {
    throw std::logic_error("Kafka fake runtime resource is not configured");
  }
  return runtime;
}

void install_fake_runtime(GlobalStateView global_state,
                          std::string_view runtime_key,
                          std::shared_ptr<detail::FakeRuntime> runtime) {
  if (global_state.contains(runtime_key)) {
    throw std::logic_error("Kafka fake runtime was installed twice");
  }
  global_state.set(runtime_key,
                   Value{detail::FakeRuntimeHandle{std::move(runtime)}});
}

[[nodiscard]] std::shared_ptr<detail::FakeRuntime>
take_fake_runtime(GlobalStateView global_state, std::string_view runtime_key) {
  const auto stored = global_state.get(runtime_key);
  if (!stored.valid()) {
    return {};
  }
  auto runtime = stored.checked_as<detail::FakeRuntimeHandle>().value;
  static_cast<void>(global_state.erase(runtime_key));
  return runtime;
}

struct FakeTransportTag {};

[[nodiscard]] Port<TS<::hgraph::kafka::detail::KafkaTransportEventBatch>>
wire_fake_transport(
    Wiring &w, detail::FakeBrokerHandle broker,
    Str runtime_key,
    ::hgraph::kafka::detail::KafkaTransportBindingsHandle bindings) {
  return ::hgraph::kafka::detail::wire_transport_source<FakeTransportTag>(
      w,
      [broker = std::move(broker), runtime_key,
       bindings](PushSourceSender sender, const NodeView &node, DateTime) {
        auto task = std::make_shared<detail::FakeRuntime>(
            broker.value, std::move(sender), bindings);
        task->start();
        auto rollback = make_scope_exit<true>([&] { task->stop(); });
        install_fake_runtime(node.global_state(), runtime_key, task);
        rollback.release();
      },
      [runtime_key](const NodeView &node) {
        if (auto task = take_fake_runtime(node.global_state(), runtime_key)) {
          task->stop();
        }
      });
}

/** Fake graph-to-broker subscription boundary. It mirrors the production sink
 *  by forwarding only the TSS delta; cost is O(A + R) per modified tick. */
struct FakeSubscriptionNode {
  static constexpr auto name = "kafka_fake_subscription_commands";

  static void
  eval(In<"subscriptions", TSS<KafkaSubscriptionKey>, InputValidity::Unchecked>
           subscriptions,
       GlobalStateView global_state,
       Scalar<"runtime_key", Str> runtime_key) {
    if (!subscriptions.modified()) {
      return;
    }
    auto task = fake_runtime(global_state, runtime_key);
    task->subscriptions(subscriptions.delta().clone());
  }
};

/** Fake graph-to-broker publish boundary. It forwards valid modified records
 *  and preserves the production sink's O(M) per-tick traversal. */
struct FakePublishSink {
  static constexpr auto name = "kafka_fake_publish_commands";

  static void
  eval(In<"publish", TSD<Int, KafkaPublishRequest>, InputValidity::Unchecked>
           publish_requests,
       GlobalStateView global_state,
       Scalar<"runtime_key", Str> runtime_key) {
    if (!publish_requests.modified()) {
      return;
    }
    auto task = fake_runtime(global_state, runtime_key);
    for (const auto &[request_id_view, request] :
         publish_requests.modified_items()) {
      auto record = request.template field<"record">();
      auto topic = request.template field<"topic">();
      if (!record.modified() || !record.valid()) {
        continue;
      }
      if (!topic.valid()) {
        throw std::invalid_argument(
            "Kafka publish record requires a valid topic");
      }
      task->publish(request_id_view.checked_as<Int>(), topic.value(),
                    record.base().value().clone());
    }
  }
};

/** Fake graph-to-broker commit boundary. It forwards only modified valid
 *  cursors and costs O(M) per modified tick. */
struct FakeCommitSink {
  static constexpr auto name = "kafka_fake_commit_commands";

  static void
  eval(In<"commits", TSD<Int, TS<KafkaCursor>>, InputValidity::Unchecked>
           commits,
       GlobalStateView global_state,
       Scalar<"runtime_key", Str> runtime_key) {
    if (!commits.modified()) {
      return;
    }
    auto task = fake_runtime(global_state, runtime_key);
    for (const auto &[request_id, cursor] : commits.modified_items()) {
      static_cast<void>(request_id);
      if (cursor.valid() && cursor.modified()) {
        task->commit(cursor.base().value().clone());
      }
    }
  }
};

struct KafkaFakeServiceImpl {
  static constexpr auto name = "kafka_fake_service_impl";

  static void compose(Wiring &w, Scalar<"config", Value> config,
                      Scalar<"broker", detail::FakeBrokerHandle> broker,
                      Scalar<"path", Str> path) {
    register_kafka_types();
    static_cast<void>(
        scalar_descriptor<detail::FakeRuntimeHandle>::value_meta());
    if (config.value().schema() !=
        scalar_descriptor<KafkaServiceConfig>::value_meta()) {
      throw std::invalid_argument(
          "Kafka service implementation requires KafkaServiceConfig");
    }
    const auto binding = service::path(path.value());
    auto subscription_keys =
        service::impl_input<KafkaSubscriptionService>(w, binding);
    auto publish_requests =
        service::impl_input<KafkaPublishService>(w, binding);
    auto commit_requests = service::impl_input<KafkaCommitService>(w, binding);

    const Str runtime_key =
        "__hgraph.kafka.testing.runtime/" + path.value();
    const auto transport_bindings =
        ::hgraph::kafka::detail::make_transport_bindings();
    auto transport =
        wire_fake_transport(w, broker.value(), runtime_key,
                            transport_bindings);
    static_cast<void>(
        wire<FakeSubscriptionNode>(w, subscription_keys, runtime_key));
    auto outputs = ::hgraph::kafka::detail::wire_service_outputs(
        w, transport, transport_bindings);

    static_cast<void>(
        wire<FakePublishSink>(w, publish_requests, runtime_key));
    static_cast<void>(
        wire<FakeCommitSink>(w, commit_requests, runtime_key));

    service::impl_output<KafkaSubscriptionService>(w, binding,
                                                   outputs.subscriptions);
    service::impl_output<KafkaPublishService>(w, binding, outputs.deliveries);
    service::impl_output<KafkaEventService>(w, binding, outputs.events);
  }
};
} // namespace

void register_fake_service(Wiring &w, service::ServicePath path,
                           Value service_config, FakeBrokerPtr broker) {
  if (!broker) {
    throw std::invalid_argument("Kafka fake service requires a broker");
  }
  service::register_services<KafkaFakeServiceImpl, KafkaSubscriptionService,
                             KafkaPublishService, KafkaCommitService,
                             KafkaEventService>(
      w, std::move(path), std::move(service_config),
      detail::FakeBrokerHandle{std::move(broker)});
}
} // namespace hgraph::kafka::testing
