#include <hgraph/kafka/testing/fake_broker.h>

#include "detail/service_bridge.h"

#include <hgraph/types/metadata/value_plan_factory.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/value/value_builder.h>

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
  FakeRuntime(FakeBrokerPtr broker,
              ::hgraph::kafka::detail::ServiceBridgeHandle bridge)
      : broker_{std::move(broker)}, bridge_{std::move(bridge)} {
    if (!broker_ || !bridge_.value) {
      throw std::invalid_argument(
          "Kafka fake service requires a broker and bridge");
    }
  }

  void start();
  void stop() noexcept;
  void subscriptions(Value delta);
  void publish(Int request_id, Str topic, Value record);
  void commit(Value cursor);

private:
  FakeBrokerPtr broker_{};
  ::hgraph::kafka::detail::ServiceBridgeHandle bridge_{};
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
template <typename T> [[nodiscard]] Value atomic(T value) {
  static_cast<void>(scalar_descriptor<T>::value_meta());
  return Value{std::move(value)};
}

template <typename Schema>
[[nodiscard]] Value
bundle(std::vector<std::pair<std::string_view, Value>> fields) {
  BundleBuilder builder{ValuePlanFactory::instance().type_for(
      scalar_descriptor<Schema>::value_meta())};
  for (auto &[name, field] : fields) {
    builder.set(name, std::move(field));
  }
  return builder.build();
}

[[nodiscard]] Str optional_string_field(const Value &record,
                                        std::string_view name) {
  const auto field = record.view().as_bundle().at(name);
  return field.data() != nullptr ? field.checked_as<Str>() : Str{};
}

[[nodiscard]] Value subscription_envelope(Value key, Value record, Value cursor,
                                          KafkaSubscriptionState state) {
  return bundle<::hgraph::kafka::detail::KafkaSubscriptionEnvelope>({
      {"subscription_key", std::move(key)},
      {"record", std::move(record)},
      {"cursor", std::move(cursor)},
      {"state", atomic(state)},
  });
}

[[nodiscard]] Value delivery_envelope(Int request_id, Value report) {
  return bundle<::hgraph::kafka::detail::KafkaDeliveryEnvelope>({
      {"request_id", atomic(request_id)},
      {"report", std::move(report)},
  });
}

[[nodiscard]] Value event_envelope(Value event) {
  return bundle<::hgraph::kafka::detail::KafkaEventEnvelope>({
      {"event", std::move(event)},
      {"stop_graph", atomic(Bool{false})},
  });
}
} // namespace

struct FakeBroker::Impl {
  mutable std::mutex mutex{};
  mutable std::condition_variable changed{};
  std::shared_ptr<::hgraph::kafka::detail::ServiceBridge> bridge{};
  std::size_t attaches{};
  Int sequence{};
  std::vector<Value> subscription_updates{};
  std::vector<FakePublishedRecord> publications{};
  std::vector<Value> commits{};
};

struct detail::FakeRuntimeAccess {
  static void
  attach(FakeBroker &broker,
         std::shared_ptr<::hgraph::kafka::detail::ServiceBridge> bridge) {
    {
      std::lock_guard lock{broker.impl_->mutex};
      broker.impl_->bridge = std::move(bridge);
      ++broker.impl_->attaches;
    }
    broker.impl_->changed.notify_all();
  }

  static void detach(FakeBroker &broker) noexcept {
    {
      std::lock_guard lock{broker.impl_->mutex};
      broker.impl_->bridge.reset();
    }
    broker.impl_->changed.notify_all();
  }

  static void subscriptions(FakeBroker &broker, Value delta) {
    {
      std::lock_guard lock{broker.impl_->mutex};
      broker.impl_->subscription_updates.push_back(std::move(delta));
    }
    broker.impl_->changed.notify_all();
  }

  static void publish(FakeBroker &broker, Int request_id, Str topic,
                      Value record) {
    std::shared_ptr<::hgraph::kafka::detail::ServiceBridge> bridge;
    Int sequence{};
    Str user_token = optional_string_field(record, "user_token");
    {
      std::lock_guard lock{broker.impl_->mutex};
      if (!broker.impl_->bridge) {
        throw std::logic_error(
            "Kafka fake broker is not attached to a running graph");
      }
      sequence = ++broker.impl_->sequence;
      broker.impl_->publications.push_back(
          FakePublishedRecord{request_id, topic, record.clone()});
      bridge = broker.impl_->bridge;
    }
    broker.impl_->changed.notify_all();

    Value report =
        make_delivery_report(std::move(user_token), sequence, std::move(topic),
                             KafkaDeliveryStatus::Delivered, Int{0}, sequence);
    if (!bridge->push(::hgraph::kafka::detail::OutputChannel::Delivery,
                      delivery_envelope(request_id, std::move(report)), 1)) {
      throw std::overflow_error("Kafka fake delivery queue is full");
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
  return impl_->changed.wait_for(
      lock, timeout, [&] { return static_cast<bool>(impl_->bridge); });
}

bool FakeBroker::wait_until_detached(std::chrono::milliseconds timeout) const {
  std::unique_lock lock{impl_->mutex};
  return impl_->changed.wait_for(lock, timeout, [&] { return !impl_->bridge; });
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

  std::shared_ptr<::hgraph::kafka::detail::ServiceBridge> bridge;
  {
    std::lock_guard lock{impl_->mutex};
    if (!impl_->bridge) {
      throw std::logic_error(
          "Kafka fake broker is not attached to a running graph");
    }
    bridge = impl_->bridge;
  }
  if (!bridge->push(::hgraph::kafka::detail::OutputChannel::Subscription,
                    subscription_envelope(std::move(subscription_key),
                                          std::move(record), std::move(cursor),
                                          state),
                    1)) {
    throw std::overflow_error("Kafka fake subscription queue is full");
  }
}

void FakeBroker::emit_event(Value event) {
  if (event.schema() != scalar_descriptor<KafkaEvent>::value_meta()) {
    throw std::invalid_argument("Kafka fake event has the wrong schema");
  }
  std::shared_ptr<::hgraph::kafka::detail::ServiceBridge> bridge;
  {
    std::lock_guard lock{impl_->mutex};
    if (!impl_->bridge) {
      throw std::logic_error(
          "Kafka fake broker is not attached to a running graph");
    }
    bridge = impl_->bridge;
  }
  if (!bridge->push(::hgraph::kafka::detail::OutputChannel::Event,
                    event_envelope(std::move(event)), 1)) {
    throw std::overflow_error("Kafka fake event queue is full");
  }
}

void detail::FakeRuntime::start() {
  bridge_.value->start();
  FakeRuntimeAccess::attach(*broker_, bridge_.value);
  attached_ = true;
}

void detail::FakeRuntime::stop() noexcept {
  if (!attached_) {
    return;
  }
  bridge_.value->stop();
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
struct FakeRuntimeNode {
  static constexpr auto name = "kafka_fake_runtime";
  using signature_args = std::tuple<
      In<"subscriptions", TSS<KafkaSubscriptionKey>, InputValidity::Unchecked>,
      In<"publish", TSD<Int, KafkaPublishRequest>, InputValidity::Unchecked>,
      In<"commits", TSD<Int, TS<KafkaCursor>>, InputValidity::Unchecked>,
      Scalar<"broker", detail::FakeBrokerHandle>,
      Scalar<"bridge", ::hgraph::kafka::detail::ServiceBridgeHandle>,
      State<detail::FakeRuntimeHandle>>;

  static void
  start(Scalar<"broker", detail::FakeBrokerHandle> broker,
        Scalar<"bridge", ::hgraph::kafka::detail::ServiceBridgeHandle> bridge,
        State<detail::FakeRuntimeHandle> state) {
    auto runtime = std::make_shared<detail::FakeRuntime>(broker.value().value,
                                                         bridge.value());
    runtime->start();
    try {
      state.set(detail::FakeRuntimeHandle{runtime});
    } catch (...) {
      runtime->stop();
      throw;
    }
  }

  static void
  eval(In<"subscriptions", TSS<KafkaSubscriptionKey>, InputValidity::Unchecked>
           subscriptions,
       In<"publish", TSD<Int, KafkaPublishRequest>, InputValidity::Unchecked>
           publish_requests,
       In<"commits", TSD<Int, TS<KafkaCursor>>, InputValidity::Unchecked>
           commits,
       Scalar<"bridge", ::hgraph::kafka::detail::ServiceBridgeHandle> bridge,
       State<detail::FakeRuntimeHandle> state) {
    auto runtime = state.get().value;
    if (!runtime) {
      throw std::logic_error("Kafka fake runtime evaluated before start");
    }

    if (subscriptions.modified()) {
      const auto &erased = static_cast<const TSSInputView &>(subscriptions);
      for (const auto key : erased.removed()) {
        if (!bridge.value().value->erase_subscription(key.clone())) {
          throw std::overflow_error(
              "Kafka fake subscription-removal queue is full");
        }
      }
      runtime->subscriptions(subscriptions.delta().clone());
    }

    if (publish_requests.modified()) {
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
        runtime->publish(request_id_view.checked_as<Int>(), topic.value(),
                         record.base().value().clone());
      }
    }

    if (commits.modified()) {
      for (const auto &[request_id, cursor] : commits.modified_items()) {
        static_cast<void>(request_id);
        if (cursor.valid() && cursor.modified()) {
          runtime->commit(cursor.base().value().clone());
        }
      }
    }
  }

  static void stop(State<detail::FakeRuntimeHandle> state) {
    if (auto runtime = state.get().value) {
      runtime->stop();
    }
    state.set(detail::FakeRuntimeHandle{});
  }
};

struct KafkaFakeServiceImpl {
  static constexpr auto name = "kafka_fake_service_impl";

  static void compose(Wiring &w, Scalar<"config", Value> config,
                      Scalar<"broker", detail::FakeBrokerHandle> broker,
                      Scalar<"path", Str> path) {
    register_kafka_types();
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

    const auto config_fields = config.value().view().as_bundle();
    const auto consumer = config_fields.at("consumer_defaults").as_bundle();
    const auto producer = config_fields.at("producer").as_bundle();
    const auto ingress_records = static_cast<std::size_t>(
        consumer.at("ingress_record_limit").checked_as<Int>());
    const auto ingress_bytes = static_cast<std::size_t>(
        consumer.at("ingress_byte_limit").checked_as<Int>());
    const auto outbound_records = static_cast<std::size_t>(
        producer.at("outbound_record_limit").checked_as<Int>());
    const auto outbound_bytes = static_cast<std::size_t>(
        producer.at("outbound_byte_limit").checked_as<Int>());

    ::hgraph::kafka::detail::ServiceBridgeHandle bridge{
        std::make_shared<::hgraph::kafka::detail::ServiceBridge>(
            ::hgraph::kafka::detail::OutputLimits{ingress_records,
                                                  ingress_bytes},
            ::hgraph::kafka::detail::OutputLimits{outbound_records,
                                                  outbound_bytes},
            ::hgraph::kafka::detail::OutputLimits{1024, 1024 * 1024})};
    auto outputs = ::hgraph::kafka::detail::wire_service_outputs(w, bridge);

    static_cast<void>(wire<FakeRuntimeNode>(w, subscription_keys,
                                            publish_requests, commit_requests,
                                            broker.value(), bridge));

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
