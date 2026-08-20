#include "impl/service_runtime.h"

#include <hgraph/fabric/metadata_codec.h>
#include <hgraph/fabric/value_builders.h>

#include <deque>
#include <map>
#include <mutex>
#include <stdexcept>
#include <utility>

namespace hgraph::fabric::detail {
struct GraphNotificationBridge::Impl
    : std::enable_shared_from_this<GraphNotificationBridge::Impl> {
  struct Key {
    Str data_id{};
    RevisionId revision{};

    friend auto operator<=>(const Key &, const Key &) = default;
  };

  struct RequestState {
    DataRevisionInput revision{};
    NotificationDeliveryStatus status{NotificationDeliveryStatus::Pending};
    Str message{};
    std::size_t retries{};
    bool queued{};
    bool in_flight{};
  };

  struct DeliveryContext {
    std::shared_ptr<Impl> owner{};
    Key key{};
  };

  static constexpr std::size_t REQUEST_LIMIT{1024};
  static constexpr std::size_t RETRY_LIMIT{8};

  mutable std::mutex mutex{};
  std::map<Key, RequestState> requests{};
  std::deque<Key> outgoing{};
  std::size_t delivered{};
  std::size_t retried{};
  std::size_t failed{};
  std::size_t stale_reports{};

  [[nodiscard]] static NotificationSubscription subscribe(void *) { return {}; }

  [[nodiscard]] static NotificationDelivery
  publish(void *context, RevisionNotification notification) {
    auto &self = *static_cast<Impl *>(context);
    DataRevisionInput revision =
        data_revision_input(decode_revision(notification.revision).view());
    if (revision.data_id != notification.data_id) {
      throw std::invalid_argument(
          "fabric graph notification key does not match its revision payload");
    }
    const Key key{revision.data_id, revision.revision};
    {
      std::lock_guard lock{self.mutex};
      const auto existing = self.requests.find(key);
      if (existing != self.requests.end()) {
        if (existing->second.revision != revision) {
          throw std::runtime_error(
              "fabric graph notification conflicts with an in-flight request");
        }
      } else {
        if (self.requests.size() >= REQUEST_LIMIT) {
          throw std::overflow_error("fabric graph notification queue is full");
        }
        self.requests.emplace(
            key, RequestState{.revision = std::move(revision), .queued = true});
        self.outgoing.push_back(key);
      }
    }
    auto delivery = std::make_shared<DeliveryContext>(
        DeliveryContext{self.shared_from_this(), key});
    static const NotificationDeliveryOps ops{&poll_delivery};
    return NotificationDelivery{std::move(delivery), ops};
  }

  [[nodiscard]] static NotificationDeliveryResult poll_delivery(void *context) {
    auto &delivery = *static_cast<DeliveryContext *>(context);
    auto &self = *delivery.owner;
    std::lock_guard lock{self.mutex};
    const auto found = self.requests.find(delivery.key);
    if (found == self.requests.end()) {
      return {NotificationDeliveryStatus::Failed,
              "fabric notification correlation expired"};
    }
    if (found->second.status == NotificationDeliveryStatus::Pending) {
      return {NotificationDeliveryStatus::Pending, {}};
    }
    NotificationDeliveryResult result{found->second.status,
                                      found->second.message};
    self.requests.erase(found);
    return result;
  }

  [[nodiscard]] Notifier notifier() {
    static const NotifierOps ops{&subscribe, &publish};
    return Notifier{shared_from_this(), ops};
  }

  [[nodiscard]] std::optional<DataRevisionInput> take_request() {
    std::lock_guard lock{mutex};
    while (!outgoing.empty()) {
      Key key = std::move(outgoing.front());
      outgoing.pop_front();
      const auto found = requests.find(key);
      if (found == requests.end() || !found->second.queued) {
        continue;
      }
      found->second.queued = false;
      found->second.in_flight = true;
      return found->second.revision;
    }
    return std::nullopt;
  }

  void complete(NotificationDeliveryInput delivery) {
    require_data_id(delivery.data_id);
    if (delivery.revision <= 0) {
      throw std::invalid_argument(
          "fabric notification delivery requires a positive revision");
    }
    const Key key{std::move(delivery.data_id), delivery.revision};
    std::lock_guard lock{mutex};
    const auto found = requests.find(key);
    if (found == requests.end()) {
      ++stale_reports;
      return;
    }
    auto &state = found->second;
    state.in_flight = false;
    if (delivery.delivered) {
      state.status = NotificationDeliveryStatus::Delivered;
      state.message = std::move(delivery.message);
      ++delivered;
      return;
    }
    if (delivery.retriable && state.retries < RETRY_LIMIT) {
      ++state.retries;
      ++retried;
      state.message = std::move(delivery.message);
      if (!state.queued) {
        state.queued = true;
        outgoing.push_back(key);
      }
      return;
    }
    state.status = NotificationDeliveryStatus::Failed;
    state.message = delivery.message.empty()
                        ? "fabric notification delivery failed"
                        : std::move(delivery.message);
    ++failed;
  }
};

GraphNotificationBridge::GraphNotificationBridge()
    : impl_(std::make_shared<Impl>()) {}
GraphNotificationBridge::~GraphNotificationBridge() = default;

Notifier GraphNotificationBridge::notifier() const { return impl_->notifier(); }

std::optional<DataRevisionInput> GraphNotificationBridge::take_request() {
  return impl_->take_request();
}

void GraphNotificationBridge::complete(NotificationDeliveryInput delivery) {
  impl_->complete(std::move(delivery));
}

bool GraphNotificationBridge::request_pending() const noexcept {
  std::lock_guard lock{impl_->mutex};
  return !impl_->outgoing.empty();
}

std::vector<std::pair<Str, Str>> GraphNotificationBridge::diagnostics() const {
  std::lock_guard lock{impl_->mutex};
  return {
      {"transport.notification.pending",
       std::to_string(impl_->requests.size())},
      {"transport.notification.delivered", std::to_string(impl_->delivered)},
      {"transport.notification.retried", std::to_string(impl_->retried)},
      {"transport.notification.failed", std::to_string(impl_->failed)},
      {"transport.notification.stale_reports",
       std::to_string(impl_->stale_reports)},
  };
}
} // namespace hgraph::fabric::detail
