#include <hgraph/fabric/metadata_codec.h>
#include <hgraph/fabric/notifier.h>
#include <hgraph/fabric/value_builders.h>

#include <algorithm>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

namespace hgraph::fabric
{
    namespace
    {
        struct MemorySubscription
        {
            struct PendingNotice
            {
                RevisionId revision{};
                persistence::store::ObjectBytes payload{};
            };

            mutable std::mutex mutex{};
            bool closed{};
            std::deque<Str> order{};
            std::unordered_map<Str, PendingNotice> pending{};
            std::function<void()> waker{};
        };

        struct MemoryNotifier
        {
            std::mutex mutex{};
            std::vector<std::weak_ptr<MemorySubscription>> subscriptions{};
        };

        struct ImmediateDelivery
        {
        };

        [[nodiscard]] const NotificationDeliveryOps &delivery_ops() noexcept
        {
            static const NotificationDeliveryOps ops{
                [](void *) {
                    return NotificationDeliveryResult{
                        NotificationDeliveryStatus::Delivered, {}};
                },
            };
            return ops;
        }

        [[nodiscard]] const NotificationSubscriptionOps &subscription_ops() noexcept
        {
            static const NotificationSubscriptionOps ops{
                [](void *context) -> std::optional<RevisionNotification> {
                    auto &subscription = *static_cast<MemorySubscription *>(context);
                    const std::scoped_lock lock{subscription.mutex};
                    if (subscription.closed || subscription.order.empty())
                    {
                        return std::nullopt;
                    }
                    Str data_id = std::move(subscription.order.front());
                    subscription.order.pop_front();
                    auto found = subscription.pending.find(data_id);
                    RevisionNotification result{
                        .data_id = std::move(data_id),
                        .revision = std::move(found->second.payload),
                    };
                    subscription.pending.erase(found);
                    return result;
                },
                [](void *context) noexcept -> std::size_t {
                    if (context == nullptr) { return 0; }
                    auto &subscription = *static_cast<MemorySubscription *>(context);
                    const std::scoped_lock lock{subscription.mutex};
                    return subscription.closed ? 0 : subscription.pending.size();
                },
                [](void *context) {
                    if (context == nullptr)
                    {
                        return NotificationSubscriptionStatus{
                            NotificationSubscriptionState::Stopped, 0, {}};
                    }
                    auto &subscription =
                        *static_cast<MemorySubscription *>(context);
                    const std::scoped_lock lock{subscription.mutex};
                    return NotificationSubscriptionStatus{
                        subscription.closed
                            ? NotificationSubscriptionState::Stopped
                            : NotificationSubscriptionState::Live,
                        1, {}};
                },
                [](void *, const RevisionNotification &) {},
                [](void *context, std::function<void()> waker) {
                    if (context == nullptr) { return; }
                    auto &subscription = *static_cast<MemorySubscription *>(context);
                    std::function<void()> notify;
                    {
                        const std::scoped_lock lock{subscription.mutex};
                        if (subscription.closed) { return; }
                        subscription.waker = std::move(waker);
                        if (!subscription.pending.empty())
                        {
                            notify = subscription.waker;
                        }
                    }
                    if (notify) { notify(); }
                },
                [](void *context) noexcept {
                    if (context == nullptr) { return; }
                    auto &subscription = *static_cast<MemorySubscription *>(context);
                    const std::scoped_lock lock{subscription.mutex};
                    subscription.closed = true;
                    subscription.order.clear();
                    subscription.pending.clear();
                    subscription.waker = {};
                },
            };
            return ops;
        }

        [[nodiscard]] const NotifierOps &notifier_ops() noexcept
        {
            static const NotifierOps ops{
                [](void *context) {
                    auto owner = static_cast<MemoryNotifier *>(context);
                    auto subscription = std::make_shared<MemorySubscription>();
                    {
                        const std::scoped_lock lock{owner->mutex};
                        owner->subscriptions.erase(
                            std::remove_if(owner->subscriptions.begin(),
                                           owner->subscriptions.end(),
                                           [](const auto &item) { return item.expired(); }),
                            owner->subscriptions.end());
                        owner->subscriptions.emplace_back(subscription);
                    }
                    return NotificationSubscription{std::move(subscription),
                                                    subscription_ops()};
                },
                [](void *context, RevisionNotification notification) {
                    auto &owner = *static_cast<MemoryNotifier *>(context);
                    const RevisionId proposed_revision =
                        data_revision_input(
                            decode_revision(notification.revision).view())
                            .revision;
                    std::vector<std::shared_ptr<MemorySubscription>> subscribers;
                    {
                        const std::scoped_lock lock{owner.mutex};
                        auto output = owner.subscriptions.begin();
                        for (auto input = owner.subscriptions.begin();
                             input != owner.subscriptions.end(); ++input)
                        {
                            if (auto subscription = input->lock())
                            {
                                subscribers.push_back(std::move(subscription));
                                *output++ = *input;
                            }
                        }
                        owner.subscriptions.erase(output, owner.subscriptions.end());
                    }
                    for (const auto &subscription : subscribers)
                    {
                        std::function<void()> waker;
                        {
                            const std::scoped_lock lock{subscription->mutex};
                            if (subscription->closed) { continue; }
                            auto [entry, inserted] = subscription->pending.try_emplace(
                                notification.data_id,
                                MemorySubscription::PendingNotice{
                                    proposed_revision,
                                    notification.revision});
                            if (inserted)
                            {
                                subscription->order.push_back(notification.data_id);
                                waker = subscription->waker;
                            }
                            else if (proposed_revision >= entry->second.revision)
                            {
                                entry->second = MemorySubscription::PendingNotice{
                                    proposed_revision,
                                    notification.revision};
                            }
                        }
                        if (waker) { waker(); }
                    }
                    return NotificationDelivery{
                        std::make_shared<ImmediateDelivery>(), delivery_ops()};
                },
            };
            return ops;
        }
    }  // namespace

    Notifier make_memory_notifier()
    {
        return Notifier{std::make_shared<MemoryNotifier>(), notifier_ops()};
    }
}  // namespace hgraph::fabric
