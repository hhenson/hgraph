#include <hgraph/fabric/notifier.h>

#include <hgraph/fabric/metadata_codec.h>
#include <hgraph/fabric/types.h>

#include <stdexcept>
#include <utility>

namespace hgraph::fabric
{
    namespace
    {
        [[nodiscard]] std::optional<RevisionNotification> empty_try_pop(void *)
        {
            return std::nullopt;
        }

        [[nodiscard]] std::size_t empty_pending(void *) noexcept { return 0; }
        void empty_close(void *) noexcept {}

        [[nodiscard]] NotificationSubscription empty_subscribe(void *)
        {
            return {};
        }

        void empty_publish(void *, RevisionNotification)
        {
            throw std::logic_error("fabric notifier is not configured");
        }
    }  // namespace

    const NotificationSubscriptionOps &
    NotificationSubscription::empty_ops() noexcept
    {
        static const NotificationSubscriptionOps ops{
            &empty_try_pop,
            &empty_pending,
            &empty_close,
        };
        return ops;
    }

    NotificationSubscription::NotificationSubscription() noexcept = default;

    NotificationSubscription::NotificationSubscription(
        std::shared_ptr<void> context, const NotificationSubscriptionOps &ops)
        : context_(std::move(context)), ops_(ops)
    {
        if (!context_ || ops_.try_pop == nullptr || ops_.pending == nullptr ||
            ops_.close == nullptr)
        {
            throw std::invalid_argument(
                "fabric notification subscription requires complete operations");
        }
    }

    NotificationSubscription::NotificationSubscription(
        NotificationSubscription &&other) noexcept
        : context_(std::move(other.context_)), ops_(other.ops_)
    {
        other.ops_ = empty_ops();
    }

    NotificationSubscription &NotificationSubscription::operator=(
        NotificationSubscription &&other) noexcept
    {
        if (this != &other)
        {
            close();
            context_ = std::move(other.context_);
            ops_     = other.ops_;
            other.ops_ = empty_ops();
        }
        return *this;
    }

    NotificationSubscription::~NotificationSubscription() { close(); }

    std::optional<RevisionNotification>
    NotificationSubscription::try_pop() const
    {
        return ops_.try_pop(context_.get());
    }

    std::size_t NotificationSubscription::pending() const noexcept
    {
        return ops_.pending(context_.get());
    }

    void NotificationSubscription::close() noexcept
    {
        ops_.close(context_.get());
        context_.reset();
        ops_ = empty_ops();
    }

    NotificationSubscription::operator bool() const noexcept
    {
        return context_ != nullptr;
    }

    const NotifierOps &Notifier::empty_ops() noexcept
    {
        static const NotifierOps ops{&empty_subscribe, &empty_publish};
        return ops;
    }

    Notifier::Notifier() noexcept = default;

    Notifier::Notifier(std::shared_ptr<void> context, const NotifierOps &ops)
        : context_(std::move(context)), ops_(ops)
    {
        if (!context_ || ops_.subscribe == nullptr || ops_.publish == nullptr)
        {
            throw std::invalid_argument("fabric notifier requires complete operations");
        }
    }

    Notifier::Notifier(Notifier &&other) noexcept
        : context_(std::move(other.context_)), ops_(other.ops_)
    {
        other.ops_ = empty_ops();
    }

    Notifier &Notifier::operator=(Notifier &&other) noexcept
    {
        if (this != &other)
        {
            context_ = std::move(other.context_);
            ops_     = other.ops_;
            other.ops_ = empty_ops();
        }
        return *this;
    }

    NotificationSubscription Notifier::subscribe() const
    {
        return ops_.subscribe(context_.get());
    }

    void Notifier::publish(RevisionNotification notification) const
    {
        require_data_id(notification.data_id);
        if (notification.revision.empty())
        {
            throw std::invalid_argument(
                "fabric revision notification payload must not be empty");
        }
        const Value decoded = decode_revision(notification.revision);
        if (decoded.view().as_bundle().at("data_id").checked_as<Str>() !=
            notification.data_id)
        {
            throw std::invalid_argument(
                "fabric revision notification data id does not match its payload");
        }
        ops_.publish(context_.get(), std::move(notification));
    }

    Notifier::operator bool() const noexcept { return context_ != nullptr; }

    void Notifier::reset() noexcept
    {
        context_.reset();
        ops_ = empty_ops();
    }
}  // namespace hgraph::fabric
