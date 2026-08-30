#ifndef HGRAPH_FABRIC_NOTIFIER_H
#define HGRAPH_FABRIC_NOTIFIER_H

#include <hgraph/fabric/export.h>

#include <hgraph/persistence/value_codec.h>

#include <hgraph/persistence/object_store.h>
#include <hgraph/types/primitive_types.h>

#include <cstddef>
#include <memory>
#include <optional>

namespace hgraph::fabric
{
    struct RevisionNotification
    {
        Str                                     data_id{};
        persistence::store::ObjectBytes         revision{};

        friend bool operator==(const RevisionNotification &,
                               const RevisionNotification &) = default;
    };

    enum class NotificationDeliveryStatus
    {
        Pending,
        Delivered,
        Failed,
    };

    struct NotificationDeliveryResult
    {
        NotificationDeliveryStatus status{NotificationDeliveryStatus::Pending};
        Str                        message{};

        friend bool operator==(const NotificationDeliveryResult &,
                               const NotificationDeliveryResult &) = default;
    };

    struct NotificationDeliveryOps
    {
        NotificationDeliveryResult (*poll)(void *context);
    };

    /** Correlated asynchronous acknowledgement for one accepted revision.
        The concrete notifier owns delivery correlation; publication polls this
        passive handle after the immutable revision and its indexes are durable. */
    class HGRAPH_FABRIC_CLASS_EXPORT NotificationDelivery final
    {
      public:
        NotificationDelivery() noexcept;
        NotificationDelivery(std::shared_ptr<void> context,
                             const NotificationDeliveryOps &ops);

        NotificationDelivery(const NotificationDelivery &) = default;
        NotificationDelivery &operator=(const NotificationDelivery &) = default;
        NotificationDelivery(NotificationDelivery &&other) noexcept;
        NotificationDelivery &operator=(NotificationDelivery &&other) noexcept;
        ~NotificationDelivery() = default;

        [[nodiscard]] NotificationDeliveryResult poll() const;
        [[nodiscard]] explicit operator bool() const noexcept;
        void reset() noexcept;

      private:
        [[nodiscard]] static const NotificationDeliveryOps &empty_ops() noexcept;

        std::shared_ptr<void>   context_{};
        NotificationDeliveryOps ops_{empty_ops()};
    };

    struct NotificationSubscriptionOps
    {
        std::optional<RevisionNotification> (*try_pop)(void *context);
        std::size_t (*pending)(void *context) noexcept;
        void (*close)(void *context) noexcept;
    };

    /** One owning subscription to a fabric notification stream. Pending
        notices are conflated by data id; distinct first-pending ids retain
        publication order. Default and moved-from handles use a non-null empty
        operations table. */
    class HGRAPH_FABRIC_CLASS_EXPORT NotificationSubscription final
    {
      public:
        NotificationSubscription() noexcept;
        NotificationSubscription(std::shared_ptr<void> context,
                                 const NotificationSubscriptionOps &ops);

        NotificationSubscription(const NotificationSubscription &) = delete;
        NotificationSubscription &operator=(const NotificationSubscription &) = delete;
        NotificationSubscription(NotificationSubscription &&other) noexcept;
        NotificationSubscription &operator=(NotificationSubscription &&other) noexcept;
        ~NotificationSubscription();

        [[nodiscard]] std::optional<RevisionNotification> try_pop() const;
        [[nodiscard]] std::size_t pending() const noexcept;
        void close() noexcept;
        [[nodiscard]] explicit operator bool() const noexcept;

      private:
        [[nodiscard]] static const NotificationSubscriptionOps &empty_ops() noexcept;

        std::shared_ptr<void>       context_{};
        NotificationSubscriptionOps ops_{empty_ops()};
    };

    struct NotifierOps
    {
        NotificationSubscription (*subscribe)(void *context);
        NotificationDelivery (*publish)(void *context,
                                        RevisionNotification notification);
    };

    /** Owning type-erased fabric notification contract. The contract validates
        complete encoded accepted-revision notices and matching data ids before
        dispatch; persistence remains authoritative. */
    class HGRAPH_FABRIC_CLASS_EXPORT Notifier final
    {
      public:
        Notifier() noexcept;
        Notifier(std::shared_ptr<void> context, const NotifierOps &ops);

        Notifier(const Notifier &) = default;
        Notifier &operator=(const Notifier &) = default;
        Notifier(Notifier &&other) noexcept;
        Notifier &operator=(Notifier &&other) noexcept;
        ~Notifier() = default;

        [[nodiscard]] NotificationSubscription subscribe() const;
        [[nodiscard]] NotificationDelivery
        publish(RevisionNotification notification) const;
        [[nodiscard]] explicit operator bool() const noexcept;
        void reset() noexcept;

        /** Pre-bind the codec publish() validates with. Called at wiring time
            from the fabric configuration path so publish(), which runs during
            evaluation, resolves nothing. */
        void bind_revision_codec(persistence::store::BoundValueCodec codec) noexcept;

      private:
        [[nodiscard]] static const NotifierOps &empty_ops() noexcept;

        std::shared_ptr<void> context_{};
        NotifierOps           ops_{empty_ops()};
        /** Bound at wiring time. Mutable so a Notifier used outside the fabric
            configuration path still works, binding once on first publish
            rather than per call. */
        mutable persistence::store::BoundValueCodec revision_codec_{};
    };

    /** Construct the broker-free, process-local conflating notifier used by
        deterministic unit tests and local fabric configuration. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT Notifier make_memory_notifier();
}  // namespace hgraph::fabric

#endif  // HGRAPH_FABRIC_NOTIFIER_H
