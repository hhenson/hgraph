#ifndef HGRAPH_FABRIC_NOTIFIER_H
#define HGRAPH_FABRIC_NOTIFIER_H

#include <hgraph/fabric/export.h>

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
    class HGRAPH_FABRIC_EXPORT NotificationSubscription final
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
        void (*publish)(void *context, RevisionNotification notification);
    };

    /** Owning type-erased fabric notification contract. The contract validates
        complete encoded revision notices and matching data ids before dispatch;
        persistence remains authoritative. */
    class HGRAPH_FABRIC_EXPORT Notifier final
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
        void publish(RevisionNotification notification) const;
        [[nodiscard]] explicit operator bool() const noexcept;
        void reset() noexcept;

      private:
        [[nodiscard]] static const NotifierOps &empty_ops() noexcept;

        std::shared_ptr<void> context_{};
        NotifierOps           ops_{empty_ops()};
    };

    /** Construct the broker-free, process-local conflating notifier used by
        deterministic unit tests and local fabric configuration. */
    [[nodiscard]] HGRAPH_FABRIC_EXPORT Notifier make_memory_notifier();
}  // namespace hgraph::fabric

#endif  // HGRAPH_FABRIC_NOTIFIER_H
