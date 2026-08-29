#ifndef HGRAPH_FABRIC_PUBLICATION_H
#define HGRAPH_FABRIC_PUBLICATION_H

#include <hgraph/fabric/config.h>
#include <hgraph/fabric/export.h>
#include <hgraph/fabric/types.h>

#include <hgraph/types/frame.h>

#include <memory>
#include <optional>
#include <vector>

namespace hgraph::fabric
{
    /** Externally visible progress through one RFC 0026 publication attempt.
        Each steady-state call to advance performs at most one durable or
        notification boundary. Initial recovery may scan and repair a
        contiguous accepted history before starting a new publication. */
    enum class PublicationState
    {
        Idle,
        Preparing,
        FrameDurable,
        RevisionDurable,
        AsOfDurable,
        LatestDurable,
        NotificationPending,
        NotificationAcknowledged,
        Published,
        Unchanged,
        AwaitingFirstOutput,
        LostRace,
    };

    [[nodiscard]] constexpr bool publication_terminal(PublicationState state) noexcept
    {
        return state == PublicationState::Published ||
               state == PublicationState::Unchanged ||
               state == PublicationState::AwaitingFirstOutput ||
               state == PublicationState::LostRace;
    }

    struct PublicationInput
    {
        /** A present Frame means the output ticked and must receive a new data
            version. An empty Frame records only an input-cut change. */
        Frame                            output{};
        std::vector<DataDependencyInput> dependencies{};
        std::optional<DataVersion>       self_predecessor{};
        /** Publisher-host UTC system time used for version and as-of allocation. */
        DateTime                         system_time{MIN_DT};
    };

    /** One data-id publisher's cold-path, notifier-independent publication
        protocol. The implementation retains the accepted head and fixed Arrow
        schema between attempts; concrete nodes own one machine in lifecycle
        state and keep ordinary graph evaluation free of fabric policy.

        A steady-state metadata step is O(number of immediate dependencies);
        an output tick additionally serialises or compares O(Frame bytes).
        First recovery is O(contiguous accepted history); retained head state
        makes later recovery proportional to revisions accepted since the last
        attempt. */
    class HGRAPH_FABRIC_EXPORT PublisherStateMachine final
    {
      public:
        PublisherStateMachine(FabricConfig config, Str data_id);
        ~PublisherStateMachine();

        PublisherStateMachine(const PublisherStateMachine &) = delete;
        PublisherStateMachine &operator=(const PublisherStateMachine &) = delete;
        PublisherStateMachine(PublisherStateMachine &&) noexcept;
        PublisherStateMachine &operator=(PublisherStateMachine &&) noexcept;

        /** Begin one attempt. The prior attempt must be terminal or idle. */
        void begin(PublicationInput input);

        /** Advance by one external boundary. Pending acknowledgement leaves the
            state unchanged. Failed acknowledgement throws after restoring the
            LatestDurable state so the accepted notification can be retried. */
        PublicationState advance();

        /** Acknowledge a notification delivered by an explicit graph transport
            edge. The machine must be paused at LatestDurable; configured
            notifier delivery continues to use advance(). */
        void acknowledge_notification();

        [[nodiscard]] PublicationState state() const noexcept;
        [[nodiscard]] std::optional<DataRevisionInput> accepted_revision() const;
        [[nodiscard]] std::optional<DataRevisionInput> candidate_revision() const;
        [[nodiscard]] const Str &data_id() const noexcept;

      private:
        struct Impl;
        std::unique_ptr<Impl> impl_;
    };
}  // namespace hgraph::fabric

#endif  // HGRAPH_FABRIC_PUBLICATION_H
