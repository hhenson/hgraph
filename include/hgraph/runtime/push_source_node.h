#ifndef HGRAPH_RUNTIME_PUSH_SOURCE_NODE_H
#define HGRAPH_RUNTIME_PUSH_SOURCE_NODE_H

#include <hgraph/runtime/executor.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <utility>

namespace hgraph
{
    class PushSourceSender;
    class TSOutputView;
    class TypeRealizationSnapshot;

    namespace detail
    {
        struct PushSourcePolicyOps;
        struct PushSourcePolicyAccess;
        class PushSourceSenderControl;
    }

    enum class PushSourcePolicyKind : std::uint8_t
    {
        Queue,
        Conflating,
        Burst,
    };

    class HGRAPH_EXPORT PushSourcePolicy
    {
      public:
        PushSourcePolicy() noexcept;

        [[nodiscard]] bool valid() const noexcept;
        [[nodiscard]] const ValueTypeMetaData &sender_schema() const;
        [[nodiscard]] bool output_compatible(const TSValueTypeMetaData &output_schema) const;

      private:
        friend struct detail::PushSourcePolicyAccess;
        friend class detail::PushSourceSenderControl;

        PushSourcePolicy(const detail::PushSourcePolicyOps *ops, const void *context) noexcept;

        const detail::PushSourcePolicyOps *ops_;
        const void                       *context_{nullptr};
    };

    namespace detail
    {
        struct PushSourcePolicyAccess
        {
            [[nodiscard]] static PushSourcePolicy make_policy(
                const PushSourcePolicyOps *ops,
                const void *context) noexcept;
            [[nodiscard]] static const MemoryUtils::StoragePlan &storage_plan(
                const PushSourcePolicy &policy);
            [[nodiscard]] static PushSourceSender make_sender(
                std::shared_ptr<PushSourceSenderControl> control) noexcept;
            static void start(const PushSourcePolicy &policy,
                              void *storage,
                              const TSValueTypeMetaData &output_schema);
            static void stop(const PushSourcePolicy &policy, void *storage);
            static bool emit_next(const PushSourcePolicy &policy,
                                  void *storage,
                                  const TSOutputView &output);
            [[nodiscard]] static std::size_t pending_items(
                const PushSourcePolicy &policy, const void *storage) noexcept;
        };
    }

    /** Raised when blocking admission cannot complete because the source is
     * not running or its graph is stopping. */
    class HGRAPH_EXPORT PushSourceStopped : public std::runtime_error
    {
      public:
        PushSourceStopped();
    };

    /**
     * Sender handed to push-source user code during node start.
     *
     * The sender is a copyable handle onto a lifetime-safe control block for
     * the owning node's policy storage. Admission marks the root real-time
     * executor only when the policy accepts a ready update.
     */
    class HGRAPH_EXPORT PushSourceSender
    {
      public:
        PushSourceSender() noexcept = default;

        [[nodiscard]] bool valid() const noexcept;
        [[nodiscard]] const TypeRealizationSnapshot *type_realization() const noexcept;

        /** Attempt admission without waiting. Returns false at capacity or
         * when the source is not accepting values. */
        [[nodiscard]] bool try_send(Value value) const;

        /** Wait for admission when a bounded queue is full. This must not
         * wait on the graph evaluation thread. Throws ``PushSourceStopped``
         * when the source stops before admission. */
        void send_blocking(Value value) const;

        template <typename T>
        [[nodiscard]] bool try_send(T &&value) const
        {
            return try_send(Value{std::forward<T>(value)});
        }

        template <typename T>
        void send_blocking(T &&value) const
        {
            send_blocking(Value{std::forward<T>(value)});
        }

      private:
        friend struct detail::PushSourcePolicyAccess;

        explicit PushSourceSender(
            std::shared_ptr<detail::PushSourceSenderControl> control) noexcept;

        std::shared_ptr<detail::PushSourceSenderControl> control_{};
    };

    using PushSourceStartCallback = std::function<void(PushSourceSender)>;
    using PushSourceStartViewCallback =
        std::function<void(PushSourceSender, const NodeView &, DateTime)>;

    struct PushSourceNodeExtension
    {
        PushSourceStartViewCallback          on_start{};
        std::function<void(const NodeView &)> on_stop{};
        const ValueTypeMetaData              *state_schema{nullptr};
        const ValueTypeMetaData              *scalar_schema{nullptr};
        bool                                  uses_scheduler{false};
        bool                                  uses_global_state{false};
        bool                                  uses_evaluation_clock{false};
        bool                                  uses_python_values{false};
    };

    [[nodiscard]] HGRAPH_EXPORT PushSourcePolicy make_push_source_policy(
        PushSourcePolicyKind kind,
        const ValueTypeMetaData &sender_schema);

    [[nodiscard]] HGRAPH_EXPORT PushSourcePolicy make_push_source_queue_policy(
        const ValueTypeMetaData &sender_schema, std::size_t max_pending = 0);

    [[nodiscard]] HGRAPH_EXPORT PushSourcePolicy make_push_source_conflating_policy(
        const ValueTypeMetaData &sender_schema);

    /**
     * Prefer these: taking the time-series meta lets the queue accept both the
     * observed delta and its authored counterpart (a ``{key: REMOVE}`` push),
     * which the ``ValueTypeMetaData`` overloads cannot know about.
     */
    [[nodiscard]] HGRAPH_EXPORT PushSourcePolicy make_push_source_policy(
        PushSourcePolicyKind kind, const ValueTypeMetaData &sender_schema,
        const ValueTypeMetaData *authored_schema);

    [[nodiscard]] HGRAPH_EXPORT PushSourcePolicy make_push_source_queue_policy(
        const TSValueTypeMetaData &output_schema, std::size_t max_pending = 0);

    /** Queue individual scalar values and deliver all pending values as one
     * homogeneous variadic tuple per graph evaluation. */
    [[nodiscard]] HGRAPH_EXPORT PushSourcePolicy make_push_source_burst_policy(
        const TSValueTypeMetaData &output_schema, std::size_t max_pending = 0);

    [[nodiscard]] HGRAPH_EXPORT PushSourcePolicy make_push_source_conflating_policy(
        const TSValueTypeMetaData &output_schema);

    /**
     * Build a root push-source node for ``output_schema``.
     *
     * The node owns policy-selected push storage. ``on_start`` is invoked with
     * a sender once the node is attached to a real-time root graph.
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder make_push_source_node(
        const TSValueTypeMetaData &output_schema,
        PushSourcePolicy policy,
        PushSourceStartCallback on_start = {},
        bool requires_phase_runner = false);

    [[nodiscard]] HGRAPH_EXPORT NodeBuilder make_push_source_node(
        const TSValueTypeMetaData &output_schema,
        PushSourceStartCallback on_start = {});

    /** Python and other dynamic runtimes use this form to project graph-scoped
     * injectables into the start callback. */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder make_push_source_node_with_view(
        const TSValueTypeMetaData &output_schema,
        PushSourcePolicy policy,
        PushSourceNodeExtension extension,
        bool requires_phase_runner = false);

}

#endif  // HGRAPH_RUNTIME_PUSH_SOURCE_NODE_H
