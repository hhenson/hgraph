#ifndef HGRAPH_RUNTIME_PUSH_SOURCE_NODE_H
#define HGRAPH_RUNTIME_PUSH_SOURCE_NODE_H

#include <hgraph/runtime/executor.h>

#include <cstdint>
#include <functional>
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
        struct PushSourcePolicyStorageRef;
    }

    enum class PushSourcePolicyKind : std::uint8_t
    {
        Queue,
        Conflating,
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
        friend struct detail::PushSourcePolicyStorageRef;

        PushSourcePolicy(const detail::PushSourcePolicyOps *ops, const void *context) noexcept;

        const detail::PushSourcePolicyOps *ops_;
        const void                       *context_{nullptr};
    };

    namespace detail
    {
        struct PushSourcePolicyStorageRef
        {
            PushSourcePolicy   policy{};
            void              *storage{nullptr};
            PushQueueEngineView push_engine{};
            const TypeRealizationSnapshot *type_realization{nullptr};

            [[nodiscard]] bool bound() const noexcept;
            void send(Value value) const;
        };

        struct PushSourcePolicyAccess
        {
            [[nodiscard]] static PushSourcePolicy make_policy(
                const PushSourcePolicyOps *ops,
                const void *context) noexcept;
            [[nodiscard]] static const MemoryUtils::StoragePlan &storage_plan(
                const PushSourcePolicy &policy);
            [[nodiscard]] static PushSourceSender make_sender(
                PushSourcePolicy policy,
                void *storage,
                PushQueueEngineView push_engine,
                const TypeRealizationSnapshot *type_realization) noexcept;
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

    /**
     * Sender handed to push-source user code during node start.
     *
     * The sender is a lightweight handle onto the owning node's policy storage.
     * Sending copies/moves an owned value into the policy and marks the root
     * real-time executor when the policy accepts a ready update.
     */
    class HGRAPH_EXPORT PushSourceSender
    {
      public:
        PushSourceSender() noexcept = default;

        [[nodiscard]] bool valid() const noexcept;
        [[nodiscard]] const TypeRealizationSnapshot *type_realization() const noexcept;

        void send(Value value) const;

        template <typename T>
        void send(T &&value) const
        {
            send(Value{std::forward<T>(value)});
        }

      private:
        friend struct detail::PushSourcePolicyAccess;

        explicit PushSourceSender(detail::PushSourcePolicyStorageRef policy_storage) noexcept;

        detail::PushSourcePolicyStorageRef policy_storage_{};
    };

    using PushSourceStartCallback = std::function<void(PushSourceSender)>;

    [[nodiscard]] HGRAPH_EXPORT PushSourcePolicy make_push_source_policy(
        PushSourcePolicyKind kind,
        const ValueTypeMetaData &sender_schema);

    [[nodiscard]] HGRAPH_EXPORT PushSourcePolicy make_push_source_queue_policy(
        const ValueTypeMetaData &sender_schema);

    [[nodiscard]] HGRAPH_EXPORT PushSourcePolicy make_push_source_conflating_policy(
        const ValueTypeMetaData &sender_schema);

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

    /**
     * Build a root push source which may also run under a simulation executor.
     *
     * The producer must make its initial simulation work deterministic: it
     * must enqueue all values needed to keep the simulation live before the
     * graph evaluation which starts that producer completes. This is intended
     * for pull-capable external replay sources. Ordinary asynchronous sources
     * remain real-time-only.
     */
    [[nodiscard]] HGRAPH_EXPORT NodeBuilder make_simulation_capable_push_source_node(
        const TSValueTypeMetaData &output_schema,
        PushSourcePolicy policy,
        PushSourceStartCallback on_start = {},
        bool requires_phase_runner = false);

    [[nodiscard]] HGRAPH_EXPORT NodeBuilder make_simulation_capable_push_source_node(
        const TSValueTypeMetaData &output_schema,
        PushSourceStartCallback on_start = {});
}

#endif  // HGRAPH_RUNTIME_PUSH_SOURCE_NODE_H
