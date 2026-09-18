#ifndef HGRAPH_RUNTIME_DISTRIBUTED_CHILD_H
#define HGRAPH_RUNTIME_DISTRIBUTED_CHILD_H

// The boundary of a distributed child graph (RFC 0037).
//
// A child evaluated in another process cannot bind its inputs to the caller's
// outputs -- there is no shared memory to bind to -- so each boundary argument
// becomes a local PULL SOURCE the driver stages a value into, and each result
// a local SINK the driver reads back. Between them sits an ordinary graph.
//
// Staging deliberately does NOT write a time-series output from outside. It
// writes a plain value into GlobalState and schedules the source node; the
// output write then happens inside the node's ``eval``, during evaluation, so
// the modified-time stamping is the runtime's own and cannot disagree with the
// cycle. That is why this needs no new evaluation phase: RFC 0037 anticipated
// a nested analogue of the root push-source phase, and routing through a
// source node removes the need for one.
//
// The boundary sources are PULL sources. That is not a choice between two
// options: a push source is a root-graph facility (the push phase in
// ``evaluate`` is compiled only for RootGraphRuntimeStorage) and a distributed
// child stands in for a NESTED graph, which never has one. A worker hosting its
// child under a root executor is an implementation detail of the host, not a
// capability of the child -- and GraphExecutorValue refuses push sources on any
// executor but RealTime regardless.

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/distributed_protocol.h>
#include <hgraph/runtime/distributed_boundary.h>
#include <hgraph/runtime/executor.h>
#include <hgraph/runtime/global_state.h>
#include <hgraph/runtime/graph.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/value/value.h>

#include <string>
#include <string_view>
#include <vector>

namespace hgraph::static_schema_detail
{
    template <> struct scalar_name<distributed::BoundaryTransferPtr>
    {
        static constexpr std::string_view value{"hgraph.distributed.boundary_transfer"};
    };
}

namespace hgraph::distributed
{
    /** The node names the driver finds the boundary by. */
    inline constexpr const char *boundary_source_name = "distributed_boundary_source";
    inline constexpr const char *boundary_sink_name   = "distributed_boundary_sink";

    /**
     * Checkpoint support for the boundary nodes (RFC 0039, "Whole-graph
     * coordinator").
     *
     * A source's owned output is its whole state, and it IS the ingress
     * baseline: a later removal has to reach a source that already holds the
     * key. A sink holds nothing. Neither keeps anything in ``GlobalState``
     * across a cycle boundary -- a stage is consumed on apply and a result is
     * collected by the driver -- so the slots are not part of an image.
     *
     * The contract signature is the slot name. The schema is already in the
     * identity's endpoint descriptors, and a transfer is derived from it.
     */
    [[nodiscard]] HGRAPH_EXPORT const NodeCheckpointOps &boundary_source_checkpoint_ops() noexcept;
    [[nodiscard]] HGRAPH_EXPORT const NodeCheckpointOps &boundary_sink_checkpoint_ops() noexcept;

    /**
     * One boundary input: applies whatever the driver staged for this cycle.
     *
     * The stage is CONSUMED on apply. A source is scheduled on every prepared
     * cycle (the driver does not track which slot changed), so leaving the
     * value in place would re-tick it on the next cycle and invent a tick the
     * caller never sent.
     */
    struct HGRAPH_CLASS_EXPORT boundary_source_impl
    {
        static constexpr auto name = boundary_source_name;
        static const NodeCheckpointOps &checkpoint_ops() noexcept { return boundary_source_checkpoint_ops(); }

        static void eval(Scalar<"slot", Str> slot, TypeArg<"tp", TsVar<"S">, AutoResolve>,
                         GlobalStateView gs, Out<TsVar<"S">> out)
        {
            const ValueView staged = gs.get(slot.value());
            if (!staged.valid()) { return; }
            apply_delta(out, staged);
            gs.erase(slot.value());
        }
    };

    /**
     * One boundary output: records this cycle's delta for the driver to read.
     *
     * ``delta_is_observable`` is the same gate ``record`` uses, so a cycle that
     * produced no externally visible change leaves nothing behind and the
     * driver reports no output rather than an empty one.
     */
    struct HGRAPH_CLASS_EXPORT boundary_sink_impl
    {
        static constexpr auto name = boundary_sink_name;
        static const NodeCheckpointOps &checkpoint_ops() noexcept { return boundary_sink_checkpoint_ops(); }

        static void eval(In<"ts", TsVar<"S">> ts, Scalar<"slot", Str> slot, GlobalStateView gs)
        {
            if (!ts.modified()) { return; }
            Value delta = capture_delta(ts.base());
            if (!delta_is_observable(ts.base(), delta.view())) { return; }
            gs.set(slot.value(), std::move(delta));
        }
    };

    /** Lossless materialized transport, selected once at wiring time. */
    struct HGRAPH_CLASS_EXPORT boundary_transfer_source_impl
    {
        static constexpr auto name = boundary_source_name;
        static const NodeCheckpointOps &checkpoint_ops() noexcept { return boundary_source_checkpoint_ops(); }
        static void eval(Scalar<"slot", Str> slot, Scalar<"transfer", BoundaryTransferPtr> transfer,
                         TypeArg<"tp", TsVar<"S">, AutoResolve>, GlobalStateView gs, Out<TsVar<"S">> out)
        {
            const auto staged = gs.get(slot.value());
            if (!staged.has_value()) return;
            transfer.value()->apply(out, staged);
            gs.erase(slot.value());
        }
    };

    struct HGRAPH_CLASS_EXPORT boundary_transfer_sink_impl
    {
        static constexpr auto name = boundary_sink_name;
        static const NodeCheckpointOps &checkpoint_ops() noexcept { return boundary_sink_checkpoint_ops(); }
        static void eval(In<"ts", TsVar<"S">, InputValidity::Unchecked> ts, Scalar<"slot", Str> slot,
                         Scalar<"transfer", BoundaryTransferPtr> transfer,
                         Scalar<"group", Int> group, Scalar<"groups", Int> groups,
                         TypeArg<"tp", TsVar<"S">, AutoResolve>, GlobalStateView gs)
        {
            if (!ts.modified()) return;
            gs.set(slot.value(), transfer.value()->capture(ts.base(), false,
                static_cast<std::size_t>(group.value()), static_cast<std::size_t>(groups.value())));
        }
    };

    /**
     * Drives one child graph through the ExternallyDriven executor.
     *
     * The whole of RFC 0037's per-cycle contract, with no transport:
     *
     *     stage(slot, delta) ... ; step(time); collect(slot) ...
     *     next_scheduled_time()   // what the child wants next
     *
     * A worker process is this class plus a codec and a pipe.
     */
    class HGRAPH_CLASS_EXPORT DistributedChildHost
    {
      public:
        DistributedChildHost(GraphBuilder graph_builder, DateTime end_time,
                             GraphExecutorPhaseRunner phase_runner = {})
        {
            GraphExecutorBuilder eb;
            eb.graph_builder(std::move(graph_builder))
                .mode(GraphExecutorMode::ExternallyDriven)
                .start_time(MIN_ST)
                .end_time(end_time)
                .phase_runner(std::move(phase_runner));
            executor_ = eb.make_executor();
        }

        void start(DateTime start_time)
        {
            executor_.view().start_external(start_time);
            locate_boundary_sources();
        }

        /**
         * Start from ``image`` instead of from nothing (RFC 0039). The sources
         * already hold their baselines, so the first cycle stages deltas, not
         * a full image.
         */
        void start_restored(DateTime start_time, const GraphCheckpointImage &image)
        {
            executor_.view().start_external_restored(start_time, image);
            locate_boundary_sources();
        }

        /** The whole graph's image at the last completed cycle. */
        [[nodiscard]] GraphCheckpointImage capture() const { return executor_.view().capture_external(); }

        /** Stage one boundary input for the next prepared cycle. */
        void stage(std::string_view slot, const ValueView &delta) const
        {
            executor_.view().graph().global_state().set(slot, delta);
        }

        /**
         * Evaluate one cycle at the caller's time, after scheduling every
         * boundary source so a staged value is seen this cycle.
         *
         * ``step`` refuses a time later than ``next_scheduled_time()``, so a
         * caller with work already due at an earlier time must evaluate that
         * first -- the child's own schedule is not overridden by preparing it.
         */
        bool step(DateTime evaluation_time) const
        {
            auto graph = executor_.view().graph();
            for (const std::size_t index : boundary_sources_)
            {
                graph.schedule_node(index, evaluation_time);
            }
            return executor_.view().step(evaluation_time);
        }

        /** Read and consume one boundary output; invalid when it did not tick. */
        [[nodiscard]] Value collect(std::string_view slot) const
        {
            auto            gs     = executor_.view().graph().global_state();
            const ValueView staged = gs.get(slot);
            if (!staged.valid()) { return Value{}; }
            Value out{staged};  // owned copy: the slot is erased below
            gs.erase(slot);
            return out;
        }

        [[nodiscard]] DateTime next_scheduled_time() const
        {
            return executor_.view().graph().next_scheduled_time();
        }

        void stop() const { executor_.view().stop_external(); }

        [[nodiscard]] GraphView graph() const { return executor_.view().graph(); }

      private:
        void locate_boundary_sources()
        {
            // Boundary sources are located once, by node name. The driver does
            // not need to know which slot each one serves: a source with
            // nothing staged returns without ticking, so scheduling all of them
            // is both correct and cheaper than maintaining a slot map.
            auto graph = executor_.view().graph();
            const std::size_t count = graph.node_count();
            for (std::size_t index = 0; index < count; ++index)
            {
                const auto *schema = graph.node_at(index).schema();
                if (schema != nullptr && schema->name() == boundary_source_name)
                {
                    boundary_sources_.push_back(index);
                }
            }
        }

        GraphExecutorValue       executor_{};
        std::vector<std::size_t> boundary_sources_{};
    };

    /**
     * Serve one cycle: apply a request to the child and report what happened.
     *
     * This is the whole of a worker's behaviour. Everything around it --
     * transport, framing, process management -- is plumbing, which is why it
     * is a pure function over the host rather than a method on a connection:
     * it can be tested without a socket, and a transport can be replaced
     * without retesting the semantics.
     *
     * A failure is REPORTED, not thrown. The caller is in another process and
     * cannot catch it; a reply carrying the rendered error is what lets the
     * ``dmap_`` node raise it as an ordinary node error on the far side.
     */
    [[nodiscard]] HGRAPH_EXPORT CycleReply serve_cycle(const DistributedChildHost &host,
                                                       const BoundarySlots &slots,
                                                       const CycleRequest &request);

    /**
     * The worker's graph as the bytes a channel carries (RFC 0039).
     *
     * Both hosting modes hold exactly these bytes, so an owner's checkpoint
     * state does not depend on how its workers were hosted -- which keeps
     * RFC 0037's attribution property: if the two modes disagree after a
     * restore, the fault is in the transport.
     */
    [[nodiscard]] HGRAPH_EXPORT std::string capture_worker_image(const DistributedChildHost &host);
    /** Start ``host`` from ``image`` and report what the restored graph wants next. */
    [[nodiscard]] HGRAPH_EXPORT DateTime start_worker_restored(DistributedChildHost &host, DateTime start_time,
                                                               std::string_view image);
}  // namespace hgraph::distributed

#endif  // HGRAPH_RUNTIME_DISTRIBUTED_CHILD_H
