#ifndef HGRAPH_RUNTIME_DISTRIBUTED_MAP_H
#define HGRAPH_RUNTIME_DISTRIBUTED_MAP_H

// ``dmap_``: a ``map_`` whose per-key children are evaluated by workers
// (RFC 0037).
//
// Per cycle the caller partitions this cycle's TSD delta by key, hands each
// worker the part it owns, waits for every reply, and applies them straight
// into its own output. Each worker hosts an ORDINARY ``map_`` over its group,
// so per-key construction, teardown and state are the existing behaviour
// rather than a reimplementation.
//
// The workers here are IN-PROCESS. That is not the end state -- the point of
// RFC 0037 is other processes -- but it is the part that carries the
// semantics, and keeping it separable means a transport failure can never be
// mistaken for a modelling one. The transport replaces who ``serve_cycle`` is
// called by, and nothing above it.

#include <hgraph/hgraph_export.h>
#include <hgraph/lib/std/operators/higher_order.h>
#include <hgraph/runtime/distributed_child.h>
#include <hgraph/runtime/distributed_protocol.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/wired_fn.h>

#include <cstddef>
#include <tuple>
#include <memory>
#include <stdexcept>
#include <vector>

namespace hgraph::distributed
{
    /**
     * What one worker runs: a staged boundary in, an ordinary ``map_``, a
     * collected boundary out.
     *
     * The kernel arrives as a wiring-time ``WiredFn`` scalar, which is what
     * lets one graph template serve any child function.
     */
    template <typename TKey, typename TValue, typename TResult>
    struct DistributedWorkerGraph
    {
        static constexpr auto name = "distributed_worker_graph";

        static void compose(Wiring &w, Scalar<"func", WiredFn> func)
        {
            auto in  = wire<boundary_source_impl, TSD<TKey, TS<TValue>>>(w, Str{"in"});
            auto out = wire<stdlib::map_>(w, func.value(), in)
                           .template as<TSD<TKey, TS<TResult>>>();
            wire<boundary_sink_impl>(w, out, Str{"out"});
        }
    };

    /** Which group a key belongs to, and the count it is taken modulo. */
    struct HGRAPH_CLASS_EXPORT GroupSelector
    {
        std::size_t group{0};
        std::size_t groups{1};
    };

    /**
     * The workers one ``dmap_`` node owns.
     *
     * Heap-held, with the node's ``State`` carrying only the pointer: the
     * start-lifecycle pattern the persistence extension uses for the same
     * reason, since a node state must be a trivially copyable registered
     * scalar and a set of live executors is neither.
     */
    class HGRAPH_CLASS_EXPORT WorkerPool
    {
      public:
        template <typename TKey, typename TValue, typename TResult>
        static std::unique_ptr<WorkerPool> build(const WiredFn &func, std::size_t workers,
                                                 DateTime start_time, DateTime end_time)
        {
            if (workers == 0) { throw std::invalid_argument("dmap_ needs at least one worker"); }
            auto pool = std::unique_ptr<WorkerPool>(new WorkerPool{});
            pool->groups_ = workers;

            // A worker graph is TOP-LEVEL, so a child that consumes a service,
            // context or shared output has no enclosing graph to resolve it
            // against and wiring already fails. That diagnostic names the
            // service, not dmap_, so it is caught here and given the reason --
            // a distributed child cannot reach them because their source lives
            // in the CALLING graph, as graph-local reference state
            // (developer_guide/services.rst).
            GraphBuilder child = [&] {
                try
                {
                    return build_graph<DistributedWorkerGraph<TKey, TValue, TResult>>(func);
                }
                catch (const std::exception &error)
                {
                    throw std::invalid_argument(
                        std::string{"dmap_: the child function cannot be wired for a distributed "
                                    "worker. Services, contexts and shared outputs are not "
                                    "available to a distributed child -- their source lives in the "
                                    "calling graph. Wiring reported: "} +
                        error.what());
                }
            }();
            reject_push_sources(child);

            const auto *delta =
                schema_descriptor<TSD<TKey, TS<TValue>>>::ts_meta()->delta_value_schema;
            const auto *result =
                schema_descriptor<TSD<TKey, TS<TResult>>>::ts_meta()->delta_value_schema;
            // The slots carry DELTA schemas: what crosses the boundary each
            // cycle is a delta, not a value.
            pool->in_slot_  = pool->slots_.add("in", delta, SlotDirection::Input);
            pool->out_slot_ = pool->slots_.add("out", result, SlotDirection::Output);

            for (std::size_t i = 0; i < workers; ++i)
            {
                auto host = std::make_unique<DistributedChildHost>(
                    i == 0 ? std::move(child)
                           : build_graph<DistributedWorkerGraph<TKey, TValue, TResult>>(func),
                    end_time);
                host->start(start_time);
                pool->hosts_.push_back(std::move(host));
            }
            pool->selectors_.reserve(workers);
            for (std::size_t i = 0; i < workers; ++i)
            {
                pool->selectors_.push_back(GroupSelector{i, workers});
            }
            return pool;
        }

        /**
         * One cycle: partition, dispatch, apply.
         *
         * Replies are applied straight into ``out`` with no merge, which is a
         * consequence of the partition rather than a shortcut -- each key
         * belongs to exactly one group, so the replies touch disjoint keys and
         * applying them in turn is applying their union.
         */
        void evaluate(const TSInputView &in, const TSOutputView &out, DateTime now) const
        {
            for (std::size_t group = 0; group < groups_; ++group)
            {
                CycleRequest request;
                request.evaluation_time = now;
                request.staged.push_back(
                    SlotDelta{in_slot_,
                              capture_dict_delta_where(in, &key_in_group, &selectors_[group])});

                const CycleReply reply = serve_cycle(*hosts_[group], slots_, request);
                if (!reply.error.empty())
                {
                    // A worker reports rather than throws, because across a
                    // process boundary it must; re-raising here is what turns
                    // it back into an ordinary node error for the caller.
                    throw std::runtime_error(reply.error);
                }
                for (const auto &collected : reply.collected)
                {
                    apply_delta(out, collected.delta.view());
                }
            }
        }

        void stop() const
        {
            for (const auto &host : hosts_) { host->stop(); }
        }

      private:
        WorkerPool() = default;

        static bool key_in_group(const void *context, const ValueView &key)
        {
            const auto &selector = *static_cast<const GroupSelector *>(context);
            // Stable within a run and independent of insertion order, which is
            // all the partition has to be: the caller is authoritative and
            // tells each worker its keys, so no worker ever recomputes this
            // (RFC 0037 -- the two sides need not agree on a hash).
            return (key_hash(key) % selector.groups) == selector.group;
        }

        static std::size_t key_hash(const ValueView &key) { return key.hash(); }

        /**
         * A push source in a distributed child is refused here, positively.
         *
         * The executor would refuse it too -- it is rejected outside RealTime
         * -- but that message is about executors. It is also not merely a
         * policy: a push source is a ROOT-graph facility, injecting events on
         * its own thread, so inside a worker its events would arrive on the
         * worker's timeline rather than the caller's and the run would stop
         * being reproducible.
         */
        static void reject_push_sources(const GraphBuilder &child)
        {
            for (const NodeBuilder &node : child.nodes())
            {
                const auto *schema = node.type().schema();
                if (schema != nullptr && schema->node_kind == NodeKind::PushSource)
                {
                    throw std::invalid_argument(
                        "dmap_: the child function contains a push source, which a distributed "
                        "child cannot have. Its events would arrive on the worker's timeline "
                        "rather than the caller's.");
                }
            }
        }

        std::vector<std::unique_ptr<DistributedChildHost>> hosts_{};
        std::vector<GroupSelector>                         selectors_{};
        BoundarySlots                                      slots_{};
        std::size_t                                        in_slot_{0};
        std::size_t                                        out_slot_{0};
        std::size_t                                        groups_{1};
    };

    /** The node-State payload: a heap handle, as the start-lifecycle pattern has it. */
    struct HGRAPH_CLASS_EXPORT DistributedMapState
    {
        WorkerPool *pool{nullptr};
    };

    /**
     * ``dmap_(func, ts, workers)`` -- ``map_`` with its children distributed.
     *
     * The contract is equality with ``map_``: the worker count is a throughput
     * decision and must not be observable in the result. Whether it is worth
     * paying for depends entirely on how much work each child does per tick,
     * which is a measurement the caller has to make -- see RFC 0037's
     * performance section, where trivial children are more than an order of
     * magnitude WORSE distributed.
     */
    template <typename TKey, typename TValue, typename TResult>
    struct dmap_impl
    {
        static constexpr auto name = "dmap_";

        static auto defaults() { return std::tuple{arg<"workers">(Int{2})}; }

        static void start(Scalar<"func", WiredFn> func, Scalar<"workers", Int> workers,
                          EngineControlView engine, State<DistributedMapState> state)
        {
            auto pool = WorkerPool::build<TKey, TValue, TResult>(
                func.value(), static_cast<std::size_t>(workers.value()), engine.start_time(),
                engine.end_time());
            state.modify().pool = pool.release();
        }

        static void eval(In<"ts", TSD<TKey, TS<TValue>>> ts, Scalar<"func", WiredFn>,
                         Scalar<"workers", Int>, State<DistributedMapState> state, DateTime now,
                         Out<TSD<TKey, TS<TResult>>> out)
        {
            state.get().pool->evaluate(ts.base(), out.base(), now);
        }

        static void stop(State<DistributedMapState> state)
        {
            auto &payload = state.modify();
            if (payload.pool == nullptr) { return; }
            payload.pool->stop();
            delete payload.pool;
            payload.pool = nullptr;
        }
    };
}  // namespace hgraph::distributed

namespace hgraph::static_schema_detail
{
    template <> struct scalar_name<distributed::DistributedMapState>
    {
        static constexpr std::string_view value{"hgraph.distributed.dmap_state"};
    };
}  // namespace hgraph::static_schema_detail

#endif  // HGRAPH_RUNTIME_DISTRIBUTED_MAP_H
