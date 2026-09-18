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
// A worker is a PROCESS by default -- that is the point of the operator. The
// in-process mode below is the same model with the transport removed, kept
// because it is what makes a wrong answer attributable: if the two modes
// disagree, the fault is in the pipe, and if they agree and differ from
// ``map_``, it is in the model. It is selected by an explicit flag and never
// by default, so a ``dmap_`` cannot quietly fail to distribute.

#include <hgraph/hgraph_export.h>
#include <hgraph/lib/std/operators/higher_order.h>
#include <hgraph/runtime/distributed_child.h>
#include <hgraph/runtime/distributed_process.h>
#include <hgraph/runtime/distributed_protocol.h>
#include <hgraph/runtime/distributed_worker.h>
#include <hgraph/runtime/node_scheduler.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/wired_fn.h>
#include <hgraph/util/scope.h>

#include <fmt/format.h>

#include <algorithm>
#include <cstddef>
#include <chrono>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <tuple>
#include <typeinfo>
#include <utility>
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
            // Every worker graph carries checkpoint identities (RFC 0039): the
            // caller and the worker process each wire this for themselves.
            w.checkpoint_worker_graph();
            auto in  = wire<boundary_source_impl, TSD<TKey, TS<TValue>>>(w, Str{"in"});
            auto out = wire<stdlib::map_>(w, func.value(), in)
                           .template as<TSD<TKey, TS<TResult>>>();
            wire<boundary_sink_impl>(w, out, Str{"out"});
        }
    };

    /** The boundary a ``dmap_`` worker is driven through, in index order. */
    template <typename TKey, typename TValue, typename TResult> BoundarySlots distributed_map_slots()
    {
        // The slots carry DELTA schemas: what crosses the boundary each cycle
        // is a delta, not a value.
        BoundarySlots slots;
        slots.add("in", schema_descriptor<TSD<TKey, TS<TValue>>>::ts_meta()->delta_value_schema,
                  SlotDirection::Input);
        slots.add("out", schema_descriptor<TSD<TKey, TS<TResult>>>::ts_meta()->delta_value_schema,
                  SlotDirection::Output);
        return slots;
    }

    /**
     * The name the caller and the worker process agree on.
     *
     * Derived rather than chosen, so the two sides cannot drift: it is the
     * worker graph's type together with the kernel's identity, both mangled
     * names, which are the identities that survive across images (the
     * ``type_info`` ADDRESS does not -- see the WiredFn notes). A caller that
     * spawns a worker for an unregistered kernel therefore fails by name, with
     * the name it looked for.
     */
    template <typename TKey, typename TValue, typename TResult>
    std::string distributed_map_recipe_key(const WiredFn &func)
    {
        if (!func.valid()) { throw std::invalid_argument("dmap_: the child function is empty"); }
        return fmt::format("hgraph.dmap_|{}|{}",
                           typeid(DistributedWorkerGraph<TKey, TValue, TResult>).name(),
                           func.identity->name());
    }

    /**
     * Make ``Kernel`` available to worker processes as a ``dmap_`` child.
     *
     * Called from a translation unit linked into both the caller and the
     * worker program -- normally one program launched twice, which is what
     * makes "the two sides build the same graph" a property of the link rather
     * than of the protocol (RFC 0037, worker bootstrap).
     */
    template <typename Kernel, typename TKey, typename TValue, typename TResult>
    void register_distributed_map_worker()
    {
        register_worker_recipe(
            distributed_map_recipe_key<TKey, TValue, TResult>(fn<Kernel>()),
            WorkerRecipe{
                +[]() -> GraphBuilder {
                    return build_graph<DistributedWorkerGraph<TKey, TValue, TResult>>(
                        WiringOptions{.allow_push_sources = false, .inherit_global_context = false}, fn<Kernel>());
                },
                +[]() -> BoundarySlots {
                    return distributed_map_slots<TKey, TValue, TResult>();
                }});
    }

    /** Which group a key belongs to, and the count it is taken modulo. */
    struct HGRAPH_CLASS_EXPORT GroupSelector
    {
        std::size_t group{0};
        std::size_t groups{1};
    };

    /** How a pool's workers are hosted. */
    enum class WorkerHosting : std::uint8_t
    {
        Process,    ///< a separate OS process per worker -- what dmap_ is for
        InProcess,  ///< the same model, no transport: a control for the above
    };

    /** What a ``dmap_`` node needs to raise its workers. */
    struct HGRAPH_CLASS_EXPORT WorkerPoolConfig
    {
        std::size_t   workers{1};
        DateTime      start_time{MIN_ST};
        DateTime      end_time{MAX_ET};
        WorkerHosting hosting{WorkerHosting::Process};
        /** The worker executable; empty means this one (RFC 0037's bootstrap rule). */
        std::string   program{};
        /** Interpreter/bootstrap arguments preceding the native worker flags. */
        std::vector<std::string> arguments{};
        /** Maximum wall-clock time for process dispatch plus its reply. */
        std::chrono::milliseconds timeout{60000};
        /** Send each prepared recipe as the first channel frame instead of argv.
         * The worker receives the fixed marker @hgraph-channel-bootstrap:1.
         */
        bool recipe_over_channel{false};
    };

    /**
     * One worker, however it is hosted.
     *
     * ``dispatch`` and ``collect`` are separate so a pool can send to every
     * worker before waiting for any of them. For processes that separation is
     * the entire benefit -- fused, the workers would run strictly one after
     * another and ``dmap_`` would be ``map_`` with a pipe tax.
     */
    class HGRAPH_CLASS_EXPORT DistributedWorker
    {
      public:
        explicit DistributedWorker(std::unique_ptr<DistributedChildHost> host)
            : host_(std::move(host))
        {
        }
        explicit DistributedWorker(WorkerProcess process,
                                   std::chrono::milliseconds timeout = std::chrono::milliseconds{60000})
            : process_(std::move(process)), timeout_(timeout)
        {
            if (timeout <= std::chrono::milliseconds::zero() || timeout > std::chrono::hours{24})
                throw std::invalid_argument("dmap_: worker timeout must be positive and at most 24 hours");
            deadline_ = std::chrono::steady_clock::now() + timeout_;
        }

        void dispatch(const BoundarySlots &slots, const CycleRequest &request)
        {
            if (host_ != nullptr)
            {
                pending_ = serve_cycle(*host_, slots, request);
                return;
            }
            send(encode_request(slots, request));
        }

        void dispatch_shared(const BoundarySlots &slots, const CycleRequest &request,
                             std::string_view encoded)
        {
            if (host_ != nullptr) pending_ = serve_cycle(*host_, slots, request);
            else send(encoded);
        }

        [[nodiscard]] CycleReply collect(const BoundarySlots &slots)
        {
            if (host_ != nullptr) { return std::move(pending_); }
            auto failed = make_scope_exit([this] { process_.terminate(); });
            auto reply  = decode_reply(slots, receive());
            failed.release();
            return reply;
        }

        /**
         * Recovery (RFC 0039). A process worker is restored by its first
         * frame and answers with a ``CycleReply``, read by ``collect``; an
         * in-process worker is restored as it is raised. Both hosting modes
         * hand back the same image bytes.
         */
        void dispatch_restore(std::string_view image) { send(encode_restore_frame(image)); }

        void dispatch_checkpoint()
        {
            if (host_ == nullptr) { send(checkpoint_frame); }
        }

        [[nodiscard]] std::string collect_checkpoint()
        {
            if (host_ != nullptr) { return capture_worker_image(*host_); }
            auto failed = make_scope_exit([this] { process_.terminate(); });
            auto image  = decode_checkpoint_reply(receive());
            failed.release();
            return image;
        }

        void terminate() noexcept { process_.terminate(); }

        void stop()
        {
            if (host_ != nullptr)
            {
                host_->stop();
                return;
            }
            const int exit_code = process_.wait_for_exit();
            if (exit_code != 0)
                throw std::runtime_error(fmt::format("dmap_: worker exited with code {} during stop", exit_code));
        }

      private:
        [[nodiscard]] std::string receive()
        {
            std::string payload;
            if (!process_.channel().receive(payload, deadline_))
            {
                // The worker went away between the request and the reply,
                // which is the shape a crash takes from here.
                throw std::runtime_error(fmt::format(
                    "dmap_: worker process {} closed the channel without replying", process_.pid()));
            }
            return payload;
        }

        void send(std::string_view payload)
        {
            deadline_ = std::chrono::steady_clock::now() + timeout_;
            auto failed = make_scope_exit([this] { process_.terminate(); });
            process_.channel().send(payload, deadline_);
            failed.release();
        }
        std::unique_ptr<DistributedChildHost> host_{};
        WorkerProcess                         process_{};
        /** In-process only: the reply ``dispatch`` already produced. */
        CycleReply                            pending_{};
        std::chrono::milliseconds timeout_{60000};
        PipeEndpoint::Deadline deadline_{PipeEndpoint::Deadline::max()};
    };

    /**
     * The workers one ``dmap_`` node owns.
     *
     * Heap-held, with the node's ``State`` carrying only the pointer: the
     * start-lifecycle pattern the persistence extension uses for the same
     * reason, since a node state must be a trivially copyable registered
     * scalar and a set of live workers is neither.
     */
    class HGRAPH_CLASS_EXPORT WorkerPool
    {
      public:
        WorkerPool(const WorkerPool &) = delete;
        WorkerPool &operator=(const WorkerPool &) = delete;

        template <typename TKey, typename TValue, typename TResult>
        static std::unique_ptr<WorkerPool> build(const WiredFn &func, const WorkerPoolConfig &config,
                                                 std::span<const std::string> restored = {})
        {
            validate_timeout(config);
            if (config.workers == 0)
            {
                throw std::invalid_argument("dmap_ needs at least one worker");
            }
            GraphBuilder child = validated_child<TKey, TValue, TResult>(func);
            const std::string key = distributed_map_recipe_key<TKey, TValue, TResult>(func);
            if (config.hosting == WorkerHosting::Process && worker_recipe(key) == nullptr)
            {
                throw std::invalid_argument(fmt::format(
                    "dmap_: no distributed worker is registered for this child function. Call "
                    "register_distributed_map_worker<Kernel, Key, Value, Result>(). Looked for: '{}'", key));
            }
            return build(child, distributed_map_slots<TKey, TValue, TResult>(), key, config, {}, restored);
        }

        /** The worker graph ``build`` would raise, for the owner's contract signature. */
        template <typename TKey, typename TValue, typename TResult>
        [[nodiscard]] static GraphBuilder worker_graph(const WiredFn &func)
        {
            return validated_child<TKey, TValue, TResult>(func);
        }

        /** Build from an already wired child, including embedding-runtime plans.
         * The named bootstrap must rebuild the same boundary in each process.
         * Phase runners wrap in-process child lifecycle/evaluation only.
         */
        static std::unique_ptr<WorkerPool> build(
            const GraphBuilder &child, BoundarySlots slots, std::string_view recipe,
            const WorkerPoolConfig &config, GraphExecutorPhaseRunner phase_runner = {},
            std::span<const std::string> restored = {})
        {
            if (config.workers == 0) { throw std::invalid_argument("dmap_ needs at least one worker"); }
            validate_timeout(config);
            reject_push_sources(child);
            require_restored_inventory(restored, config.workers);
            auto pool = std::unique_ptr<WorkerPool>(new WorkerPool{});
            UnwindCleanupGuard failed{[&pool] { pool->terminate(); }};
            pool->groups_ = config.workers;
            pool->workers_.reserve(config.workers);
            pool->slots_ = std::move(slots);
            pool->in_slot_ = pool->slots_.index_of("in");
            for (std::size_t i = 0; i < config.workers; ++i)
            {
                if (config.hosting == WorkerHosting::InProcess)
                {
                    pool->workers_.emplace_back(
                        raise_host(child, config, phase_runner, restored.empty() ? nullptr : &restored[i], i));
                }
                else
                {
                    pool->workers_.emplace_back(spawn_worker(config.program, recipe, config.start_time,
                                                            config.end_time, config.arguments), config.timeout);
                }
                pool->selectors_.push_back(GroupSelector{i, config.workers});
            }
            if (config.hosting == WorkerHosting::Process) { pool->restore_processes(restored); }
            return pool;
        }

        /** The in-process pool, for a test or a debugging run. */
        template <typename TKey, typename TValue, typename TResult>
        static std::unique_ptr<WorkerPool> build(const WiredFn &func, std::size_t workers,
                                                 DateTime start_time, DateTime end_time)
        {
            return build<TKey, TValue, TResult>(
                func, WorkerPoolConfig{workers, start_time, end_time, WorkerHosting::InProcess, {}});
        }

        /**
         * One cycle: partition, dispatch to every worker, then collect.
         *
         * Replies are applied straight into ``out`` with no merge, which is a
         * consequence of the partition rather than a shortcut -- each key
         * belongs to exactly one group, so the replies touch disjoint keys and
         * applying them in turn is applying their union.
         *
         * Returns the earliest time any worker's children want next, or
         * ``MAX_DT`` when none of them want anything. The caller must honour
         * it: a child that schedules itself has no other way to be woken,
         * since nothing in the calling graph ticks on its behalf.
         */
        [[nodiscard]] DateTime evaluate(const TSInputView &in, const TSOutputView &out,
                                        DateTime now)
        {
            UnwindCleanupGuard failed{[this] { terminate(); }};
            for (std::size_t group = 0; group < groups_; ++group)
            {
                CycleRequest request;
                request.evaluation_time = now;

                // A cycle the node was woken for by its own schedule has no
                // input tick to partition. Staging an empty delta anyway would
                // apply one in the child and invent a tick the caller never
                // sent, so the same gate the boundary sink uses decides
                // whether there is anything to send.
                if (in.modified())
                {
                    Value delta = capture_dict_delta_where(in, &key_in_group, &selectors_[group]);
                    if (delta.view().valid() && delta_is_observable(in, delta.view()))
                    {
                        request.staged.push_back(SlotDelta{in_slot_, std::move(delta)});
                    }
                }
                workers_[group].dispatch(slots_, request);
            }

            DateTime next = MAX_DT;
            for (std::size_t group = 0; group < groups_; ++group)
            {
                const CycleReply reply = workers_[group].collect(slots_);
                if (!reply.error.empty())
                {
                    // A worker reports rather than throws, because across a
                    // process boundary it must; re-raising here is what turns
                    // it back into an ordinary node error for the caller.
                    throw std::runtime_error(
                        fmt::format("dmap_: partition {} failed: {}", group, reply.error));
                }
                for (const auto &collected : reply.collected)
                {
                    apply_delta(out, collected.delta.view());
                }
                next = std::min(next, reply.next_scheduled_time);
            }
            return next;
        }

        void stop()
        {
            for (auto &worker : workers_) { worker.stop(); }
        }

        /** Prepared workers share a boundary but own disjoint mapped children. */
        static std::unique_ptr<WorkerPool> build_partitioned(
            std::span<const GraphBuilder> children, BoundarySlots slots,
            std::span<const std::string> recipes, const WorkerPoolConfig &config,
            GraphExecutorPhaseRunner phase_runner = {}, std::span<const std::string> restored = {})
        {
            validate_timeout(config);
            if (children.empty() || children.size() != config.workers || recipes.size() != children.size())
                throw std::invalid_argument("dmap_: inconsistent worker plan inventory");
            require_restored_inventory(restored, children.size());
            auto pool = std::unique_ptr<WorkerPool>{new WorkerPool{}};
            UnwindCleanupGuard failed_pool{[&pool] { pool->terminate(); }};
            pool->groups_ = children.size();
            pool->slots_ = std::move(slots);
            pool->workers_.reserve(children.size());
            pool->output_extents_.resize(children.size());
            for (std::size_t group = 0; group < children.size(); ++group)
            {
                reject_push_sources(children[group]);
                if (config.hosting == WorkerHosting::InProcess)
                {
                    pool->workers_.emplace_back(raise_host(children[group], config, phase_runner,
                                                           restored.empty() ? nullptr : &restored[group], group));
                }
                else
                {
                    const std::string_view recipe_key = config.recipe_over_channel
                        ? std::string_view{"@hgraph-channel-bootstrap:1"} : std::string_view{recipes[group]};
                    auto process = spawn_worker(config.program, recipe_key, config.start_time,
                                                config.end_time, config.arguments);
                    auto failed = make_scope_exit([&process] { process.terminate(); });
                    if (config.recipe_over_channel)
                        process.channel().send(recipes[group], std::chrono::steady_clock::now() + config.timeout);
                    pool->workers_.emplace_back(std::move(process), config.timeout);
                    failed.release();
                }
            }
            if (config.hosting == WorkerHosting::Process) { pool->restore_processes(restored); }
            return pool;
        }

        /**
         * One image per worker, in worker order (RFC 0039). Every worker is
         * asked before any is waited for, as a cycle does. A worker that
         * cannot capture fails the capture: no completed day is published.
         */
        [[nodiscard]] std::vector<std::string> capture()
        {
            UnwindCleanupGuard failed{[this] { terminate(); }};
            for (auto &worker : workers_) { worker.dispatch_checkpoint(); }
            std::vector<std::string> images;
            images.reserve(workers_.size());
            for (std::size_t group = 0; group < workers_.size(); ++group)
            {
                try { images.push_back(workers_[group].collect_checkpoint()); }
                catch (const std::exception &error)
                {
                    throw std::runtime_error(
                        fmt::format("dmap_: partition {} cannot be checkpointed: {}", group, error.what()));
                }
            }
            return images;
        }

        [[nodiscard]] std::size_t worker_count() const noexcept { return workers_.size(); }
        [[nodiscard]] std::span<const std::size_t> output_extents() const noexcept { return output_extents_; }
        void restore_output_extents(std::span<const std::size_t> extents)
        {
            if (extents.size() != output_extents_.size())
                throw std::invalid_argument("dmap_: restored output extents do not match the worker plan");
            output_extents_.assign(extents.begin(), extents.end());
        }

        /** Capture each broadcast boundary once, then fan out before collecting.
         * Cost is linear in changed data times the configured worker count;
         * there is no repeated per-key search or reconstruction here.
         */
        template <typename Apply> DateTime exchange(const CycleRequest &request, Apply &&apply)
        {
            UnwindCleanupGuard failed{[this] { terminate(); }};
            const auto encoded = encode_request(slots_, request);
            for (auto &worker : workers_) { worker.dispatch_shared(slots_, request, encoded); }
            DateTime next = MAX_DT;
            for (std::size_t group = 0; group < workers_.size(); ++group)
            {
                auto reply = workers_[group].collect(slots_);
                if (!reply.error.empty())
                    throw std::runtime_error(fmt::format("dmap_: partition {} failed: {}", group, reply.error));
                apply(group, reply);
                next = std::min(next, reply.next_scheduled_time);
            }
            return next;
        }

        void output_extent(std::size_t worker, std::size_t size) { output_extents_.at(worker) = size; }
        [[nodiscard]] std::size_t output_extent() const
        {
            return output_extents_.empty() ? 0 : *std::max_element(output_extents_.begin(), output_extents_.end());
        }

      private:
        WorkerPool() = default;

        static void require_restored_inventory(std::span<const std::string> restored, std::size_t workers)
        {
            // Placement is hash % workers and is not stored, so a different
            // count would silently re-partition restored keys.
            if (!restored.empty() && restored.size() != workers)
                throw std::invalid_argument(fmt::format(
                    "dmap_: the checkpoint holds {} worker images and the plan has {} workers",
                    restored.size(), workers));
        }

        // A restored worker wants nothing: an image never holds a pending
        // schedule. One that reports otherwise has a wake-up its owner cannot
        // honour, because the owner's own bootstrap schedule is discarded.
        static void require_idle(DateTime next, std::size_t group)
        {
            if (next != MAX_DT)
                throw std::runtime_error(fmt::format(
                    "dmap_: restored partition {} reports a pending schedule", group));
        }

        static std::unique_ptr<DistributedChildHost> raise_host(
            const GraphBuilder &child, const WorkerPoolConfig &config,
            const GraphExecutorPhaseRunner &phase_runner, const std::string *image, std::size_t group)
        {
            auto host = std::make_unique<DistributedChildHost>(child, config.end_time, phase_runner);
            if (image == nullptr) { host->start(config.start_time); }
            else
            {
                try { require_idle(start_worker_restored(*host, config.start_time, *image), group); }
                catch (const std::exception &error)
                {
                    throw std::runtime_error(
                        fmt::format("dmap_: partition {} refused its image: {}", group, error.what()));
                }
            }
            return host;
        }

        /** Send every restore before waiting for any reply, as a cycle does. */
        void restore_processes(std::span<const std::string> restored)
        {
            if (restored.empty()) { return; }
            for (std::size_t group = 0; group < workers_.size(); ++group) { workers_[group].dispatch_restore(restored[group]); }
            for (std::size_t group = 0; group < workers_.size(); ++group)
            {
                const auto reply = workers_[group].collect(slots_);
                if (!reply.error.empty())
                    throw std::runtime_error(
                        fmt::format("dmap_: partition {} refused its image: {}", group, reply.error));
                require_idle(reply.next_scheduled_time, group);
            }
        }

        static void validate_timeout(const WorkerPoolConfig &config)
        {
            // Also bounds chrono arithmetic and platform millisecond waits.
            if (config.timeout <= std::chrono::milliseconds::zero() ||
                config.timeout > std::chrono::hours{24})
                throw std::invalid_argument("dmap_: worker timeout must be positive and at most 24 hours");
        }
        void terminate() noexcept
        { for (auto &worker : workers_) worker.terminate(); }

        /**
         * Wire the child, turning what a distributed worker cannot host into a
         * diagnostic that names the cause.
         *
         * A worker graph is TOP-LEVEL, so a child that consumes a service,
         * context or shared output has no enclosing graph to resolve it
         * against and wiring already fails. That diagnostic names the service,
         * not ``dmap_``, so it is caught here and given the reason -- a
         * distributed child cannot reach them because their source lives in
         * the CALLING graph, as graph-local reference state
         * (developer_guide/services.rst).
         */
        template <typename TKey, typename TValue, typename TResult>
        static GraphBuilder validated_child(const WiredFn &func)
        {
            GraphBuilder child = [&] {
                try
                {
                    return build_graph<DistributedWorkerGraph<TKey, TValue, TResult>>(
                        WiringOptions{.allow_push_sources = false, .inherit_global_context = false}, func);
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
            return child;
        }

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

        std::vector<DistributedWorker> workers_{};
        std::vector<std::size_t>        output_extents_{};
        std::vector<GroupSelector>     selectors_{};
        BoundarySlots                  slots_{};
        std::size_t                    in_slot_{0};
        std::size_t                    groups_{1};
    };

    /** The node-State payload: a heap handle, as the start-lifecycle pattern has it. */
    struct HGRAPH_CLASS_EXPORT DistributedMapState
    {
        WorkerPool *pool{nullptr};
    };

    /**
     * The checkpoint contract of a ``dmap_`` owner (RFC 0039).
     *
     * The owner is a dynamic-graph owner whose children live in workers, so
     * its state is one graph image per worker, in worker order, plus the
     * output extents. ``restore`` runs before the owner starts and the workers
     * are raised IN its start, so the restored state is parked in the graph's
     * ``GlobalState`` and ``claim``-ed by the start that follows.
     *
     * ``signature`` is the owner's contract: the worker count, the hosting
     * mode and every worker node's identity. It refuses a plan that hosts a
     * node a worker scope recorded as unrecoverable, which is how a
     * recoverable component learns that at wiring.
     */
    namespace dmap_checkpoint
    {
        struct Restored
        {
            std::vector<std::string> images{};
            std::vector<std::size_t> extents{};
        };
        [[nodiscard]] HGRAPH_EXPORT NodeCheckpointState capture(const NodeView &node, const CaptureGraphCheckpoint &);
        HGRAPH_EXPORT void restore(const NodeView &node, const NodeCheckpointState &image, DateTime,
                                   const RestoreGraphCheckpoint &);
        [[nodiscard]] HGRAPH_EXPORT std::optional<Restored> claim(const NodeView &node);
        [[nodiscard]] HGRAPH_EXPORT std::string signature(std::span<const GraphBuilder> children,
                                                          const WorkerPoolConfig &config);
    }

    /**
     * ``dmap_(func, ts, workers)`` -- ``map_`` with its children distributed.
     *
     * The contract is equality with ``map_``: the worker count is a throughput
     * decision and must not be observable in the result. Whether it is worth
     * paying for depends entirely on how much work each child does per tick,
     * which is a measurement the caller has to make -- see RFC 0037's
     * performance section, where trivial children are more than an order of
     * magnitude WORSE distributed.
     *
     * ``in_process`` runs the workers here instead of as processes. It is the
     * control for the distributed path rather than a deployment option, and
     * defaults to false so that ``dmap_`` always means what its name says.
     * ``program`` names the worker executable; empty means this one.
     */
    template <typename TKey, typename TValue, typename TResult>
    struct dmap_impl
    {
        static constexpr auto name = "dmap_";

        static auto defaults()
        {
            return std::tuple{arg<"workers">(Int{2}), arg<"in_process">(Bool{false}),
                              arg<"program">(Str{})};
        }

        // Every worker hosts the same graph, so one stands for them all in
        // the contract; the count is signed beside it.
        static const NodeCheckpointOps &checkpoint_ops() noexcept
        {
            static const NodeCheckpointOps ops{
                .supported = true,
                .capture_impl = &dmap_checkpoint::capture,
                .restore_impl = &dmap_checkpoint::restore,
                .signature_impl = +[](const NodeBuilder &builder) {
                    const auto scalars = builder.scalars().view().as_bundle();
                    const Int  workers = scalars.at("workers").template checked_as<Int>();
                    if (workers <= 0) { throw std::invalid_argument("dmap_ needs at least one worker"); }
                    WorkerPoolConfig config;
                    config.workers = static_cast<std::size_t>(workers);
                    config.hosting = scalars.at("in_process").template checked_as<Bool>() ? WorkerHosting::InProcess
                                                                                          : WorkerHosting::Process;
                    const GraphBuilder child = WorkerPool::worker_graph<TKey, TValue, TResult>(
                        scalars.at("func").template checked_as<WiredFn>());
                    return dmap_checkpoint::signature({&child, 1}, config);
                },
            };
            return ops;
        }

        static void start(Scalar<"func", WiredFn> func, Scalar<"workers", Int> workers,
                          Scalar<"in_process", Bool> in_process, Scalar<"program", Str> program,
                          NodeView node, EngineControlView engine, State<DistributedMapState> state)
        {
            if (workers.value() <= 0)
                throw std::invalid_argument("dmap_ needs at least one worker");
            WorkerPoolConfig config;
            config.workers    = static_cast<std::size_t>(workers.value());
            config.start_time = engine.start_time();
            config.end_time   = engine.end_time();
            config.hosting =
                in_process.value() ? WorkerHosting::InProcess : WorkerHosting::Process;
            config.program = program.value();

            // Workers the coordinator restored start from their images (RFC 0039).
            const auto restored = dmap_checkpoint::claim(node);
            auto pool = WorkerPool::build<TKey, TValue, TResult>(func.value(), config,
                restored ? std::span<const std::string>{restored->images} : std::span<const std::string>{});
            state.modify().pool = pool.release();
        }

        static void eval(In<"ts", TSD<TKey, TS<TValue>>> ts, Scalar<"func", WiredFn>,
                         Scalar<"workers", Int>, Scalar<"in_process", Bool>, Scalar<"program", Str>,
                         State<DistributedMapState> state, NodeScheduler scheduler, DateTime now,
                         Out<TSD<TKey, TS<TResult>>> out)
        {
            const DateTime next = state.get().pool->evaluate(ts.base(), out.base(), now);
            if (next == MAX_DT) { return; }
            // This node is the only thing that can wake a distributed child:
            // its children are in another process, invisible to this graph's
            // scheduler. Clamped to the next representable instant because a
            // schedule taken during ``eval`` must be in the future, and a
            // child that re-scheduled itself within this very cycle reports
            // the cycle's own time.
            scheduler.schedule(std::max(next, now + MIN_TD));
        }

        static void stop(State<DistributedMapState> state)
        {
            auto &payload = state.modify();
            if (payload.pool == nullptr) { return; }
            std::unique_ptr<WorkerPool> pool{std::exchange(payload.pool, nullptr)};
            pool->stop();
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
