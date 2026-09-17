#include <hgraph/runtime/spawn.h>
#include <hgraph/runtime/distributed_child.h>
#include <hgraph/runtime/distributed_process.h>
#include <hgraph/manifest/schema_descriptor.h>
#include <hgraph/runtime/executor_activity.h>
#include <hgraph/util/scope.h>

#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>
#include <unordered_map>

namespace hgraph::spawn_detail
{
    using distributed::BoundaryTransfer;
    using distributed::BoundaryTransferPtr;

    struct StagePlan
    {
        distributed::BoundarySlots slots;
        std::string recipe;
        std::string bootstrap;
        std::string boundary_identity;
        std::size_t flow_slot{static_cast<std::size_t>(-1)};
        bool output{false};
    };
    struct ExternalInput
    {
        std::size_t stage;
        std::size_t slot;
        BoundaryTransferPtr transfer;
    };
    struct Plan
    {
        std::vector<StagePlan> stages;
        std::vector<ExternalInput> inputs;
        const TSValueTypeMetaData *input_schema{};
        SpawnConfig config;
        SpawnWaitRunner wait_runner;
    };
    using PlanPtr = std::shared_ptr<const Plan>;

    struct Delta
    {
        std::size_t slot;
        Value payload;
    };
    struct Frame
    {
        DateTime time;
        std::vector<Delta> deltas;
        std::size_t bytes{0};
    };
    struct Channel
    {
        std::deque<Frame> frames;
        std::size_t bytes{0};
        std::size_t admitted{0}; // Includes frames currently being evaluated.
        std::condition_variable space;
    };
    struct StageState
    {
        distributed::WorkerProcess process;
        std::thread thread;
        std::condition_variable work;
        Channel side;
        Channel flow;
        DateTime completed{MIN_DT};
        DateTime next{MAX_DT};
        bool ready{false};
        bool busy{true};
    };

    /** Graphs execute only in worker processes. Transport threads own channels,
        encoded messages and cursors; they never evaluate a graph or hold a GIL. */
    class Runtime
    {
      public:
        Runtime(PlanPtr plan, DateTime start, DateTime end)
            : plan_(std::move(plan)), start_(start), end_(end), frontier_(start - MIN_TD)
        {
            stages_.reserve(plan_->stages.size());
            pending_.resize(plan_->stages.size());
            for (const auto &stage : plan_->stages)
            {
                auto state = std::make_unique<StageState>();
                (void)stage;
                state->completed = frontier_;
                stages_.push_back(std::move(state));
            }
        }
        ~Runtime()
        {
            cancel();
            join();
        }
        Runtime(const Runtime &) = delete;
        Runtime &operator=(const Runtime &) = delete;

        ExecutorActivity activity()
        {
            static const ExecutorActivityOps ops{
                [](void *self, bool wait) { return static_cast<Runtime *>(self)->next_time(wait); },
                [](void *self, DateTime time) { static_cast<Runtime *>(self)->complete(time); }};
            return {this, ops};
        }
        void start(ExecutorActivityWake wake)
        {
            wake_ = std::move(wake);
            // Launch serially: process creation must not race inherited handle
            // setup. Transport begins only after every child has been launched.
            for (std::size_t index = 0; index < stages_.size(); ++index)
                stages_[index]->process = distributed::spawn_worker(plan_->config.worker_program,
                    std::string{spawn_worker_prefix} + plan_->stages[index].recipe,
                    start_, end_, plan_->config.worker_arguments);
            for (std::size_t index = 0; index < stages_.size(); ++index)
                stages_[index]->thread = std::thread([this, index] { run_stage(index); });
            wait_call([&] {
                std::unique_lock lock{mutex_};
                condition_.wait(lock, [&] {
                    return !error_.empty() || std::all_of(stages_.begin(), stages_.end(),
                        [](const auto &stage) { return stage->ready; });
                });
                check_error();
            });
        }
        void capture(const TSInputView &inputs, DateTime time)
        {
            const auto bundle = inputs.as_bundle();
            for (std::size_t i = 0; i < plan_->inputs.size(); ++i)
            {
                const auto input = bundle[i];
                if (!first_ && !input.modified()) continue;
                const auto &binding = plan_->inputs[i];
                auto payload = binding.transfer->capture(input, first_);
                auto &frame = pending_[binding.stage];
                if (!frame) frame = Frame{time, {}, 0};
                const auto size = payload.view().checked_as<Str>().size();
                if (size > plan_->config.capacity_bytes || frame->bytes > plan_->config.capacity_bytes - size)
                    throw std::runtime_error("spawn_: an input frame exceeds capacity_bytes");
                frame->bytes += size;
                frame->deltas.push_back({binding.slot, std::move(payload)});
            }
            first_ = false;
        }
        void finish()
        {
            wait_call([&] {
                {
                    std::lock_guard lock{mutex_};
                    sealed_ = true;
                }
                for (auto &stage : stages_) stage->work.notify_one();
                join();
                std::lock_guard lock{mutex_};
                check_error();
            });
        }

      private:
        void wait_call(const std::function<void()> &action) const
        {
            if (plan_->wait_runner) plan_->wait_runner(action);
            else action();
        }
        void check_error() const
        {
            if (!error_.empty()) throw std::runtime_error("spawn_: " + error_);
        }
        void cancel() noexcept
        {
            {
                std::lock_guard lock{mutex_};
                cancelled_ = true;
            }
            condition_.notify_all();
            for (auto &stage : stages_)
            {
                stage->work.notify_all();
                stage->side.space.notify_all();
                stage->flow.space.notify_all();
            }
        }
        void join() noexcept
        {
            for (auto &stage : stages_)
                if (stage->thread.joinable()) stage->thread.join();
        }
        bool room(const Channel &channel, std::size_t bytes) const
        {
            return channel.admitted < plan_->config.capacity_frames &&
                bytes <= plan_->config.capacity_bytes - channel.bytes;
        }
        bool send(std::unique_lock<std::mutex> &lock, Channel &channel, Frame frame)
        {
            if (frame.bytes > plan_->config.capacity_bytes)
                throw std::runtime_error("a boundary frame exceeds capacity_bytes");
            channel.space.wait(lock, [&] { return cancelled_ || room(channel, frame.bytes); });
            if (cancelled_) return false;
            ++channel.admitted;
            channel.bytes += frame.bytes;
            channel.frames.push_back(std::move(frame));
            return true;
        }
        static DateTime first_time(const Channel &channel)
        { return channel.frames.empty() ? MAX_DT : channel.frames.front().time; }
        static std::optional<std::size_t> pop(Channel &channel, DateTime time, std::vector<Delta> &deltas)
        {
            if (first_time(channel) != time) return std::nullopt;
            auto frame = std::move(channel.frames.front());
            channel.frames.pop_front();
            for (auto &delta : frame.deltas) deltas.push_back(std::move(delta));
            return frame.bytes;
        }
        DateTime next_time(bool wait)
        {
            DateTime next = MAX_DT;
            const auto inspect = [&] {
                std::unique_lock lock{mutex_};
                const auto settled = [&] {
                    check_error();
                    next = MAX_DT;
                    bool quiet = true;
                    for (const auto &stage : stages_)
                    {
                        if (stage->next > frontier_ && stage->next < end_)
                            next = std::min(next, stage->next);
                        quiet = quiet && stage->ready && !stage->busy && stage->completed >= frontier_;
                    }
                    return next != MAX_DT || quiet;
                };
                if (wait) condition_.wait(lock, settled);
                else static_cast<void>(settled());
            };
            if (wait) wait_call(inspect); else inspect();
            return next;
        }
        void complete(DateTime time)
        {
            wait_call([&] {
                std::unique_lock lock{mutex_};
                check_error();
                // Publication is ordered: stage every same-time external frame
                // BEFORE making the completed owner frontier visible.
                for (std::size_t i = 0; i < pending_.size(); ++i)
                {
                    if (!pending_[i]) continue;
                    if (pending_[i]->time != time) throw std::logic_error("spawn_: pending frame crossed a cycle boundary");
                    if (!send(lock, stages_[i]->side, std::move(*pending_[i]))) check_error();
                    pending_[i].reset();
                }
                frontier_ = time;
                lock.unlock();
                for (auto &stage : stages_) stage->work.notify_one();
            });
        }
        void run_stage(std::size_t index) noexcept
        {
            auto &state = *stages_[index];
            auto &channel = state.process.channel();
            const auto &plan = plan_->stages[index];
            const auto deadline = [&] { return std::chrono::steady_clock::now() + plan_->config.worker_timeout; };
            const auto receive = [&](distributed::PipeEndpoint::Deadline until) {
                std::string payload;
                if (!channel.receive(payload, until)) throw std::runtime_error("worker process exited before replying");
                auto reply = distributed::decode_reply(plan.slots, payload);
                if (!reply.error.empty()) throw std::runtime_error(reply.error);
                return reply;
            };
            static_cast<void>(fallback_on_exception(false, [&] {
                const auto startup_deadline = deadline();
                channel.send(plan.bootstrap, startup_deadline);
                channel.send(plan.boundary_identity, startup_deadline);
                const auto initial = receive(startup_deadline);
                {
                    std::lock_guard lock{mutex_};
                    state.ready = true;
                    state.next = initial.next_scheduled_time;
                }
                condition_.notify_all();
                wake_.notify();
                for (;;)
                {
                    std::vector<Delta> deltas;
                    DateTime time;
                    std::optional<std::size_t> side_bytes, flow_bytes;
                    {
                        std::unique_lock lock{mutex_};
                        for (;;)
                        {
                            if (cancelled_) break;
                            const auto allowed = index == 0 ? frontier_ : std::min(frontier_, stages_[index - 1]->completed);
                            time = std::min({state.next, first_time(state.side), first_time(state.flow)});
                            if (time <= allowed && time < end_) break;
                            // Nothing due through allowed: publishing progress
                            // here does not evaluate a graph or manufacture ticks.
                            const bool progressed = state.completed < allowed || state.busy;
                            state.completed = allowed;
                            state.busy = false;
                            if (progressed)
                            {
                                condition_.notify_all();
                                if (index + 1 < stages_.size()) stages_[index + 1]->work.notify_one();
                                wake_.notify();
                            }
                            if (sealed_ && state.completed >= frontier_) break;
                            // No exchange is outstanding while idle, so transport deadlines
                            // cannot observe process death. Poll without evaluating the child
                            // or waking the owner unless the worker has actually exited.
                            state.work.wait_for(lock, std::min(plan_->config.worker_timeout,
                                                             std::chrono::milliseconds{100}));
                            if (const auto code = state.process.try_wait_for_exit())
                                throw std::runtime_error("worker process exited while idle (" +
                                                         std::to_string(*code) + ")");
                        }
                        if (cancelled_ || (sealed_ && state.completed >= frontier_ &&
                            std::min({state.next, first_time(state.side), first_time(state.flow)}) > frontier_)) break;
                        state.busy = true;
                        side_bytes = pop(state.side, time, deltas);
                        flow_bytes = pop(state.flow, time, deltas);
                    }
                    distributed::CycleRequest request{time, {}};
                    for (auto &delta : deltas)
                        request.staged.push_back({delta.slot, std::move(delta.payload)});
                    const auto cycle_deadline = deadline();
                    channel.send(distributed::encode_request(plan.slots, request), cycle_deadline);
                    auto reply = receive(cycle_deadline);
                    auto output = reply.collected.empty() ? Value{} : std::move(reply.collected.front().delta);
                    const auto next = reply.next_scheduled_time;
                    if (next != MAX_DT && next <= time)
                        throw std::runtime_error("worker requested a non-increasing evaluation time");
                    {
                        std::unique_lock lock{mutex_};
                        if (output.has_value())
                        {
                            Frame frame{time, {}, output.view().checked_as<Str>().size()};
                            frame.deltas.push_back({plan_->stages[index + 1].flow_slot, std::move(output)});
                            if (!send(lock, stages_[index + 1]->flow, std::move(frame))) break;
                        }
                        // Retire capacity only after evaluation and forwarding.
                        const auto retire = [](Channel &channel, std::optional<std::size_t> bytes) {
                            if (!bytes) return;
                            channel.bytes -= *bytes;
                            --channel.admitted;
                            channel.space.notify_one();
                        };
                        retire(state.side, side_bytes);
                        retire(state.flow, flow_bytes);
                        state.completed = time;
                        state.next = next;
                    }
                    condition_.notify_all();
                    if (index + 1 < stages_.size()) stages_[index + 1]->work.notify_one();
                    wake_.notify();
                }
                const auto stop_deadline = deadline();
                channel.send("stop", stop_deadline);
                static_cast<void>(receive(stop_deadline));
                if (state.process.wait_for_exit() != 0)
                    throw std::runtime_error("worker process failed during shutdown");
                return true;
            }, [&](const char *message) {
                {
                    std::lock_guard lock{mutex_};
                    if (error_.empty()) error_ = "stage " + std::to_string(index) + ": " + message;
                    cancelled_ = true;
                }
                state.process.terminate();
                cancel();
                wake_.notify();
            }));
        }

        PlanPtr plan_;
        DateTime start_;
        DateTime end_;
        DateTime frontier_;
        std::vector<std::unique_ptr<StageState>> stages_;
        std::vector<std::optional<Frame>> pending_; // owner thread only
        std::mutex mutex_;
        std::condition_variable condition_;
        ExecutorActivityWake wake_;
        std::string error_;
        bool first_{true};
        bool sealed_{false};
        bool cancelled_{false};
    };
    struct StateData { Runtime *runtime{nullptr}; };
}

namespace hgraph::static_schema_detail
{
    template <> struct scalar_name<spawn_detail::PlanPtr>
    { static constexpr std::string_view value{"hgraph.spawn.plan"}; };
    template <> struct scalar_name<spawn_detail::StateData>
    { static constexpr std::string_view value{"hgraph.spawn.state"}; };
}

namespace hgraph::spawn_detail
{
    /** A primitive is required to own workers and capture completed cycles.
        All inputs are active and may be invalid. Evaluation serializes changed
        endpoints into owned payloads; the executor completion callback publishes
        them only after the owner's cycle succeeds. Capture costs O(input count
        + changed payload bytes). Stop drains the granted frontier and joins.
        No live endpoint or owner executor is retained by a transport thread. */
    struct SpawnNode
    {
        static constexpr auto name = "spawn_";
        static void start(Scalar<"plan", PlanPtr> plan, EngineControlView engine,
                          State<StateData> state, NodeScheduler scheduler, DateTime now)
        {
            auto runtime = std::make_unique<Runtime>(plan.value(), now, engine.end_time());
            auto activity = runtime->activity();
            auto wake = engine.attach_activity(activity);
            annotate_on_exception([&] {
                runtime->start(std::move(wake));
                scheduler.schedule(now);
            }, [&] {
                engine.detach_activity(activity);
                // start() may have launched threads which need the embedding
                // lock to finish. Destruction must use the same wait boundary.
                if (plan.value()->wait_runner) plan.value()->wait_runner([&] { runtime.reset(); });
                else runtime.reset();
            });
            state.modify().runtime = runtime.release();
        }
        static void eval(In<"inputs", TsVar<"I">, InputValidity::Unchecked> inputs,
                         Scalar<"plan", PlanPtr>, State<StateData> state, DateTime now)
        { state.get().runtime->capture(inputs.base(), now); }
        static void stop(State<StateData> state, EngineControlView engine)
        {
            std::unique_ptr<Runtime> runtime{std::exchange(state.modify().runtime, nullptr)};
            if (!runtime) return;
            engine.detach_activity(runtime->activity());
            runtime->finish();
        }
    };
}

namespace hgraph
{
    SpawnStage process_stage(SpawnStage stage, std::string recipe, std::string bootstrap)
    {
        if (recipe.empty()) throw std::invalid_argument("spawn_: worker recipe must not be empty");
        stage.recipe = std::move(recipe);
        stage.bootstrap = std::move(bootstrap);
        return stage;
    }

    SpawnWorkerPlan prepare_spawn_worker(WiredFn function, std::span<const TSValueTypeMetaData *const> schemas)
    {
        if (!function.valid() || function.variadic || function.arity != schemas.size())
            throw std::invalid_argument("spawn_: worker signature does not match its inputs");
        SpawnWorkerPlan plan;
        GlobalState state;
        Wiring child{state, WiringOptions{.allow_push_sources = false, .inherit_global_context = false}};
        std::vector<WiringPortRef> inputs;
        const auto append_identity = [&](const TSValueTypeMetaData *schema) {
            const auto bytes = manifest::ts_descriptor(schema);
            plan.boundary_identity += std::to_string(bytes.size()) + ":";
            plan.boundary_identity.append(reinterpret_cast<const char *>(bytes.data()), bytes.size());
        };
        plan.boundary_identity = std::to_string(schemas.size()) + ":";
        for (std::size_t i = 0; i < schemas.size(); ++i)
        {
            append_identity(schemas[i]);
            auto transfer = std::make_shared<const distributed::BoundaryTransfer>(schemas[i]);
            const Str slot = "__spawn_input_" + std::to_string(i);
            plan.slots.add(slot, distributed::BoundaryTransfer::payload_schema(), distributed::SlotDirection::Input);
            inputs.push_back(wire<distributed::boundary_transfer_source_impl>(child, slot, transfer, schemas[i]).erased());
        }
        const auto result = function.wire(child, inputs);
        if ((result.schema != nullptr) != function.has_output)
            throw std::invalid_argument("spawn_: stage result does not match its declared signature");
        if (result.schema)
        {
            plan.output = TypeRegistry::instance().dereference(result.schema);
            append_identity(plan.output);
            auto transfer = std::make_shared<const distributed::BoundaryTransfer>(plan.output);
            plan.slots.add("__spawn_output", distributed::BoundaryTransfer::payload_schema(), distributed::SlotDirection::Output);
            const auto *sink_schema = TypeRegistry::instance().un_named_tsb({{"ts", plan.output}});
            NodeTypeMetaData meta;
            meta.display_name = "spawn_boundary_sink";
            meta.input_schema = sink_schema;
            meta.node_kind = NodeKind::Sink;
            meta.valid_inputs = std::vector<std::size_t>{};
            NodeCallbacks callbacks;
            callbacks.evaluate = [transfer](const NodeView &node, DateTime now) {
                const auto root = node.input(now);
                const auto bundle = root.as_bundle();
                const auto input = bundle[0];
                if (input.modified()) node.global_state().set("__spawn_output", transfer->capture(input));
            };
            auto sink = NodeBuilder::native(std::move(meta), std::move(callbacks));
            sink.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(sink_schema, {&result, 1}));
            static_cast<void>(child.add_node(std::type_index(typeid(SpawnWorkerPlan)), std::move(sink), {&result, 1}, Value{}));
        }
        else plan.boundary_identity += "sink";
        plan.graph = std::move(child).finish();
        return plan;
    }

    SpawnStage bind_(WiredFn function, std::vector<std::pair<std::string, WiringPortRef>> bindings)
    { return bind_(SpawnStage{function, {}, {}}, std::move(bindings)); }
    SpawnStage bind_(SpawnStage stage, std::vector<std::pair<std::string, WiringPortRef>> bindings)
    {
        std::unordered_map<std::string, bool> seen;
        for (const auto &binding : stage.bindings) seen.emplace(binding.first, true);
        for (auto &binding : bindings)
        {
            if (binding.first.empty() || !seen.emplace(binding.first, true).second)
                throw std::invalid_argument("bind_: duplicate or empty input name");
            stage.bindings.push_back(std::move(binding));
        }
        return stage;
    }
    SpawnPipeline pipeline_(std::vector<SpawnStage> stages)
    {
        if (stages.empty()) throw std::invalid_argument("pipeline_: expected at least one stage");
        return {std::move(stages)};
    }
    void wire_spawn(Wiring &wiring, SpawnStage sink, std::span<const WiringArg> arguments,
                    SpawnConfig config, SpawnWaitRunner wait_runner)
    {
        wire_spawn(wiring, pipeline_({std::move(sink)}), arguments, config, std::move(wait_runner));
    }
    void wire_spawn(Wiring &wiring, SpawnPipeline pipeline, std::span<const WiringArg> arguments,
                    SpawnConfig config, SpawnWaitRunner wait_runner)
    {
        using namespace spawn_detail;
        if (pipeline.stages.empty()) throw std::invalid_argument("spawn_: empty pipeline");
        if (!config.capacity_frames || !config.capacity_bytes)
            throw std::invalid_argument("spawn_: capacities must be positive");
        if (config.worker_timeout.count() <= 0 || config.worker_timeout > std::chrono::hours{24})
            throw std::invalid_argument("spawn_: worker_timeout must be positive and at most 24 hours");
        auto plan = std::make_shared<Plan>();
        plan->config = config;
        plan->wait_runner = std::move(wait_runner);
        std::vector<WiringPortRef> external;
        std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
        const TSValueTypeMetaData *previous_output = nullptr;
        for (std::size_t index = 0; index < pipeline.stages.size(); ++index)
        {
            auto &stage = pipeline.stages[index];
            auto &function = stage.function;
            if (!function.valid() || function.variadic)
                throw std::invalid_argument("spawn_: each stage requires a fixed callable signature");
            if (function.has_output != (index + 1 < pipeline.stages.size()))
                throw std::invalid_argument("spawn_: intermediate stages must return a time series and the final stage must be a sink");
            const auto names = function.param_names();
            std::unordered_map<std::string_view, std::size_t> named;
            for (std::size_t i = 0; i < names.size(); ++i)
                if (!names[i].empty()) named.emplace(names[i], i);
            std::vector<std::optional<WiringPortRef>> ports(function.arity);
            const auto bind = [&](std::string_view name, const WiringPortRef &port) {
                const auto it = named.find(name);
                if (it == named.end()) throw std::invalid_argument("spawn_: unknown bound input '" + std::string{name} + "'");
                if (ports[it->second]) throw std::invalid_argument("spawn_: duplicate bound input '" + std::string{name} + "'");
                ports[it->second] = port;
            };
            for (const auto &[name, port] : stage.bindings) bind(name, port);
            if (index == 0)
            {
                std::size_t positional = 0;
                for (const auto &argument : arguments)
                {
                    if (argument.kind != WiringArg::Kind::TimeSeries)
                        throw std::invalid_argument("spawn_: native scalar configuration must be bound with spawn_fn");
                    if (!argument.name.empty()) { bind(argument.name, argument.port); continue; }
                    while (positional < ports.size() && ports[positional]) ++positional;
                    if (positional == ports.size()) throw std::invalid_argument("spawn_: too many input arguments");
                    ports[positional++] = argument.port;
                }
            }
            StagePlan prepared;
            std::vector<const TSValueTypeMetaData *> input_schemas;
            for (std::size_t i = 0; i < ports.size(); ++i)
            {
                const TSValueTypeMetaData *schema;
                if (ports[i]) schema = TypeRegistry::instance().dereference(ports[i]->schema);
                else
                {
                    if (index == 0 || prepared.flow_slot != static_cast<std::size_t>(-1))
                        throw std::invalid_argument("spawn_: missing input or ambiguous pipeline flow: expected one unbound input");
                    prepared.flow_slot = i;
                    schema = previous_output;
                }
                auto transfer = std::make_shared<const BoundaryTransfer>(schema);
                input_schemas.push_back(schema);
                if (ports[i])
                {
                    fields.emplace_back("input_" + std::to_string(external.size()), schema);
                    external.push_back(*ports[i]);
                    plan->inputs.push_back({index, i, std::move(transfer)});
                }
            }
            if (index != 0 && prepared.flow_slot == static_cast<std::size_t>(-1))
                throw std::invalid_argument("spawn_: a downstream stage requires one unbound flow input");
            auto worker = prepare_spawn_worker(function, input_schemas);
            previous_output = worker.output;
            prepared.output = worker.output != nullptr;
            prepared.slots = std::move(worker.slots);
            prepared.boundary_identity = std::move(worker.boundary_identity);
            if (stage.recipe.empty())
                throw std::invalid_argument("spawn_: process stage requires a registered worker recipe");
            prepared.recipe = stage.recipe;
            prepared.bootstrap = stage.describe ? stage.describe(input_schemas) : stage.bootstrap;
            plan->stages.push_back(std::move(prepared));
        }
        plan->input_schema = TypeRegistry::instance().un_named_tsb(fields);
        const auto source = WiringPortRef::structural_source(plan->input_schema, std::move(external));
        // A freshly owned immutable plan gives every spawn distinct identity,
        // including two identical sink calls with observable side effects.
        wire<SpawnNode>(wiring, Port<void>{wiring, source}, PlanPtr{std::move(plan)});
    }
}
