#include <hgraph/runtime/distributed_map_wiring.h>
#include <hgraph/lib/std/operators/impl/higher_order_impl.h>
#include <hgraph/lib/std/operators/impl/collection_impl.h>
#include <hgraph/manifest/canonical.h>
#include <hgraph/types/record_replay.h>

namespace hgraph::distributed
{
    struct distributed_keys_impl
    {
        // Stateless: its owned key set is its whole state. The transfer is
        // derived from the schema already in the identity, so the partition
        // is what is left of the contract.
        static const NodeCheckpointOps &checkpoint_ops() noexcept
        {
            static const NodeCheckpointOps ops{
                .supported = true,
                .signature_impl = +[](const NodeBuilder &builder) {
                    const auto scalars = builder.scalars().view().as_bundle();
                    manifest::CanonicalWriter writer;
                    writer.varint(1);
                    writer.varint(static_cast<std::uint64_t>(scalars.at("group").checked_as<Int>()));
                    writer.varint(static_cast<std::uint64_t>(scalars.at("groups").checked_as<Int>()));
                    const auto &bytes = writer.bytes();
                    return std::string{reinterpret_cast<const char *>(bytes.data()), bytes.size()};
                },
            };
            return ops;
        }
        static void eval(In<"keys", TSS<ScalarVar<"K">>> keys,
                         Scalar<"group", Int> group, Scalar<"groups", Int> groups,
                         Scalar<"transfer", BoundaryTransferPtr> transfer,
                         Out<TSS<ScalarVar<"K">>> out)
        {
            std::vector<Value> added, removed;
            const auto owns = [&](const ValueView &key) {
                return transfer.value()->key_hash(key) % static_cast<std::size_t>(groups.value()) == static_cast<std::size_t>(group.value());
            };
            const auto key_set = keys.base().as_set();
            for (const auto &key : key_set.added()) if (owns(key)) added.emplace_back(key);
            for (const auto &key : key_set.removed()) if (owns(key)) removed.emplace_back(key);
            stdlib::collection_impl_detail::apply_tss_delta(out, removed, added);
        }
    };

    DistributedMapPlan prepare_distributed_map(
        const WiredFn &func, std::span<const DistributedMapInput> inputs,
        std::optional<std::string> key_arg, std::size_t group, std::size_t groups)
    try
    {
        namespace ho = stdlib::higher_order_impl_detail;
        if (groups == 0 || group >= groups) throw std::invalid_argument("dmap_: invalid worker partition");
        GlobalState worker_state;
        Wiring worker{worker_state, WiringOptions{.allow_push_sources = false, .inherit_global_context = false}};
        // Every worker graph carries checkpoint identities (RFC 0039). At this
        // level a dmap_ worker is all the runtime's own -- sources, the key
        // partition, the map_, the sink -- so it is all boundary; the child
        // template the map_ wires is the user's. The caller may be wiring
        // from inside a component and the worker process never is, so the
        // ambient component id is cleared: a component in the child has to get
        // the same id on both sides.
        worker.checkpoint_worker_graph();
        (void)worker.checkpoint_component(std::string{worker_boundary_checkpoint_scope});
        const record_replay::scope isolated{record_replay::Mode::None, {}};
        DistributedMapPlan plan;
        std::vector<WiringPortRef> positional;
        std::vector<std::pair<std::string, WiringPortRef>> named;
        std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
        for (std::size_t i = 0; i < inputs.size(); ++i)
        {
            const auto *schema = TypeRegistry::instance().dereference(inputs[i].schema);
            auto transfer = std::make_shared<const BoundaryTransfer>(schema, "dmap_ input " + std::to_string(i));
            const Str slot = "__hgraph_distributed_input_" + std::to_string(i);
            auto source = wire<boundary_transfer_source_impl>(worker, slot, transfer, schema).erased();
            source.arg_tag = inputs[i].tag;
            if (inputs[i].name.empty()) positional.push_back(source);
            else named.emplace_back(inputs[i].name, source);
            fields.emplace_back(slot, schema);
            plan.input_transfers.push_back(std::move(transfer));
            plan.slots.add(slot, BoundaryTransfer::payload_schema(), SlotDirection::Input);
        }
        plan.input_schema = TypeRegistry::instance().un_named_tsb(fields);
        auto keys = ho::split_keys_kwarg(named);
        // The common binder and classifier own named arguments, key injection,
        // pass_through, no_key, and whole-collection parameter acceptance.
        auto key_name = key_arg.value_or("key");
        auto bind = [&](std::string_view name) {
            return bind_wired_fn_args<WiringPortRef>("dmap_", func, positional, named, name);
        };
        auto bound = [&] {
            if (key_arg) return bind(key_name);
            try { return bind("key"); }
            catch (const std::invalid_argument &) { key_name = "ndx"; return bind(key_name); }
        }();
        std::vector<const TSValueTypeMetaData *> schemas;
        std::vector<std::uint8_t> tags;
        for (const auto &port : bound.ordered) { schemas.push_back(port.schema); tags.push_back(static_cast<std::uint8_t>(port.arg_tag)); }
        const auto *key_type = keys ? keys->schema->value_schema->element_type : nullptr;
        auto classified = ho::classify_map_args(func, bound.takes_leading_key, schemas, tags, key_type, false);
        const TSValueTypeMetaData *collection = nullptr;
        for (std::size_t i = 0; i < schemas.size(); ++i)
        {
            if (bound.ordered[i].arg_tag == WiringPortRef::ArgTag::PassThrough) continue;
            if (schemas[i]->kind == TSTypeKind::TSD && classified.is_multiplexed[i]) { collection = schemas[i]; break; }
            if (schemas[i]->kind == TSTypeKind::TSL) { collection = schemas[i]; break; }
        }
        WiringPortRef result;
        if (collection != nullptr && collection->kind == TSTypeKind::TSL)
        {
            if (keys) throw std::invalid_argument("map_: '__keys__' applies to TSD maps only");
            if (!key_arg && key_name != "ndx") { key_name = "ndx"; bound = bind(key_name); }
            result = ho::wire_map_tsl(worker, func, key_name, bound.takes_leading_key,
                                     std::move(bound.ordered), func.has_output, TslMapPartition{group, groups});
        }
        else
        {
            std::vector<std::size_t> multiplexed;
            for (std::size_t i = 0; i < classified.is_multiplexed.size(); ++i)
                if (classified.is_multiplexed[i]) multiplexed.push_back(i);
            auto lifecycle = ho::wire_keyed_lifecycle_keys(worker, keys, multiplexed, classified, bound.ordered, "dmap_");
            auto key_transfer = std::make_shared<const BoundaryTransfer>(lifecycle.schema, "the keys of a dmap_");
            auto partitioned = wire<distributed_keys_impl>(worker, Port<void>{worker, lifecycle},
                                                           static_cast<Int>(group), static_cast<Int>(groups), key_transfer);
            result = ho::wire_map(worker, Scalar<"func", WiredFn>{func}, key_name,
                                  std::move(bound.ordered), partitioned.erased(), func.has_output);
        }
        if (result.schema != nullptr)
        {
            plan.output = TypeRegistry::instance().dereference(result.schema);
            plan.output_transfer = std::make_shared<const BoundaryTransfer>(plan.output, "the output of a dmap_");
            // The endpoint schema fixes the materialized boundary before
            // binding. Generic TsVar inference would retain nested REF types.
            const auto *sink_input = TypeRegistry::instance().un_named_tsb({{"ts", plan.output}});
            NodeTypeMetaData meta;
            meta.display_name = boundary_sink_name;
            meta.input_schema = sink_input;
            meta.node_kind = NodeKind::Sink;
            meta.valid_inputs = std::vector<std::size_t>{};
            NodeCallbacks callbacks;
            const auto filtered_groups = plan.output->kind == TSTypeKind::TSL ? groups : 0;
            callbacks.evaluate = [transfer = plan.output_transfer, group, filtered_groups](const NodeView &node, DateTime now) {
                const auto root = node.input(now);
                auto inputs = root.as_bundle();
                const auto input = inputs[0];
                if (input.modified()) node.global_state().set("__hgraph_distributed_output", transfer->capture(input, false, group, filtered_groups));
            };
            NodeTypeDescriptor descriptor;
            descriptor.schema = std::move(meta);
            descriptor.callbacks = std::move(callbacks);
            descriptor.ops.checkpoint_ops = &boundary_sink_checkpoint_ops();
            auto sink = NodeBuilder::from_descriptor(std::move(descriptor));
            sink.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(sink_input, {&result, 1}));
            static_cast<void>(worker.add_node(std::type_index(typeid(boundary_transfer_sink_impl)), std::move(sink), {&result, 1}, Value{}));
            plan.slots.add("__hgraph_distributed_output", BoundaryTransfer::payload_schema(), SlotDirection::Output);
        }
        plan.child = std::move(worker).finish();
        return plan;
    }
    catch (const std::exception &error)
    {
        throw std::invalid_argument(
            std::string{"dmap_: the child cannot be wired as an isolated worker. Captured outer ports, "
                        "parent services/contexts and push sources are unavailable. Wiring reported: "} + error.what());
    }

    DistributedMapPlan prepare_distributed_map_pool(
        const WiredFn &func, std::span<const DistributedMapInput> inputs,
        std::optional<std::string> key_arg, WorkerPoolConfig config)
    {
        if (config.workers == 0) throw std::invalid_argument("dmap_ needs a positive worker count");
        auto plan = prepare_distributed_map(func, inputs, key_arg, 0, config.workers);
        plan.config = std::move(config);
        plan.children.reserve(plan.config.workers);
        plan.children.push_back(plan.child);
        for (std::size_t group = 1; group < plan.config.workers; ++group)
            plan.children.push_back(prepare_distributed_map(func, inputs, key_arg, group, plan.config.workers).child);
        plan.recipes.resize(plan.config.workers);
        return plan;
    }

    DistributedMapPlan prepare_distributed_map(const WiredFn &func, const TSValueTypeMetaData *input)
    {
        const DistributedMapInput descriptor{input};
        return prepare_distributed_map_pool(func, {&descriptor, 1}, {}, WorkerPoolConfig{});
    }

    void bind_distributed_map_recipe(DistributedMapPlan &plan, std::string_view name)
    {
        for (std::size_t group = 0; group < plan.recipes.size(); ++group)
            plan.recipes[group] = prepared_worker_recipe_key(name, group, plan.config.workers);
    }
}

namespace hgraph::static_schema_detail
{
    template <> struct scalar_name<distributed::DistributedMapPlanPtr>
    {
        static constexpr std::string_view value{"hgraph.distributed.map_plan"};
    };
}

namespace hgraph::distributed
{
    struct prepared_dmap_lifecycle
    {
        static const NodeCheckpointOps &checkpoint_ops() noexcept
        {
            static const NodeCheckpointOps ops{
                .supported = true,
                .capture_impl = &worker_checkpoint::capture,
                .restore_impl = &worker_checkpoint::restore,
                .live_schedule_impl = &worker_checkpoint::live_schedule,
                .signature_impl = +[](const NodeBuilder &builder) {
                    const auto &plan = *builder.scalars().view().as_bundle().at("plan").checked_as<DistributedMapPlanPtr>();
                    return worker_checkpoint::signature(plan.children, plan.config);
                },
            };
            return ops;
        }
        static void start(Scalar<"plan", DistributedMapPlanPtr> plan, NodeView node,
                          EngineControlView engine, State<DistributedMapState> state, NodeScheduler scheduler)
        {
            auto config = plan.value()->config;
            config.start_time = engine.start_time();
            config.end_time = engine.end_time();
            // Workers the coordinator restored start from their images
            // (RFC 0039); the owner's own output was restored as any other.
            const auto restored = worker_checkpoint::claim(node);
            auto pool = WorkerPool::build_partitioned(plan.value()->children, plan.value()->slots,
                plan.value()->recipes, config, plan.value()->phase_runner,
                restored ? std::span<const std::string>{restored->images} : std::span<const std::string>{});
            if (restored) { pool->restore_output_extents(restored->extents); }
            state.modify().pool = pool.release();
            scheduler.schedule(engine.start_time());
        }
        static void stop(State<DistributedMapState> state)
        {
            std::unique_ptr<WorkerPool> pool{std::exchange(state.modify().pool, nullptr)};
            if (pool) pool->stop();
        }
        static void evaluate(const TSInputView &inputs, const DistributedMapPlan &plan,
                             WorkerPool &pool, NodeScheduler scheduler, DateTime now, const TSOutputView *output)
        {
            CycleRequest request;
            request.evaluation_time = now;
            auto bundle = inputs.as_bundle();
            for (std::size_t i = 0; i < plan.input_transfers.size(); ++i)
            {
                auto input = bundle[i];
                if (input.modified()) request.staged.push_back({i, plan.input_transfers[i]->capture(input)});
            }
            const auto next = pool.exchange(request, [&](std::size_t group, const CycleReply &reply) {
                for (const auto &delta : reply.collected)
                {
                    if (output == nullptr) throw std::logic_error("dmap_: sink worker returned an output");
                    plan.output_transfer->apply(*output, delta.delta.view(), true);
                    if (auto size = plan.output_transfer->root_list_size(delta.delta.view())) pool.output_extent(group, *size);
                }
            });
            if (output != nullptr && plan.output->is_unbounded_tsl() && output->as_list().size() > pool.output_extent())
                output->as_list().resize(pool.output_extent());
            if (next != MAX_DT) scheduler.schedule(std::max(next, now + MIN_TD));
        }
    };
    struct prepared_dmap_impl : prepared_dmap_lifecycle
    {
        static constexpr auto name = "dmap_";
        static void eval(In<"inputs", TsVar<"I">, InputValidity::Unchecked> inputs,
                         Scalar<"plan", DistributedMapPlanPtr> plan, TypeArg<"result", TsVar<"O">>,
                         State<DistributedMapState> state, NodeScheduler scheduler, DateTime now, Out<TsVar<"O">> out)
        {
            evaluate(inputs.base(), *plan.value(), *state.get().pool, scheduler, now, &out);
        }
    };
    struct prepared_dmap_sink_impl : prepared_dmap_lifecycle
    {
        static constexpr auto name = "dmap_sink";
        static void eval(In<"inputs", TsVar<"I">, InputValidity::Unchecked> inputs,
                         Scalar<"plan", DistributedMapPlanPtr> plan, State<DistributedMapState> state,
                         NodeScheduler scheduler, DateTime now)
        {
            evaluate(inputs.base(), *plan.value(), *state.get().pool, scheduler, now, nullptr);
        }
    };
    Port<void> wire_distributed_map(Wiring &wiring, std::span<const WiringPortRef> inputs,
                                          DistributedMapPlanPtr plan)
    {
        // What recovers is a component inside the child (RFC 0039). If the
        // children host the component recovery is configured for, this node
        // stands in for it in the owner graph: wired in its scope, so the
        // completed-day session finds a member where it looks for one, with
        // its inputs entering through component input boundaries, and the
        // worker images cover that component. Wired inside a component of the
        // owner's instead, the workers are that component's, whole.
        worker_checkpoint::HostedComponentScope standing_in{
            wiring, worker_checkpoint::hosted_component(wiring, plan->children)};
        if (standing_in.hosting())
        {
            auto hosting = std::make_shared<DistributedMapPlan>(*plan);
            hosting->config.hosted_component = standing_in.component();
            plan = std::move(hosting);
        }
        std::vector<WiringPortRef> entering;
        entering.reserve(inputs.size());
        for (std::size_t index = 0; index < inputs.size(); ++index)
            entering.push_back(standing_in.input(inputs[index], "input_" + std::to_string(index)));
        auto root = WiringPortRef::structural_source(plan->input_schema, std::move(entering));
        if (plan->output == nullptr)
        {
            wire<prepared_dmap_sink_impl>(wiring, Port<void>{wiring, root}, plan);
            return {};
        }
        return wire<prepared_dmap_impl>(wiring, Port<void>{wiring, root}, plan, plan->output);
    }
    Port<void> wire_distributed_map(Wiring &wiring, const WiringPortRef &input, DistributedMapPlanPtr plan)
    {
        return wire_distributed_map(wiring, {&input, 1}, std::move(plan));
    }
}
