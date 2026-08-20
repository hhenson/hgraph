#include <hgraph/fabric/operators.h>

#include <hgraph/fabric/planning.h>
#include <hgraph/fabric/value_builders.h>

#include <hgraph/lib/std/operators/collection.h>
#include <hgraph/lib/std/operators/container.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/types/operator_dispatch.h>

#include <algorithm>
#include <memory>
#include <numeric>
#include <stdexcept>
#include <tuple>
#include <typeindex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace hgraph::fabric
{
    namespace
    {
        using IngressSignal =
            TSB<"hgraph.fabric::IngressSignal", Field<"version", TS<Int>>,
                Field<"revision", TS<Int>>>;
        using IngressSignals = TSD<Str, IngressSignal>;

        struct SubscribeDataPlanningSource
        {
            static constexpr auto name = "hgraph.fabric.subscribe_data.planned";
            using signature_args =
                std::tuple<Scalar<"data_id", Str>,
                           Scalar<"mode", SubscriptionMode>,
                           Scalar<"as_of", DateTime>, Out<TS<Frame>>>;

            static void start(Scalar<"data_id", Str>,
                              Scalar<"mode", SubscriptionMode>,
                              Scalar<"as_of", DateTime>)
            {
                throw std::logic_error(
                    "hgraph.fabric.subscribe_data runtime is introduced by "
                    "RFC 0026 subscription checkpoints");
            }

            static void eval() {}
        };

        /** One root endpoint for accepted lineage signals. Concrete memory and
            Kafka ingress strategies replace this contract source in the
            subscription checkpoints. */
        struct IngressCoordinatorContractSource
        {
            static constexpr auto name = "hgraph.fabric.ingress_coordinator.contract";

            static void start()
            {
                throw std::logic_error(
                    "hgraph.fabric ingress runtime is introduced by RFC 0026 "
                    "subscription checkpoints");
            }

            static void eval(Out<IngressSignals>) {}
        };

        struct PublishDataWithoutCutSink
        {
            static constexpr auto name = "hgraph.fabric.publish_data.no_dependencies";

            static void eval(In<"value", TS<Frame>>,
                             Scalar<"data_id", Str>)
            {
                throw std::logic_error(
                    "hgraph.fabric.publish_data runtime wiring is introduced "
                    "by RFC 0026 coordinator checkpoints");
            }
        };

        struct PublishDataWithCutSink
        {
            static constexpr auto name = "hgraph.fabric.publish_data.with_cut";

            static void eval(
                In<"value", TS<Frame>>,
                In<"cut", TSD<Str, IngressSignal>,
                   InputActivity::Structural, InputValidity::Unchecked>,
                Scalar<"data_id", Str>)
            {
                throw std::logic_error(
                    "hgraph.fabric.publish_data runtime wiring is introduced "
                    "by RFC 0026 coordinator checkpoints");
            }
        };

        struct SubscriptionDeclaration
        {
            Str                                          data_id{};
            SubscriptionMode                             mode{SubscriptionMode::Live};
            DateTime                                     as_of{MIN_DT};
            DelayedBindingWiringPort<TS<Frame>>          delayed{};
            WiringPortRef                                output{};
        };

        struct PublisherDeclaration
        {
            Str                 data_id{};
            WiringPortRef       value{};
            DependencySelection dependencies{DependencySelection::automatic()};
            std::vector<Str>    resolved_dependencies{};
            bool                wired{false};
        };

        struct DeclarationState
        {
            std::vector<SubscriptionDeclaration> subscriptions{};
            std::vector<PublisherDeclaration>    publishers{};
            std::optional<Port<IngressSignals>>  coordinator{};
            bool                                 finalizer_registered{false};
        };

        [[nodiscard]] NodeTypeRef subscribe_source_type()
        {
            NodeBuilder builder;
            builder.implementation<SubscribeDataPlanningSource>();
            return builder.type();
        }

        [[nodiscard]] Str source_data_id(const NodeBuilder &builder)
        {
            const Value &scalars = builder.scalars();
            if (!scalars.has_value())
            {
                throw std::logic_error(
                    "fabric subscription source has no scalar configuration");
            }
            return scalars.view().as_bundle().at("data_id").checked_as<Str>();
        }

        struct SourceCollection
        {
            std::unordered_set<const WiringInstance *> wiring_nodes{};
            std::unordered_set<const GraphBuilder *>   compiled_graphs{};
            std::vector<Str>                           data_ids{};

            void add(Str data_id)
            {
                if (std::ranges::find(data_ids, data_id) == data_ids.end())
                {
                    data_ids.push_back(std::move(data_id));
                }
            }
        };

        void collect_compiled_graph(const GraphBuilder &graph,
                                    SourceCollection &collection);

        void collect_compiled_child(void *raw_collection,
                                    const GraphBuilder &child)
        {
            collect_compiled_graph(
                child, *static_cast<SourceCollection *>(raw_collection));
        }

        void collect_compiled_graph(const GraphBuilder &graph,
                                    SourceCollection &collection)
        {
            if (!collection.compiled_graphs.insert(&graph).second) { return; }
            const NodeTypeRef source_type = subscribe_source_type();
            for (const NodeBuilder &node : graph.nodes())
            {
                if (node.type() == source_type)
                {
                    collection.add(source_data_id(node));
                    continue;
                }
                node.visit_child_graphs(&collection, &collect_compiled_child);
            }
        }

        void collect_subscription_sources(const WiringPortRef &source,
                                          SourceCollection &collection)
        {
            using SourceKind = WiringPortRef::SourceKind;
            switch (source.source_kind())
            {
                case SourceKind::Null:
                    return;
                case SourceKind::Structural:
                    for (const auto &child : source.structural_children())
                    {
                        collect_subscription_sources(child, collection);
                    }
                    return;
                case SourceKind::Delayed:
                {
                    const auto &resolved = source.delayed_state()->source;
                    if (!resolved)
                    {
                        throw std::logic_error(
                            "fabric planner encountered an unbound delayed source");
                    }
                    collect_subscription_sources(*resolved, collection);
                    return;
                }
                case SourceKind::Peered:
                {
                    const WiringInstance *node = source.peered_node();
                    if (!collection.wiring_nodes.insert(node).second) { return; }
                    if (node->definition ==
                        std::type_index(typeid(SubscribeDataPlanningSource)))
                    {
                        collection.add(source_data_id(node->builder));
                        return;
                    }
                    node->builder.visit_child_graphs(&collection,
                                                     &collect_compiled_child);
                    for (const auto &input : node->inputs)
                    {
                        collect_subscription_sources(input.source, collection);
                    }
                    return;
                }
                case SourceKind::Boundary:
                    throw std::logic_error(
                        "fabric planner expected a materialised top-level source");
                case SourceKind::Unbound:
                    throw std::logic_error(
                        "fabric planner encountered an unbound wiring source");
            }
        }

        [[nodiscard]] std::vector<Str> canonical_ids(std::vector<Str> ids)
        {
            std::ranges::sort(ids, canonical_data_id_less);
            ids.erase(std::ranges::unique(ids).begin(), ids.end());
            return ids;
        }

        [[nodiscard]] DependencyPlanInput build_plan(
            const DeclarationState &state)
        {
            DependencyPlanInput plan;
            for (const auto &subscription : state.subscriptions)
            {
                plan.roots.push_back(subscription.data_id);
            }
            for (const auto &publisher : state.publishers)
            {
                plan.roots.insert(plan.roots.end(),
                                  publisher.resolved_dependencies.begin(),
                                  publisher.resolved_dependencies.end());
                plan.publishers.push_back(PlannedPublisherInput{
                    .data_id = publisher.data_id,
                    .dependencies = publisher.resolved_dependencies,
                });
            }
            plan.roots = canonical_ids(std::move(plan.roots));

            std::unordered_map<Str, std::size_t> index;
            for (std::size_t root = 0; root < plan.roots.size(); ++root)
            {
                index.emplace(plan.roots[root], root);
            }
            std::vector<std::size_t> parent(plan.roots.size());
            std::iota(parent.begin(), parent.end(), std::size_t{});
            const auto find = [&](std::size_t item) -> std::size_t {
                while (parent[item] != item)
                {
                    parent[item] = parent[parent[item]];
                    item = parent[item];
                }
                return item;
            };
            const auto unite = [&](std::size_t lhs, std::size_t rhs) {
                lhs = find(lhs);
                rhs = find(rhs);
                if (lhs != rhs) { parent[rhs] = lhs; }
            };
            for (const auto &publisher : plan.publishers)
            {
                if (publisher.dependencies.empty()) { continue; }
                const std::size_t first = index.at(publisher.dependencies.front());
                for (std::size_t dependency = 1;
                     dependency < publisher.dependencies.size(); ++dependency)
                {
                    unite(first, index.at(publisher.dependencies[dependency]));
                }
            }

            std::unordered_map<std::size_t, std::vector<Str>> forests;
            for (std::size_t root = 0; root < plan.roots.size(); ++root)
            {
                forests[find(root)].push_back(plan.roots[root]);
            }
            plan.forests.reserve(forests.size());
            for (auto &[_, roots] : forests)
            {
                plan.forests.push_back(
                    ConsistencyForestInput{.roots = std::move(roots)});
            }
            return dependency_plan_input(make_dependency_plan(std::move(plan)).view());
        }

        [[nodiscard]] WiringPortRef combine_cut(
            Wiring &wiring, Port<IngressSignals> coordinator,
            const std::vector<Str> &dependencies)
        {
            std::vector<WiringArg> args;
            args.reserve(dependencies.size() + 1);
            WiringArg keys;
            keys.kind = WiringArg::Kind::Scalar;
            keys.scalar_value = stdlib::make_list<Str>(dependencies.begin(),
                                                       dependencies.end());
            keys.scalar_meta = keys.scalar_value.schema();
            args.push_back(std::move(keys));

            for (const auto &data_id : dependencies)
            {
                auto signal = wire<stdlib::getitem_>(wiring, coordinator, data_id)
                                  .as<IngressSignal>();
                WiringArg value;
                value.kind = WiringArg::Kind::TimeSeries;
                value.port = signal.erased();
                args.push_back(std::move(value));
            }
            return wire_operator(wiring, "combine_tsd", args, true)
                .output.erased();
        }

        void finalize(DeclarationState &state, Wiring &wiring)
        {
            for (auto &subscription : state.subscriptions)
            {
                if (!subscription.delayed.bound())
                {
                    auto source = wire<SubscribeDataPlanningSource>(
                        wiring, subscription.data_id, subscription.mode,
                        subscription.as_of);
                    subscription.delayed(source);
                }
            }

            for (auto &publisher : state.publishers)
            {
                SourceCollection collection;
                if (publisher.dependencies.is_automatic())
                {
                    collect_subscription_sources(publisher.value, collection);
                }
                else
                {
                    for (const auto &dependency :
                         publisher.dependencies.dependencies())
                    {
                        if (dependency.root_identity() != wiring.identity())
                        {
                            throw std::logic_error(
                                "fabric explicit dependency belongs to another "
                                "wired root");
                        }
                        collect_subscription_sources(dependency.source(),
                                                     collection);
                    }
                }
                publisher.resolved_dependencies =
                    canonical_ids(std::move(collection.data_ids));
                if (std::ranges::find(publisher.resolved_dependencies,
                                      publisher.data_id) !=
                    publisher.resolved_dependencies.end())
                {
                    throw std::invalid_argument(
                        "hgraph.fabric.publish_data: publisher must not depend "
                        "on its own data id");
                }
            }

            DependencyPlanInput plan = build_plan(state);
            wiring.set_trait(DEPENDENCY_PLAN_TRAIT,
                             make_dependency_plan(plan));

            if (!plan.roots.empty() && !state.coordinator.has_value())
            {
                state.coordinator =
                    wire<IngressCoordinatorContractSource>(wiring);
            }
            for (auto &publisher : state.publishers)
            {
                if (publisher.wired) { continue; }
                Port<void> value{wiring, publisher.value};
                if (publisher.resolved_dependencies.empty())
                {
                    wire<PublishDataWithoutCutSink>(wiring, value,
                                                    publisher.data_id);
                }
                else
                {
                    if (!state.coordinator.has_value())
                    {
                        throw std::logic_error(
                            "fabric dependency plan has no ingress coordinator");
                    }
                    WiringPortRef cut = combine_cut(
                        wiring, *state.coordinator,
                        publisher.resolved_dependencies);
                    wire<PublishDataWithCutSink>(wiring, value,
                                                 Port<void>{wiring, cut},
                                                 publisher.data_id);
                }
                publisher.wired = true;
            }
        }

        [[nodiscard]] std::shared_ptr<DeclarationState> declarations(
            Wiring &wiring)
        {
            auto state = std::static_pointer_cast<DeclarationState>(
                wiring.acquire_extension_state(
                    std::type_index(typeid(DeclarationState)),
                    [] { return std::make_shared<DeclarationState>(); }));
            if (!state->finalizer_registered)
            {
                state->finalizer_registered = true;
                std::weak_ptr<DeclarationState> weak = state;
                wiring.register_pre_rank_finalizer(
                    [weak](Wiring &target) {
                        if (const auto locked = weak.lock())
                        {
                            finalize(*locked, target);
                        }
                    });
            }
            return state;
        }

        void require_subscription_arguments(Str const &data_id,
                                            SubscriptionMode mode,
                                            DateTime as_of)
        {
            require_data_id(data_id);
            const bool supplied_as_of = as_of != MIN_DT;
            switch (mode)
            {
                case SubscriptionMode::Snapshot:
                    if (!supplied_as_of)
                    {
                        throw std::invalid_argument(
                            "hgraph.fabric.subscribe_data: Snapshot requires as_of");
                    }
                    return;
                case SubscriptionMode::Live:
                case SubscriptionMode::Replay:
                    if (supplied_as_of)
                    {
                        throw std::invalid_argument(
                            "hgraph.fabric.subscribe_data: as_of is valid only for Snapshot");
                    }
                    return;
            }
            throw std::invalid_argument(
                "hgraph.fabric.subscribe_data: unsupported subscription mode");
        }

        void record_publisher(Wiring &wiring, Str data_id,
                              WiringPortRef value,
                              DependencySelection dependencies)
        {
            auto state = declarations(wiring);
            if (std::ranges::any_of(
                    state->publishers,
                    [&](const PublisherDeclaration &publisher) {
                        return publisher.data_id == data_id;
                    }))
            {
                throw std::invalid_argument(
                    "hgraph.fabric.publish_data: data id already has a "
                    "publisher in this wiring root");
            }
            state->publishers.push_back(PublisherDeclaration{
                .data_id = std::move(data_id),
                .value = std::move(value),
                .dependencies = std::move(dependencies),
            });
        }

        struct SubscribeDataPlanningGraph
        {
            static constexpr auto name =
                "hgraph.fabric.subscribe_data.planning_graph";

            static auto defaults()
            {
                return std::tuple{
                    arg<"as_of">(MIN_DT),
                };
            }

            static Port<TS<Frame>> compose(
                Wiring &wiring, Scalar<"data_id", Str> data_id,
                Scalar<"mode", SubscriptionMode> mode,
                Scalar<"as_of", DateTime> as_of)
            {
                require_subscription_arguments(data_id.value(), mode.value(),
                                               as_of.value());
                auto delayed = delayed_binding<TS<Frame>>(wiring);
                auto output = delayed();
                declarations(wiring)->subscriptions.push_back(
                    SubscriptionDeclaration{
                        .data_id = data_id.value(),
                        .mode = mode.value(),
                        .as_of = as_of.value(),
                        .delayed = delayed,
                        .output = output.erased(),
                    });
                return output;
            }
        };

        struct PublishDataPlanningGraph
        {
            static constexpr auto name =
                "hgraph.fabric.publish_data.planning_graph";

            static void compose(Wiring &wiring,
                                Scalar<"data_id", Str> data_id,
                                NamedPort<"value", TS<Frame>> value)
            {
                require_data_id(data_id.value());
                record_publisher(wiring, data_id.value(), value.erased(),
                                 DependencySelection::automatic());
            }
        };

        using PublishDataExplicit =
            Operator<"hgraph.fabric._publish_data_explicit",
                     Scalar<"data_id", Str>, In<"value", TS<Frame>>,
                     VarIn<"dependencies", TS<Frame>>>;

        struct PublishDataExplicitPlanningGraph
        {
            static constexpr auto name =
                "hgraph.fabric.publish_data.explicit_planning_graph";

            static void compose(Wiring &wiring,
                                Scalar<"data_id", Str> data_id,
                                NamedPort<"value", TS<Frame>> value,
                                VarIn<"dependencies", TS<Frame>> dependencies)
            {
                if (dependencies.empty())
                {
                    throw std::invalid_argument(
                        "explicit fabric dependencies must not be empty");
                }
                std::vector<DependencyHandle> handles;
                handles.reserve(dependencies.size());
                for (const auto &dependency : dependencies)
                {
                    handles.push_back(dependency_handle(
                        wiring, Port<TS<Frame>>{wiring, dependency}));
                }
                record_publisher(
                    wiring, data_id.value(), value.erased(),
                    DependencySelection::explicit_dependencies(
                        std::move(handles)));
            }
        };

        void install_fabric_operators()
        {
            register_fabric_types();
            register_graph_overload<SubscribeData, SubscribeDataPlanningGraph>();
            register_graph_overload<PublishData, PublishDataPlanningGraph>();
            register_graph_overload<PublishDataExplicit,
                                    PublishDataExplicitPlanningGraph>();
        }
    }  // namespace

    DependencyHandle::DependencyHandle(std::uint64_t root_identity, Str data_id,
                                       WiringPortRef source)
        : root_identity_(root_identity), data_id_(std::move(data_id)),
          source_(std::move(source))
    {
    }

    std::uint64_t DependencyHandle::root_identity() const noexcept
    {
        return root_identity_;
    }

    std::string_view DependencyHandle::data_id() const noexcept
    {
        return data_id_;
    }

    const WiringPortRef &DependencyHandle::source() const noexcept
    {
        return source_;
    }

    DependencySelection DependencySelection::automatic()
    {
        return DependencySelection{};
    }

    DependencySelection DependencySelection::explicit_dependencies(
        std::vector<DependencyHandle> dependencies)
    {
        if (dependencies.empty())
        {
            throw std::invalid_argument(
                "explicit fabric dependencies must not be empty");
        }
        const std::uint64_t root = dependencies.front().root_identity();
        for (std::size_t index = 0; index < dependencies.size(); ++index)
        {
            if (dependencies[index].root_identity() != root)
            {
                throw std::invalid_argument(
                    "explicit fabric dependencies must belong to one wiring root");
            }
            for (std::size_t previous = 0; previous < index; ++previous)
            {
                if (dependencies[index].data_id() ==
                    dependencies[previous].data_id())
                {
                    throw std::invalid_argument(
                        "explicit fabric dependency data ids must be unique");
                }
            }
        }
        DependencySelection result;
        result.automatic_    = false;
        result.dependencies_ = std::move(dependencies);
        return result;
    }

    bool DependencySelection::is_automatic() const noexcept
    {
        return automatic_;
    }

    const std::vector<DependencyHandle> &
    DependencySelection::dependencies() const noexcept
    {
        return dependencies_;
    }

    DependencyHandle dependency_handle(Wiring &wiring,
                                       Port<TS<Frame>> subscription)
    {
        if (subscription.wiring() != &wiring)
        {
            throw std::invalid_argument(
                "fabric dependency handle requires the same wiring root");
        }
        for (const auto &declaration : declarations(wiring)->subscriptions)
        {
            if (declaration.output.same_source_as(subscription.erased()))
            {
                return DependencyHandle{wiring.identity(), declaration.data_id,
                                        declaration.output};
            }
        }
        throw std::invalid_argument(
            "fabric dependency handle requires a direct subscribe_data result");
    }

    Port<TS<Frame>> subscribe_data(Wiring &wiring, Str data_id,
                                   SubscriptionMode mode,
                                   std::optional<DateTime> as_of)
    {
        const DateTime resolved_as_of = as_of.value_or(MIN_DT);
        require_subscription_arguments(data_id, mode, resolved_as_of);
        return wire<SubscribeData, TS<Frame>>(wiring, data_id, mode,
                                              resolved_as_of);
    }

    void publish_data(Wiring &wiring, Str data_id, Port<TS<Frame>> value,
                      DependencySelection dependencies)
    {
        require_data_id(data_id);
        if (value.wiring() != &wiring)
        {
            throw std::invalid_argument(
                "hgraph.fabric.publish_data requires a value from the same wiring");
        }
        for (const auto &dependency : dependencies.dependencies())
        {
            if (dependency.root_identity() != wiring.identity())
            {
                throw std::invalid_argument(
                    "explicit fabric dependency belongs to a different wiring root");
            }
        }
        wire<PublishData>(wiring, data_id, value);
        if (!dependencies.is_automatic())
        {
            auto state = declarations(wiring);
            if (state->publishers.empty() ||
                state->publishers.back().data_id != data_id)
            {
                throw std::logic_error(
                    "fabric publisher declaration was not recorded");
            }
            state->publishers.back().dependencies = std::move(dependencies);
        }
    }

    void register_fabric_operators()
    {
        auto &registry = OperatorRegistry::instance();
        registry.register_installer("hgraph.fabric", &install_fabric_operators);
        registry.run_installers();
    }
}  // namespace hgraph::fabric
