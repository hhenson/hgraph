#include <hgraph/fabric/operators.h>

#include <hgraph/fabric/planning.h>
#include <hgraph/fabric/value_builders.h>

#include "impl/subscription_runtime.h"

#include <hgraph/lib/std/operators/collection.h>
#include <hgraph/lib/std/operators/container.h>
#include <hgraph/lib/std/value_util.h>
#include <hgraph/types/operator_dispatch.h>

#include <algorithm>
#include <map>
#include <memory>
#include <set>
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
        using detail::IngressSignal;
        using detail::IngressSignals;

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
            SubscriptionMode                             mode{SubscriptionMode::Auto};
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
            std::map<Str, std::pair<SubscriptionMode, DateTime>>
                resolved_subscription_policies{};
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
            builder.implementation<detail::SubscribeDataRuntimeSource>();
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

        [[nodiscard]] std::pair<SubscriptionMode, DateTime>
        source_policy(const NodeBuilder &builder)
        {
            const auto scalars = builder.scalars().view().as_bundle();
            return {
                scalars.at("mode").checked_as<SubscriptionMode>(),
                scalars.at("as_of").checked_as<DateTime>(),
            };
        }

        struct SourceCollection
        {
            std::unordered_set<const WiringInstance *> wiring_nodes{};
            std::unordered_map<const GraphBuilder *,
                               std::unordered_set<std::size_t>> compiled_nodes{};
            std::vector<Str>                           data_ids{};
            std::map<Str, std::pair<SubscriptionMode, DateTime>> policies{};

            void add(const NodeBuilder &builder)
            {
                Str data_id = source_data_id(builder);
                const auto policy = source_policy(builder);
                const auto [found, inserted] = policies.emplace(data_id, policy);
                if (!inserted && found->second != policy)
                {
                    throw std::invalid_argument(
                        "fabric dependency discovery found one data id with "
                        "multiple subscription policies");
                }
                if (std::ranges::find(data_ids, data_id) == data_ids.end())
                {
                    data_ids.push_back(std::move(data_id));
                }
            }
        };

        void collect_compiled_node(const GraphBuilder &graph,
                                   std::size_t node_index,
                                   SourceCollection &collection);

        void collect_compiled_child(void *raw_collection,
                                    ChildGraphInspectionView child)
        {
            if (child.graph == nullptr)
            {
                throw std::logic_error(
                    "fabric planner encountered an invalid child graph view");
            }
            if (child.output_binding == nullptr ||
                child.output_binding->kind ==
                    NestedGraphOutputBinding::Kind::ParentInput)
            {
                return;
            }
            collect_compiled_node(
                *child.graph, child.output_binding->source.node,
                *static_cast<SourceCollection *>(raw_collection));
        }

        void collect_compiled_node(const GraphBuilder &graph,
                                   std::size_t node_index,
                                   SourceCollection &collection)
        {
            if (node_index >= graph.node_count())
            {
                throw std::logic_error(
                    "fabric planner encountered an invalid child output node");
            }
            if (!collection.compiled_nodes[&graph].insert(node_index).second)
            {
                return;
            }
            const NodeTypeRef source_type = subscribe_source_type();
            const NodeBuilder &node = graph.nodes()[node_index];
            if (node.type() == source_type)
            {
                collection.add(node);
                return;
            }
            node.visit_child_graphs(&collection, &collect_compiled_child);
            for (const GraphEdge &edge : graph.edges())
            {
                if (edge.target_node == node_index)
                {
                    collect_compiled_node(
                        graph, graph_edge_source_node(edge.source_node),
                        collection);
                }
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
                        std::type_index(typeid(detail::SubscribeDataRuntimeSource)))
                    {
                        collection.add(node->builder);
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

            plan.forests.reserve(plan.roots.size());
            for (const auto &root : plan.roots)
            {
                plan.forests.push_back(
                    ConsistencyForestInput{.roots = {root}});
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

        struct SubscriptionPolicy
        {
            SubscriptionMode mode{SubscriptionMode::Auto};
            DateTime         as_of{MIN_DT};

            friend auto operator<=>(const SubscriptionPolicy &,
                                    const SubscriptionPolicy &) = default;
        };

        [[nodiscard]] Port<IngressSignals> combine_signal_ports(
            Wiring &wiring, const std::map<Str, WiringPortRef> &signals)
        {
            if (signals.empty())
            {
                throw std::invalid_argument(
                    "fabric ingress combination requires at least one signal");
            }
            std::vector<WiringArg> args;
            args.reserve(signals.size() + 1);
            WiringArg keys;
            keys.kind = WiringArg::Kind::Scalar;
            std::vector<Str> data_ids;
            data_ids.reserve(signals.size());
            for (const auto &[data_id, signal] : signals)
            {
                static_cast<void>(signal);
                data_ids.push_back(data_id);
            }
            keys.scalar_value = stdlib::make_list<Str>(data_ids.begin(),
                                                       data_ids.end());
            keys.scalar_meta = keys.scalar_value.schema();
            args.push_back(std::move(keys));
            for (const auto &[data_id, signal] : signals)
            {
                static_cast<void>(data_id);
                WiringArg value;
                value.kind = WiringArg::Kind::TimeSeries;
                value.port = signal;
                args.push_back(std::move(value));
            }
            return Port<IngressSignals>{
                wiring,
                wire_operator(wiring, "combine_tsd", args, true)
                    .output.erased()};
        }

        [[nodiscard]] Port<IngressSignals> wire_subscription_ingress(
            DeclarationState &state, Wiring &wiring)
        {
            std::map<SubscriptionPolicy, std::vector<std::size_t>> groups;
            std::map<Str, SubscriptionPolicy> policies_by_data_id;
            for (std::size_t index = 0; index < state.subscriptions.size();
                 ++index)
            {
                const auto &subscription = state.subscriptions[index];
                const SubscriptionPolicy policy{subscription.mode,
                                                subscription.as_of};
                const auto [found, inserted] = policies_by_data_id.emplace(
                    subscription.data_id, policy);
                if (!inserted && found->second != policy)
                {
                    throw std::invalid_argument(
                        "hgraph.fabric.subscribe_data: one data id cannot use "
                        "multiple subscription policies in the same wiring root");
                }
                groups[policy].push_back(index);
            }

            std::map<SubscriptionPolicy, Port<IngressSignals>> group_outputs;
            for (const auto &[policy, indexes] : groups)
            {
                std::vector<Str> roots;
                roots.reserve(indexes.size());
                for (const auto index : indexes)
                {
                    roots.push_back(state.subscriptions[index].data_id);
                }
                group_outputs.emplace(
                    policy, detail::wire_ingress_group(
                                wiring, canonical_ids(std::move(roots)),
                                policy.mode, policy.as_of));
            }

            std::map<Str, WiringPortRef> signals;
            for (auto &subscription : state.subscriptions)
            {
                const SubscriptionPolicy policy{subscription.mode,
                                                subscription.as_of};
                const auto group = group_outputs.find(policy);
                if (group == group_outputs.end())
                {
                    throw std::logic_error(
                        "fabric subscription policy group was not wired");
                }
                auto signal = wire<stdlib::getitem_>(
                                  wiring, group->second, subscription.data_id)
                                  .as<IngressSignal>();
                if (!subscription.delayed.bound())
                {
                    auto source = wire<detail::SubscribeDataRuntimeSource>(
                        wiring, signal, subscription.data_id,
                        subscription.mode, subscription.as_of);
                    subscription.delayed(source);
                }
                signals.try_emplace(subscription.data_id, signal.erased());
            }

            return combine_signal_ports(wiring, signals);
        }

        [[nodiscard]] Port<IngressSignals> wire_discovered_ingress(
            Wiring &wiring,
            const std::map<Str, std::pair<SubscriptionMode, DateTime>> &policies)
        {
            std::map<SubscriptionPolicy, std::vector<Str>> groups;
            for (const auto &[data_id, policy] : policies)
            {
                groups[SubscriptionPolicy{policy.first, policy.second}]
                    .push_back(data_id);
            }
            std::map<Str, WiringPortRef> signals;
            for (auto &[policy, roots] : groups)
            {
                auto output = detail::wire_ingress_group(
                    wiring, canonical_ids(std::move(roots)), policy.mode,
                    policy.as_of);
                for (const auto &[data_id, selected_policy] : policies)
                {
                    if (selected_policy !=
                        std::pair{policy.mode, policy.as_of})
                    {
                        continue;
                    }
                    signals.emplace(
                        data_id,
                        wire<stdlib::getitem_>(wiring, output, data_id)
                            .as<IngressSignal>()
                            .erased());
                }
            }
            return combine_signal_ports(wiring, signals);
        }

        [[nodiscard]] Port<IngressSignals> merge_ingress(
            Wiring &wiring, Port<IngressSignals> first,
            std::span<const Str> first_ids, Port<IngressSignals> second,
            const std::map<Str, std::pair<SubscriptionMode, DateTime>> &second_ids)
        {
            std::map<Str, WiringPortRef> signals;
            for (const auto &data_id : first_ids)
            {
                signals.emplace(
                    data_id,
                    wire<stdlib::getitem_>(wiring, first, data_id)
                        .as<IngressSignal>()
                        .erased());
            }
            for (const auto &[data_id, policy] : second_ids)
            {
                static_cast<void>(policy);
                signals.emplace(
                    data_id,
                    wire<stdlib::getitem_>(wiring, second, data_id)
                        .as<IngressSignal>()
                        .erased());
            }
            return combine_signal_ports(wiring, signals);
        }

        void finalize(DeclarationState &state, Wiring &wiring)
        {
            if (!state.subscriptions.empty() && !state.coordinator.has_value())
            {
                state.coordinator = wire_subscription_ingress(state, wiring);
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
                publisher.resolved_subscription_policies =
                    std::move(collection.policies);
                if (std::ranges::find(publisher.resolved_dependencies,
                                      publisher.data_id) !=
                    publisher.resolved_dependencies.end())
                {
                    throw std::invalid_argument(
                        "hgraph.fabric.publish_data: publisher must not depend "
                        "on its own data id");
                }
            }

            std::set<Str, decltype(&canonical_data_id_less)> direct_ids{
                &canonical_data_id_less};
            for (const auto &subscription : state.subscriptions)
            {
                direct_ids.insert(subscription.data_id);
            }
            std::map<Str, std::pair<SubscriptionMode, DateTime>> missing;
            for (const auto &publisher : state.publishers)
            {
                for (const auto &[data_id, policy] :
                     publisher.resolved_subscription_policies)
                {
                    if (direct_ids.contains(data_id)) { continue; }
                    const auto [found, inserted] = missing.emplace(data_id, policy);
                    if (!inserted && found->second != policy)
                    {
                        throw std::invalid_argument(
                            "fabric dependency discovery found conflicting "
                            "subscription policies for one data id");
                    }
                }
            }
            if (!missing.empty())
            {
                auto discovered = wire_discovered_ingress(wiring, missing);
                if (state.coordinator.has_value())
                {
                    std::vector<Str> ids{direct_ids.begin(), direct_ids.end()};
                    state.coordinator = merge_ingress(
                        wiring, *state.coordinator, ids, discovered, missing);
                }
                else
                {
                    state.coordinator = discovered;
                }
            }

            DependencyPlanInput plan = build_plan(state);
            wiring.set_trait(DEPENDENCY_PLAN_TRAIT,
                             make_dependency_plan(plan));

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
                case SubscriptionMode::Auto:
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
                    arg<"mode">(SubscriptionMode::Auto),
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
