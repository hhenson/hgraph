#include <hgraph/fabric/operators.h>

#include <hgraph/fabric/value_builders.h>

#include <hgraph/types/operator_dispatch.h>

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <tuple>
#include <typeindex>
#include <utility>

namespace hgraph::fabric
{
    namespace
    {
        struct SubscriptionDeclaration
        {
            Str           data_id{};
            WiringPortRef output{};
        };

        struct PublisherDeclaration
        {
            Str                 data_id{};
            DependencySelection dependencies{DependencySelection::automatic()};
        };

        struct DeclarationState
        {
            std::vector<SubscriptionDeclaration> subscriptions{};
            std::vector<PublisherDeclaration> publishers{};
        };

        [[nodiscard]] std::shared_ptr<DeclarationState> declarations(Wiring &wiring)
        {
            return std::static_pointer_cast<DeclarationState>(
                wiring.acquire_extension_state(
                    std::type_index(typeid(DeclarationState)),
                    [] { return std::make_shared<DeclarationState>(); }));
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

        /** Checkpoint-1 source shape. The selected production source replaces
            this overload in checkpoint 5. It owns no state and never reaches
            eval: start fails clearly if a contract-only graph is executed. */
        struct SubscribeDataContractSource
        {
            static constexpr auto name = "hgraph.fabric.subscribe_data.contract";
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

        /** Checkpoint-1 sink shape. It has O(1) empty state and only rejects a
            value tick; the publication state machine replaces it in
            checkpoint 2. */
        struct PublishDataContractSink
        {
            static constexpr auto name = "hgraph.fabric.publish_data.contract";

            static void eval(In<"value", TS<Frame>>,
                             Scalar<"data_id", Str>)
            {
                throw std::logic_error(
                    "hgraph.fabric.publish_data runtime is introduced by "
                    "RFC 0026 publication checkpoint");
            }
        };

        struct SubscribeDataContractGraph
        {
            static constexpr auto name =
                "hgraph.fabric.subscribe_data.contract_graph";

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
                auto output = wire<SubscribeDataContractSource>(
                    wiring, data_id.value(), mode.value(), as_of.value());
                declarations(wiring)->subscriptions.push_back(
                    SubscriptionDeclaration{data_id.value(), output.erased()});
                return output;
            }
        };

        struct PublishDataContractGraph
        {
            static constexpr auto name =
                "hgraph.fabric.publish_data.contract_graph";

            static void compose(Wiring &wiring,
                                Scalar<"data_id", Str> data_id,
                                NamedPort<"value", TS<Frame>> value)
            {
                require_data_id(data_id.value());
                declarations(wiring)->publishers.push_back(PublisherDeclaration{
                    .data_id = data_id.value(),
                    .dependencies = DependencySelection::automatic(),
                });
                wire<PublishDataContractSink>(wiring, value, data_id.value());
            }
        };

        void install_fabric_operators()
        {
            register_fabric_types();
            register_graph_overload<SubscribeData, SubscribeDataContractGraph>();
            register_graph_overload<PublishData, PublishDataContractGraph>();
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
                if (dependencies[index].source().same_source_as(
                        dependencies[previous].source()))
                {
                    throw std::invalid_argument(
                        "explicit fabric dependencies must be unique");
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
