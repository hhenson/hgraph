#include <hgraph/types/keyed_service_transport.h>

#include <hgraph/runtime/feedback_node.h>
#include <hgraph/runtime/service_node.h>
#include <hgraph/runtime/shared_output_node.h>

#include <array>
#include <memory>
#include <optional>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace hgraph::keyed_service_transport
{
    namespace
    {
        struct request_reply_feedback_source_marker
        {
        };

        struct request_reply_feedback_sink_marker
        {
        };

        struct PendingSubscriptionClient
        {
            std::string                    base_path{};
            std::string                    request_path{};
            std::string                    response_path{};
            const ValueTypeMetaData       *key_schema{nullptr};
            WiringPortRef                  key{};
            WiringPortRef                  request_source{};
            WiringPortRef                  response_source{};
            std::type_index                capture_role{typeid(void)};
        };

        struct PendingClient
        {
            std::string                base_path{};
            std::string                request_path{};
            std::string                response_path{};
            const TSValueTypeMetaData *request_schema{nullptr};
            WiringPortRef              request{};
            WiringPortRef              request_source{};
            WiringPortRef              request_id{};
            WiringPortRef              response_source{};
            std::type_index            capture_role{typeid(void)};
        };

        struct PendingOutput
        {
            std::string                 base_path{};
            std::string                 response_path{};
            const TSValueTypeMetaData  *response_dict_schema{nullptr};
            WiringPortRef               output{};
            WiringPortRef               response_source{};
            std::type_index             capture_role{typeid(void)};
            std::shared_ptr<const bool> boundary_dependency{};
            bool                        full_feedback_on_boundary_dependency{true};
        };

        struct Planner
        {
            std::unordered_map<std::string, const WiringInstance *>    implementation_inputs{};
            std::unordered_map<std::string, KeyedServiceTransportPlan> plans{};
            std::vector<PendingClient>                                 clients{};
            std::vector<PendingSubscriptionClient>                     subscription_clients{};
            std::vector<PendingOutput>                                 outputs{};
            std::size_t                                                finalized_clients{0};
            std::size_t                                                finalized_subscription_clients{0};
            std::size_t                                                finalized_outputs{0};

            void finalize(Wiring &w);
        };

        [[nodiscard]] bool port_depends_on(const WiringPortRef &port, const WiringInstance *target,
                                           std::unordered_set<const WiringInstance *> &visited);

        [[nodiscard]] bool node_depends_on(const WiringInstance *node, const WiringInstance *target,
                                           std::unordered_set<const WiringInstance *> &visited)
        {
            if (node == target)
            {
                return true;
            }
            if (node == nullptr || !visited.insert(node).second)
            {
                return false;
            }
            for (const WiringInputRef &input : node->inputs)
            {
                // Passive/recovery links do not establish data causality and
                // are deliberately excluded from the transport decision.
                if (input.rank_dependency && port_depends_on(input.source, target, visited))
                {
                    return true;
                }
            }
            return false;
        }

        [[nodiscard]] bool port_depends_on(const WiringPortRef &port, const WiringInstance *target,
                                           std::unordered_set<const WiringInstance *> &visited)
        {
            if (port.is_delayed_source())
            {
                const auto &state = port.delayed_state();
                return state != nullptr && state->source.has_value() &&
                       port_depends_on(*state->source, target, visited);
            }
            if (port.is_peered_source())
            {
                return node_depends_on(port.peered_node(), target, visited);
            }
            if (port.is_structural_source())
            {
                for (const WiringPortRef &child : port.structural_children())
                {
                    if (port_depends_on(child, target, visited))
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        [[nodiscard]] bool output_depends_on(const WiringPortRef &output, const WiringInstance *request_source)
        {
            std::unordered_set<const WiringInstance *> visited;
            return port_depends_on(output, request_source, visited);
        }

        [[nodiscard]] WiringPortRef response_feedback(Wiring &w, WiringPortRef response,
                                                      const TSValueTypeMetaData &schema)
        {
            WiringPortRef feedback =
                w.add_unique_node(std::type_index(typeid(request_reply_feedback_source_marker)),
                                  make_feedback_source_node(schema), std::span<const WiringPortRef>{}, Value{});
            std::array<WiringPortRef, 2> sources{
                graph_wiring_detail::adapt_source_for_input(w, &schema, std::move(response)),
                feedback,
            };
            std::array<WiringInputRef, 2> inputs{{
                WiringInputRef{.source = sources[0]},
                WiringInputRef{.source = sources[1], .rank_dependency = false},
            }};
            NodeBuilder                   sink = make_feedback_sink_node(schema);
            sink.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                sink.type().schema()->input_schema, std::span<const WiringPortRef>{sources.data(), sources.size()}));
            static_cast<void>(
                w.add_unique_node(std::type_index(typeid(request_reply_feedback_sink_marker)), std::move(sink),
                                  std::span<const WiringInputRef>{inputs.data(), inputs.size()}, Value{}));
            return feedback;
        }

        [[nodiscard]] const WiringInstance *publish_response(Wiring &w, const PendingOutput &pending,
                                                             KeyedServiceTransportPlan plan)
        {
            WiringPortRef output = pending.output;
            if (plan == KeyedServiceTransportPlan::FullFeedback)
            {
                if (!pending.full_feedback_on_boundary_dependency)
                {
                    throw std::logic_error(
                        "subscription transport cannot select response feedback");
                }
                output = response_feedback(w, std::move(output), *pending.response_dict_schema);
            }
            else
            {
                output =
                    graph_wiring_detail::adapt_source_for_input(w, pending.response_dict_schema, std::move(output));
            }

            std::array<WiringPortRef, 2>  sources{std::move(output), pending.response_source};
            std::array<WiringInputRef, 2> inputs{{
                WiringInputRef{.source = sources[0]},
                WiringInputRef{.source = sources[1], .rank_dependency = false},
            }};
            NodeBuilder builder = make_shared_output_capture_node(pending.response_path, *pending.response_dict_schema);
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                builder.type().schema()->input_schema, std::span<const WiringPortRef>{sources.data(), sources.size()}));
            WiringPortRef capture = w.add_node(pending.capture_role, std::move(builder),
                                               std::span<const WiringInputRef>{inputs.data(), inputs.size()}, Value{});
            w.add_same_cycle_pair(capture.peered_node(), pending.response_source.peered_node());
            w.register_service_rank_anchor(pending.response_path, capture.peered_node());
            return capture.peered_node();
        }

        void publish_request(Wiring &w, const PendingClient &pending, KeyedServiceTransportPlan plan)
        {
            const bool                    direct = plan == KeyedServiceTransportPlan::Direct;
            std::array<WiringPortRef, 3>  sources{pending.request, pending.request_source, pending.request_id};
            std::array<WiringInputRef, 3> inputs{{
                WiringInputRef{.source = sources[0]},
                WiringInputRef{.source = sources[1], .rank_dependency = false},
                WiringInputRef{.source = sources[2]},
            }};
            NodeBuilder                   builder =
                make_request_input_capture_node(pending.request_path, *pending.request_schema, direct);
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                builder.type().schema()->input_schema, std::span<const WiringPortRef>{sources.data(), sources.size()}));
            WiringPortRef capture = w.add_node(pending.capture_role, std::move(builder),
                                               std::span<const WiringInputRef>{inputs.data(), inputs.size()}, Value{});
            if (direct)
            {
                w.register_service_client_rank(pending.request_path, "request/reply service", capture.peered_node(),
                                               false);
            }
            if (plan != KeyedServiceTransportPlan::FullFeedback)
            {
                w.register_service_client_rank(pending.response_path, "request/reply service",
                                               pending.response_source.peered_node(), true);
            }
        }

        void publish_subscription_request(
            Wiring &w, PendingSubscriptionClient &pending,
            KeyedServiceTransportPlan plan)
        {
            const bool direct = plan == KeyedServiceTransportPlan::Direct;
            std::array<WiringPortRef, 2> capture_sources{
                pending.key, pending.request_source};
            std::array<WiringInputRef, 2> capture_inputs{{
                WiringInputRef{.source = capture_sources[0]},
                WiringInputRef{
                    .source = capture_sources[1], .rank_dependency = false},
            }};
            NodeBuilder capture_builder = make_subscription_key_capture_node(
                pending.request_path, *pending.key_schema, direct);
            capture_builder.input_endpoint(
                graph_wiring_detail::input_endpoint_for_sources(
                    capture_builder.type().schema()->input_schema,
                    std::span<const WiringPortRef>{
                        capture_sources.data(), capture_sources.size()}));
            WiringPortRef capture = w.add_node(
                pending.capture_role, std::move(capture_builder),
                std::span<const WiringInputRef>{
                    capture_inputs.data(), capture_inputs.size()},
                Value{});
            if (direct)
            {
                w.register_service_client_rank(
                    pending.request_path, "subscription service",
                    capture.peered_node(), false);
            }
            w.register_service_client_rank(
                pending.response_path, "subscription service",
                pending.response_source.peered_node(), true);
        }

        void Planner::finalize(Wiring &w)
        {
            while (finalized_outputs < outputs.size())
            {
                const PendingOutput &pending = outputs[finalized_outputs++];
                const auto           input = implementation_inputs.find(pending.base_path);
                if (input == implementation_inputs.end() || input->second == nullptr)
                {
                    throw std::logic_error("keyed service implementation '" + pending.base_path +
                                           "' published an output without its request input");
                }

                KeyedServiceTransportPlan plan = KeyedServiceTransportPlan::Direct;
                if (pending.full_feedback_on_boundary_dependency
                    && pending.boundary_dependency != nullptr
                    && *pending.boundary_dependency)
                {
                    plan = KeyedServiceTransportPlan::FullFeedback;
                }
                else if ((pending.boundary_dependency != nullptr
                          && *pending.boundary_dependency)
                         || output_depends_on(pending.output, input->second))
                {
                    plan = KeyedServiceTransportPlan::RequestDeferred;
                }
                auto [it, inserted] = plans.try_emplace(pending.base_path, plan);
                if (!inserted && it->second != plan)
                {
                    throw std::logic_error("keyed service implementation '" + pending.base_path +
                                           "' produced conflicting transport plans");
                }
                static_cast<void>(publish_response(w, pending, plan));
            }

            while (finalized_clients < clients.size())
            {
                const PendingClient &pending = clients[finalized_clients++];
                const auto           selected = plans.find(pending.base_path);
                if (selected == plans.end())
                {
                    if (w.kind() == WiringKind::TopLevel)
                    {
                        throw std::logic_error("request/reply implementation '" + pending.base_path +
                                               "' did not publish a response transport");
                    }
                    // A child externalizes the service. Both request modes hand
                    // off to the owner on the next tick, so retain the
                    // conservative unranked capture in the compiled child.
                    publish_request(w, pending, KeyedServiceTransportPlan::RequestDeferred);
                    continue;
                }
                publish_request(w, pending, selected->second);
            }

            while (finalized_subscription_clients < subscription_clients.size())
            {
                PendingSubscriptionClient &pending =
                    subscription_clients[finalized_subscription_clients++];
                const auto selected = plans.find(pending.base_path);
                if (selected == plans.end())
                {
                    if (w.kind() == WiringKind::TopLevel)
                    {
                        throw std::logic_error(
                            "subscription implementation '" + pending.base_path
                            + "' did not publish a response transport");
                    }
                    publish_subscription_request(
                        w, pending, KeyedServiceTransportPlan::RequestDeferred);
                    continue;
                }
                publish_subscription_request(w, pending, selected->second);
            }
        }

        [[nodiscard]] std::shared_ptr<Planner> planner_for(Wiring &w)
        {
            auto state = std::static_pointer_cast<Planner>(w.acquire_extension_state(
                std::type_index(typeid(Planner)), [] { return std::make_shared<Planner>(); }));
            if (state->clients.empty() && state->subscription_clients.empty()
                && state->outputs.empty() && state->implementation_inputs.empty())
            {
                std::weak_ptr<Planner> weak = state;
                w.register_pre_rank_finalizer(
                    [weak](Wiring &target)
                    {
                        if (const auto planner = weak.lock())
                        {
                            planner->finalize(target);
                        }
                    });
            }
            return state;
        }
    }  // namespace

    void defer_request_reply_client(
        Wiring &w, std::string base_path, std::string request_path,
        std::string response_path, const TSValueTypeMetaData &request_schema,
        WiringPortRef request, WiringPortRef request_source,
        WiringPortRef request_id, WiringPortRef response_source,
        std::type_index capture_role)
    {
        planner_for(w)->clients.push_back(PendingClient{
            .base_path = std::move(base_path),
            .request_path = std::move(request_path),
            .response_path = std::move(response_path),
            .request_schema = &request_schema,
            .request = std::move(request),
            .request_source = std::move(request_source),
            .request_id = std::move(request_id),
            .response_source = std::move(response_source),
            .capture_role = capture_role,
        });
    }

    WiringPortRef defer_subscription_client(
        Wiring &w, std::string base_path, std::string request_path,
        std::string response_path, const ValueTypeMetaData &key_schema,
        const TSValueTypeMetaData &response_schema, WiringPortRef key,
        WiringPortRef request_source, WiringPortRef response,
        WiringPortRef response_source, std::type_index capture_role,
        std::type_index gate_role)
    {
        std::array<WiringPortRef, 3> gate_sources{
            std::move(response), key, request_source};
        std::array<WiringInputRef, 3> gate_inputs{{
            WiringInputRef{.source = gate_sources[0]},
            WiringInputRef{.source = gate_sources[1]},
            WiringInputRef{.source = gate_sources[2]},
        }};
        NodeBuilder gate_builder = make_subscription_response_gate_node(
            key_schema, response_schema, /*response_same_cycle=*/true);
        gate_builder.input_endpoint(
            graph_wiring_detail::input_endpoint_for_sources(
                gate_builder.type().schema()->input_schema,
                std::span<const WiringPortRef>{
                    gate_sources.data(), gate_sources.size()}));
        WiringPortRef gated = w.add_node(
            gate_role, std::move(gate_builder),
            std::span<const WiringInputRef>{
                gate_inputs.data(), gate_inputs.size()},
            Value{});
        planner_for(w)->subscription_clients.push_back(PendingSubscriptionClient{
            .base_path       = std::move(base_path),
            .request_path    = std::move(request_path),
            .response_path   = std::move(response_path),
            .key_schema      = &key_schema,
            .key             = std::move(key),
            .request_source  = std::move(request_source),
            .response_source = std::move(response_source),
            .capture_role    = capture_role,
        });
        return gated;
    }

    void register_implementation_input(Wiring &w, std::string base_path, const WiringInstance *request_source)
    {
        if (request_source == nullptr)
        {
            throw std::invalid_argument("keyed service implementation input must name a source");
        }
        auto planner = planner_for(w);
        auto [it, inserted] = planner->implementation_inputs.try_emplace(std::move(base_path), request_source);
        if (!inserted && it->second != request_source)
        {
            throw std::invalid_argument("duplicate keyed service implementation input for '" + it->first + "'");
        }
    }

    namespace
    {
        void defer_output(
            Wiring &w, std::string base_path, std::string response_path,
            const TSValueTypeMetaData &response_dict_schema,
            WiringPortRef output, WiringPortRef response_source,
            std::type_index capture_role,
            bool full_feedback_on_boundary_dependency)
        {
            auto dependency = w.service_implementation_boundary_dependency();
            if (dependency == nullptr)
            {
                throw std::logic_error(
                    "keyed service output must be wired inside its implementation scope");
            }
            planner_for(w)->outputs.push_back(PendingOutput{
                .base_path                = std::move(base_path),
                .response_path            = std::move(response_path),
                .response_dict_schema     = &response_dict_schema,
                .output                   = std::move(output),
                .response_source          = std::move(response_source),
                .capture_role             = capture_role,
                .boundary_dependency      = std::move(dependency),
                .full_feedback_on_boundary_dependency =
                    full_feedback_on_boundary_dependency,
            });
        }
    }  // namespace

    void defer_request_reply_implementation_output(
        Wiring &w, std::string base_path, std::string response_path,
        const TSValueTypeMetaData &response_dict_schema, WiringPortRef output,
        WiringPortRef response_source, std::type_index capture_role)
    {
        defer_output(
            w, std::move(base_path), std::move(response_path),
            response_dict_schema, std::move(output), std::move(response_source),
            capture_role, true);
    }

    void defer_subscription_implementation_output(
        Wiring &w, std::string base_path, std::string response_path,
        const TSValueTypeMetaData &response_dict_schema, WiringPortRef output,
        WiringPortRef response_source, std::type_index capture_role)
    {
        defer_output(
            w, std::move(base_path), std::move(response_path),
            response_dict_schema, std::move(output), std::move(response_source),
            capture_role, false);
    }
}  // namespace hgraph::keyed_service_transport
