#include <hgraph/types/request_reply_transport.h>

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

namespace hgraph::request_reply_transport
{
    namespace
    {
        struct request_reply_feedback_source_marker
        {
        };

        struct request_reply_feedback_sink_marker
        {
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
        };

        struct Planner
        {
            std::unordered_map<std::string, const WiringInstance *>    implementation_inputs{};
            std::unordered_map<std::string, RequestReplyTransportPlan> plans{};
            std::vector<PendingClient>                                 clients{};
            std::vector<PendingOutput>                                 outputs{};
            std::size_t                                                finalized_clients{0};
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
                                                             RequestReplyTransportPlan plan)
        {
            WiringPortRef output = pending.output;
            if (plan == RequestReplyTransportPlan::FullFeedback)
            {
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

        void publish_request(Wiring &w, const PendingClient &pending, RequestReplyTransportPlan plan)
        {
            const bool                    direct = plan == RequestReplyTransportPlan::Direct;
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
            if (plan != RequestReplyTransportPlan::FullFeedback)
            {
                w.register_service_client_rank(pending.response_path, "request/reply service",
                                               pending.response_source.peered_node(), true);
            }
        }

        void Planner::finalize(Wiring &w)
        {
            while (finalized_outputs < outputs.size())
            {
                const PendingOutput &pending = outputs[finalized_outputs++];
                const auto           input = implementation_inputs.find(pending.base_path);
                if (input == implementation_inputs.end() || input->second == nullptr)
                {
                    throw std::logic_error("request/reply implementation '" + pending.base_path +
                                           "' published an output without its request input");
                }

                RequestReplyTransportPlan plan = RequestReplyTransportPlan::Direct;
                if (pending.boundary_dependency != nullptr && *pending.boundary_dependency)
                {
                    plan = RequestReplyTransportPlan::FullFeedback;
                }
                else if (output_depends_on(pending.output, input->second))
                {
                    plan = RequestReplyTransportPlan::RequestDeferred;
                }
                auto [it, inserted] = plans.try_emplace(pending.base_path, plan);
                if (!inserted && it->second != plan)
                {
                    throw std::logic_error("request/reply implementation '" + pending.base_path +
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
                    publish_request(w, pending, RequestReplyTransportPlan::RequestDeferred);
                    continue;
                }
                publish_request(w, pending, selected->second);
            }
        }

        [[nodiscard]] std::shared_ptr<Planner> planner_for(Wiring &w)
        {
            auto state = std::static_pointer_cast<Planner>(w.acquire_extension_state(
                std::type_index(typeid(Planner)), [] { return std::make_shared<Planner>(); }));
            if (state->clients.empty() && state->outputs.empty() && state->implementation_inputs.empty())
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

    void defer_client(Wiring &w, std::string base_path, std::string request_path, std::string response_path,
                      const TSValueTypeMetaData &request_schema, WiringPortRef request, WiringPortRef request_source,
                      WiringPortRef request_id, WiringPortRef response_source, std::type_index capture_role)
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

    void register_implementation_input(Wiring &w, std::string base_path, const WiringInstance *request_source)
    {
        if (request_source == nullptr)
        {
            throw std::invalid_argument("request/reply implementation input must name a source");
        }
        auto planner = planner_for(w);
        auto [it, inserted] = planner->implementation_inputs.try_emplace(std::move(base_path), request_source);
        if (!inserted && it->second != request_source)
        {
            throw std::invalid_argument("duplicate request/reply implementation input for '" + it->first + "'");
        }
    }

    void defer_implementation_output(Wiring &w, std::string base_path, std::string response_path,
                                     const TSValueTypeMetaData &response_dict_schema, WiringPortRef output,
                                     WiringPortRef response_source, std::type_index capture_role)
    {
        auto dependency = w.service_implementation_boundary_dependency();
        if (dependency == nullptr)
        {
            throw std::logic_error("request/reply output must be wired inside its implementation scope");
        }
        planner_for(w)->outputs.push_back(PendingOutput{
            .base_path = std::move(base_path),
            .response_path = std::move(response_path),
            .response_dict_schema = &response_dict_schema,
            .output = std::move(output),
            .response_source = std::move(response_source),
            .capture_role = capture_role,
            .boundary_dependency = std::move(dependency),
        });
    }
}  // namespace hgraph::request_reply_transport
