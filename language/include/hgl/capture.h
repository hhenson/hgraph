#ifndef HGL_CAPTURE_H
#define HGL_CAPTURE_H

#include <hgraph/runtime/canonical_error.h>
#include <hgraph/types/subgraph_wiring.h>
#include <tuple>

namespace hgl {
    struct CapturedPorts {
        std::optional<hgraph::WiringPortRef> value;
        hgraph::WiringPortRef error;
    };
    template <typename Schema> struct Captured {
        hgraph::Port<Schema> value;
        hgraph::Port<hgraph::TS<hgraph::CanonicalError>> error;
    };
    template <> struct Captured<void> {
        hgraph::Port<hgraph::TS<hgraph::CanonicalError>> error;
    };

    struct CapturedErrorEndpoint {
        static void eval(hgraph::In<"error", hgraph::TS<hgraph::CanonicalError>> source,
                         hgraph::Out<hgraph::TS<hgraph::CanonicalError>> output) {
            output.set(source.value());
        }
    };
    [[nodiscard]] inline CapturedPorts capture_node(hgraph::Wiring &wiring, hgraph::WiringPortRef source) {
        const auto *error = hgraph::schema_descriptor<hgraph::TS<hgraph::CanonicalError>>::ts_meta();
        wiring.activate_error_capture(source.peered_node(), error, hgraph::ErrorCaptureOptions{.canonical_errors = true});
        CapturedPorts result;
        if (source.schema) { result.value = source; }
        auto endpoint = hgraph::WiringPortRef::peered_source(source.peered_node(), {}, error, hgraph::GraphEdgeSourceKind::ErrorOutput);
        // A normal connection may itself be the terminal of a composed graph.
        // Materialize that endpoint using the ordinary atomic publication path.
        result.error = hgraph::wire<CapturedErrorEndpoint>(wiring, hgraph::Port<hgraph::TS<hgraph::CanonicalError>>{wiring, endpoint}).erased();
        return result;
    }
    template <typename Schema> [[nodiscard]] Captured<Schema> capture_node(const hgraph::Port<Schema> &source) {
        auto &wiring = source.checked_wiring();
        auto ports = capture_node(wiring, source.erased());
        if constexpr (std::is_same_v<Schema, void>) { return {hgraph::Port<hgraph::TS<hgraph::CanonicalError>>{wiring, ports.error}}; }
        else { return {source, hgraph::Port<hgraph::TS<hgraph::CanonicalError>>{wiring, ports.error}}; }
    }

    struct CaptureGraphTag {};
    [[nodiscard]] inline CapturedPorts capture_compiled(hgraph::Wiring &wiring, hgraph::CompiledSubGraph compiled,
                                                       std::vector<hgraph::WiringPortRef> inputs) {
        for (auto &captured : compiled.captured_inputs) { inputs.push_back(std::move(captured)); }
        compiled.captured_inputs.clear();
        for (auto &external : compiled.external_service_inputs) {
            inputs.push_back(hgraph::subgraph_wiring_detail::materialize_external_service_input(wiring, std::move(external)));
        }
        compiled.external_service_inputs.clear();
        auto &registry = hgraph::TypeRegistry::instance();
        const auto *error = hgraph::schema_descriptor<hgraph::TS<hgraph::CanonicalError>>::ts_meta();
        const bool has_value = compiled.output_schema != nullptr;
        const auto *normal = compiled.output_schema;
        const auto *output = has_value ? registry.un_named_tsb({{"value", normal}, {"error", error}}) : error;
        std::vector<std::pair<std::string, const hgraph::TSValueTypeMetaData *>> fields;
        for (std::size_t index = 0; index < compiled.input_schemas.size(); ++index) {
            fields.emplace_back(std::to_string(index), compiled.input_schemas[index]);
        }
        hgraph::NodeTypeMetaData metadata;
        metadata.display_name = "capture";
        metadata.input_schema = fields.empty() ? nullptr : registry.un_named_tsb(fields);
        metadata.output_schema = output;
        hgraph::SingleNestedGraphNodeSpec specification;
        specification.graph_builder = std::move(compiled.graph_builder);
        specification.input_bindings = std::move(compiled.input_bindings);
        specification.output_binding = std::move(compiled.output_binding);
        if (specification.output_binding) { specification.output_binding->target_path = {0}; }
        auto builder = hgraph::try_except_node(std::move(metadata), std::move(specification), {},
                                               hgraph::ErrorCaptureOptions{.canonical_errors = true});
        builder.input_endpoint(hgraph::graph_wiring_detail::input_endpoint_for_sources(
            builder.type().schema()->input_schema, std::span<const hgraph::WiringPortRef>{inputs}));
        auto source = wiring.add_node(std::type_index(typeid(CaptureGraphTag)), std::move(builder), inputs, hgraph::Value{});
        CapturedPorts result;
        if (has_value) {
            result.value = hgraph::WiringPortRef::peered_source(source.peered_node(), {0}, normal);
            result.error = hgraph::WiringPortRef::peered_source(source.peered_node(), {1}, error);
        } else { result.error = std::move(source); }
        return result;
    }
    template <typename Call, typename... Args>
    [[nodiscard]] auto capture_call(hgraph::Wiring &wiring, Call call, const Args &...arguments) {
        auto child = wiring.child_wiring();
        std::vector<hgraph::WiringPortRef> inputs;
        std::vector<const hgraph::TSValueTypeMetaData *> schemas;
        const auto rebind_port = [&](hgraph::WiringPortRef &port) {
            inputs.push_back(port);
            schemas.push_back(port.schema);
            port = hgraph::WiringPortRef::boundary_source(inputs.size() - 1, {}, port.schema);
        };
        const auto rebind = [&]<typename Arg>(const Arg &argument) {
            if constexpr (requires { argument.erased(); typename Arg::schema; }) {
                auto source = argument.erased();
                rebind_port(source);
                return hgraph::Port<typename Arg::schema>{child, std::move(source)};
            } else {
                auto result = argument;
                if constexpr (requires { result.ports; }) {
                    for (auto &port : result.ports) {
                        if constexpr (requires { port.second; }) { rebind_port(port.second); }
                        else { rebind_port(port); }
                    }
                }
                return result;
            }
        };
        // Braced initialization fixes argument preparation in written order.
        auto rebound = std::tuple{rebind(arguments)...};
        using Result = decltype(std::apply([&](const auto &...args) { return call(child, args...); }, rebound));
        if constexpr (std::is_void_v<Result>) {
            std::apply([&](const auto &...args) { call(child, args...); }, rebound);
            auto compiled = std::move(child).finish_subgraph(std::nullopt, std::move(schemas));
            auto ports = capture_compiled(wiring, std::move(compiled), std::move(inputs));
            return Captured<void>{hgraph::Port<hgraph::TS<hgraph::CanonicalError>>{wiring, ports.error}};
        } else {
            auto output = std::apply([&](const auto &...args) { return call(child, args...); }, rebound);
            auto compiled = std::move(child).finish_subgraph(output.erased(), std::move(schemas));
            auto ports = capture_compiled(wiring, std::move(compiled), std::move(inputs));
            return Captured<typename Result::schema>{hgraph::Port<typename Result::schema>{wiring, *ports.value},
                hgraph::Port<hgraph::TS<hgraph::CanonicalError>>{wiring, ports.error}};
        }
    }
    template <typename Schema, typename Graph, typename... Args>
    [[nodiscard]] Captured<Schema> capture_graph(hgraph::Wiring &wiring, const Args &...arguments) {
        auto plan = hgraph::subgraph_wiring_detail::build_subgraph_call<Graph>(wiring, arguments...);
        auto ports = capture_compiled(wiring, std::move(plan.compiled), std::move(plan.inputs));
        if constexpr (std::is_same_v<Schema, void>) { return {hgraph::Port<hgraph::TS<hgraph::CanonicalError>>{wiring, ports.error}}; }
        else { return {hgraph::Port<Schema>{wiring, *ports.value}, hgraph::Port<hgraph::TS<hgraph::CanonicalError>>{wiring, ports.error}}; }
    }
}
#endif
