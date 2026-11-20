#include <hgraph/types/impl/compiled_subgraph_wiring.h>

#include <numeric>

namespace hgraph {
namespace {
    using namespace subgraph_wiring_detail;

    struct CompiledSwitchSite {};
    struct CompiledMapSite {};
    struct CompiledTryExceptSite {};

    void check_child_inputs(const CompiledSubGraph &child, std::span<const TSValueTypeMetaData *const> sources)
    {
        if (child.input_schemas.size() != sources.size() + child.captured_inputs.size() +
                                          child.external_service_inputs.size()) {
            throw std::invalid_argument("compiled child has a different number of inputs");
        }
        for (std::size_t i = 0; i < sources.size(); ++i) {
            if (!graph_wiring_detail::input_binds_output_schema(child.input_schemas[i], sources[i])) {
                throw std::invalid_argument("compiled child input " + std::to_string(i) +
                                            " cannot bind the supplied source schema");
            }
        }
        if (child.output_binding.has_value() != (child.output_schema != nullptr)) {
            throw std::invalid_argument("compiled child output binding and schema must agree");
        }
    }

    WiringPortRef wire_compiled_switch(Wiring &w, WiringPortRef key, std::span<const WiringPortRef> inputs,
                                       std::vector<SwitchBranchGraph> branches,
                                       std::optional<SwitchBranchGraph> fallback, bool reload, bool returns_value)
    {
        auto &registry = TypeRegistry::instance();
        const auto *key_schema = registry.dereference(key.schema);
        if (key_schema == nullptr || key_schema->kind != TSTypeKind::TS) {
            throw std::invalid_argument("switch_: key must be a scalar time series");
        }
        if (branches.empty() && !fallback) { throw std::invalid_argument("switch_: requires at least one case"); }
        WiringPortRef key_boundary = key;
        key_boundary.schema = key_schema;
        std::vector<WiringPortRef> slots(inputs.begin(), inputs.end());
        std::vector<const TSValueTypeMetaData *> schemas{key_schema};
        for (auto &slot : slots) {
            slot = graph_wiring_detail::adapt_source_for_input(w, slot.schema, std::move(slot));
            schemas.push_back(slot.schema);
        }
        std::vector<std::size_t> positions(inputs.size());
        std::iota(positions.begin(), positions.end(), std::size_t{0});
        std::vector<ExternalServiceSlot> services;
        const TSValueTypeMetaData *output = nullptr;
        std::optional<bool> has_output;
        auto bind = [&](SwitchBranchGraph &entry) {
            check_child_inputs(entry.graph, std::span{schemas}.subspan(entry.takes_key ? 0 : 1));
            return bind_switch_branch(std::move(entry.graph), entry.takes_key, positions, key_boundary, slots,
                                      output, has_output, services);
        };
        SwitchNodeSpec spec;
        spec.reload_on_ticked = reload;
        spec.branches.reserve(branches.size());
        for (auto &entry : branches) {
            if (entry.key.schema() != key_schema->value_schema) {
                throw std::invalid_argument("switch_: case key must match the selector scalar type");
            }
            spec.branches.push_back(SwitchBranch{.key = std::move(entry.key), .spec = bind(entry)});
        }
        if (fallback) { spec.default_branch = bind(*fallback); }
        materialize_external_service_slots(w, std::move(services), slots);
        configure_switch_outputs(spec, output, has_output, returns_value);
        return add_compiled_switch(w, std::move(key), std::move(slots), std::move(spec), output,
                                   std::nullopt, typeid(CompiledSwitchSite), "switch_");
    }

    template <typename Spec>
    WiringPortRef wire_compiled_map(Wiring &w, WiringPortRef collection, std::span<const WiringPortRef> broadcast,
                                    CompiledSubGraph child, MapKeyArg key, std::optional<WiringPortRef> keys,
                                    const TSValueTypeMetaData *schema, bool returns_value)
    {
        auto &registry = TypeRegistry::instance();
        const auto *key_ts = [&] {
            if constexpr (std::is_same_v<Spec, MapNodeSpec>) { return registry.ts(schema->key_type()); }
            else { return ts_type<TS<Int>>(); }
        }();
        const auto *element = mapped_element_schema(child);
        if (returns_value != (element != nullptr)) {
            throw std::invalid_argument(returns_value ? "map_: child must produce an output" : "map_sink_: child must be a sink");
        }
        Spec spec;
        std::vector<const TSValueTypeMetaData *> child_schemas;
        if (key == MapKeyArg::First) {
            child_schemas.push_back(key_ts);
            spec.args.push_back(MapArgSource{.kind = MapArgSourceKind::Key});
        }
        std::vector<WiringPortRef> inputs{std::move(collection)};
        std::vector<const TSValueTypeMetaData *> schemas{inputs.front().schema};
        child_schemas.push_back(schema->element_ts());
        for (const auto &port : broadcast) {
            inputs.push_back(port);
            schemas.push_back(port.schema);
            child_schemas.push_back(port.schema);
        }
        check_child_inputs(child, child_schemas);
        for (auto &port : child.captured_inputs) {
            schemas.push_back(port.schema);
            inputs.push_back(std::move(port));
        }
        for (auto &service : child.external_service_inputs) {
            auto port = materialize_external_service_input(w, std::move(service));
            schemas.push_back(port.schema);
            inputs.push_back(std::move(port));
        }
        spec.multiplexed_inputs.push_back(0);
        spec.args.push_back(MapArgSource{.kind = MapArgSourceKind::Element, .outer_index = 0});
        for (std::size_t i = 1; i < inputs.size(); ++i) {
            spec.args.push_back(MapArgSource{.kind = MapArgSourceKind::OuterInput, .outer_index = i});
        }
        spec.child.graph_builder = std::move(child.graph_builder);
        spec.child.input_bindings = std::move(child.input_bindings);
        spec.child.output_binding = std::move(child.output_binding);
        const TSValueTypeMetaData *output = nullptr;
        if constexpr (std::is_same_v<Spec, MapNodeSpec>) {
            spec.key_output_schema = key == MapKeyArg::First ? key_ts : nullptr;
            if (!keys) { keys = tsd_key_set_ref(inputs[0]); }
            if (registry.dereference(keys->schema) != registry.tss(schema->key_type())) {
                throw std::invalid_argument("map_: explicit keys must be a set of the dictionary key type");
            }
            if (element != nullptr) { output = registry.tsd(schema->key_type(), element); }
        } else {
            spec.index_output_schema = key == MapKeyArg::First ? key_ts : nullptr;
            if (keys) { throw std::invalid_argument("map_: explicit keys require a dictionary"); }
            if (element != nullptr) { output = registry.tsl(element); }
        }
        if (element != nullptr) { bind_map_terminal(spec, child, element, "map_"); }
        return add_compiled_map(w, std::move(inputs), std::move(schemas), std::move(spec), output,
                                typeid(CompiledMapSite), std::nullopt, std::move(keys));
    }

    WiringPortRef wire_collection(Wiring &w, WiringPortRef collection, std::span<const WiringPortRef> broadcast,
                                 CompiledSubGraph child, MapKeyArg key, std::optional<WiringPortRef> keys,
                                 bool returns_value)
    {
        const auto *schema = TypeRegistry::instance().dereference(collection.schema);
        if (schema != nullptr && schema->kind == TSTypeKind::TSD) {
            return wire_compiled_map<MapNodeSpec>(w, std::move(collection), broadcast, std::move(child),
                                                  key, std::move(keys), schema, returns_value);
        }
        if (schema != nullptr && schema->is_unbounded_tsl()) {
            return wire_compiled_map<TslMapNodeSpec>(w, std::move(collection), broadcast, std::move(child),
                                                     key, std::move(keys), schema, returns_value);
        }
        throw std::invalid_argument("map_: collection must be a dictionary or dynamic list");
    }
}

WiringPortRef compiled::switch_(Wiring &w, WiringPortRef key, std::span<const WiringPortRef> inputs,
                      std::vector<SwitchBranchGraph> branches, std::optional<SwitchBranchGraph> fallback, bool reload)
{
    return wire_compiled_switch(w, std::move(key), inputs, std::move(branches), std::move(fallback), reload, true);
}

void compiled::switch_sink_(Wiring &w, WiringPortRef key, std::span<const WiringPortRef> inputs,
                  std::vector<SwitchBranchGraph> branches, std::optional<SwitchBranchGraph> fallback, bool reload)
{
    wire_compiled_switch(w, std::move(key), inputs, std::move(branches), std::move(fallback), reload, false);
}

WiringPortRef compiled::try_except_(Wiring &w, std::span<const WiringPortRef> inputs,
                                    CompiledSubGraph child, ErrorCaptureOptions options)
{
    std::vector<const TSValueTypeMetaData *> schemas;
    schemas.reserve(inputs.size());
    for (const auto &input : inputs) { schemas.push_back(input.schema); }
    check_child_inputs(child, schemas);

    std::vector<WiringPortRef> sources;
    sources.reserve(inputs.size());
    for (std::size_t i = 0; i < inputs.size(); ++i) {
        sources.push_back(graph_wiring_detail::adapt_source_for_input(w, child.input_schemas[i], inputs[i]));
    }

    auto plan = subgraph_wiring_detail::bind_subgraph_inputs(w, std::move(sources), std::move(child));
    const auto *output = subgraph_wiring_detail::try_except_output_schema(plan.compiled, options);
    auto builder = subgraph_wiring_detail::make_try_except_builder(plan, output, options, "try_except");
    return w.add_unique_node(typeid(CompiledTryExceptSite), std::move(builder), plan.inputs, {});
}

WiringPortRef compiled::map_(Wiring &w, WiringPortRef collection, std::span<const WiringPortRef> broadcast,
                   CompiledSubGraph child, MapKeyArg key, std::optional<WiringPortRef> keys)
{
    return wire_collection(w, std::move(collection), broadcast, std::move(child), key, std::move(keys), true);
}

void compiled::map_sink_(Wiring &w, WiringPortRef collection, std::span<const WiringPortRef> broadcast,
               CompiledSubGraph child, MapKeyArg key, std::optional<WiringPortRef> keys)
{
    wire_collection(w, std::move(collection), broadcast, std::move(child), key, std::move(keys), false);
}
}
