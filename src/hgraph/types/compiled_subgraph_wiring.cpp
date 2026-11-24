#include <hgraph/types/impl/compiled_subgraph_wiring.h>

#include <hgraph/runtime/ordered_reduce_node.h>
#include <hgraph/runtime/reduce_node.h>
#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/types/impl/default_wiring.h>

#include <iterator>
#include <numeric>

namespace hgraph {
namespace {
    using namespace subgraph_wiring_detail;

    struct CompiledSwitchSite {};
    struct CompiledMapSite {};
    struct CompiledMeshSite {};
    struct CompiledMeshPeer {};
    struct CompiledReduceSite {};
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

    constexpr std::array<std::size_t, 1> first_collection{0};

    std::vector<WiringPortRef> collection_and_broadcasts(WiringPortRef collection,
                                                         std::span<const WiringPortRef> broadcast)
    {
        std::vector<WiringPortRef> inputs;
        inputs.reserve(broadcast.size() + 1);
        inputs.push_back(std::move(collection));
        inputs.insert(inputs.end(), broadcast.begin(), broadcast.end());
        return inputs;
    }

    const TSValueTypeMetaData *collection_schema(std::span<const WiringPortRef> inputs,
                                                 std::span<const std::size_t> multiplexed)
    {
        if (multiplexed.empty()) { throw std::invalid_argument("map_: at least one collection must be multiplexed"); }
        if (multiplexed.front() >= inputs.size()) { throw std::invalid_argument("map_: invalid multiplexed index"); }
        return TypeRegistry::instance().dereference(inputs[multiplexed.front()].schema);
    }

    const TSValueTypeMetaData *checked_map_collection(const TSValueTypeMetaData *input,
                                                      const TSValueTypeMetaData *first)
    {
        const auto *collection = TypeRegistry::instance().dereference(input);
        if (first->is_unbounded_tsl()) {
            if (!collection || !collection->is_unbounded_tsl()) {
                throw std::invalid_argument("map_: every multiplexed list must be dynamic");
            }
        } else if (!collection || collection->kind != TSTypeKind::TSD || collection->key_type() != first->key_type()) {
            throw std::invalid_argument("map_: multiplexed dictionaries must have the same key type");
        }
        return collection;
    }

    template <typename Spec>
    std::vector<const TSValueTypeMetaData *> bind_map_arguments(
        Wiring &w, Spec &spec, CompiledSubGraph &child, std::vector<WiringPortRef> &inputs,
        std::span<const std::size_t> multiplexed, MapKeyArg key, const TSValueTypeMetaData *key_ts,
        const TSValueTypeMetaData *schema)
    {
        std::vector<const TSValueTypeMetaData *> child_schemas;
        if (key == MapKeyArg::First) {
            child_schemas.push_back(key_ts);
            spec.args.push_back(MapArgSource{.kind = MapArgSourceKind::Key});
        }
        std::vector<const TSValueTypeMetaData *> schemas;
        const auto offset = child_schemas.size();
        for (const auto &port : inputs) {
            schemas.push_back(port.schema);
            child_schemas.push_back(port.schema);
            spec.args.push_back(MapArgSource{.kind = MapArgSourceKind::OuterInput, .outer_index = schemas.size() - 1});
        }
        for (const auto index : multiplexed) {
            if (index >= inputs.size()) { throw std::invalid_argument("map_: invalid multiplexed index"); }
            auto &source = spec.args[offset + index];
            if (source.kind == MapArgSourceKind::Element) {
                throw std::invalid_argument("map_: multiplexed indices must be unique");
            }
            const auto *collection = checked_map_collection(schemas[index], schema);
            if constexpr (std::is_same_v<Spec, MeshNodeSpec>) {
                inputs[index] = graph_wiring_detail::adapt_source_for_input(w, collection, std::move(inputs[index]));
                schemas[index] = inputs[index].schema;
            }
            source.kind = MapArgSourceKind::Element;
            child_schemas[offset + index] = collection->element_ts();
            spec.multiplexed_inputs.push_back(index);
        }
        const auto explicit_count = inputs.size();
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
        for (std::size_t i = explicit_count; i < inputs.size(); ++i) {
            spec.args.push_back(MapArgSource{.kind = MapArgSourceKind::OuterInput, .outer_index = i});
        }
        return schemas;
    }

    template <typename Spec>
    WiringPortRef wire_compiled_map(Wiring &w, std::vector<WiringPortRef> inputs, std::span<const std::size_t> multiplexed,
                                    CompiledSubGraph child, MapKeyArg key, std::optional<WiringPortRef> keys,
                                    const TSValueTypeMetaData *schema, bool returns_value)
    {
        auto &registry = TypeRegistry::instance();
        const auto *key_ts = [&] {
            if constexpr (!std::is_same_v<Spec, TslMapNodeSpec>) { return registry.ts(schema->key_type()); }
            else { return ts_type<TS<Int>>(); }
        }();
        const auto *element = mapped_element_schema(child);
        if (returns_value != (element != nullptr)) {
            throw std::invalid_argument(returns_value ? "map_: child must produce an output" : "map_sink_: child must be a sink");
        }
        Spec spec;
        auto schemas = bind_map_arguments(w, spec, child, inputs, multiplexed, key, key_ts, schema);
        spec.child.graph_builder = std::move(child.graph_builder);
        spec.child.input_bindings = std::move(child.input_bindings);
        spec.child.output_binding = std::move(child.output_binding);
        const TSValueTypeMetaData *output = nullptr;
        if constexpr (!std::is_same_v<Spec, TslMapNodeSpec>) {
            spec.key_output_schema = std::is_same_v<Spec, MeshNodeSpec> || key == MapKeyArg::First ? key_ts : nullptr;
            if (!keys && multiplexed.size() > 1) {
                throw std::invalid_argument("map_: multiple dictionaries require an explicit lifecycle key set");
            }
            if (!keys) { keys = tsd_key_set_ref(inputs[multiplexed.front()]); }
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
                                std::is_same_v<Spec, MeshNodeSpec> ? typeid(CompiledMeshSite) : typeid(CompiledMapSite),
                                std::nullopt, std::move(keys));
    }

    WiringPortRef wire_collection(Wiring &w, std::vector<WiringPortRef> inputs, std::span<const std::size_t> multiplexed,
                                 CompiledSubGraph child, MapKeyArg key, std::optional<WiringPortRef> keys,
                                 bool returns_value)
    {
        const auto *schema = collection_schema(inputs, multiplexed);
        if (schema != nullptr && schema->kind == TSTypeKind::TSD) {
            return wire_compiled_map<MapNodeSpec>(w, std::move(inputs), multiplexed, std::move(child),
                                                  key, std::move(keys), schema, returns_value);
        }
        if (schema != nullptr && schema->is_unbounded_tsl()) {
            return wire_compiled_map<TslMapNodeSpec>(w, std::move(inputs), multiplexed, std::move(child),
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
    return wire_collection(w, collection_and_broadcasts(std::move(collection), broadcast), first_collection, std::move(child), key, std::move(keys), true);
}

void compiled::map_sink_(Wiring &w, WiringPortRef collection, std::span<const WiringPortRef> broadcast,
               CompiledSubGraph child, MapKeyArg key, std::optional<WiringPortRef> keys)
{
    wire_collection(w, collection_and_broadcasts(std::move(collection), broadcast), first_collection, std::move(child), key, std::move(keys), false);
}

WiringPortRef compiled::mesh_(Wiring &w, WiringPortRef collection, std::span<const WiringPortRef> broadcast,
                              CompiledSubGraph child, MapKeyArg key, std::optional<WiringPortRef> keys)
{
    auto inputs = collection_and_broadcasts(std::move(collection), broadcast);
    return mesh_(w, inputs, first_collection, std::move(child), key, std::move(keys));
}

WiringPortRef compiled::map_(Wiring &w, std::span<const WiringPortRef> inputs,
                             std::span<const std::size_t> multiplexed, CompiledSubGraph child,
                             MapKeyArg key, std::optional<WiringPortRef> keys)
{
    return wire_collection(w, {inputs.begin(), inputs.end()}, multiplexed, std::move(child), key, std::move(keys), true);
}

void compiled::map_sink_(Wiring &w, std::span<const WiringPortRef> inputs,
                         std::span<const std::size_t> multiplexed, CompiledSubGraph child,
                         MapKeyArg key, std::optional<WiringPortRef> keys)
{
    wire_collection(w, {inputs.begin(), inputs.end()}, multiplexed, std::move(child), key, std::move(keys), false);
}

WiringPortRef compiled::mesh_(Wiring &w, std::span<const WiringPortRef> inputs,
                              std::span<const std::size_t> multiplexed, CompiledSubGraph child,
                              MapKeyArg key, std::optional<WiringPortRef> keys)
{
    const auto *schema = collection_schema(inputs, multiplexed);
    if (!schema || schema->kind != TSTypeKind::TSD) { throw std::invalid_argument("mesh_: requires a dictionary"); }
    return wire_compiled_map<MeshNodeSpec>(w, {inputs.begin(), inputs.end()}, multiplexed, std::move(child),
                                           key, std::move(keys), schema, true);
}

WiringPortRef compiled::mesh_peer(Wiring &w, WiringPortRef key, const TSValueTypeMetaData *mesh_schema)
{
    auto &registry = TypeRegistry::instance();
    if (!mesh_schema || mesh_schema->kind != TSTypeKind::TSD ||
        registry.dereference(key.schema) != registry.ts(mesh_schema->key_type())) {
        throw std::invalid_argument("mesh_peer: key must match the requested mesh dictionary");
    }
    const auto *element = mesh_schema->element_ts();
    const auto *key_schema = registry.ts(mesh_schema->key_type());
    return subgraph_wiring_detail::wire_mesh_peer(w, key, WiringPortRef::null_source(element),
                                                 key_schema, element, typeid(CompiledMeshPeer));
}

namespace {
    using ReduceInputFields = std::vector<std::pair<std::string, const TSValueTypeMetaData *>>;

    void append_reduce_captures(ReduceInputFields &fields, std::span<const WiringPortRef> captures)
    {
        for (std::size_t i = 0; i < captures.size(); ++i) {
            fields.emplace_back("capture_" + std::to_string(i), captures[i].schema);
        }
    }

    std::vector<WiringInputRef> reduce_input_refs(std::span<const WiringPortRef> inputs,
                                                  std::span<const std::size_t> passive_captures,
                                                  std::size_t first_capture)
    {
        std::vector<std::size_t> materializers;
        materializers.reserve(passive_captures.size());
        for (const auto index : passive_captures) { materializers.push_back(first_capture + index); }
        return higher_order_input_refs(inputs, materializers);
    }

    SingleNestedGraphNodeSpec reduce_child(CompiledSubGraph child)
    {
        SingleNestedGraphNodeSpec spec;
        spec.graph_builder = std::move(child.graph_builder);
        spec.input_bindings = std::move(child.input_bindings);
        spec.output_binding = std::move(child.output_binding);
        return spec;
    }

    WiringPortRef ordered_fixed_list(Wiring &w, const WiringPortRef &collection, const TSValueTypeMetaData *schema,
                                     const WiringPortRef &zero, const CompiledSubGraph &child,
                                     std::span<const WiringPortRef> captures,
                                     std::span<const std::size_t> passive_captures)
    {
        const auto *element = schema->element_ts();
        if (!time_series_value_equivalent(element, zero.schema)) {
            throw std::invalid_argument("reduce_: an ordered fixed list requires a zero of its element type");
        }
        // The operator folds fixed positions, defaulting each invalid position to
        // zero. Its first position is the accumulator, unlike a dynamic fold's seed.
        auto leaf = [&](std::size_t i) {
            auto port = subgraph_wiring_detail::tsl_element_ref(collection, i, element);
            return stdlib::wire_default_sources(w, std::move(port), zero);
        };
        auto accumulator = leaf(0);
        ReduceInputFields fields{{"lhs", child.input_schemas[0]}, {"rhs", child.input_schemas[1]}};
        append_reduce_captures(fields, captures);
        const auto *input_schema = TypeRegistry::instance().un_named_tsb(fields);
        for (std::size_t i = 1; i < schema->fixed_size(); ++i) {
            std::vector<WiringPortRef> inputs{
                graph_wiring_detail::adapt_source_for_input(w, child.input_schemas[0], accumulator),
                graph_wiring_detail::adapt_source_for_input(w, child.input_schemas[1], leaf(i))};
            inputs.insert(inputs.end(), captures.begin(), captures.end());
            NodeTypeMetaData meta;
            meta.display_name = "reduce_ordered";
            meta.input_schema = input_schema;
            meta.output_schema = child.output_schema;
            if (!captures.empty()) { configure_passive_transport_inputs(meta, inputs); }
            auto builder = single_nested_graph_node(std::move(meta), reduce_child(child));
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(input_schema, inputs));
            if (captures.empty()) {
                accumulator = w.add_unique_node(typeid(CompiledReduceSite), std::move(builder), inputs, {});
            } else {
                const auto refs = reduce_input_refs(inputs, passive_captures, 2);
                accumulator = w.add_unique_node(typeid(CompiledReduceSite), std::move(builder), refs, {});
            }
        }
        return accumulator;
    }
}

WiringPortRef compiled::reduce_(Wiring &w, WiringPortRef collection, CompiledSubGraph child,
                                std::optional<WiringPortRef> zero, ReduceOrder order)
{
    auto &registry = TypeRegistry::instance();
    const auto *schema = registry.dereference(collection.schema);
    if (!schema || (schema->kind != TSTypeKind::TSD && schema->kind != TSTypeKind::TSL)) {
        throw std::invalid_argument("reduce_: requires a dictionary or list");
    }
    collection = graph_wiring_detail::adapt_source_for_input(w, schema, std::move(collection));
    if (!child.external_service_inputs.empty()) {
        throw std::invalid_argument("reduce_: the combiner must have only its two explicit inputs");
    }
    const bool ordered = order == ReduceOrder::Ordered;
    if (ordered && !zero) { throw std::invalid_argument("reduce_: an ordered fold requires zero"); }
    if (ordered && schema->kind == TSTypeKind::TSD && schema->key_type() != scalar_descriptor<Int>::value_meta()) {
        throw std::invalid_argument("reduce_: an ordered dictionary requires contiguous integer keys");
    }
    const auto *element = schema->element_ts();
    if (zero) {
        const auto *zero_type = registry.dereference(zero->schema);
        if (!ordered && !time_series_value_equivalent(element, zero_type)) {
            throw std::invalid_argument("reduce_: zero must match the collection element");
        }
        *zero = graph_wiring_detail::adapt_source_for_input(w, zero_type, std::move(*zero));
        if (zero->is_structural_source()) {
            *zero = wire<stdlib::pass_through_node>(w, Port<void>{w, std::move(*zero)}).erased();
        }
    }
    const auto *result = ordered ? zero->schema : element;
    check_child_inputs(child, std::array{result, element});
    if (!child.output_schema || !time_series_value_equivalent(child.output_schema, result)) {
        throw std::invalid_argument("reduce_: the combiner output must match the accumulator");
    }
    auto captures = std::move(child.captured_inputs);
    std::vector<const TSValueTypeMetaData *> capture_schemas(child.input_schemas.begin() + 2,
                                                           child.input_schemas.end());
    const auto passive_captures = materialize_structural_broadcast_inputs(w, captures, capture_schemas, {});
    if (ordered && schema->kind == TSTypeKind::TSL && !schema->is_unbounded_tsl()) {
        return ordered_fixed_list(w, collection, schema, *zero, child, captures, passive_captures);
    }
    ReduceInputFields fields{{"ts", schema}};
    std::vector<WiringPortRef> inputs{std::move(collection)};
    if (zero) {
        fields.emplace_back("zero", zero->schema);
        inputs.push_back(std::move(*zero));
    }
    const auto first_capture = inputs.size();
    append_reduce_captures(fields, captures);
    inputs.insert(inputs.end(), std::make_move_iterator(captures.begin()), std::make_move_iterator(captures.end()));
    NodeTypeMetaData meta;
    meta.display_name = ordered ? "reduce_ordered" : "reduce";
    meta.input_schema = registry.un_named_tsb(fields);
    meta.output_schema = result;
    const auto *input_schema = meta.input_schema;
    if (inputs.size() != first_capture) { configure_passive_transport_inputs(meta, inputs); }
    NodeBuilder builder = ordered
        ? ordered_reduce_node(std::move(meta), OrderedReduceNodeSpec{.child = reduce_child(std::move(child))})
        : reduce_node(std::move(meta), ReduceNodeSpec{.child = reduce_child(std::move(child)), .has_zero = zero.has_value()});
    builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(input_schema, inputs));
    if (inputs.size() == first_capture) {
        return w.add_unique_node(typeid(CompiledReduceSite), std::move(builder), inputs, {});
    }
    const auto refs = reduce_input_refs(inputs, passive_captures, first_capture);
    return w.add_unique_node(typeid(CompiledReduceSite), std::move(builder), refs, {});
}
}
