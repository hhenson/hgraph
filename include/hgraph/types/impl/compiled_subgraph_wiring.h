#ifndef HGRAPH_TYPES_IMPL_COMPILED_SUBGRAPH_WIRING_H
#define HGRAPH_TYPES_IMPL_COMPILED_SUBGRAPH_WIRING_H

#include <hgraph/runtime/map_node.h>
#include <hgraph/runtime/mesh_node.h>
#include <hgraph/runtime/switch_node.h>
#include <hgraph/runtime/tsl_map_node.h>
#include <hgraph/types/subgraph_wiring.h>

#include <fmt/format.h>

#include <algorithm>

namespace hgraph::subgraph_wiring_detail
{
    // Both wiring paths bind the subscriber's second input to the selected sibling at runtime.
    [[nodiscard]] inline WiringPortRef wire_mesh_peer(Wiring &w, const WiringPortRef &key,
                                                      const WiringPortRef &value_placeholder,
                                                      const TSValueTypeMetaData *key_schema,
                                                      const TSValueTypeMetaData *out_schema,
                                                      std::type_index tag)
    {
        WiringPortRef item = graph_wiring_detail::adapt_source_for_input(w, key_schema, key);

        std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields{
            {"item", item.schema}, {"value", out_schema}};
        const auto *input_schema = TypeRegistry::instance().un_named_tsb(fields);

        WiringNodeSchema node_schema;
        node_schema.input  = input_schema;
        node_schema.output = out_schema;

        std::array<WiringPortRef, 2> inputs{std::move(item), value_placeholder};
        return w.add_node(
            tag, node_schema,
            std::span<const WiringPortRef>{inputs.data(), inputs.size()}, Value{},
            [&]() {
                NodeTypeMetaData meta;
                meta.display_name  = "mesh_subscribe";
                meta.input_schema  = input_schema;
                meta.output_schema = out_schema;
                NodeBuilder builder = mesh_subscribe_node(std::move(meta));
                builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                    input_schema, std::span<const WiringPortRef>{inputs.data(), inputs.size()}));
                return builder;
            });
    }

    inline void append_external_service_inputs(
        Wiring &w,
        std::vector<NestedServiceInput> external_inputs,
        std::vector<const TSValueTypeMetaData *> &schemas,
        std::vector<std::uint8_t> &arg_tags,
        std::vector<WiringPortRef> &ports)
    {
        for (NestedServiceInput &input : external_inputs)
        {
            WiringPortRef source = subgraph_wiring_detail::materialize_external_service_input(
                w, std::move(input));
            schemas.push_back(source.schema);
            arg_tags.push_back(static_cast<std::uint8_t>(source.arg_tag));
            ports.push_back(std::move(source));
        }
    }

    struct ExternalServiceSlot
    {
        NestedServiceInput input{};
        std::size_t        slot{0};
    };

    [[nodiscard]] inline std::size_t external_service_slot(
        NestedServiceInput input,
        std::vector<ExternalServiceSlot> &external_services,
        std::vector<WiringPortRef> &slots)
    {
        const auto existing = std::ranges::find_if(
            external_services, [&](const ExternalServiceSlot &candidate) {
                return candidate.input.service_path == input.service_path &&
                       candidate.input.service_kind == input.service_kind &&
                       candidate.input.definition == input.definition &&
                       candidate.input.source_schema == input.source_schema &&
                       candidate.input.arg_tag == input.arg_tag &&
                       candidate.input.builder.label() == input.builder.label();
            });
        if (existing != external_services.end()) { return existing->slot; }

        const std::size_t slot = slots.size();
        slots.push_back(WiringPortRef::null_source(input.source_schema));
        external_services.push_back(ExternalServiceSlot{std::move(input), slot});
        return slot;
    }

    inline void materialize_external_service_slots(
        Wiring &w, std::vector<ExternalServiceSlot> external_services,
        std::vector<WiringPortRef> &slots)
    {
        for (ExternalServiceSlot &external : external_services)
        {
            slots[external.slot] = subgraph_wiring_detail::materialize_external_service_input(
                w, std::move(external.input));
        }
    }

    inline void configure_passive_transport_inputs(
        NodeTypeMetaData &meta,
        std::span<const WiringPortRef> inputs)
    {
        std::vector<std::size_t> active;
        active.reserve(inputs.size());
        bool has_passive = false;
        for (std::size_t index = 0; index < inputs.size(); ++index)
        {
            if (inputs[index].arg_tag == WiringPortRef::ArgTag::Passive)
            {
                has_passive = true;
            }
            else
            {
                active.push_back(index);
            }
        }
        if (has_passive) { meta.active_inputs = std::move(active); }
    }

    [[nodiscard]] inline std::vector<WiringInputRef> higher_order_input_refs(
        std::span<const WiringPortRef> inputs,
        std::span<const std::size_t> passive_materializers = {})
    {
        std::vector<WiringInputRef> refs;
        refs.reserve(inputs.size());
        for (std::size_t index = 0; index < inputs.size(); ++index)
        {
            refs.push_back(WiringInputRef{
                .source = inputs[index],
                // Passive transports normally omit their producer edge so
                // higher-order feedback can close a cycle. A synthetic
                // materializer is the exception: the consumer needs its
                // initial REF publication before the first child sample.
                .rank_dependency =
                    inputs[index].arg_tag != WiringPortRef::ArgTag::Passive ||
                    std::find(passive_materializers.begin(), passive_materializers.end(), index) !=
                        passive_materializers.end(),
            });
        }
        return refs;
    }

    /** Nested map-like runtimes rebind broadcast arguments from one root
        output handle. A fixed TSB/TSL assembled from child ports has no
        such root, so materialize its structural REF once in the parent.
        The multiplexed inputs retain their collection storage and every
        map/mesh child shares the live broadcast reference. */
    [[nodiscard]] inline std::vector<std::size_t> materialize_structural_broadcast_inputs(
        Wiring &w,
        std::vector<WiringPortRef> &inputs,
        std::vector<const TSValueTypeMetaData *> &schemas,
        std::span<const std::size_t> multiplexed_inputs)
    {
        auto &registry = TypeRegistry::instance();
        std::vector<std::size_t> passive_materializers;
        for (std::size_t index = 0; index < inputs.size(); ++index)
        {
            if (std::find(multiplexed_inputs.begin(), multiplexed_inputs.end(), index) !=
                    multiplexed_inputs.end() ||
                !inputs[index].is_structural_source())
            {
                continue;
            }

            const auto tag = inputs[index].arg_tag;
            inputs[index].arg_tag = WiringPortRef::ArgTag::None;
            inputs[index] = graph_wiring_detail::adapt_source_for_input(
                w, registry.ref(inputs[index].schema),
                std::move(inputs[index]));
            inputs[index].arg_tag = tag;
            schemas[index] = inputs[index].schema;
            if (tag == WiringPortRef::ArgTag::Passive)
            {
                passive_materializers.push_back(index);
            }
        }
        return passive_materializers;
    }

    [[nodiscard]] inline const TSValueTypeMetaData *switch_branch_output_child_schema(
        const TSValueTypeMetaData &schema,
        std::size_t index)
    {
        switch (schema.kind)
        {
            case TSTypeKind::TSB:
                if (index >= schema.field_count())
                {
                    throw std::out_of_range("switch_: branch output source path is out of range for TSB");
                }
                return schema.fields()[index].type;

            case TSTypeKind::TSL:
                if (schema.is_unbounded_tsl())
                {
                    throw std::invalid_argument(
                        "switch_: branch output source path requires fixed-size TSL prefixes");
                }
                if (index >= schema.fixed_size())
                {
                    throw std::out_of_range("switch_: branch output source path is out of range for TSL");
                }
                return schema.element_ts();

            default:
                throw std::invalid_argument(
                    "switch_: branch output source path can only traverse TSB or fixed-size TSL");
        }
    }

    [[nodiscard]] inline std::size_t switch_branch_output_child_count(
        const TSValueTypeMetaData &schema)
    {
        switch (schema.kind)
        {
            case TSTypeKind::TSB:
                return schema.field_count();

            case TSTypeKind::TSL:
                if (schema.is_unbounded_tsl())
                {
                    throw std::invalid_argument(
                        "switch_: branch output source path requires fixed-size TSL prefixes");
                }
                return schema.fixed_size();

            default:
                throw std::invalid_argument(
                    "switch_: branch output source path can only traverse TSB or fixed-size TSL");
        }
    }

    [[nodiscard]] inline TSEndpointSchema switch_branch_output_endpoint_schema_for(
        const TSValueTypeMetaData *schema,
        const std::vector<std::size_t> &source_path,
        bool peered_terminal,
        std::size_t depth = 0)
    {
        if (schema == nullptr)
        {
            throw std::invalid_argument("switch_: branch output binding requires an output schema");
        }
        if (depth == source_path.size())
        {
            return peered_terminal ? TSEndpointSchema::peered(schema) : TSEndpointSchema::local(schema);
        }

        const auto selected = source_path[depth];
        const auto count    = switch_branch_output_child_count(*schema);
        if (selected >= count)
        {
            throw std::out_of_range("switch_: branch output source path is out of range");
        }

        std::vector<TSEndpointSchema> children;
        children.reserve(count);
        for (std::size_t index = 0; index < count; ++index)
        {
            const auto *child_schema = switch_branch_output_child_schema(*schema, index);
            children.push_back(index == selected
                                   ? switch_branch_output_endpoint_schema_for(
                                         child_schema, source_path, peered_terminal, depth + 1)
                                   : TSEndpointSchema::local(child_schema));
        }
        return TSEndpointSchema::non_peered(schema, std::move(children));
    }

    [[nodiscard]] inline const TSValueTypeMetaData *switch_branch_output_schema_at(
        const TSValueTypeMetaData *schema,
        const std::vector<std::size_t> &source_path)
    {
        if (schema == nullptr)
        {
            throw std::invalid_argument("switch_: branch output binding requires an output schema");
        }
        for (const std::size_t index : source_path)
        {
            schema = switch_branch_output_child_schema(*schema, index);
        }
        return schema;
    }

    /** The one place a switch's output mode is decided (nested_graphs.rst,
        "switch_ output modes"): a REF output copies the selected terminal's
        token; a value output forwards to the terminal when any branch needs
        its terminal preserved, and is otherwise owned by the switch. */
    [[nodiscard]] inline SwitchOutputMode switch_output_mode_for(
        const TSValueTypeMetaData *output_schema, bool any_branch_preserves_terminal)
    {
        if (output_schema != nullptr && output_schema->kind == TSTypeKind::REF)
        {
            return SwitchOutputMode::RefCopy;
        }
        return any_branch_preserves_terminal ? SwitchOutputMode::Forwarding
                                             : SwitchOutputMode::Local;
    }

    /** True when a branch's whole-node terminal already owns forwarding
        or structural endpoint topology (a nested map_ or switch_ result,
        say): re-homing that endpoint onto switch-owned storage would
        strand the tree it forwards through. */
    [[nodiscard]] inline bool switch_terminal_owns_endpoint_topology(const SingleNestedGraphNodeSpec &spec)
    {
        const NodeBuilder &terminal = spec.graph_builder.nodes().at(spec.output_binding->source.node);
        const TSEndpointSchema &terminal_override = terminal.output_endpoint();
        const NodeTypeMetaData *terminal_meta = terminal.type().schema();
        const TSEndpointSchema &terminal_declared =
            terminal_meta != nullptr ? terminal_meta->output_endpoint_schema : terminal_override;
        const TSEndpointSchema &terminal_endpoint =
            !terminal_override.empty() ? terminal_override : terminal_declared;
        return !terminal_endpoint.empty() &&
               (terminal_endpoint.is_peered() || terminal_endpoint.is_non_peered());
    }

    [[nodiscard]] inline bool switch_branch_requires_preserved_terminal(
        const SingleNestedGraphNodeSpec &spec,
        const TSValueTypeMetaData *switch_output_schema)
    {
        if (!spec.output_binding.has_value() ||
            spec.output_binding->kind != NestedGraphOutputBinding::Kind::ChildOutput)
        {
            throw std::logic_error("switch_: every branch must terminate at a child output");
        }

        const NestedGraphEndpoint &source = spec.output_binding->source;
        if (!source.path.empty()) { return true; }

        const NodeTypeMetaData *terminal_meta =
            spec.graph_builder.nodes().at(source.node).type().schema();
        const auto *terminal_schema =
            terminal_meta != nullptr ? terminal_meta->output_schema : nullptr;
        const auto *branch_output_schema =
            switch_branch_output_schema_at(terminal_schema, source.path);
        if (switch_output_schema != nullptr &&
            !time_series_schema_equivalent(
                branch_output_schema, switch_output_schema) &&
            time_series_value_equivalent(branch_output_schema, switch_output_schema))
        {
            return true;
        }
        return switch_terminal_owns_endpoint_topology(spec);
    }

    /** The schema a compiled branch publishes through its switch
        (nested_graphs.rst, "switch_ output modes"). A branch that passes
        through time-series it does not compute publishes their reference,
        REF<logical>: a boundary input returned unchanged, or a composed
        structure whose structural-REF adapter references its fields. Any
        such branch makes the switch take the REF shape, so the switch
        copies the reference token rather than a value. This is the
        hgraph switch_ contract (its output is always
        ``as_reference(output)``): a field passed straight through keeps
        the identity of its upstream output, and a consumer re-pointed
        between the switch and that upstream sees the same reference.
        An operator-authored REF terminal under a value declaration keeps
        the value output that forwards to it. */
    [[nodiscard]] inline const TSValueTypeMetaData *switch_branch_published_schema(
        const CompiledSubGraph &compiled)
    {
        const auto *logical = compiled.output_schema;
        if (logical == nullptr) { return logical; }
        const bool passes_input_through =
            compiled.output_binding.has_value() &&
            compiled.output_binding->kind == NestedGraphOutputBinding::Kind::ParentInput;
        return passes_input_through || compiled.terminal_is_structural_adapter
                   ? TypeRegistry::instance().ref(logical)
                   : logical;
    }

    /** A branch that returns a boundary input unchanged (``ParentInput``)
        has no child terminal of its own. Give it the structural-REF node
        over that input: it publishes a reference to whatever the input is
        bound to (the upstream output, a composite over the upstream
        fields, or the upstream token when the input is itself a REF). A
        value-copying terminal would give the passed-through series a new
        identity. ``source_path`` is already over the outer switch input
        root, where ``{0}`` is the key. */
    inline void materialize_switch_pass_through_terminal(
        SingleNestedGraphNodeSpec &spec, const char *operation_name,
        const TSValueTypeMetaData *output_schema, const WiringPortRef *key_source,
        std::span<const WiringPortRef> slot_sources)
    {
        const auto &source_path = spec.output_binding->parent_source_path;
        const WiringPortRef *terminal_source = nullptr;
        if (source_path[0] == 0)
        {
            if (key_source == nullptr)
            {
                throw std::logic_error(std::string{operation_name} +
                                       ": direct branch output source is the key input");
            }
            terminal_source = key_source;
        }
        else
        {
            const std::size_t slot = source_path[0] - 1;
            if (slot >= slot_sources.size())
            {
                throw std::logic_error(std::string{operation_name} +
                                       ": direct branch output source is outside the input slots");
            }
            terminal_source = &slot_sources[slot];
        }
        std::size_t path_index = 1;
        for (; path_index < source_path.size() && terminal_source->is_structural_source(); ++path_index)
        {
            const auto &children = terminal_source->structural_children();
            if (source_path[path_index] >= children.size())
            {
                throw std::logic_error(std::string{operation_name} +
                                       ": direct branch output path is outside a structural source");
            }
            terminal_source = &children[source_path[path_index]];
        }

        // Below a peered port the rest of the path is one bound field,
        // described by the branch's own result schema.
        WiringPortRef source = *terminal_source;
        if (path_index != source_path.size()) { source.schema = output_schema; }

        const std::size_t terminal_index = spec.graph_builder.node_count();
        spec.graph_builder.add_node(
            graph_wiring_detail::reference_terminal_builder(output_schema, source));
        spec.input_bindings.push_back(NestedGraphInputBinding{
            .source_path = source_path,
            .target = NestedGraphEndpoint{.node = terminal_index, .path = {0}},
        });
        spec.output_binding = NestedGraphOutputBinding{
            .source = NestedGraphEndpoint{.node = terminal_index},
        };
    }

    inline void configure_switch_branch_output(SingleNestedGraphNodeSpec &spec,
                                                const TSValueTypeMetaData *switch_output_schema,
                                                SwitchOutputMode output_mode)
    {
        if (!spec.output_binding.has_value() ||
            spec.output_binding->kind != NestedGraphOutputBinding::Kind::ChildOutput)
        {
            throw std::logic_error("switch_: every branch must terminate at a child output");
        }

        const NestedGraphEndpoint &source = spec.output_binding->source;
        NodeBuilder &terminal = spec.graph_builder.node_at(source.node);
        const NodeTypeMetaData *terminal_meta = terminal.type().schema();
        const auto *terminal_schema = terminal_meta != nullptr ? terminal_meta->output_schema : nullptr;
        const auto *branch_output_schema = switch_branch_output_schema_at(terminal_schema, source.path);
        spec.output_terminal_is_reference =
            branch_output_schema != nullptr && branch_output_schema->kind == TSTypeKind::REF;

        const bool preserve_terminal =
            output_mode == SwitchOutputMode::Forwarding ||
            (output_mode == SwitchOutputMode::RefCopy &&
             switch_branch_requires_preserved_terminal(spec, switch_output_schema));
        if (preserve_terminal) { return; }

        // A REF terminal owns the selected reference token. Re-homing it
        // into another REF output would turn its local mutation into a
        // target-link write and add an unnecessary reference endpoint
        // layer. The enclosing switch copies the token instead.
        if (switch_output_schema != nullptr &&
            switch_output_schema->kind == TSTypeKind::REF &&
            branch_output_schema->kind == TSTypeKind::REF)
        {
            return;
        }

        // A VALUE terminal that already owns forwarding or structural
        // endpoint topology (a map_ result, say) keeps it under a
        // REF-shaped switch: the switch publishes a reference to that
        // endpoint, and re-homing it would strand the forwarding tree.
        if (switch_output_schema != nullptr &&
            switch_output_schema->kind == TSTypeKind::REF &&
            source.path.empty() && switch_terminal_owns_endpoint_topology(spec))
        {
            return;
        }

        // When a VALUE branch participates in a REF-shaped switch, its
        // terminal must own the value inside the branch's A/B graph slot.
        // The switch publishes a reference to that terminal. All other
        // branches write directly into the fixed switch output.
        const bool peered_terminal =
            switch_output_schema == nullptr || switch_output_schema->kind != TSTypeKind::REF ||
            branch_output_schema->kind == TSTypeKind::REF;
        terminal.output_endpoint(
            switch_branch_output_endpoint_schema_for(terminal_schema, source.path, peered_terminal));
    }

    [[nodiscard]] inline SingleNestedGraphNodeSpec bind_switch_branch(
        CompiledSubGraph compiled, bool takes_key, std::span<const std::size_t> ordered_slots,
        const WiringPortRef &key_source, std::vector<WiringPortRef> &slot_sources,
        const TSValueTypeMetaData *&output_schema, std::optional<bool> &branches_have_output,
        std::vector<ExternalServiceSlot> &external_services)
    {
        const std::size_t declared_arity = ordered_slots.size() + (takes_key ? 1 : 0);
        std::vector<std::size_t> captured_slots;
        captured_slots.reserve(compiled.captured_inputs.size());
        for (const WiringPortRef &captured : compiled.captured_inputs)
        {
            const auto existing = std::find_if(
                slot_sources.begin(), slot_sources.end(),
                [&](const WiringPortRef &source) { return source.same_source_as(captured); });
            if (existing != slot_sources.end())
            {
                captured_slots.push_back(static_cast<std::size_t>(existing - slot_sources.begin()));
            }
            else
            {
                captured_slots.push_back(slot_sources.size());
                slot_sources.push_back(captured);
            }
        }
        std::vector<std::size_t> external_slots;
        external_slots.reserve(compiled.external_service_inputs.size());
        for (NestedServiceInput &external : compiled.external_service_inputs)
        {
            external_slots.push_back(external_service_slot(
                std::move(external), external_services, slot_sources));
        }

        const bool branch_has_output = compiled.output_schema != nullptr;
        const auto *branch_output_schema = switch_branch_published_schema(compiled);
        if (!branches_have_output.has_value()) { branches_have_output = branch_has_output; }
        else if (*branches_have_output != branch_has_output)
        {
            throw std::invalid_argument(
                "switch_: branches must either all produce an output or all be sinks");
        }
        if (branch_has_output && output_schema == nullptr) { output_schema = branch_output_schema; }
        else if (branch_has_output &&
                 !time_series_schema_equivalent(output_schema, branch_output_schema))
        {
            // Branches may differ only in REF-ness (one produces the
            // value, another the reference - hgraph parity): the switch
            // output takes the REFERENCE shape; value branches adapt at
            // the boundary binding.
            if (time_series_value_equivalent(output_schema, branch_output_schema))
            {
                if (branch_output_schema->kind == TSTypeKind::REF) { output_schema = branch_output_schema; }
            }
            else
            {
                throw std::invalid_argument("switch_: all branches must produce the same output schema");
            }
        }

        SingleNestedGraphNodeSpec spec;
        spec.graph_builder  = std::move(compiled.graph_builder);
        spec.input_bindings = std::move(compiled.input_bindings);
        spec.output_binding = compiled.output_binding;
        // Re-target boundary ordinals onto the outer input root: ordinal 0
        // is the key when the branch consumes it; every other parameter
        // maps through its resolved outer slot (+1 past the key input).
        const std::size_t offset = takes_key ? 1 : 0;
        const auto retarget_boundary_path = [&](std::vector<std::size_t> &path) {
            if (path.empty())
            {
                throw std::logic_error("switch_: boundary binding path is empty");
            }
            const std::size_t ordinal = path[0];
            if (ordinal >= declared_arity)
            {
                const std::size_t appended_index = ordinal - declared_arity;
                if (appended_index < captured_slots.size())
                {
                    path[0] = 1 + captured_slots[appended_index];
                    return;
                }
                const std::size_t external_index = appended_index - captured_slots.size();
                if (external_index >= external_slots.size())
                {
                    throw std::logic_error("switch_: appended boundary ordinal is out of range");
                }
                path[0] = 1 + external_slots[external_index];
                return;
            }
            if (takes_key && ordinal == 0)
            {
                path[0] = 0;
                return;
            }
            if (ordinal < offset || ordinal - offset >= ordered_slots.size())
            {
                throw std::logic_error("switch_: boundary binding ordinal is not mapped to an outer input");
            }
            path[0] = 1 + ordered_slots[ordinal - offset];
        };
        for (NestedGraphInputBinding &binding : spec.input_bindings)
        {
            retarget_boundary_path(binding.source_path);
        }
        if (spec.output_binding.has_value() &&
            spec.output_binding->kind == NestedGraphOutputBinding::Kind::ParentInput)
        {
            retarget_boundary_path(spec.output_binding->parent_source_path);
            materialize_switch_pass_through_terminal(
                spec, "switch_", compiled.output_schema, &key_source,
                {slot_sources.data(), slot_sources.size()});
        }
        return spec;
    }

    /** Add the already-compiled branch set through the one switch runtime. */
    [[nodiscard]] inline WiringPortRef add_compiled_switch(
        Wiring &w, WiringPortRef key, std::vector<WiringPortRef> ts,
        SwitchNodeSpec spec, const TSValueTypeMetaData *output_schema,
        std::optional<Value> config, std::type_index definition, const char *display_name)
    {
        const auto *key_schema = TypeRegistry::instance().dereference(key.schema);
        std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
        fields.reserve(1 + ts.size());
        fields.emplace_back("key", key_schema);
        for (std::size_t i = 0; i < ts.size(); ++i)
        {
            fields.emplace_back(std::to_string(i), ts[i].schema);
        }
        const auto *input_schema = TypeRegistry::instance().un_named_tsb(fields);

        std::vector<WiringPortRef> inputs;
        inputs.reserve(1 + ts.size());
        inputs.push_back(std::move(key));
        for (WiringPortRef &port : ts) { inputs.push_back(std::move(port)); }

        WiringNodeSchema node_schema;
        node_schema.input  = input_schema;
        node_schema.output = output_schema;

        auto make_builder = [&] {
            NodeTypeMetaData meta;
            meta.display_name  = display_name;
            meta.input_schema  = input_schema;
            meta.output_schema = output_schema;

            NodeBuilder builder = switch_node(std::move(meta), std::move(spec));
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
                input_schema, std::span<const WiringPortRef>{inputs.data(), inputs.size()}));
            return builder;
        };
        if (config) {
            return w.add_node(definition, node_schema, std::span<const WiringPortRef>{inputs},
                              std::move(*config), make_builder);
        }
        auto builder = make_builder();
        Value runtime_scalars = builder.scalars();
        return w.add_unique_node(definition, std::move(builder), std::span<const WiringPortRef>{inputs},
                                 std::move(runtime_scalars));
    }

    inline void configure_switch_outputs(
        SwitchNodeSpec &spec, const TSValueTypeMetaData *output_schema,
        std::optional<bool> branches_have_output, bool output_required)
    {
        if (!branches_have_output.has_value() || *branches_have_output != output_required)
        {
            throw std::invalid_argument(output_required
                                            ? "switch_: every branch must produce an output"
                                            : "switch_sink_: every branch must be a sink");
        }
        if (output_required)
        {
            const auto requires_preserved_terminal = [output_schema](const SingleNestedGraphNodeSpec &branch) {
                return switch_branch_requires_preserved_terminal(branch, output_schema);
            };
            spec.output_mode = switch_output_mode_for(
                output_schema,
                std::any_of(spec.branches.begin(), spec.branches.end(),
                            [&](const SwitchBranch &branch) {
                                return requires_preserved_terminal(branch.spec);
                            }) ||
                    (spec.default_branch.has_value() &&
                     requires_preserved_terminal(*spec.default_branch)));
            for (SwitchBranch &branch : spec.branches)
            {
                configure_switch_branch_output(
                    branch.spec, output_schema, spec.output_mode);
            }
            if (spec.default_branch.has_value())
            {
                configure_switch_branch_output(
                    *spec.default_branch, output_schema, spec.output_mode);
            }
        }
    }

    /** Schema a keyed/dynamic-list container must allocate for one child.
        A synthetic structural adapter remains an implementation detail,
        while an operator-authored REF terminal is the endpoint that the
        container actually forwards to and must therefore be retained. */
    [[nodiscard]] inline const TSValueTypeMetaData *mapped_element_schema(
        const CompiledSubGraph &compiled)
    {
        const auto *logical = compiled.output_schema;
        const auto *terminal = compiled.terminal_output_schema;
        if (logical == nullptr || terminal == nullptr ||
            compiled.terminal_is_structural_adapter ||
            terminal->kind != TSTypeKind::REF)
        {
            return logical;
        }

        return time_series_value_equivalent(logical, terminal) ? terminal : logical;
    }

    /** Configure a whole child terminal against one public container
        element. A terminal with endpoint topology of its own, including
        the synthetic REF used for composed fixed structures, stays owned
        by the child and the element forwards to it. Only an ordinary
        local terminal with the exact public schema is re-homed so it can
        write directly into the container element. */
    [[nodiscard]] inline MapOutputBindingMode configure_mapped_child_terminal(
        NodeBuilder &terminal,
        const TSValueTypeMetaData *output_schema,
        const TSValueTypeMetaData *terminal_output_schema,
        std::string_view operation_name)
    {
        const NodeTypeMetaData *terminal_meta = terminal.type().schema();
        const auto *declared_terminal_schema =
            terminal_meta != nullptr ? terminal_meta->output_schema : nullptr;
        if (output_schema == nullptr || terminal_output_schema == nullptr ||
            declared_terminal_schema == nullptr ||
            !time_series_schema_equivalent(
                declared_terminal_schema, terminal_output_schema))
        {
            throw std::logic_error(fmt::format(
                "{}: compiled child terminal schema is inconsistent",
                operation_name));
        }

        const bool exact_match = time_series_schema_equivalent(
            output_schema, terminal_output_schema);
        if (!exact_match &&
            !time_series_value_equivalent(output_schema, terminal_output_schema))
        {
            throw std::invalid_argument(fmt::format(
                "{}: child terminal schema is incompatible with its public output",
                operation_name));
        }

        const TSEndpointSchema &terminal_override = terminal.output_endpoint();
        const TSEndpointSchema &terminal_declared =
            terminal_meta != nullptr
                ? terminal_meta->output_endpoint_schema
                : terminal_override;
        const TSEndpointSchema &terminal_endpoint =
            !terminal_override.empty() ? terminal_override : terminal_declared;
        if (!exact_match || terminal_output_schema->kind == TSTypeKind::REF ||
            (!terminal_endpoint.empty() &&
             (terminal_endpoint.is_peered() || terminal_endpoint.is_non_peered())))
        {
            return MapOutputBindingMode::OutputElementForwardsToChildTerminal;
        }

        terminal.output_endpoint(TSEndpointSchema::peered(output_schema));
        return MapOutputBindingMode::ChildTerminalWritesElement;
    }

    template <typename Spec>
    inline void bind_map_terminal(Spec &spec, const CompiledSubGraph &compiled,
                                  const TSValueTypeMetaData *element, std::string_view operation_name)
    {
        const auto &binding = *spec.child.output_binding;
        if (binding.kind == NestedGraphOutputBinding::Kind::ParentInput) {
            spec.output_binding_mode = MapOutputBindingMode::OutputElementForwardsToParentSource;
        } else if (!binding.source.path.empty()) {
            // A projected terminal keeps its owner and exposes the selected field or element.
            spec.output_binding_mode = MapOutputBindingMode::OutputElementForwardsToChildTerminal;
        } else {
            auto &terminal = spec.child.graph_builder.node_at(binding.source.node);
            spec.output_binding_mode = configure_mapped_child_terminal(
                terminal, element, compiled.terminal_output_schema, operation_name);
        }
    }

    inline void bind_map_terminal(TslMapNodeSpec &spec, const CompiledSubGraph &compiled,
                                  const TSValueTypeMetaData *element, std::string_view operation_name)
    {
        const auto &binding = *spec.child.output_binding;
        if (binding.kind == NestedGraphOutputBinding::Kind::ParentInput) {
            throw std::invalid_argument("map_: dynamic TSL child pass-through outputs are not supported");
        }
        if (!binding.source.path.empty()) {
            throw std::invalid_argument("map_: dynamic TSL function output must be a whole node output");
        }
        if (binding.source.node >= spec.child.graph_builder.node_count()) {
            throw std::invalid_argument("map_: dynamic TSL function output node is out of range");
        }

        NodeBuilder &terminal =
            spec.child.graph_builder.node_at(binding.source.node);
        spec.output_binding_mode = configure_mapped_child_terminal(
            terminal, element,
            compiled.terminal_output_schema, operation_name);
    }

    template <typename Spec>
    [[nodiscard]] WiringPortRef add_compiled_map(
        Wiring &w, std::vector<WiringPortRef> inputs, std::vector<const TSValueTypeMetaData *> schemas,
        Spec spec, const TSValueTypeMetaData *output_schema, std::type_index definition,
        std::optional<Value> config, std::optional<WiringPortRef> keys = {})
    {
        auto &registry = TypeRegistry::instance();
        const auto passive_materializers =
            materialize_structural_broadcast_inputs(w, inputs, schemas, spec.multiplexed_inputs);
        std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
        fields.reserve(inputs.size() + keys.has_value());
        if constexpr (!std::is_same_v<Spec, TslMapNodeSpec>) {
            for (const auto slot : spec.multiplexed_inputs) { schemas[slot] = registry.dereference(schemas[slot]); }
        }
        for (std::size_t i = 0; i < schemas.size(); ++i) { fields.emplace_back(std::to_string(i), schemas[i]); }
        if constexpr (!std::is_same_v<Spec, TslMapNodeSpec>) {
            spec.keys_input_index = inputs.size();
            fields.emplace_back("__keys__", keys.value().schema);
            inputs.push_back(std::move(*keys));
        }
        const auto *input_schema = registry.un_named_tsb(fields);
        const auto refs = higher_order_input_refs(inputs, passive_materializers);
        auto make_builder = [&] {
            NodeTypeMetaData meta;
            meta.display_name = std::is_same_v<Spec, MeshNodeSpec> ? "mesh_" : "map_";
            meta.input_schema = input_schema;
            meta.output_schema = output_schema;
            configure_passive_transport_inputs(meta, inputs);
            auto builder = [&] {
                if constexpr (std::is_same_v<Spec, MeshNodeSpec>) {
                    return mesh_node(std::move(meta), std::move(spec));
                } else if constexpr (std::is_same_v<Spec, MapNodeSpec>) {
                    return map_node(std::move(meta), std::move(spec));
                } else {
                    return tsl_map_node(std::move(meta), std::move(spec));
                }
            }();
            builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(input_schema, inputs));
            return builder;
        };
        if (config) {
            return w.add_node(definition, WiringNodeSchema{.input = input_schema, .output = output_schema}, refs,
                              std::move(*config), make_builder);
        }
        auto builder = make_builder();
        Value runtime_scalars = builder.scalars();
        return w.add_unique_node(definition, std::move(builder), refs, std::move(runtime_scalars));
    }

}

#endif  // HGRAPH_TYPES_IMPL_COMPILED_SUBGRAPH_WIRING_H
