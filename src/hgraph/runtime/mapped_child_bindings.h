#ifndef HGRAPH_RUNTIME_MAPPED_CHILD_BINDINGS_H
#define HGRAPH_RUNTIME_MAPPED_CHILD_BINDINGS_H

#include <hgraph/runtime/map_node.h>
#include <hgraph/runtime/nested_bindings.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/static_schema.h>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

namespace hgraph::runtime_detail
{
    struct MappedChildSourceOverride
    {
        std::size_t source_index{0};
        TSOutputHandle source{};
    };

    [[nodiscard]] inline std::span<const std::size_t> mapped_child_path_suffix(
        const std::vector<std::size_t> &path)
    {
        if (path.empty()) { throw std::logic_error("Mapped child source path requires an argument ordinal"); }
        return std::span<const std::size_t>{path}.subspan(1);
    }

    [[nodiscard]] inline std::size_t mapped_list_index(const ValueView &key)
    {
        if (!key.has_value() || key.schema() != scalar_descriptor<Int>::value_meta())
        {
            throw std::invalid_argument("mapped TSL child requires an int64 index");
        }
        const Int index = key.as<Int>();
        if (index < 0 ||
            static_cast<std::uint64_t>(index) >
                static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max()))
        {
            throw std::out_of_range("mapped TSL child index is out of range");
        }
        return static_cast<std::size_t>(index);
    }

    [[nodiscard]] inline TSOutputView mapped_child_input_source(
        const TSInputView &root_input,
        const MapArgSource &arg,
        const ValueView &key,
        const TSOutputView &key_source,
        const std::vector<std::size_t> &source_path)
    {
        switch (arg.kind)
        {
            case MapArgSourceKind::Key:
                return key_source.borrowed_ref();
            case MapArgSourceKind::Element:
            {
                auto mux_source = root_input.indexed_child_at(arg.outer_index).bound_output();
                if (!mux_source.bound()) { return {}; }
                if (mux_source.schema()->kind == TSTypeKind::TSD)
                {
                    auto dict = mux_source.as_dict();
                    if (!dict.contains(key)) { return {}; }
                    return walk_ts_path(dict.at(key), mapped_child_path_suffix(source_path));
                }
                if (mux_source.schema()->kind == TSTypeKind::TSL)
                {
                    auto              list  = mux_source.as_list();
                    const std::size_t index = mapped_list_index(key);
                    if (index >= list.size()) { return {}; }
                    return walk_ts_path(list.at(index), mapped_child_path_suffix(source_path));
                }
                throw std::invalid_argument("mapped child element source must be a TSD or TSL");
            }
            case MapArgSourceKind::OuterInput:
            {
                auto outer_input = root_input.indexed_child_at(arg.outer_index);
                return walk_source_to_output(std::move(outer_input), mapped_child_path_suffix(source_path));
            }
        }
        return {};
    }

    [[nodiscard]] inline TSOutputView mapped_output_element(
        const NodeView &parent,
        DateTime evaluation_time,
        const ValueView &key)
    {
        auto output = parent.output(evaluation_time);
        if (output.schema()->kind == TSTypeKind::TSD)
        {
            auto dict = output.as_dict();
            return dict.contains(key) ? dict.at(key) : TSOutputView{};
        }
        if (output.schema()->kind == TSTypeKind::TSL)
        {
            auto list = output.as_list();
            return list[mapped_list_index(key)];
        }
        throw std::invalid_argument("mapped child output must be a TSD or TSL");
    }

    inline void bind_mapped_child_inputs(
        const NodeView &parent,
        const GraphView &child,
        DateTime evaluation_time,
        const SingleNestedGraphNodeSpec &spec,
        std::span<const MapArgSource> args,
        const ValueView &key,
        const TSOutputView &key_source,
        std::optional<MappedChildSourceOverride> source_override = std::nullopt,
        bool silent_repoint = false,
        bool sampled = false)
    {
        if (spec.input_bindings.empty()) { return; }

        auto root_input = parent.input(evaluation_time);
        for (const NestedGraphInputBinding &binding : spec.input_bindings)
        {
            const std::size_t source_index = binding.source_path[0];
            if (source_index >= args.size())
            {
                throw std::out_of_range("mapped child input binding source ordinal is out of range");
            }
            const MapArgSource &arg = args[source_index];

            TSOutputView source{};
            if (source_override.has_value() && source_index == source_override->source_index)
            {
                source = source_override->source.view(evaluation_time);
            }
            else
            {
                source = mapped_child_input_source(root_input.borrowed_ref(), arg, key, key_source,
                                                   binding.source_path);
            }

            auto target = walk_ts_path(child.node_at(binding.target.node).input(evaluation_time),
                                       binding.target.path);
            if (source.bound() &&
                !graph_wiring_detail::input_accepts_output_schema(target.schema(), source.schema()))
            {
                throw std::invalid_argument(
                    "mapped child boundary source ordinal " + std::to_string(source_index) +
                    " resolved through outer input " + std::to_string(arg.outer_index) +
                    " as '" + std::string{source.schema()->name()} +
                    "' for child input '" + std::string{target.schema()->name()} + "'");
            }
            if (sampled)
            {
                bind_sampled_input_to_source(std::move(target), source,
                                             evaluation_time);
            }
            else if (silent_repoint)
            {
                rebind_input_to_source_silent(std::move(target), source);
            }
            else
            {
                bind_input_to_source(std::move(target), source);
            }
        }
    }

    inline void bind_mapped_child_output(
        const NodeView &parent,
        const GraphView &child,
        DateTime evaluation_time,
        const std::optional<NestedGraphOutputBinding> &output_binding,
        std::span<const MapArgSource> args,
        const ValueView &key,
        const TSOutputView &key_source,
        MapOutputBindingMode mode = MapOutputBindingMode::ChildTerminalWritesElement,
        bool silent_repoint = false)
    {
        if (!output_binding.has_value()) { return; }

        auto element = mapped_output_element(parent, evaluation_time, key);
        if (!element.bound()) { return; }

        if (output_binding->kind == NestedGraphOutputBinding::Kind::ParentInput)
        {
            if (mode != MapOutputBindingMode::OutputElementForwardsToParentSource)
            {
                throw std::logic_error("mapped child parent-input output requires a forwarding map output element");
            }
            if (output_binding->parent_source_path.empty())
            {
                throw std::logic_error("mapped child parent-input output requires a source ordinal");
            }
            const std::size_t source_index = output_binding->parent_source_path[0];
            if (source_index >= args.size())
            {
                throw std::out_of_range("mapped child output binding source ordinal is out of range");
            }

            auto root_input = parent.input(evaluation_time);
            auto source = mapped_child_input_source(root_input.borrowed_ref(), args[source_index], key,
                                                    key_source, output_binding->parent_source_path);
            auto target = silent_repoint ? element.handle().view(MIN_DT) : element.borrowed_ref();
            static_cast<void>(bind_forwarding_output_tree_to_source(std::move(target), source));
            return;
        }

        auto child_terminal = walk_ts_path(
            child.node_at(output_binding->source.node).output(evaluation_time),
            output_binding->source.path);
        switch (mode)
        {
            case MapOutputBindingMode::ChildTerminalWritesElement:
                bind_forwarding_output_to_source(child_terminal, element);
                break;
            case MapOutputBindingMode::OutputElementForwardsToChildTerminal:
                // Preserve the child's stable terminal endpoint rather than
                // flattening its current forwarding chain. Projected outputs
                // can retarget while the child evaluates; subscribing to the
                // terminal lets that transition propagate to the map element.
                static_cast<void>(bind_forwarding_output_tree_to_source(
                    element.borrowed_ref(), child_terminal, false,
                    ForwardingSourceMode::PreserveEndpoint));
                break;
            case MapOutputBindingMode::OutputElementForwardsToParentSource:
                throw std::logic_error("mapped child child-output binding cannot use parent-source mode");
        }
    }

    inline void clear_mapped_output_element_binding(
        const NodeView &parent,
        DateTime evaluation_time,
        const ValueView &key,
        MapOutputBindingMode mode)
    {
        if (mode == MapOutputBindingMode::ChildTerminalWritesElement) { return; }

        auto element = mapped_output_element(parent, evaluation_time, key);
        if (!element.bound()) { return; }
        static_cast<void>(clear_forwarding_output_tree(std::move(element)));
    }

    /**
     * Reconcile container delta bookkeeping with a mapped child's final
     * output state. A forwarding terminal can publish a source tick and then
     * retarget later in the same child evaluation; observer notification is
     * intentionally deduplicated for that cycle, but the owning container
     * still needs to see the terminal's final validity.
     */
    [[nodiscard]] inline bool mapped_output_data_tree_modified(
        const TSDataView &data,
        const TSValueTypeMetaData *schema,
        DateTime evaluation_time)
    {
        if (!data.valid()) { return false; }
        if (data.modified(evaluation_time)) { return true; }

        const std::size_t child_count =
            schema != nullptr && schema->kind == TSTypeKind::TSB
                ? schema->field_count()
            : schema != nullptr && schema->kind == TSTypeKind::TSL
                ? schema->fixed_size()
                : 0;
        for (std::size_t index = 0; index < child_count; ++index)
        {
            TSDataView child;
            if (detail::has_input_children(data))
            {
                auto projection = detail::input_child_projection(data, index);
                child = std::move(projection.visible);
            }
            else
            {
                child = data.indexed_child_at(index);
            }
            if (mapped_output_data_tree_modified(child, child.schema(),
                                                 evaluation_time))
            {
                return true;
            }
        }
        return false;
    }

    inline void finalize_mapped_child_output(
        const NodeView &parent,
        DateTime evaluation_time,
        const std::optional<NestedGraphOutputBinding> &output_binding,
        const ValueView &key)
    {
        if (!output_binding.has_value()) { return; }

        auto element = mapped_output_element(parent, evaluation_time, key);
        if (!element.bound() ||
            !mapped_output_data_tree_modified(element.data_view(),
                                              element.schema(),
                                              evaluation_time))
        {
            return;
        }

        auto data = element.data_view().borrowed_ref();
        const auto &ops = data.ops();
        auto *tracking = ops.mutable_tracking_impl(ops.context, data.mutable_data());
        if (tracking == nullptr)
        {
            throw std::logic_error(
                "mapped child output element has no mutable tracking");
        }
        static_cast<void>(tracking->record_modified(evaluation_time));
        tracking->parent.notify_child_modified(evaluation_time);
    }
}  // namespace hgraph::runtime_detail

#endif  // HGRAPH_RUNTIME_MAPPED_CHILD_BINDINGS_H
