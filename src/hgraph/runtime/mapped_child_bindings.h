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

    struct MappedArgAccessPlan;
    using MappedArgResolveFn = TSOutputView (*)(const MappedArgAccessPlan &, const TSInputView &,
                                                const ValueView &, const TSOutputView &,
                                                const std::vector<std::size_t> &);

    struct MappedArgAccessOps
    {
        MappedArgResolveFn resolve{nullptr};
    };

    struct MappedArgAccessPlan
    {
        MapArgSource             source{};
        const MappedArgAccessOps *ops{nullptr};
        bool                     refreshes_projected_children{false};
    };

    struct MappedOutputDataPlan;
    using MappedOutputModifiedFn = bool (*)(const MappedOutputDataPlan &, const TSDataView &, DateTime);

    struct MappedOutputDataOps
    {
        MappedOutputModifiedFn modified{nullptr};
    };

    struct MappedOutputDataPlan
    {
        const MappedOutputDataOps        *ops{nullptr};
        std::vector<MappedOutputDataPlan> children{};
    };

    struct MappedOutputAccessPlan;
    using MappedOutputElementFn = TSOutputView (*)(const MappedOutputAccessPlan &, const NodeView &,
                                                   DateTime, const ValueView &);

    struct MappedOutputAccessOps
    {
        MappedOutputElementFn element{nullptr};
    };

    struct MappedOutputAccessPlan
    {
        const MappedOutputAccessOps *ops{nullptr};
        MappedOutputDataPlan         data{};
    };

    struct MappedChildAccessPlan
    {
        // Compiled once in the owning map/mesh node context. Child lifecycle
        // code dispatches through these passive callbacks instead of testing
        // TSD/TSL or endpoint representation on every bind/evaluation.
        std::vector<MappedArgAccessPlan> args{};
        MappedOutputAccessPlan           output{};
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

    [[nodiscard]] inline TSOutputView mapped_key_source(
        const MappedArgAccessPlan &,
        const TSInputView &,
        const ValueView &,
        const TSOutputView &key_source,
        const std::vector<std::size_t> &)
    {
        return key_source.borrowed_ref();
    }

    [[nodiscard]] inline TSOutputView mapped_dict_element_source(
        const MappedArgAccessPlan &plan,
        const TSInputView &root_input,
        const ValueView &key,
        const TSOutputView &,
        const std::vector<std::size_t> &source_path)
    {
        auto mux_source = root_input.indexed_child_at(plan.source.outer_index).bound_output();
        if (!mux_source.bound()) { return {}; }
        auto dict = mux_source.as_dict();
        if (!dict.contains(key)) { return {}; }
        return walk_ts_path(dict.at(key), mapped_child_path_suffix(source_path));
    }

    [[nodiscard]] inline TSOutputView mapped_list_element_source(
        const MappedArgAccessPlan &plan,
        const TSInputView &root_input,
        const ValueView &key,
        const TSOutputView &,
        const std::vector<std::size_t> &source_path)
    {
        auto mux_source = root_input.indexed_child_at(plan.source.outer_index).bound_output();
        if (!mux_source.bound()) { return {}; }
        auto list = mux_source.as_list();
        const std::size_t index = mapped_list_index(key);
        if (index >= list.size()) { return {}; }
        return walk_ts_path(list.at(index), mapped_child_path_suffix(source_path));
    }

    [[nodiscard]] inline TSOutputView mapped_outer_input_source(
        const MappedArgAccessPlan &plan,
        const TSInputView &root_input,
        const ValueView &,
        const TSOutputView &,
        const std::vector<std::size_t> &source_path)
    {
        auto outer_input = root_input.indexed_child_at(plan.source.outer_index);
        return walk_source_to_output(std::move(outer_input), mapped_child_path_suffix(source_path));
    }

    [[nodiscard]] inline TSOutputView mapped_child_input_source(
        const TSInputView &root_input,
        const MappedArgAccessPlan &plan,
        const ValueView &key,
        const TSOutputView &key_source,
        const std::vector<std::size_t> &source_path)
    {
        return plan.ops->resolve(plan, root_input, key, key_source, source_path);
    }

    [[nodiscard]] inline TSOutputView mapped_dict_output_element(
        const MappedOutputAccessPlan &,
        const NodeView &parent,
        DateTime evaluation_time,
        const ValueView &key)
    {
        auto output = parent.output(evaluation_time);
        auto dict = output.as_dict();
        return dict.contains(key) ? dict.at(key) : TSOutputView{};
    }

    [[nodiscard]] inline TSOutputView mapped_list_output_element(
        const MappedOutputAccessPlan &,
        const NodeView &parent,
        DateTime evaluation_time,
        const ValueView &key)
    {
        auto output = parent.output(evaluation_time);
        auto list = output.as_list();
        return list[mapped_list_index(key)];
    }

    [[nodiscard]] inline TSOutputView mapped_no_output_element(
        const MappedOutputAccessPlan &,
        const NodeView &,
        DateTime,
        const ValueView &)
    {
        return {};
    }

    [[nodiscard]] inline TSOutputView mapped_output_element(
        const MappedOutputAccessPlan &plan,
        const NodeView &parent,
        DateTime evaluation_time,
        const ValueView &key)
    {
        return plan.ops->element(plan, parent, evaluation_time, key);
    }

    [[nodiscard]] inline bool mapped_output_leaf_modified(
        const MappedOutputDataPlan &,
        const TSDataView &data,
        DateTime evaluation_time)
    {
        return data.valid() && data.modified(evaluation_time);
    }

    [[nodiscard]] inline bool mapped_output_indexed_tree_modified(
        const MappedOutputDataPlan &plan,
        const TSDataView &data,
        DateTime evaluation_time)
    {
        if (!data.valid()) { return false; }
        if (data.modified(evaluation_time)) { return true; }
        for (std::size_t index = 0; index < plan.children.size(); ++index)
        {
            const auto &child_plan = plan.children[index];
            if (child_plan.ops->modified(child_plan, data.indexed_child_at(index), evaluation_time))
            {
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] inline bool mapped_output_input_tree_modified(
        const MappedOutputDataPlan &plan,
        const TSDataView &data,
        DateTime evaluation_time)
    {
        if (!data.valid()) { return false; }
        if (data.modified(evaluation_time)) { return true; }
        for (std::size_t index = 0; index < plan.children.size(); ++index)
        {
            auto projection = detail::input_child_projection(data, index);
            const auto &child_plan = plan.children[index];
            if (child_plan.ops->modified(child_plan, projection.visible, evaluation_time))
            {
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] inline const MappedOutputDataOps &mapped_output_leaf_ops() noexcept
    {
        static const MappedOutputDataOps ops{&mapped_output_leaf_modified};
        return ops;
    }

    [[nodiscard]] inline const MappedOutputDataOps &mapped_output_indexed_ops() noexcept
    {
        static const MappedOutputDataOps ops{&mapped_output_indexed_tree_modified};
        return ops;
    }

    [[nodiscard]] inline const MappedOutputDataOps &mapped_output_input_ops() noexcept
    {
        static const MappedOutputDataOps ops{&mapped_output_input_tree_modified};
        return ops;
    }

    [[nodiscard]] inline MappedOutputDataPlan mapped_owned_output_data_plan(
        const TSValueTypeMetaData *schema)
    {
        MappedOutputDataPlan plan{.ops = &mapped_output_leaf_ops()};
        if (schema == nullptr) { return plan; }
        if (schema->kind == TSTypeKind::TSB)
        {
            plan.ops = &mapped_output_indexed_ops();
            plan.children.reserve(schema->field_count());
            for (std::size_t index = 0; index < schema->field_count(); ++index)
            {
                plan.children.push_back(mapped_owned_output_data_plan(schema->fields()[index].type));
            }
        }
        else if (schema->kind == TSTypeKind::TSL && schema->fixed_size() != 0)
        {
            plan.ops = &mapped_output_indexed_ops();
            plan.children.reserve(schema->fixed_size());
            for (std::size_t index = 0; index < schema->fixed_size(); ++index)
            {
                plan.children.push_back(mapped_owned_output_data_plan(schema->element_ts()));
            }
        }
        return plan;
    }

    [[nodiscard]] inline MappedOutputDataPlan mapped_endpoint_output_data_plan(
        const TSEndpointSchema &endpoint)
    {
        if (!endpoint.is_non_peered())
        {
            return MappedOutputDataPlan{.ops = &mapped_output_leaf_ops()};
        }
        MappedOutputDataPlan plan{.ops = &mapped_output_input_ops()};
        plan.children.reserve(endpoint.child_count());
        for (std::size_t index = 0; index < endpoint.child_count(); ++index)
        {
            plan.children.push_back(mapped_endpoint_output_data_plan(endpoint.child(index)));
        }
        return plan;
    }

    [[nodiscard]] inline MappedChildAccessPlan compile_mapped_child_access_plan(
        std::span<const MapArgSource> args,
        TSTypeKind multiplexed_kind,
        const TSValueTypeMetaData *input_schema,
        const TSValueTypeMetaData *output_schema,
        const TSEndpointSchema *output_element_endpoint = nullptr)
    {
        static const MappedArgAccessOps key_ops{&mapped_key_source};
        static const MappedArgAccessOps dict_element_ops{&mapped_dict_element_source};
        static const MappedArgAccessOps list_element_ops{&mapped_list_element_source};
        static const MappedArgAccessOps outer_ops{&mapped_outer_input_source};
        static const MappedOutputAccessOps no_output_ops{&mapped_no_output_element};
        static const MappedOutputAccessOps dict_output_ops{&mapped_dict_output_element};
        static const MappedOutputAccessOps list_output_ops{&mapped_list_output_element};

        MappedChildAccessPlan plan;
        plan.args.reserve(args.size());
        for (const auto &source : args)
        {
            const MappedArgAccessOps *ops = &outer_ops;
            switch (source.kind)
            {
                case MapArgSourceKind::Key: ops = &key_ops; break;
                case MapArgSourceKind::OuterInput: ops = &outer_ops; break;
                case MapArgSourceKind::Element:
                    ops = multiplexed_kind == TSTypeKind::TSD ? &dict_element_ops : &list_element_ops;
                    break;
            }
            bool refreshes_projected_children = false;
            if (source.kind == MapArgSourceKind::OuterInput && input_schema != nullptr &&
                source.outer_index < input_schema->field_count())
            {
                const auto *schema = input_schema->fields()[source.outer_index].type;
                refreshes_projected_children = schema != nullptr &&
                                               (schema->kind == TSTypeKind::TSB ||
                                                schema->kind == TSTypeKind::TSL);
            }
            plan.args.push_back(MappedArgAccessPlan{
                .source = source,
                .ops = ops,
                .refreshes_projected_children = refreshes_projected_children,
            });
        }

        plan.output.ops = &no_output_ops;
        plan.output.data = MappedOutputDataPlan{.ops = &mapped_output_leaf_ops()};
        if (output_schema != nullptr)
        {
            plan.output.ops = output_schema->kind == TSTypeKind::TSD ? &dict_output_ops : &list_output_ops;
            const auto *element_schema = output_schema->element_ts();
            plan.output.data = output_element_endpoint != nullptr
                                   ? mapped_endpoint_output_data_plan(*output_element_endpoint)
                                   : mapped_owned_output_data_plan(element_schema);
        }
        return plan;
    }

    inline void bind_mapped_child_inputs(
        const NodeView &parent,
        const GraphView &child,
        DateTime evaluation_time,
        const SingleNestedGraphNodeSpec &spec,
        const MappedChildAccessPlan &access,
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
            if (source_index >= access.args.size())
            {
                throw std::out_of_range("mapped child input binding source ordinal is out of range");
            }
            const auto &arg = access.args[source_index];

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
                    " resolved through outer input " + std::to_string(arg.source.outer_index) +
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
        const MappedChildAccessPlan &access,
        const ValueView &key,
        const TSOutputView &key_source,
        MapOutputBindingMode mode = MapOutputBindingMode::ChildTerminalWritesElement,
        bool silent_repoint = false)
    {
        if (!output_binding.has_value()) { return; }

        auto element = mapped_output_element(access.output, parent, evaluation_time, key);
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
            if (source_index >= access.args.size())
            {
                throw std::out_of_range("mapped child output binding source ordinal is out of range");
            }

            auto root_input = parent.input(evaluation_time);
            auto source = mapped_child_input_source(root_input.borrowed_ref(), access.args[source_index], key,
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
        const MappedOutputAccessPlan &access,
        MapOutputBindingMode mode)
    {
        if (mode == MapOutputBindingMode::ChildTerminalWritesElement) { return; }

        auto element = mapped_output_element(access, parent, evaluation_time, key);
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
        const MappedOutputDataPlan &plan,
        const TSDataView &data,
        DateTime evaluation_time)
    {
        return plan.ops->modified(plan, data, evaluation_time);
    }

    inline void finalize_mapped_child_output(
        const NodeView &parent,
        DateTime evaluation_time,
        const std::optional<NestedGraphOutputBinding> &output_binding,
        const MappedOutputAccessPlan &access,
        const ValueView &key)
    {
        if (!output_binding.has_value()) { return; }

        auto element = mapped_output_element(access, parent, evaluation_time, key);
        if (!element.bound() ||
            !mapped_output_data_tree_modified(access.data, element.data_view(), evaluation_time))
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
