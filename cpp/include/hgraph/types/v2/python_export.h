#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/v2/node_builder.h>
#include <hgraph/types/v2/static_signature.h>

#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace hgraph::v2
{
    namespace detail
    {
        [[nodiscard]] inline const TSMeta *cpp_ts_meta_or_none(nb::handle meta)
        {
            if (meta.is_none()) { return nullptr; }

            nb::object cpp_type = nb::borrow(meta).attr("cpp_type");
            if (cpp_type.is_none()) { return nullptr; }
            return nb::cast<const TSMeta *>(cpp_type);
        }

        [[nodiscard]] inline const TSMeta *resolved_input_schema(nb::handle resolved_wiring_signature,
                                                                 std::string_view default_name)
        {
            std::vector<std::pair<std::string, const TSMeta *>> fields;
            const std::vector<std::string> args = nb::cast<std::vector<std::string>>(nb::borrow(resolved_wiring_signature).attr("args"));
            const std::unordered_set<std::string> time_series_args =
                nb::cast<std::unordered_set<std::string>>(nb::borrow(resolved_wiring_signature).attr("time_series_args"));
            nb::object input_types = nb::borrow(resolved_wiring_signature).attr("input_types");

            fields.reserve(args.size());
            for (const auto &arg : args) {
                if (!time_series_args.contains(arg)) { continue; }
                const TSMeta *schema = cpp_ts_meta_or_none(py_getitem(input_types, nb::str(arg.c_str())));
                if (schema == nullptr) { return nullptr; }
                fields.emplace_back(arg, schema);
            }

            if (fields.empty()) { return nullptr; }
            return TSTypeRegistry::instance().tsb(fields, std::string{default_name} + ".inputs");
        }

        [[nodiscard]] inline const TSMeta *resolved_output_schema(nb::handle resolved_wiring_signature)
        {
            return cpp_ts_meta_or_none(nb::borrow(resolved_wiring_signature).attr("output_type"));
        }

        [[nodiscard]] inline nb::dict python_scalars_dict(nb::handle scalars)
        {
            if (scalars.is_none()) { return nb::dict(); }

            nb::object value = nb::borrow(scalars);
            if (nb::isinstance<nb::dict>(value)) { return nb::cast<nb::dict>(value); }
            return nb::cast<nb::dict>(py_call(nb::module_::import_("builtins").attr("dict"), nb::make_tuple(value)));
        }

        [[nodiscard]] inline nb::object resolved_recordable_state_builder_or_none(nb::handle resolved_wiring_signature)
        {
            if (!nb::cast<bool>(nb::borrow(resolved_wiring_signature).attr("uses_recordable_state"))) { return nb::none(); }

            nb::object recordable_state = nb::borrow(resolved_wiring_signature).attr("recordable_state");
            if (recordable_state.is_none()) { return nb::none(); }

            nb::object tsb_type = recordable_state.attr("tsb_type");
            if (tsb_type.is_none()) { return nb::none(); }

            nb::object factory = hgraph_module().attr("TimeSeriesBuilderFactory").attr("instance")();
            return factory.attr("make_output_builder")(tsb_type);
        }

        [[nodiscard]] inline std::vector<std::string> names_from_frozenset_or_default(
            nb::handle resolved_wiring_signature,
            const char *attribute_name,
            bool default_to_all_time_series_args)
        {
            nb::object names_obj = nb::borrow(resolved_wiring_signature).attr(attribute_name);
            if (!names_obj.is_none()) {
                std::vector<std::string> names;
                names.reserve(static_cast<size_t>(nb::len(names_obj)));
                for (auto item : names_obj) { names.push_back(nb::cast<std::string>(item)); }
                return names;
            }
            if (!default_to_all_time_series_args) { return {}; }
            nb::object time_series_args = nb::borrow(resolved_wiring_signature).attr("time_series_args");
            std::vector<std::string> names;
            names.reserve(static_cast<size_t>(nb::len(time_series_args)));
            for (auto item : time_series_args) { names.push_back(nb::cast<std::string>(item)); }
            return names;
        }

        inline void apply_selector_policies(NodeBuilder &builder,
                                            nb::handle resolved_wiring_signature,
                                            const TSMeta *input_schema)
        {
            if (input_schema == nullptr) { return; }

            auto apply_slots = [&](const char *attribute_name, bool default_to_all_time_series_args, auto add_slot) {
                for (const auto &name : names_from_frozenset_or_default(
                         resolved_wiring_signature, attribute_name, default_to_all_time_series_args)) {
                    for (size_t slot = 0; slot < input_schema->field_count(); ++slot) {
                        if (input_schema->fields()[slot].name == name) {
                            add_slot(slot);
                            break;
                        }
                    }
                }
            };

            apply_slots("active_inputs", true, [&](size_t slot) { builder.active_input(slot); });
            apply_slots("valid_inputs", true, [&](size_t slot) { builder.valid_input(slot); });
            apply_slots("all_valid_inputs", false, [&](size_t slot) { builder.all_valid_input(slot); });
        }

        [[nodiscard]] inline std::string runtime_label_or(nb::handle node_signature, std::string_view default_name)
        {
            std::string runtime_label{default_name};
            nb::object label = nb::borrow(node_signature).attr("label");
            if (label.is_valid() && !label.is_none()) {
                const std::string resolved_label = nb::cast<std::string>(label);
                if (!resolved_label.empty()) { runtime_label = resolved_label; }
            }
            return runtime_label;
        }
    }  // namespace detail

    /**
     * Create a Python wiring node class from a static C++ implementation.
     *
     * The reflected StaticNodeSignature drives the exported Python signature,
     * while the builder factory captures the resolved schemas chosen by Python
     * wiring so the eventual runtime builder can construct matching C++ nodes.
     */
    template <typename TImplementation>
    [[nodiscard]] nb::object make_compute_wiring_node(std::string_view name = {})
    {
        const std::string node_name = detail::node_name_or<TImplementation>(name);

        nb::object builder_factory = nb::cpp_function(
            [node_name](nb::handle resolved_wiring_signature, nb::handle node_signature, nb::handle scalars) -> NodeBuilder {
                const TSMeta *input_schema = detail::resolved_input_schema(resolved_wiring_signature, node_name);
                const TSMeta *output_schema = detail::resolved_output_schema(resolved_wiring_signature);
                const bool needs_resolved_schemas = !StaticNodeSignature<TImplementation>::unresolved_input_names().empty();
                std::string runtime_label = detail::runtime_label_or(node_signature, node_name);

                NodeBuilder builder;
                if (needs_resolved_schemas && input_schema != nullptr) { builder.input_schema(input_schema); }
                if (needs_resolved_schemas && output_schema != nullptr) { builder.output_schema(output_schema); }
                builder.implementation<TImplementation>()
                    .label(std::move(runtime_label))
                    .python_signature(nb::borrow(node_signature))
                    .python_scalars(detail::python_scalars_dict(scalars))
                    .python_input_builder(nb::none())
                    .python_output_builder(nb::none())
                    .python_error_builder(nb::none())
                    .python_recordable_state_builder(detail::resolved_recordable_state_builder_or_none(resolved_wiring_signature))
                    .implementation_name(node_name)
                    .requires_resolved_schemas(needs_resolved_schemas);
                detail::apply_selector_policies(builder, resolved_wiring_signature, input_schema);

                return builder;
            });

        nb::object wiring_node_cls =
            nb::module_::import_("hgraph._wiring._wiring_node_class._cpp_static_wiring_node_class").attr("CppStaticWiringNodeClass");

        return detail::py_call(
            wiring_node_cls,
            nb::make_tuple(StaticNodeSignature<TImplementation>::wiring_signature(node_name), builder_factory));
    }

    /**
     * Create a Python wiring node class from an existing Python wiring-node implementation signature.
     *
     * This is used for early v2 parity nodes where Python already defines the
     * public contract, including scalar defaults and overload shape, but the
     * runtime implementation is moved to a v2 C++ node.
     */
    template <typename TImplementation>
    [[nodiscard]] nb::object make_compute_wiring_node_from_python_impl(
        std::string_view python_module,
        std::string_view python_symbol,
        std::string_view exported_name = {})
    {
        const std::string node_name = detail::node_name_or<TImplementation>(exported_name);
        nb::object python_signature =
            nb::module_::import_(std::string{python_module}.c_str()).attr(std::string{python_symbol}.c_str()).attr("signature");

        nb::object builder_factory = nb::cpp_function(
            [node_name](nb::handle resolved_wiring_signature, nb::handle node_signature, nb::handle scalars) -> NodeBuilder {
                const TSMeta *input_schema = detail::resolved_input_schema(resolved_wiring_signature, node_name);
                const TSMeta *output_schema = detail::resolved_output_schema(resolved_wiring_signature);
                std::string runtime_label = detail::runtime_label_or(node_signature, node_name);

                NodeBuilder builder;
                if (input_schema != nullptr) { builder.input_schema(input_schema); }
                if (output_schema != nullptr) { builder.output_schema(output_schema); }

                builder.implementation<TImplementation>()
                    .label(std::move(runtime_label))
                    .python_signature(nb::borrow(node_signature))
                    .python_scalars(detail::python_scalars_dict(scalars))
                    .python_input_builder(nb::none())
                    .python_output_builder(nb::none())
                    .python_error_builder(nb::none())
                    .python_recordable_state_builder(detail::resolved_recordable_state_builder_or_none(resolved_wiring_signature))
                    .implementation_name(node_name)
                    .requires_resolved_schemas(true);
                detail::apply_selector_policies(builder, resolved_wiring_signature, input_schema);

                return builder;
            });

        nb::object wiring_node_cls =
            nb::module_::import_("hgraph._wiring._wiring_node_class._cpp_static_wiring_node_class").attr("CppStaticWiringNodeClass");

        return detail::py_call(wiring_node_cls, nb::make_tuple(python_signature, builder_factory));
    }

    /** Export a static C++ compute node into a nanobind module. */
    template <typename TImplementation>
    void export_compute_node(nb::module_ &m, std::string_view name)
    {
        nb::object node = make_compute_wiring_node<TImplementation>(name);
        const std::string exported_name = detail::node_name_or<TImplementation>(name);
        m.attr(exported_name.c_str()) = node;
    }

    template <typename TImplementation>
    void export_compute_node_from_python_impl(nb::module_ &m,
                                              std::string_view python_module,
                                              std::string_view python_symbol,
                                              std::string_view exported_name)
    {
        nb::object node =
            make_compute_wiring_node_from_python_impl<TImplementation>(python_module, python_symbol, exported_name);
        m.attr(std::string{exported_name}.c_str()) = node;
    }
}  // namespace hgraph::v2
