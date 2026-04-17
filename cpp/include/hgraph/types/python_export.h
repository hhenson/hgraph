#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/node_builder.h>
#include <hgraph/types/static_signature.h>

#include <string>
#include <string_view>
#include <optional>
#include <unordered_set>
#include <vector>

namespace hgraph
{
    namespace detail
    {
        [[nodiscard]] inline nb::object upgrade_python_wiring_node(nb::handle node,
                                                                   nb::handle builder_factory,
                                                                   nb::handle signature_override = nb::none())
        {
            nb::object copy_fn = nb::module_::import_("copy").attr("copy");
            nb::object wiring_node = py_call(copy_fn, nb::make_tuple(nb::borrow(node)));
            nb::object cpp_static_wiring_node_cls =
                nb::module_::import_("hgraph._wiring._wiring_node_class._cpp_static_wiring_node_class").attr("CppStaticWiringNodeClass");

            if (!nb::isinstance(wiring_node, cpp_static_wiring_node_cls)) {
                nb::setattr(wiring_node, "__class__", cpp_static_wiring_node_cls);
            }
            nb::setattr(wiring_node, "_builder_factory", nb::borrow(builder_factory));

            if (signature_override.is_valid() && !signature_override.is_none()) {
                nb::object signature = nb::borrow(signature_override);
                nb::setattr(wiring_node, "signature", signature);

                if (nb::hasattr(wiring_node, "overload_list")) {
                    nb::object overload_list = py_call(copy_fn, nb::make_tuple(wiring_node.attr("overload_list")));
                    nb::setattr(wiring_node, "overload_list", overload_list);
                    nb::setattr(overload_list, "arg_count_cache", nb::dict());

                    if (nb::hasattr(overload_list, "overloads")) {
                        nb::list updated_overloads;
                        nb::object calc_rank = overload_list.attr("_calc_rank");
                        nb::object rank = py_call(calc_rank, nb::make_tuple(signature));

                        for (auto entry_handle : overload_list.attr("overloads")) {
                            nb::tuple entry = nb::cast<nb::tuple>(entry_handle);
                            nb::object impl = nb::borrow(entry[0]);
                            if (impl.ptr() == wiring_node.ptr()) {
                                updated_overloads.append(nb::make_tuple(impl, rank));
                            } else {
                                updated_overloads.append(nb::borrow(entry_handle));
                            }
                        }

                        nb::setattr(overload_list, "overloads", updated_overloads);
                    }
                }
            }
            return wiring_node;
        }

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

        inline void apply_selector_policies(NodeBuilder &builder,
                                            nb::handle node_signature,
                                            nb::handle resolved_wiring_signature,
                                            const TSMeta *input_schema)
        {
            if (input_schema == nullptr || input_schema->kind != TSKind::TSB) { return; }

            auto resolve_slots = [&](const char *attribute_name) {
                nb::object names_obj = nb::getattr(nb::borrow(node_signature), attribute_name, nb::none());
                if (names_obj.is_none()) {
                    names_obj = nb::getattr(nb::borrow(resolved_wiring_signature), attribute_name, nb::none());
                }

                if (names_obj.is_none()) { return std::optional<std::vector<size_t>>{}; }

                std::vector<size_t> slots;
                slots.reserve(static_cast<size_t>(nb::len(names_obj)));
                for (auto item : names_obj) {
                    const auto name = nb::cast<std::string>(item);
                    for (size_t slot = 0; slot < input_schema->field_count(); ++slot) {
                        if (input_schema->fields()[slot].name == name) { slots.push_back(slot); }
                    }
                }
                return std::optional<std::vector<size_t>>{std::move(slots)};
            };

            if (auto slots = resolve_slots("active_inputs")) { builder.set_active_inputs(std::move(*slots)); }
            if (auto slots = resolve_slots("valid_inputs")) { builder.set_valid_inputs(std::move(*slots)); }
            if (auto slots = resolve_slots("all_valid_inputs")) { builder.set_all_valid_inputs(std::move(*slots)); }
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
                detail::apply_selector_policies(builder, node_signature, resolved_wiring_signature, input_schema);
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
        std::string_view exported_name = {},
        nb::object signature_override = nb::none())
    {
        const std::string node_name = detail::node_name_or<TImplementation>(exported_name);
        nb::object python_wiring_node =
            nb::module_::import_(std::string{python_module}.c_str()).attr(std::string{python_symbol}.c_str());
        nb::object python_signature = python_wiring_node.attr("signature");

        nb::object builder_factory = nb::cpp_function(
            [node_name](nb::handle resolved_wiring_signature, nb::handle node_signature, nb::handle scalars) -> NodeBuilder {
                const TSMeta *input_schema = detail::resolved_input_schema(resolved_wiring_signature, node_name);
                const TSMeta *output_schema = detail::resolved_output_schema(resolved_wiring_signature);
                std::string runtime_label = detail::runtime_label_or(node_signature, node_name);

                NodeBuilder builder;
                if (input_schema != nullptr) { builder.input_schema(input_schema); }
                if (output_schema != nullptr) { builder.output_schema(output_schema); }
                detail::apply_selector_policies(builder, node_signature, resolved_wiring_signature, input_schema);

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

                return builder;
            });

        return detail::upgrade_python_wiring_node(python_wiring_node, builder_factory, signature_override);
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
                                              std::string_view exported_name,
                                              nb::object signature_override = nb::none())
    {
        nb::object node =
            make_compute_wiring_node_from_python_impl<TImplementation>(
                python_module,
                python_symbol,
                exported_name,
                std::move(signature_override));
        m.attr(std::string{exported_name}.c_str()) = node;
    }
}  // namespace hgraph
