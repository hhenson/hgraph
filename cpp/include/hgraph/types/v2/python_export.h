#pragma once

#include <hgraph/hgraph_base.h>
#include <hgraph/types/v2/static_signature.h>

#include <string>
#include <string_view>

namespace hgraph::v2
{
    template <typename TImplementation>
    [[nodiscard]] nb::object make_compute_wiring_node(std::string_view name = {})
    {
        const std::string node_name = detail::node_name_or<TImplementation>(name);
        const TSMeta *input_schema = StaticNodeSignature<TImplementation>::input_schema(node_name);
        const TSMeta *output_schema = StaticNodeSignature<TImplementation>::output_schema();

        nb::object builder_factory = nb::cpp_function(
            [node_name, input_schema, output_schema](nb::handle, nb::handle node_signature, nb::handle scalars) -> nb::object {
                nb::object builder_cls =
                    nb::module_::import_("hgraph._wiring._wiring_node_class._cpp_static_wiring_node_class").attr("CppStaticNodeBuilder");

                nb::dict kwargs;
                kwargs["signature"] = nb::borrow(node_signature);
                kwargs["scalars"] = nb::borrow(scalars);
                kwargs["input_builder"] = nb::none();
                kwargs["output_builder"] = nb::none();
                kwargs["error_builder"] = nb::none();
                kwargs["recordable_state_builder"] = nb::none();
                kwargs["implementation_name"] = node_name;
                kwargs["input_schema"] = input_schema != nullptr ? nb::cast(input_schema, nb::rv_policy::reference) : nb::none();
                kwargs["output_schema"] = output_schema != nullptr ? nb::cast(output_schema, nb::rv_policy::reference) : nb::none();

                return detail::py_call(builder_cls, nb::tuple(), kwargs);
            });

        nb::object wiring_node_cls =
            nb::module_::import_("hgraph._wiring._wiring_node_class._cpp_static_wiring_node_class").attr("CppStaticWiringNodeClass");

        return detail::py_call(
            wiring_node_cls,
            nb::make_tuple(StaticNodeSignature<TImplementation>::wiring_signature(node_name), builder_factory));
    }

    template <typename TImplementation>
    void export_compute_node(nb::module_ &m, std::string_view name = {})
    {
        nb::object node = make_compute_wiring_node<TImplementation>(name);
        const std::string exported_name = detail::node_name_or<TImplementation>(name);
        m.attr(exported_name.c_str()) = node;
    }
}  // namespace hgraph::v2
