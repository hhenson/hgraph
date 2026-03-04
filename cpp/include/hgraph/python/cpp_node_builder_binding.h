#pragma once

#include <hgraph/builders/nodes/cpp_node_builder.h>

#include <concepts>

namespace hgraph {
    template<typename... Specs>
    struct CppNodeSpecList {};

    template<typename Spec>
    concept CppNodeSpecWithFactoryName =
        requires {
            { Spec::py_factory_name } -> std::convertible_to<const char*>;
        };

    inline node_signature_s_ptr cpp_node_extract_signature_arg(const nb::args& args, const nb::kwargs& kwargs) {
        if (kwargs.contains("signature")) {
            return nb::cast<node_signature_s_ptr>(kwargs["signature"]);
        }
        if (!args.empty()) {
            return nb::cast<node_signature_s_ptr>(args[0]);
        }
        throw nb::type_error("C++ node builder factory requires 'signature'");
    }

    inline nb::dict cpp_node_extract_scalars_arg(const nb::args& args, const nb::kwargs& kwargs) {
        if (kwargs.contains("scalars")) {
            return nb::cast<nb::dict>(kwargs["scalars"]);
        }
        if (args.size() >= 2) {
            return nb::cast<nb::dict>(args[1]);
        }
        throw nb::type_error("C++ node builder factory requires 'scalars'");
    }

    template<CppNodeSpecWithFactoryName Spec>
    inline node_builder_s_ptr make_cpp_node_builder_from_python_args(const nb::args& args, const nb::kwargs& kwargs) {
        auto signature = cpp_node_extract_signature_arg(args, kwargs);
        auto scalars = cpp_node_extract_scalars_arg(args, kwargs);
        return nb::ref<NodeBuilder>(new CppNodeBuilder<Spec>(std::move(signature), std::move(scalars)));
    }

    template<CppNodeSpecWithFactoryName Spec>
    inline void bind_cpp_node_builder_factory(nb::module_& m) {
        m.def(
            Spec::py_factory_name,
            [](const nb::args& args, const nb::kwargs& kwargs) -> node_builder_s_ptr {
                return make_cpp_node_builder_from_python_args<Spec>(args, kwargs);
            },
            "Create a C++ node builder from signature/scalars");
    }

    template<typename... Specs>
    inline void bind_cpp_node_builder_factories(nb::module_& m, CppNodeSpecList<Specs...>) {
        (bind_cpp_node_builder_factory<Specs>(m), ...);
    }
}  // namespace hgraph

