#pragma once

#include <hgraph/nodes/ops/const_default_node.h>
#include <hgraph/nodes/ops/nothing_node.h>
#include <hgraph/nodes/ops/null_sink_node.h>
#include <hgraph/python/cpp_node_builder_binding.h>

namespace hgraph {
    namespace ops {
        using bound_node_specs = CppNodeSpecList<NothingSpec, NullSinkSpec, ConstDefaultSpec>;

        inline void bind_node_builders(nb::module_& m) {
            bind_cpp_node_builder_factories(m, bound_node_specs{});
        }
    }  // namespace ops
}  // namespace hgraph
