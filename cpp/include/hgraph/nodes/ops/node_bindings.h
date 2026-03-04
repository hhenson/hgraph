#pragma once

#include <hgraph/nodes/ops/const_default_node.h>
#include <hgraph/nodes/ops/enum_operator_nodes.h>
#include <hgraph/nodes/ops/number_operator_nodes.h>
#include <hgraph/nodes/ops/nothing_node.h>
#include <hgraph/nodes/ops/null_sink_node.h>
#include <hgraph/python/cpp_node_builder_binding.h>

namespace hgraph {
    namespace ops {
        using bound_node_specs =
            CppNodeSpecList<
                NothingSpec,
                NullSinkSpec,
                ConstDefaultSpec,
                AddFloatToIntSpec,
                AddIntToIntSpec,
                AddFloatToFloatSpec,
                AddIntToFloatSpec,
                SubIntFromIntSpec,
                SubFloatFromFloatSpec,
                SubIntFromFloatSpec,
                SubFloatFromIntSpec,
                MulFloatAndIntSpec,
                MulIntAndIntSpec,
                MulFloatAndFloatSpec,
                MulIntAndFloatSpec,
                EqFloatIntSpec,
                EqIntFloatSpec,
                EqFloatFloatSpec,
                LnImplSpec,
                DivNumbersSpec,
                FloorDivNumbersSpec,
                FloorDivIntsSpec,
                ModNumbersSpec,
                ModIntsSpec,
                PowIntFloatSpec,
                PowFloatIntSpec,
                DivmodNumbersSpec,
                DivmodIntsSpec,
                EqEnumSpec,
                LtEnumSpec,
                LeEnumSpec,
                GtEnumSpec,
                GeEnumSpec,
                GetattrEnumNameSpec>;

        inline void bind_node_builders(nb::module_& m) {
            bind_cpp_node_builder_factories(m, bound_node_specs{});
        }
    }  // namespace ops
}  // namespace hgraph
