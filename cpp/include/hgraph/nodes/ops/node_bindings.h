#pragma once

#include <hgraph/nodes/ops/bool_operator_nodes.h>
#include <hgraph/nodes/ops/const_default_node.h>
#include <hgraph/nodes/ops/enum_operator_nodes.h>
#include <hgraph/nodes/ops/number_operator_nodes.h>
#include <hgraph/nodes/ops/nothing_node.h>
#include <hgraph/nodes/ops/null_sink_node.h>
#include <hgraph/nodes/ops/str_operator_nodes.h>
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
                LShiftIntsSpec,
                RShiftIntsSpec,
                BitAndIntsSpec,
                BitOrIntsSpec,
                BitXorIntsSpec,
                EqFloatIntSpec,
                EqIntIntSpec,
                EqIntFloatSpec,
                EqFloatFloatSpec,
                NeIntIntSpec,
                NeIntFloatSpec,
                NeFloatIntSpec,
                NeFloatFloatSpec,
                LtIntIntSpec,
                LtIntFloatSpec,
                LtFloatIntSpec,
                LtFloatFloatSpec,
                LeIntIntSpec,
                LeIntFloatSpec,
                LeFloatIntSpec,
                LeFloatFloatSpec,
                GtIntIntSpec,
                GtIntFloatSpec,
                GtFloatIntSpec,
                GtFloatFloatSpec,
                GeIntIntSpec,
                GeIntFloatSpec,
                GeFloatIntSpec,
                GeFloatFloatSpec,
                NegIntSpec,
                NegFloatSpec,
                PosIntSpec,
                PosFloatSpec,
                InvertIntSpec,
                AbsIntSpec,
                AbsFloatSpec,
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
                AddStrSpec,
                MulStrsSpec,
                ContainsStrSpec,
                SubstrDefaultSpec,
                AndBooleansSpec,
                OrBooleansSpec,
                NotBooleanSpec,
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
