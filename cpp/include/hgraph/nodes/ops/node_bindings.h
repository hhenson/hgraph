#pragma once

#include <hgraph/nodes/ops/access_operator_nodes.h>
#include <hgraph/nodes/ops/bool_operator_nodes.h>
#include <hgraph/nodes/ops/const_default_node.h>
#include <hgraph/nodes/ops/date_time_operator_nodes.h>
#include <hgraph/nodes/ops/enum_operator_nodes.h>
#include <hgraph/nodes/ops/flow_control_operator_nodes.h>
#include <hgraph/nodes/ops/graph_operator_nodes.h>
#include <hgraph/nodes/ops/number_operator_nodes.h>
#include <hgraph/nodes/ops/nothing_node.h>
#include <hgraph/nodes/ops/null_sink_node.h>
#include <hgraph/nodes/ops/scalar_operator_nodes.h>
#include <hgraph/nodes/ops/str_operator_nodes.h>
#include <hgraph/nodes/ops/time_series_property_operator_nodes.h>
#include <hgraph/nodes/ops/tuple_operator_nodes.h>
#include <hgraph/nodes/ops/type_operator_nodes.h>
#include <hgraph/python/cpp_node_builder_binding.h>

namespace hgraph {
    namespace ops {
        using graph_operator_node_specs =
            CppNodeSpecList<
                AssertDefaultSpec,
                AssertDefaultTsSpec,
                PrintSpec,
                LogSpec,
                DebugPrintImplSpec>;

        using core_node_specs =
            CppNodeSpecList<
                NothingSpec,
                NullSinkSpec,
                ConstDefaultSpec>;

        using access_node_specs =
            CppNodeSpecList<
                SetattrCsSpec,
                GetattrCsSpec,
                GetattrTypeNameSpec,
                GetitemTupleFixedSpec,
                GetitemTupleSpec,
                GetitemFrozendictSpec,
                TsdGetItemDefaultSpec,
                TsdGetItemsSpec,
                KeysTsdAsTssSpec,
                KeysTsdAsSetSpec,
                LenTsdSpec,
                GetItemSeriesSpec,
                GetItemSeriesTsSpec,
                GetattrJsonStrSpec,
                GetattrJsonBoolSpec,
                GetattrJsonIntSpec,
                GetattrJsonFloatSpec,
                GetattrJsonObjSpec,
                GetitemJsonStrSpec,
                GetitemJsonIntSpec>;

        using number_node_specs =
            CppNodeSpecList<
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
                DivmodIntsSpec>;

        using scalar_node_specs =
            CppNodeSpecList<
                AddScalarsSpec,
                SubScalarsSpec,
                MulScalarsSpec,
                PowScalarsSpec,
                LShiftScalarsSpec,
                RShiftScalarsSpec,
                BitAndScalarsSpec,
                BitOrScalarsSpec,
                BitXorScalarsSpec,
                EqScalarsSpec,
                NeScalarsSpec,
                LtScalarsSpec,
                LeScalarsSpec,
                GtScalarsSpec,
                GeScalarsSpec,
                NegScalarSpec,
                PosScalarSpec,
                InvertScalarSpec,
                AbsScalarSpec,
                LenScalarSpec,
                CmpScalarsSpec,
                NotScalarSpec,
                AndScalarsSpec,
                OrScalarsSpec,
                ContainsScalarSpec>;

        using string_node_specs =
            CppNodeSpecList<
                AddStrSpec,
                MulStrsSpec,
                ContainsStrSpec,
                SubstrDefaultSpec,
                StrDefaultSpec,
                StrBytesSpec,
                MatchDefaultSpec,
                ReplaceDefaultSpec,
                SplitDefaultSpec,
                JoinStrTupleSpec,
                FormatSpec>;

        using bool_node_specs =
            CppNodeSpecList<
                AndBooleansSpec,
                OrBooleansSpec,
                NotBooleanSpec>;

        using enum_node_specs =
            CppNodeSpecList<
                EqEnumSpec,
                LtEnumSpec,
                LeEnumSpec,
                GtEnumSpec,
                GeEnumSpec,
                MinEnumUnarySpec,
                MinEnumBinarySpec,
                MaxEnumUnarySpec,
                MaxEnumBinarySpec,
                GetattrEnumNameSpec>;

        using tuple_node_specs =
            CppNodeSpecList<
                MulTupleIntSpec,
                AndTuplesSpec,
                OrTuplesSpec,
                MinTupleUnarySpec,
                MaxTupleUnarySpec,
                SumTupleUnarySpec,
                MeanTupleUnarySpec,
                StdTupleUnarySpec,
                VarTupleUnarySpec,
                IndexOfTupleSpec,
                AddTupleScalarSpec,
                SubTupleScalarSpec>;

        using flow_control_node_specs =
            CppNodeSpecList<
                AllDefaultSpec,
                AllTsdSpec,
                AnyDefaultSpec,
                AnyTsdSpec,
                MergeTsScalarSpec,
                IndexOfImplSpec,
                IfTrueImplSpec,
                IfCmpImplSpec,
                IfThenElseImplSpec>;

        using type_node_specs =
            CppNodeSpecList<
                TypeCsSchemaSpec,
                TypeCsTypevarSpec,
                TypeScalarSpec,
                CastImplSpec,
                DowncastImplSpec,
                DowncastRefImplSpec>;

        using date_time_node_specs =
            CppNodeSpecList<
                ExplodeDateImplSpec,
                AddDateTimeNodeSpec,
                DateTimeDateAsDateTimeSpec,
                DatetimePropertiesSpec,
                DatetimeMethodsSpec,
                DatePropertiesSpec,
                DateMethodsSpec,
                TimePropertiesSpec,
                TimeMethodsSpec,
                TimedeltaPropertiesSpec,
                TimedeltaMethodsSpec,
                AddDatetimeTimedeltaSpec,
                AddDateTimedeltaSpec,
                SubDatetimeTimedeltaSpec,
                SubDateTimedeltaSpec,
                SubDatesSpec,
                SubDatetimesSpec,
                MulTimedeltaNumberSpec,
                MulNumberTimedeltaSpec,
                DivTimedeltaNumberSpec,
                DivTimedeltasSpec,
                FloorDivTimedeltasSpec>;

        using time_series_property_node_specs =
            CppNodeSpecList<
                ModifiedImplSpec,
                LastModifiedTimeImplSpec,
                LastModifiedDateImplSpec,
                EvaluationTimeInRangeDateTimeSpec>;

        using bound_node_specs = CppNodeSpecListConcat_t<
            graph_operator_node_specs,
            core_node_specs,
            access_node_specs,
            number_node_specs,
            scalar_node_specs,
            string_node_specs,
            bool_node_specs,
            enum_node_specs,
            tuple_node_specs,
            flow_control_node_specs,
            date_time_node_specs,
            type_node_specs,
            time_series_property_node_specs>;

        inline void bind_node_builders(nb::module_& m) {
            bind_cpp_node_builder_factories(m, bound_node_specs{});
        }
    }  // namespace ops
}  // namespace hgraph
