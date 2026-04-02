#include <hgraph/nodes/nest_graph_node.h>
#include <hgraph/nodes/nested_node.h>
#include <hgraph/nodes/nested_evaluation_engine.h>
#include <hgraph/nodes/tsd_map_node.h>
#include <hgraph/nodes/reduce_node.h>
#include <hgraph/nodes/component_node.h>
#include <hgraph/nodes/switch_node.h>
#include <hgraph/nodes/try_except_node.h>
#include <hgraph/nodes/non_associative_reduce_node.h>
#include <hgraph/nodes/mesh_node.h>
#include <hgraph/nodes/last_value_pull_node.h>
#include <hgraph/nodes/context_node.h>
#include <hgraph/nodes/python_generator_node.h>
#include <hgraph/nodes/push_queue_node.h>
#include <hgraph/types/v2/python_export.h>

namespace
{
    using namespace hgraph::v2;

    struct StaticSumNode
    {
        StaticSumNode() = delete;
        ~StaticSumNode() = delete;

        static constexpr auto name = "static_sum";

        static void eval(In<"lhs", TS<int>> lhs, In<"rhs", TS<int>> rhs, Out<TS<int>> out)
        {
            out.set(lhs.value() + rhs.value());
        }
    };

    struct StaticPolicyNode
    {
        StaticPolicyNode() = delete;
        ~StaticPolicyNode() = delete;

        static constexpr auto name = "static_policy";

        static void eval(In<"lhs", TS<int>> lhs,
                         In<"rhs", TS<int>, InputActivity::Passive, InputValidity::Unchecked> rhs,
                         In<"strict", TS<int>, InputActivity::Active, InputValidity::AllValid> strict,
                         Out<TS<int>> out)
        {
            out.set(lhs.value() + rhs.value() + strict.value());
        }
    };

    struct StaticGetItemNode
    {
        StaticGetItemNode() = delete;
        ~StaticGetItemNode() = delete;

        static constexpr auto name = "static_get_item";

        using K = ScalarVar<"K">;
        using V = TsVar<"V">;

        static void eval(In<"ts", TSD<K, V>> ts, In<"key", TS<K>> key, Out<V> out)
        {
            static_cast<void>(ts);
            static_cast<void>(key);
            static_cast<void>(out);
        }
    };

    struct StaticTypedStateNode
    {
        StaticTypedStateNode() = delete;
        ~StaticTypedStateNode() = delete;

        static constexpr auto name = "static_typed_state";

        static void start(State<int> state)
        {
            state.view().set_scalar(0);
        }

        static void eval(In<"lhs", TS<int>> lhs, State<int> state, Out<TS<int>> out)
        {
            auto &sum = state.view().template checked_as<int>();
            sum += lhs.value();
            out.set(sum);
        }
    };

    struct StaticRecordableStateNode
    {
        StaticRecordableStateNode() = delete;
        ~StaticRecordableStateNode() = delete;

        static constexpr auto name = "static_recordable_state";
        using LocalState = TSB<Field<"last", TS<int>>>;

        static void eval(In<"lhs", TS<int>> lhs, RecordableState<LocalState> state, Out<TS<int>> out)
        {
            auto last = state.view().field("last");
            if (last.valid()) { out.set(last.value().as_atomic().as<int>()); }
            last.value().set_scalar(lhs.value());
            state.mark_modified(last);
        }
    };
}

void export_nodes(nb::module_ &m) {
    using namespace hgraph;
    auto v2 = m.def_submodule("v2", "Experimental v2 static node exports");
    hgraph::v2::export_compute_node<StaticSumNode>(v2);
    hgraph::v2::export_compute_node<StaticPolicyNode>(v2);
    hgraph::v2::export_compute_node<StaticGetItemNode>(v2);
    hgraph::v2::export_compute_node<StaticTypedStateNode>(v2);
    hgraph::v2::export_compute_node<StaticRecordableStateNode>(v2);
}
