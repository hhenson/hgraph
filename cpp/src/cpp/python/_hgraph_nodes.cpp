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
#include <hgraph/nodes/v2/basic_nodes.h>
#include <hgraph/nodes/last_value_pull_node.h>
#include <hgraph/nodes/context_node.h>
#include <hgraph/nodes/python_generator_node.h>
#include <hgraph/nodes/push_queue_node.h>
#include <hgraph/types/v2/evaluation_engine.h>
#include <hgraph/types/v2/graph_builder.h>
#include <hgraph/types/v2/python_export.h>

namespace
{
    using hgraph::TSMeta;
    using hgraph::engine_time_t;
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

    struct StaticClockNode
    {
        StaticClockNode() = delete;
        ~StaticClockNode() = delete;

        static constexpr auto name = "static_clock";

        static void eval(In<"lhs", TS<int>> lhs, EvaluationClock clock, Out<TS<int>> out)
        {
            static_cast<void>(clock);
            out.set(lhs.value());
        }
    };

    struct StaticTickNode
    {
        StaticTickNode() = delete;
        ~StaticTickNode() = delete;

        static constexpr auto name = "static_tick";
        static constexpr auto node_type = NodeTypeEnum::PULL_SOURCE_NODE;

        static void start(Graph &graph, Node &node, engine_time_t evaluation_time)
        {
            graph.schedule_node(node.node_index(), evaluation_time, true);
        }

        static void eval(Out<TS<int>> out)
        {
            out.set(42);
        }
    };

    struct StaticSinkNode
    {
        StaticSinkNode() = delete;
        ~StaticSinkNode() = delete;

        static constexpr auto name = "static_sink";
        static constexpr auto node_type = NodeTypeEnum::SINK_NODE;

        static inline int call_count{0};
        static inline int last_value{0};

        static void reset()
        {
            call_count = 0;
            last_value = 0;
        }

        static void eval(In<"ts", TS<int>> ts)
        {
            ++call_count;
            last_value = ts.value();
        }
    };

    [[nodiscard]] EvaluationMode normalize_evaluation_mode(nb::handle value)
    {
        nb::object mode = nb::borrow(value);
        if (nb::hasattr(mode, "name")) {
            const std::string name = nb::cast<std::string>(mode.attr("name"));
            if (name == "REAL_TIME") { return EvaluationMode::REAL_TIME; }
            if (name == "SIMULATION") { return EvaluationMode::SIMULATION; }
        }

        const int mode_value = nb::cast<int>(mode);
        switch (mode_value) {
            case static_cast<int>(EvaluationMode::REAL_TIME): return EvaluationMode::REAL_TIME;
            case static_cast<int>(EvaluationMode::SIMULATION): return EvaluationMode::SIMULATION;
            default: throw std::invalid_argument(fmt::format("Unknown v2 evaluation mode {}", mode_value));
        }
    }

    [[nodiscard]] nb::object object_or_none(const nb::object &value)
    {
        return value.is_valid() ? nb::borrow(value) : nb::none();
    }

    [[nodiscard]] nb::dict dict_or_empty(const nb::object &value)
    {
        if (value.is_valid() && !value.is_none()) { return nb::cast<nb::dict>(value); }
        return nb::dict();
    }

    struct V2GraphExecutor
    {
        V2GraphExecutor(GraphBuilder graph_builder, nb::object run_mode, std::vector<nb::object> observers, bool cleanup_on_error)
            : m_graph_builder(std::move(graph_builder)),
              m_run_mode(normalize_evaluation_mode(run_mode)),
              m_cleanup_on_error(cleanup_on_error)
        {
            if (!observers.empty()) {
                throw std::invalid_argument("v2 Python execution does not yet support Python lifecycle observers");
            }
        }

        [[nodiscard]] EvaluationMode run_mode() const noexcept { return m_run_mode; }

        [[nodiscard]] nb::object graph() const
        {
            throw std::logic_error("v2 Python graph access is not exposed yet; only execution is supported");
        }

        void run(engine_time_t start_time, engine_time_t end_time)
        {
            static_cast<void>(m_cleanup_on_error);
            auto engine = EvaluationEngineBuilder{}
                              .graph_builder(m_graph_builder)
                              .evaluation_mode(m_run_mode)
                              .start_time(start_time)
                              .end_time(end_time)
                              .build();
            engine.run();
        }

      private:
        GraphBuilder m_graph_builder;
        EvaluationMode m_run_mode{EvaluationMode::SIMULATION};
        bool m_cleanup_on_error{true};
    };

}

void export_nodes(nb::module_ &m) {
    using namespace hgraph;
    auto v2 = m.def_submodule("v2", "Experimental v2 static node exports");
    nb::class_<hgraph::v2::NodeBuilder>(v2, "NodeBuilder")
        .def(
            "make_instance",
            [](const hgraph::v2::NodeBuilder &self, nb::tuple owning_graph_id, int64_t node_ndx) -> nb::object {
                static_cast<void>(self);
                static_cast<void>(owning_graph_id);
                static_cast<void>(node_ndx);
                throw std::logic_error(
                    "v2 NodeBuilder only supports execution through _hgraph.v2.GraphBuilder and _hgraph.v2.GraphExecutor");
            },
            "owning_graph_id"_a,
            "node_ndx"_a)
        .def("release_instance",
             [](const hgraph::v2::NodeBuilder &self, nb::handle item) {
                 static_cast<void>(self);
                 static_cast<void>(item);
             },
             "item"_a)
        .def_prop_ro("signature", [](const hgraph::v2::NodeBuilder &self) { return object_or_none(self.signature()); })
        .def_prop_ro("scalars", [](const hgraph::v2::NodeBuilder &self) { return dict_or_empty(self.scalars()); })
        .def_prop_ro("input_builder",
                     [](const hgraph::v2::NodeBuilder &self) { return object_or_none(self.input_builder()); })
        .def_prop_ro("output_builder",
                     [](const hgraph::v2::NodeBuilder &self) { return object_or_none(self.output_builder()); })
        .def_prop_ro("error_builder",
                     [](const hgraph::v2::NodeBuilder &self) { return object_or_none(self.error_builder()); })
        .def_prop_ro(
            "recordable_state_builder",
            [](const hgraph::v2::NodeBuilder &self) { return object_or_none(self.recordable_state_builder()); })
        .def_prop_ro("implementation_name", [](const hgraph::v2::NodeBuilder &self) { return self.implementation_name(); })
        .def_prop_ro(
            "input_schema",
            [](const hgraph::v2::NodeBuilder &self) -> nb::object {
                return self.input_schema() != nullptr ? nb::cast(self.input_schema(), nb::rv_policy::reference) : nb::none();
            })
        .def_prop_ro(
            "output_schema",
            [](const hgraph::v2::NodeBuilder &self) -> nb::object {
                return self.output_schema() != nullptr ? nb::cast(self.output_schema(), nb::rv_policy::reference) : nb::none();
            })
        .def_prop_ro(
            "requires_resolved_schemas",
            [](const hgraph::v2::NodeBuilder &self) { return self.requires_resolved_schemas(); })
        .def(
            "__repr__",
            [](const hgraph::v2::NodeBuilder &self) {
                return fmt::format("v2.NodeBuilder@{:p}[impl={}]", static_cast<const void *>(&self), self.implementation_name());
            })
        .def("__str__",
             [](const hgraph::v2::NodeBuilder &self) {
                 return fmt::format("v2.NodeBuilder@{:p}[impl={}]", static_cast<const void *>(&self), self.implementation_name());
             });

    nb::class_<hgraph::v2::Edge>(v2, "Edge")
        .def(
            nb::init<int64_t, hgraph::v2::Path, int64_t, hgraph::v2::Path>(),
            "src_node"_a,
            "output_path"_a = hgraph::v2::Path{},
            "dst_node"_a,
            "input_path"_a = hgraph::v2::Path{})
        .def_rw("src_node", &hgraph::v2::Edge::src_node)
        .def_rw("output_path", &hgraph::v2::Edge::output_path)
        .def_rw("dst_node", &hgraph::v2::Edge::dst_node)
        .def_rw("input_path", &hgraph::v2::Edge::input_path)
        .def("__eq__", &hgraph::v2::Edge::operator==)
        .def("__lt__", &hgraph::v2::Edge::operator<)
        .def(
            "__hash__",
            [](const hgraph::v2::Edge &self) {
                size_t hash = std::hash<int64_t>{}(self.src_node);
                hash ^= std::hash<int64_t>{}(self.dst_node) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
                for (const auto slot : self.output_path) {
                    hash ^= std::hash<int64_t>{}(slot) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
                }
                for (const auto slot : self.input_path) {
                    hash ^= std::hash<int64_t>{}(slot) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
                }
                return hash;
            });

    nb::class_<hgraph::v2::GraphBuilder>(v2, "GraphBuilder")
        .def(nb::init<>())
        .def(nb::init<std::vector<hgraph::v2::NodeBuilder>, std::vector<hgraph::v2::Edge>>(), "node_builders"_a, "edges"_a)
        .def(
            "add_node",
            [](hgraph::v2::GraphBuilder &self, const hgraph::v2::NodeBuilder &node_builder) -> hgraph::v2::GraphBuilder & {
                return self.add_node(node_builder);
            },
            "node_builder"_a,
            nb::rv_policy::reference_internal)
        .def(
            "add_edge",
            [](hgraph::v2::GraphBuilder &self, const hgraph::v2::Edge &edge) -> hgraph::v2::GraphBuilder & {
                return self.add_edge(edge);
            },
            "edge"_a,
            nb::rv_policy::reference_internal)
        .def(
            "make_instance",
            [](const hgraph::v2::GraphBuilder &self, nb::tuple graph_id, nb::handle parent_node, std::string label) -> nb::object {
                static_cast<void>(self);
                static_cast<void>(graph_id);
                static_cast<void>(parent_node);
                static_cast<void>(label);
                throw std::logic_error(
                    "v2 GraphBuilder only supports execution through _hgraph.v2.GraphExecutor");
            },
            "graph_id"_a,
            "parent_node"_a = nb::none(),
            "label"_a = "")
        .def(
            "make_and_connect_nodes",
            [](const hgraph::v2::GraphBuilder &self, nb::tuple graph_id, int64_t first_node_ndx) -> nb::object {
                static_cast<void>(self);
                static_cast<void>(graph_id);
                static_cast<void>(first_node_ndx);
                throw std::logic_error(
                    "v2 GraphBuilder only supports execution through _hgraph.v2.GraphExecutor");
            },
            "graph_id"_a,
            "first_node_ndx"_a)
        .def("release_instance",
             [](const hgraph::v2::GraphBuilder &self, nb::handle item) {
                 static_cast<void>(self);
                 static_cast<void>(item);
             },
             "item"_a)
        .def_prop_ro("size", &hgraph::v2::GraphBuilder::size)
        .def_prop_ro("alignment", &hgraph::v2::GraphBuilder::alignment)
        .def_prop_ro("node_builders",
                     [](const hgraph::v2::GraphBuilder &self) { return nb::tuple(nb::cast(self.node_builders())); })
        .def_prop_ro("edges", [](const hgraph::v2::GraphBuilder &self) { return nb::tuple(nb::cast(self.edges())); })
        .def(
            "__repr__",
            [](const hgraph::v2::GraphBuilder &self) {
                return fmt::format(
                    "v2.GraphBuilder@{:p}[nodes={}, edges={}]",
                    static_cast<const void *>(&self),
                    self.node_builders().size(),
                    self.edges().size());
            })
        .def("__str__",
             [](const hgraph::v2::GraphBuilder &self) {
                 return fmt::format(
                     "v2.GraphBuilder@{:p}[nodes={}, edges={}]",
                     static_cast<const void *>(&self),
                     self.node_builders().size(),
                     self.edges().size());
             });

    nb::enum_<hgraph::v2::EvaluationMode>(v2, "EvaluationMode")
        .value("REAL_TIME", hgraph::v2::EvaluationMode::REAL_TIME)
        .value("SIMULATION", hgraph::v2::EvaluationMode::SIMULATION)
        .export_values();

    nb::class_<hgraph::v2::EvaluationEngine>(v2, "EvaluationEngine")
        .def_prop_ro("evaluation_mode", &hgraph::v2::EvaluationEngine::evaluation_mode)
        .def_prop_ro("start_time", &hgraph::v2::EvaluationEngine::start_time)
        .def_prop_ro("end_time", &hgraph::v2::EvaluationEngine::end_time)
        .def("run", &hgraph::v2::EvaluationEngine::run);

    nb::class_<V2GraphExecutor>(v2, "GraphExecutor")
        .def(
            nb::init<hgraph::v2::GraphBuilder, nb::object, std::vector<nb::object>, bool>(),
            "graph_builder"_a,
            "run_mode"_a,
            "observers"_a = std::vector<nb::object>{},
            "cleanup_on_error"_a = true)
        .def_prop_ro("run_mode", &V2GraphExecutor::run_mode)
        .def_prop_ro("graph", &V2GraphExecutor::graph)
        .def("run", &V2GraphExecutor::run, "start_time"_a, "end_time"_a);

    nb::class_<hgraph::v2::EvaluationEngineBuilder>(v2, "EvaluationEngineBuilder")
        .def(nb::init<>())
        .def(
            "graph_builder",
            [](hgraph::v2::EvaluationEngineBuilder &self,
               const hgraph::v2::GraphBuilder &graph_builder) -> hgraph::v2::EvaluationEngineBuilder & {
                return self.graph_builder(graph_builder);
            },
            "graph_builder"_a,
            nb::rv_policy::reference_internal)
        .def(
            "evaluation_mode",
            [](hgraph::v2::EvaluationEngineBuilder &self,
               hgraph::v2::EvaluationMode evaluation_mode) -> hgraph::v2::EvaluationEngineBuilder & {
                return self.evaluation_mode(evaluation_mode);
            },
            "evaluation_mode"_a,
            nb::rv_policy::reference_internal)
        .def(
            "start_time",
            [](hgraph::v2::EvaluationEngineBuilder &self,
               engine_time_t start_time) -> hgraph::v2::EvaluationEngineBuilder & { return self.start_time(start_time); },
            "start_time"_a,
            nb::rv_policy::reference_internal)
        .def(
            "end_time",
            [](hgraph::v2::EvaluationEngineBuilder &self,
               engine_time_t end_time) -> hgraph::v2::EvaluationEngineBuilder & { return self.end_time(end_time); },
            "end_time"_a,
            nb::rv_policy::reference_internal)
        .def("build", &hgraph::v2::EvaluationEngineBuilder::build);

    v2.def("reset_static_sink_state", &StaticSinkNode::reset);
    v2.def("static_sink_call_count", [] { return StaticSinkNode::call_count; });
    v2.def("static_sink_last_value", [] { return StaticSinkNode::last_value; });

    hgraph::v2::export_compute_node<StaticSumNode>(v2);
    hgraph::v2::export_compute_node<StaticPolicyNode>(v2);
    hgraph::v2::export_compute_node<StaticGetItemNode>(v2);
    hgraph::v2::export_compute_node<StaticTypedStateNode>(v2);
    hgraph::v2::export_compute_node<StaticRecordableStateNode>(v2);
    hgraph::v2::export_compute_node<StaticClockNode>(v2);
    hgraph::v2::export_compute_node<StaticTickNode>(v2);
    hgraph::v2::export_compute_node<StaticSinkNode>(v2);
    hgraph::v2::export_compute_node_from_python_impl<hgraph::nodes::v2::ConstNode>(
        v2, "hgraph._impl._operators._time_series_conversion", "const_default", "const");
    hgraph::v2::export_compute_node_from_python_impl<hgraph::nodes::v2::NothingNode>(
        v2, "hgraph._impl._operators._graph_operators", "nothing_impl", "nothing");
    hgraph::v2::export_compute_node_from_python_impl<hgraph::nodes::v2::NullSinkNode>(
        v2, "hgraph._impl._operators._graph_operators", "null_sink_impl", "null_sink");
    hgraph::v2::export_compute_node_from_python_impl<hgraph::nodes::v2::DebugPrintNode>(
        v2, "hgraph._impl._operators._graph_operators", "debug_print_impl", "debug_print");
}
