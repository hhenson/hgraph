#include <catch2/catch_test_macros.hpp>

#include <hg/components/core.h>
#include <hg/graph/engine/back_test_engine.h>
#include <hg/graph/graph.h>
#include <hg/graph/wiring/graph_wiring.h>

#include <memory>

TEST_CASE("Test base graph data structures", "[graph]") {
    using namespace hg;

    ConstGenerator::s_ptr const_world{ConstGenerator::ts_const(create_scalar_value(std::string("World")))};
    LogGenerator::s_ptr log_hello{LogGenerator::ts_log("Hello")};

    std::vector<NodeGenerator::s_ptr> node_generators{const_world, log_hello};
    std::vector<Edge> edges{};
    edges.emplace_back(Edge{0, {0}, 1, {0}});
    BackTestGraphExecutor ge{std::make_shared<GraphGenerator>(node_generators, edges)};
    ge.run();
}

TEST_CASE("Test curve graph data structures", "[graph]") {
    using namespace hg;

//    std::vector<std::tuple<engine_time_t, ScalarValue>> curve{};
//    curve.emplace_back(MIN_ST, create_scalar_value<std::string>("1"));
//    curve.emplace_back(MIN_ST+MIN_TD, create_scalar_value<std::string>("2"));
//    curve.emplace_back(MIN_ST+MIN_TD*2, create_scalar_value<std::string>("3"));
//
//    std::vector<std::function<Node::ptr (size_t, Graph *)>> node_generators{};
//    node_generators.emplace_back([&curve](size_t ndx, Graph *graph) -> Node::u_ptr { return std::make_unique<Curve<TS>>(ndx, graph, curve); });
//    node_generators.emplace_back([](size_t ndx, Graph *graph) -> Node::u_ptr { return std::make_unique<Log<TS>>(ndx, graph, "Count"); });
//
//    Graph::u_ptr g{Graph::make_graph(node_generators)};
//    boost::polymorphic_downcast<BoundInput *>(boost::polymorphic_downcast<EvaluableNode *>(g->nodes()[1].node.get())->input())
//        ->bind_output(boost::polymorphic_downcast<SourceNode *>(g->nodes()[0].node.get())->output());
//
//    BackTestGraphExecutor ge{std::move(g)};
//    ge.run();
}
