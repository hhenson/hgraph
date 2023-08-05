//
// Created by Howard Henson on 23/04/2023.
//

#include <catch2/catch_test_macros.hpp>
#include <hg/graph/data_types/ts.h>

TEST_CASE("Construct TimeSeriesScalarValueInputGenerator", "[generator]") {
    using namespace hg;
    auto generator{std::make_shared<TimeSeriesScalarValueInputGenerator>()};
    Node* node_{};
    auto ts{make_unique_ptr_from_generator<Input>(generator, node_)};
    REQUIRE((bool)ts);
}

TEST_CASE("Construct TimeSeriesScalarValueOutputGenerator", "[generator]") {
    using namespace hg;
    auto generator{std::make_shared<TimeSeriesScalarValueOutputGenerator>()};
    Output* parent{};
    auto ts{make_unique_ptr_from_generator<Output>(generator, parent)};
    REQUIRE((bool)ts);
}

TEST_CASE("Construct TimeSeriesScalarValuePullQueueGenerator", "[generator]") {
    using namespace hg;
    auto generator{std::make_shared<TimeSeriesScalarValuePullQueueGenerator>()};
    Node* node_{};
    auto ts{make_unique_ptr_from_generator<Queue>(generator, node_)};
    REQUIRE((bool)ts);
}

TEST_CASE("Construct NamedCollectionOutputGenerator", "[generator]") {
    using namespace hg;
    std::vector<std::string>            element_names_{};
    std::vector<OutputGenerator::s_ptr> element_generators_{};
    auto generator{std::make_shared<NamedCollectionOutputGenerator>(element_names_, element_generators_)};
    Node::ptr owning_node{};
    auto ts{make_unique_ptr_from_generator<Output>(generator, owning_node)};
    REQUIRE((bool)ts);
}

TEST_CASE("Construct UnBoundNamedCollectionInputGenerator", "[generator]") {
    using namespace hg;
    std::vector<std::string>           element_names_{};
    std::vector<InputGenerator::s_ptr> element_generators_{};
    auto generator{std::make_shared<UnBoundNamedCollectionInputGenerator>(element_names_, element_generators_)};
    auto ts{make_unique_ptr_from_generator<Input>(generator, nullptr)};
    REQUIRE((bool)ts);
}

TEST_CASE("Construct NamedCollectionQueueGenerator", "[generator]") {
    using namespace hg;
    std::vector<std::string>           element_names_{};
    std::vector<QueueGenerator::s_ptr> element_generators_{};
    auto generator{std::make_shared<NamedCollectionQueueGenerator>(element_names_, element_generators_)};
    auto ts{make_unique_ptr_from_generator<Queue>(generator, nullptr)};
    REQUIRE((bool)ts);
}

namespace hg
{
    class SimpleNode : public PullSourceNode
    {

      public:
        SimpleNode(size_t id, Graph *owning_graph, ResolvedNodeSignature::s_ptr signature)
            : PullSourceNode(id, owning_graph, std::move(signature)) {}
    };

    class SimpleNodeGenerator : public BasicNodeGenerator<SimpleNode, NodeTypeEnum::PULL_SOURCE_NODE>
    {
      public:
        using s_ptr = std::shared_ptr<SimpleNodeGenerator>;
        using BasicNodeGenerator<SimpleNode, NodeTypeEnum::PULL_SOURCE_NODE>::BasicNodeGenerator;

        static SimpleNodeGenerator::s_ptr create_generator() {
            ResolvedNodeSignature s{
                NodeTypeEnum::GENERATOR_NODE,
                "simple_node",
                "",
                {},
                {"out"},
                NamedCollectionQueueGenerator::make_generator(
                    {{"out", std::make_shared<TimeSeriesScalarValuePullQueueGenerator>()}}),
                {},
                NamedCollectionOutputGenerator::make_generator({{"out", std::make_shared<TimeSeriesScalarValueOutputGenerator>()}}),
                {}};
            auto signature{std::make_shared<ResolvedNodeSignature>(std::move(s))};
            return std::make_shared<SimpleNodeGenerator>(signature);
        }
    };
}

TEST_CASE("Construct BasicNodeGenerator", "[generator]") {
    using namespace hg;

    size_t node_id{};
    Graph* graph_{};
    auto generator{SimpleNodeGenerator::create_generator()};
    auto ts{make_unique_ptr_from_generator<Node>(generator, node_id, graph_)};
    REQUIRE((bool)ts);
}

TEST_CASE("Construct GraphGenerator", "[generator]") {
    using namespace hg;
    std::vector<node_generator_ptr> node_generators{SimpleNodeGenerator::create_generator()};
    std::vector<Edge> edges{};
    auto generator{std::make_shared<GraphGenerator>(node_generators, edges)};
    GraphExecutor* graph_{};
    auto ts{make_unique_ptr_from_generator<Graph>(generator, graph_)};
    REQUIRE((bool)ts);
}