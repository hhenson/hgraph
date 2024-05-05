
#include <catch2/catch_test_macros.hpp>

#include <hgraph/types/node.h>
#include <hgraph/util/lifecycle.h>
#include <pybind11/embed.h>
#include <iostream>

TEST_CASE("Test node signature", "[graph_node]") {
    using namespace hgraph;
    py::scoped_interpreter guard{};

    NodeSignature node_signature{};
    node_signature.name = "test";
    node_signature.args = {"ts", "s"};
    node_signature.time_series_inputs = {{"ts", py::str("TS[int]")}};
    node_signature.scalars = {{"s", py::str("int")}};
    node_signature.time_series_output = py::str("TS[str]");
    node_signature.injectable_inputs = InjectableTypesEnum::SCHEDULER | InjectableTypesEnum::CLOCK;

    REQUIRE(node_signature.uses_scheduler());

    REQUIRE(node_signature.signature() == "test(ts: TS[int], s: int) -> TS[str]");
}

struct MockLifecycle : hgraph::ComponentLifeCycle {
protected:
    void initialise() override { std::cout << "Initialise: " << is_initialising() << std::endl; }

    void start() override {std::cout << "Start: " << is_starting() << std::endl;}

    void stop() override {std::cout << "Stop: " << is_stopping() <<  std::endl;}

    void dispose() override {std::cout << "Dispose: " << is_disposing() << std::endl;}
};

TEST_CASE("Test the life-cycle component", "[graph_node]") {
    using namespace hgraph;
    MockLifecycle mock{};
    REQUIRE_FALSE(mock.is_initialised());
    REQUIRE_FALSE(mock.is_initialising());
    REQUIRE_FALSE(mock.is_disposing());
    REQUIRE_FALSE(mock.is_started());
    REQUIRE_FALSE(mock.is_starting());
    REQUIRE_FALSE(mock.is_stopping());

    initialise_component(mock);
    REQUIRE(mock.is_initialised());
    REQUIRE_FALSE(mock.is_initialising());
    REQUIRE_FALSE(mock.is_disposing());
    REQUIRE_FALSE(mock.is_started());
    REQUIRE_FALSE(mock.is_starting());
    REQUIRE_FALSE(mock.is_stopping());

    start_component(mock);
    REQUIRE(mock.is_initialised());
    REQUIRE_FALSE(mock.is_initialising());
    REQUIRE_FALSE(mock.is_disposing());
    REQUIRE(mock.is_started());
    REQUIRE_FALSE(mock.is_starting());
    REQUIRE_FALSE(mock.is_stopping());

    stop_component(mock);
    REQUIRE(mock.is_initialised());
    REQUIRE_FALSE(mock.is_initialising());
    REQUIRE_FALSE(mock.is_disposing());
    REQUIRE_FALSE(mock.is_started());
    REQUIRE_FALSE(mock.is_starting());
    REQUIRE_FALSE(mock.is_stopping());

    dispose_component(mock);
    REQUIRE_FALSE(mock.is_initialised());
    REQUIRE_FALSE(mock.is_initialising());
    REQUIRE_FALSE(mock.is_disposing());
    REQUIRE_FALSE(mock.is_started());
    REQUIRE_FALSE(mock.is_starting());
    REQUIRE_FALSE(mock.is_stopping());
}