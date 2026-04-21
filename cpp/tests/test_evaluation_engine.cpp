#include <catch2/catch_test_macros.hpp>

#include <hgraph/runtime/evaluation_engine.h>

#include <memory>

namespace hgraph
{
    TEST_CASE("can create an empty evaluation engine")
    {
        const auto start_time = MIN_ST;
        const auto end_time = start_time + MIN_TD;

        auto clock = std::make_shared<SimulationEvaluationClock>(start_time);
        auto engine = std::make_shared<EvaluationEngineImpl>(clock, start_time, end_time, EvaluationMode::SIMULATION);

        REQUIRE(engine != nullptr);
        REQUIRE(engine->evaluation_clock() != nullptr);
    }
}  // namespace hgraph
