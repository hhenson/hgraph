#include <hgraph/lib/std/std_operators.h>
#include <hgraph/runtime/evaluation_trace.h>
#include <hgraph/runtime/executor.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <string>
#include <string_view>
#include <vector>

namespace
{
    using namespace hgraph;

    struct TraceAddOne
    {
        static constexpr auto name = "trace_add_one";

        static void eval(In<"ts", TS<Int>> ts, Out<TS<Int>> out)
        {
            out.set(ts.value() + 1);
        }
    };

    [[nodiscard]] std::vector<std::string> run_trace(EvaluationTraceOptions options)
    {
        stdlib::register_standard_operators();
        std::vector<std::string> lines;
        EvaluationTrace trace{std::move(options), [&](std::string_view line) {
                                  lines.emplace_back(line);
                              }};

        Wiring wiring;
        auto input = wire<stdlib::const_>(wiring, Int{41}).as<TS<Int>>();
        auto output = wire<TraceAddOne>(wiring, input);
        static_cast<void>(wire<stdlib::null_sink>(wiring, output));
        GraphBuilder graph = std::move(wiring).finish();
        graph.label("trace_graph");

        GraphExecutorBuilder executor_builder;
        executor_builder.graph_builder(std::move(graph)).add_lifecycle_observer(&trace);
        GraphExecutorValue executor = executor_builder.make_executor();
        executor.view().run();
        return lines;
    }

    [[nodiscard]] std::string joined(const std::vector<std::string> &lines)
    {
        std::string result;
        for (const auto &line : lines)
        {
            result += line;
            result += '\n';
        }
        return result;
    }
}  // namespace

TEST_CASE("evaluation trace: native observer renders graph and node evaluation")
{
    EvaluationTraceOptions options;
    options.start = false;
    options.stop = false;

    const std::string trace = joined(run_trace(std::move(options)));
    CHECK_THAT(trace, Catch::Matchers::ContainsSubstring("Eval Start trace_graph"));
    CHECK_THAT(trace, Catch::Matchers::ContainsSubstring("trace_add_one"));
    CHECK_THAT(trace, Catch::Matchers::ContainsSubstring("*inputs*="));
    CHECK_THAT(trace, Catch::Matchers::ContainsSubstring("41"));
    CHECK_THAT(trace, Catch::Matchers::ContainsSubstring(" *->* 42 [OUT]"));
    CHECK_THAT(trace, Catch::Matchers::ContainsSubstring("Eval Done"));
    CHECK_THAT(trace, !Catch::Matchers::ContainsSubstring("Starting Graph"));
    CHECK_THAT(trace, !Catch::Matchers::ContainsSubstring("Stopped node"));
}

TEST_CASE("evaluation trace: filter restricts output to matching node paths")
{
    EvaluationTraceOptions options;
    options.filter = "trace_add_one";

    const auto lines = run_trace(std::move(options));
    REQUIRE_FALSE(lines.empty());
    for (const auto &line : lines)
    {
        CHECK_THAT(line, Catch::Matchers::ContainsSubstring("trace_add_one"));
    }
}
