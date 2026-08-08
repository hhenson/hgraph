#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/runtime/diagnostic_path.h>
#include <hgraph/runtime/logger.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_registry.h>

#include <spdlog/sinks/ringbuffer_sink.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <memory>
#include <string>
#include <vector>

// The LOGGER injectable (ruling 2026-07-04: spdlog behind LoggerView) and
// the log_ operator over it.

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    struct CapturedLog
    {
        std::shared_ptr<spdlog::sinks::ringbuffer_sink_mt> sink;

        CapturedLog() : sink(std::make_shared<spdlog::sinks::ringbuffer_sink_mt>(16))
        {
            log::set_logger(std::make_shared<spdlog::logger>("hgraph-test", sink));
        }

        ~CapturedLog() { log::set_logger(nullptr); }

        [[nodiscard]] std::string joined() const
        {
            std::string all;
            for (const auto &line : sink->last_formatted()) { all += line; }
            return all;
        }
    };

    struct LoggingNode
    {
        static constexpr auto name = "logging_node";

        static void start(LoggerView log) { log.info("logging node started"); }

        static void eval(In<"ts", TS<Int>> ts, LoggerView log, Out<TS<Int>> out)
        {
            log.warn("tick value {}", ts.value());
            out.set(ts.value());
        }
    };

    struct LoggerRunGraph
    {
        static constexpr auto name = "logger_run_graph";

        static void compose(Wiring &w)
        {
            auto source = wire<stdlib::const_>(w, Int{7}).as<TS<Int>>();
            auto output = wire<LoggingNode>(w, source);
            static_cast<void>(wire<stdlib::null_sink>(w, output));
        }
    };

    class CapturedContextLogger final : public spdlog::logger, public ContextualLogger
    {
      public:
        explicit CapturedContextLogger(
            const std::shared_ptr<spdlog::sinks::ringbuffer_sink_mt> &sink)
            : spdlog::logger("hgraph-run-test", sink)
        {
        }

        void log_with_context(spdlog::level::level_enum level,
                              std::string_view message,
                              NodePtr node) override
        {
            paths.push_back(diagnostic::node_path(NodeView{node}));
            spdlog::logger::log(
                spdlog::source_loc{}, level,
                spdlog::string_view_t{message.data(), message.size()});
        }

        std::vector<std::string> paths;
    };

    struct QuietLogGraph
    {
        [[maybe_unused]] static constexpr auto name = "quiet_log_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            auto fmt_source = wire<stdlib::const_>(w, Str{"quiet {}"});
            wire<stdlib::log_>(w, fmt_source.as<TS<Str>>(), ts);   // default level: info
            return ts;
        }
    };

    struct LogOperatorGraph
    {
        [[maybe_unused]] static constexpr auto name = "log_operator_graph";

        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> ts)
        {
            auto fmt_source = wire<stdlib::const_>(w, Str{"observed {}"});
            wire<stdlib::log_>(w, fmt_source.as<TS<Str>>(), ts, arg<"level">(Int{4}));   // error level
            return ts;
        }
    };

    struct VariadicLogGraph
    {
        static constexpr auto name = "variadic_log_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Str>> text, Port<TS<Int>> number)
        {
            auto positional = wire<stdlib::const_>(w, Str{"positional {} {}"}).as<TS<Str>>();
            wire<stdlib::log_>(w, positional, text, number, arg<"level">(Int{40}));

            auto named = wire<stdlib::const_>(w, Str{"named {text} {number}"}).as<TS<Str>>();
            wire<stdlib::log_>(w, named, arg<"text">(text), arg<"number">(number),
                               arg<"level">(Int{40}));
            return text;
        }
    };

    struct EmptyLogGraph
    {
        static constexpr auto name = "empty_log_graph";

        static Port<TS<Int>> compose(Wiring &w)
        {
            auto value = wire<stdlib::const_>(w, Int{1}).as<TS<Int>>();
            auto format = wire<stdlib::const_>(w, Str{"no arguments"}).as<TS<Str>>();
            wire<stdlib::log_>(w, format, arg<"level">(Int{40}));
            return value;
        }
    };

    struct SampledLogGraph
    {
        static constexpr auto name = "sampled_log_graph";

        static Port<TS<Str>> compose(Wiring &w, Port<TS<Str>> text)
        {
            auto format = wire<stdlib::const_>(w, Str{"sampled {}"}).as<TS<Str>>();
            wire<stdlib::log_>(w, format, text, arg<"level">(Int{40}),
                               arg<"sample_count">(Int{3}));
            return text;
        }
    };
}  // namespace

TEST_CASE("logger: the LoggerView injectable logs from start and eval hooks")
{
    stdlib::register_standard_operators();
    CapturedLog captured;

    CHECK_OUTPUT(eval_node<LoggingNode>(values<Int>(7, none, 9)), values<Int>(7, none, 9));

    const std::string all = captured.joined();
    CHECK_THAT(all, Catch::Matchers::ContainsSubstring("logging node started"));
    CHECK_THAT(all, Catch::Matchers::ContainsSubstring("tick value 7"));
    CHECK_THAT(all, Catch::Matchers::ContainsSubstring("tick value 9"));
}

TEST_CASE("logger: an executor-owned logger is inherited by root and nested graphs")
{
    stdlib::register_standard_operators();
    CapturedLog process_log;

    auto run_sink = std::make_shared<spdlog::sinks::ringbuffer_sink_mt>(32);
    auto run_logger = std::make_shared<CapturedContextLogger>(run_sink);
    run_logger->set_level(spdlog::level::debug);

    GraphExecutorBuilder builder;
    builder.graph_builder(build_graph<LoggerRunGraph>())
        .logger(run_logger)
        .start_time(MIN_ST)
        .end_time(MIN_ST + TimeDelta{10});
    GraphExecutorValue executor = builder.make_executor();
    GraphView root = executor.view().graph();

    CHECK(executor.view().logger() == run_logger.get());
    CHECK(root.logger() == run_logger.get());
    GraphBuilder child_builder;
    GraphValue child = child_builder.make_nested_graph(root.node_at(0).pointer());
    CHECK(child.view().logger() == run_logger.get());

    executor.view().run();

    std::string run_output;
    for (const auto &line : run_sink->last_formatted()) { run_output += line; }
    CHECK_THAT(run_output, Catch::Matchers::ContainsSubstring("Starting graph run"));
    CHECK_THAT(run_output, Catch::Matchers::ContainsSubstring("logging node started"));
    CHECK_THAT(run_output, Catch::Matchers::ContainsSubstring("tick value 7"));
    CHECK_THAT(run_output, Catch::Matchers::ContainsSubstring("Finished graph run"));
    REQUIRE_FALSE(run_logger->paths.empty());
    CHECK_THAT(run_logger->paths.front(), Catch::Matchers::ContainsSubstring("logging_node"));
    CHECK_THAT(process_log.joined(), !Catch::Matchers::ContainsSubstring("tick value 7"));
}

TEST_CASE("logger: the log_ operator formats and logs through the injectable")
{
    stdlib::register_standard_operators();
    CapturedLog captured;

    CHECK_OUTPUT(eval_node<LogOperatorGraph>(values<Int>(42)), values<Int>(42));
    CHECK_THAT(captured.joined(), Catch::Matchers::ContainsSubstring("observed 42"));
}

TEST_CASE("logger: log_ skips formatting when the level is filtered out")
{
    stdlib::register_standard_operators();
    CapturedLog captured;
    log::logger().set_level(spdlog::level::err);   // info/warn filtered

    CHECK_OUTPUT(eval_node<QuietLogGraph>(values<Int>(1)), values<Int>(1));
    CHECK_THAT(captured.joined(), !Catch::Matchers::ContainsSubstring("quiet 1"));
    CHECK(!LoggerView{&log::logger()}.should_log(2));   // info filtered
    CHECK(LoggerView{&log::logger()}.should_log(4));    // error passes
}

TEST_CASE("logger: log_ formats positional and named time-series arguments")
{
    stdlib::register_standard_operators();
    CapturedLog captured;

    CHECK_OUTPUT(eval_node<VariadicLogGraph>(values<Str>("value"), values<Int>(42)),
                 values<Str>("value"));

    const std::string all = captured.joined();
    CHECK_THAT(all, Catch::Matchers::ContainsSubstring("positional value 42"));
    CHECK_THAT(all, Catch::Matchers::ContainsSubstring("named value 42"));
}

TEST_CASE("logger: log_ supports an empty format-argument pack")
{
    stdlib::register_standard_operators();
    CapturedLog captured;

    CHECK_OUTPUT(eval_node<EmptyLogGraph>(), values<Int>(1));
    CHECK_THAT(captured.joined(), Catch::Matchers::ContainsSubstring("no arguments"));
}

TEST_CASE("logger: log_ sample_count emits every nth evaluation")
{
    stdlib::register_standard_operators();
    CapturedLog captured;

    CHECK_OUTPUT(eval_node<SampledLogGraph>(values<Str>("a", "b", "c", "d", "e")),
                 values<Str>("a", "b", "c", "d", "e"));

    const std::string all = captured.joined();
    CHECK_THAT(all, Catch::Matchers::ContainsSubstring("sampled c"));
    CHECK_THAT(all, !Catch::Matchers::ContainsSubstring("sampled a"));
    CHECK_THAT(all, !Catch::Matchers::ContainsSubstring("sampled b"));
    CHECK_THAT(all, !Catch::Matchers::ContainsSubstring("sampled d"));
    CHECK_THAT(all, !Catch::Matchers::ContainsSubstring("sampled e"));
}

TEST_CASE("logger: reset_logger drops the cached logger and rebuilds the default")
{
    auto sink = std::make_shared<spdlog::sinks::ringbuffer_sink_mt>(4);
    log::set_logger(std::make_shared<spdlog::logger>("hgraph-custom", sink));
    CHECK(log::logger().name() == "hgraph-custom");

    log::reset_logger();

    // The registry copy is gone too, so the next access builds a genuinely
    // fresh default logger (fresh sinks — the point of the reset: Windows
    // stdout sinks cache the raw OS handle at construction).
    CHECK(spdlog::get("hgraph") == nullptr);
    CHECK(log::logger().name() == "hgraph");

    log::reset_logger();   // leave no cached logger behind for other tests
}
