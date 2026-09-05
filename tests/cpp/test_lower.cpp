#include <hgraph/lib/std/lower.h>
#include <hgraph/lib/std/operators/registration.h>
#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/types/value/table_codec.h>

#include <arrow/api.h>
#include <catch2/catch_test_macros.hpp>

#include <array>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace
{
    using namespace hgraph;

    struct AddGraph
    {
        [[maybe_unused]] static constexpr auto name = "lower_add_graph";

        static Port<TS<Int>> compose(Wiring &w, NamedPort<"lhs", TS<Int>> lhs, NamedPort<"rhs", TS<Int>> rhs)
        {
            return wire<stdlib::add_>(w, lhs, rhs).as<TS<Int>>();
        }
    };

    struct SinkGraph
    {
        [[maybe_unused]] static constexpr auto name = "lower_sink_graph";

        static void compose(Wiring &w, NamedPort<"value", TS<Int>> value) { wire<stdlib::null_sink>(w, value); }
    };

    struct DictGraph
    {
        [[maybe_unused]] static constexpr auto name = "lower_dict_graph";

        static Port<TSD<Str, TS<Int>>> compose(Wiring &w, NamedPort<"values", TSD<Str, TS<Int>>> values)
        {
            return wire<stdlib::pass_through_node>(w, values).as<TSD<Str, TS<Int>>>();
        }
    };

    struct IdentityGraph
    {
        [[maybe_unused]] static constexpr auto name = "lower_identity_graph";

        static Port<TS<Int>> compose(Wiring &w, NamedPort<"value", TS<Int>> value)
        {
            return wire<stdlib::pass_through_node>(w, value).as<TS<Int>>();
        }
    };

    struct PhaseRunnerIdentityNode
    {
        [[maybe_unused]] static constexpr auto name = "phase_runner_identity_node";
        static constexpr bool requires_phase_runner = true;

        static void eval(In<"value", TS<Int>> value, Out<TS<Int>> out)
        {
            out.set(value.value());
        }
    };

    struct PhaseRunnerIdentityGraph
    {
        [[maybe_unused]] static constexpr auto name = "phase_runner_identity_graph";

        static Port<TS<Int>> compose(Wiring &w, NamedPort<"value", TS<Int>> value)
        {
            return wire<PhaseRunnerIdentityNode>(w, value).as<TS<Int>>();
        }
    };

    void require_arrow(const arrow::Status &status)
    {
        if (!status.ok())
        {
            throw std::runtime_error(status.ToString());
        }
    }

    [[nodiscard]] Frame keyed_input_frame(std::initializer_list<std::tuple<DateTime, Str, Int>> rows)
    {
        std::unique_ptr<arrow::ArrayBuilder> date_builder;
        std::unique_ptr<arrow::ArrayBuilder> key_builder;
        std::unique_ptr<arrow::ArrayBuilder> value_builder;
        require_arrow(
            arrow::MakeBuilder(arrow::default_memory_pool(), arrow::timestamp(arrow::TimeUnit::MICRO), &date_builder));
        require_arrow(arrow::MakeBuilder(arrow::default_memory_pool(), arrow::utf8(), &key_builder));
        require_arrow(arrow::MakeBuilder(arrow::default_memory_pool(), arrow::int64(), &value_builder));
        for (const auto &[when, key, value] : rows)
        {
            require_arrow(
                static_cast<arrow::TimestampBuilder &>(*date_builder).Append(when.time_since_epoch().count()));
            require_arrow(static_cast<arrow::StringBuilder &>(*key_builder).Append(key));
            require_arrow(static_cast<arrow::Int64Builder &>(*value_builder).Append(value));
        }
        std::shared_ptr<arrow::Array> dates;
        std::shared_ptr<arrow::Array> keys;
        std::shared_ptr<arrow::Array> values;
        require_arrow(date_builder->Finish(&dates));
        require_arrow(key_builder->Finish(&keys));
        require_arrow(value_builder->Finish(&values));
        return Frame{arrow::Table::Make(arrow::schema({
                                            arrow::field("date", arrow::timestamp(arrow::TimeUnit::MICRO)),
                                            arrow::field("key", arrow::utf8()),
                                            arrow::field("value", arrow::int64()),
                                        }),
                                        {std::move(dates), std::move(keys), std::move(values)})};
    }

    [[nodiscard]] Frame input_frame(std::initializer_list<std::pair<DateTime, Int>> rows)
    {
        const auto &converter = table_converter(scalar_descriptor<Int>::value_meta(), "date", "as_of");
        FrameRecorder recorder{converter};
        for (const auto &[when, value] : rows)
        {
            const Value boxed{value};
            recorder.append(when, when, boxed.view());
        }
        return recorder.finish();
    }

    [[nodiscard]] Frame versioned_input_frame(std::initializer_list<std::tuple<DateTime, DateTime, Int>> rows)
    {
        const auto &converter = table_converter(scalar_descriptor<Int>::value_meta(), "date", "as_of");
        FrameRecorder recorder{converter};
        for (const auto &[when, as_of, value] : rows)
        {
            const Value boxed{value};
            recorder.append(when, as_of, boxed.view());
        }
        return recorder.finish();
    }

    [[nodiscard]] DateTime date_at(const Frame &frame, std::int64_t row)
    {
        return frame_cell(frame, "date", scalar_descriptor<DateTime>::value_meta(), row).view().checked_as<DateTime>();
    }

    [[nodiscard]] Int value_at(const Frame &frame, std::int64_t row)
    {
        return frame_cell(frame, "value", scalar_descriptor<Int>::value_meta(), row).view().checked_as<Int>();
    }

    [[nodiscard]] Str key_at(const Frame &frame, std::int64_t row)
    {
        return frame_cell(frame, "key", scalar_descriptor<Str>::value_meta(), row).view().checked_as<Str>();
    }
} // namespace

TEST_CASE("lower executes a C++ graph over staggered Arrow frame inputs")
{
    stdlib::register_standard_operators();
    const std::array inputs{
        input_frame({{MIN_ST, Int{1}}, {MIN_ST + MIN_TD, Int{2}}}),
        input_frame({{MIN_ST, Int{3}}, {MIN_ST + MIN_TD * 2, Int{4}}}),
    };

    const auto result = stdlib::lower<AddGraph>(inputs);
    REQUIRE(result.has_value());
    REQUIRE(frame_rows(*result) == 3);
    CHECK(date_at(*result, 0) == MIN_ST);
    CHECK(date_at(*result, 1) == MIN_ST + MIN_TD);
    CHECK(date_at(*result, 2) == MIN_ST + MIN_TD * 2);
    CHECK(value_at(*result, 0) == Int{4});
    CHECK(value_at(*result, 1) == Int{5});
    CHECK(value_at(*result, 2) == Int{6});
    const std::vector<std::string> expected_columns{"date", "value"};
    CHECK(frame_column_names(*result) == expected_columns);
}

TEST_CASE("lower forwards its executor phase runner")
{
    stdlib::register_standard_operators();
    const std::array inputs{
        input_frame({{MIN_ST, Int{1}}, {MIN_ST + MIN_TD, Int{2}}}),
        input_frame({{MIN_ST, Int{3}}, {MIN_ST + MIN_TD * 2, Int{4}}}),
    };

    std::vector<GraphExecutorPhase> phases;
    stdlib::LowerOptions options;
    options.phase_runner = [&](GraphExecutorPhase phase,
                               GraphExecutorPhaseAction action) {
        phases.push_back(phase);
        action();
    };
    const auto result = stdlib::lower<AddGraph>(inputs, std::move(options));

    REQUIRE(result.has_value());
    REQUIRE(phases.size() == 5);
    CHECK(phases.front() == GraphExecutorPhase::Start);
    CHECK(phases[1] == GraphExecutorPhase::Evaluation);
    CHECK(phases[2] == GraphExecutorPhase::Evaluation);
    CHECK(phases[3] == GraphExecutorPhase::Evaluation);
    CHECK(phases.back() == GraphExecutorPhase::Stop);
}

TEST_CASE("lower can restrict its phase runner to opted-in graph plans")
{
    stdlib::register_standard_operators();
    const std::array inputs{
        input_frame({{MIN_ST, Int{1}}, {MIN_ST + MIN_TD, Int{2}}}),
    };

    std::vector<GraphExecutorPhase> native_phases;
    stdlib::LowerOptions native_options;
    native_options.phase_runner_requires_graph_opt_in = true;
    native_options.phase_runner = [&](GraphExecutorPhase phase,
                                      GraphExecutorPhaseAction action) {
        native_phases.push_back(phase);
        action();
    };
    const auto native_result = stdlib::lower<IdentityGraph>(inputs,
                                                            std::move(native_options));
    REQUIRE(native_result.has_value());
    CHECK(native_phases.empty());

    std::vector<GraphExecutorPhase> opted_in_phases;
    stdlib::LowerOptions opted_in_options;
    opted_in_options.phase_runner_requires_graph_opt_in = true;
    opted_in_options.phase_runner = [&](GraphExecutorPhase phase,
                                        GraphExecutorPhaseAction action) {
        opted_in_phases.push_back(phase);
        action();
    };
    const auto opted_in_result = stdlib::lower<PhaseRunnerIdentityGraph>(
        inputs, std::move(opted_in_options));
    REQUIRE(opted_in_result.has_value());
    const std::vector expected{
        GraphExecutorPhase::Start,
        GraphExecutorPhase::Evaluation,
        GraphExecutorPhase::Evaluation,
        GraphExecutorPhase::Stop,
    };
    CHECK(opted_in_phases == expected);
}

TEST_CASE("lower can retain one fixed as-of column and keeps private state private")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    context.state().view().set("seed", Value{Int{7}});
    const DateTime fixed_as_of = MIN_ST + TimeDelta{123};
    const std::array inputs{
        input_frame({{MIN_ST, Int{1}}}),
        input_frame({{MIN_ST, Int{2}}}),
    };

    stdlib::LowerOptions options;
    options.no_as_of_support = false;
    options.as_of = fixed_as_of;
    const auto result = stdlib::lower<AddGraph>(inputs, options);

    REQUIRE(result.has_value());
    const std::vector<std::string> expected_columns{"date", "as_of", "value"};
    CHECK(frame_column_names(*result) == expected_columns);
    CHECK(frame_cell(*result, "as_of", scalar_descriptor<DateTime>::value_meta(), 0).view().checked_as<DateTime>() ==
          fixed_as_of);
    CHECK(context.state().view().get_as<Int>("seed") == Int{7});
    CHECK_FALSE(context.state().view().contains("__hgraph.lower.result__"));
}

TEST_CASE("lower: a prepared execution run after its context exited copies nothing back")
{
    // Review finding on the seed binding: prepare/run may be split across the
    // context's lifetime; the execution holds the shared binding, which the
    // context detaches on exit, so the deferred run never writes through a
    // dead state and still produces its result.
    stdlib::register_standard_operators();
    const std::array inputs{
        input_frame({{MIN_ST, Int{1}}}),
        input_frame({{MIN_ST, Int{2}}}),
    };
    stdlib::LowerExecution execution;
    {
        GlobalContext context;
        context.state().view().set("seed", Value{Int{7}});
        execution = stdlib::prepare_lower(fn<AddGraph>(), inputs, {});
    }
    execution.run();
    REQUIRE(execution.ran());
    CHECK(execution.result().has_value());
}

TEST_CASE("lower supports sink graphs and validates frame arity")
{
    stdlib::register_standard_operators();
    const std::array input{input_frame({{MIN_ST, Int{1}}})};
    CHECK_FALSE(stdlib::lower<SinkGraph>(input).has_value());
    CHECK_THROWS_AS(stdlib::lower<AddGraph>(input), std::invalid_argument);
}

TEST_CASE("lower replays and snapshots keyed time-series frames")
{
    stdlib::register_standard_operators();
    const std::array input{keyed_input_frame({
        {MIN_ST, Str{"a"}, Int{1}},
        {MIN_ST + MIN_TD, Str{"b"}, Int{2}},
    })};

    const auto result = stdlib::lower<DictGraph>(input);

    REQUIRE(result.has_value());
    REQUIRE(frame_rows(*result) == 3);
    const std::vector<std::string> expected_columns{"date", "key", "value"};
    CHECK(frame_column_names(*result) == expected_columns);
    CHECK(key_at(*result, 0) == Str{"a"});
    CHECK(value_at(*result, 0) == Int{1});
    CHECK(key_at(*result, 1) == Str{"a"});
    CHECK(value_at(*result, 1) == Int{1});
    CHECK(key_at(*result, 2) == Str{"b"});
    CHECK(value_at(*result, 2) == Int{2});
}

TEST_CASE("lower selects the latest visible as-of version")
{
    stdlib::register_standard_operators();
    const DateTime first_as_of = MIN_ST + TimeDelta{10};
    const DateTime second_as_of = MIN_ST + TimeDelta{20};
    const std::array input{versioned_input_frame({
        {MIN_ST, first_as_of, Int{1}},
        {MIN_ST, second_as_of, Int{2}},
    })};

    stdlib::LowerOptions options;
    options.no_as_of_support = false;
    options.as_of = first_as_of + TimeDelta{1};
    const auto first = stdlib::lower<IdentityGraph>(input, options);
    REQUIRE(first.has_value());
    REQUIRE(frame_rows(*first) == 1);
    CHECK(value_at(*first, 0) == Int{1});

    options.as_of = second_as_of + TimeDelta{1};
    const auto second = stdlib::lower<IdentityGraph>(input, options);
    REQUIRE(second.has_value());
    REQUIRE(frame_rows(*second) == 1);
    CHECK(value_at(*second, 0) == Int{2});

    const auto without_as_of = stdlib::lower<IdentityGraph>(input);
    REQUIRE(without_as_of.has_value());
    REQUIRE(frame_rows(*without_as_of) == 1);
    CHECK(value_at(*without_as_of, 0) == Int{1});
}
