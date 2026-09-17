#include <hgraph/lib/std/component.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/runtime/component_checkpoint.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/ts_data/checkpoint.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/value_builder.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;

    struct WindowCheckpointObserver final : Notifiable
    {
        std::size_t notifications{};
        void notify(DateTime) override { ++notifications; }
    };

    void push(const TSDataView &view, Int value, Int offset)
    {
        Value payload{value};
        view.as_window().begin_mutation(MIN_ST + MIN_TD * offset).push(payload.view());
    }

    template<bool Duration, bool Mean>
    struct WindowStrategy
    {
        static Port<TS<Float>> compose(Wiring &w, NamedPort<"ts", TS<Float>> input,
                                      NamedPort<"reset", SIGNAL> reset)
        {
            auto window = [&] {
                if constexpr (Duration)
                    return wire<stdlib::to_window>(w, input, MIN_TD * 4, MIN_TD * 2, reset);
                else
                    return wire<stdlib::to_window>(w, input, Int{3}, Int{2}, reset);
            }();
            if constexpr (Mean) { return wire<stdlib::mean>(w, window).template as<TS<Float>>(); }
            else { return wire<stdlib::sum_>(w, window).template as<TS<Float>>(); }
        }
    };

    template<bool Duration, bool Mean>
    struct WindowComponent
    {
        static Port<TS<Float>> compose(Wiring &w, Port<TS<Float>> input, Port<SIGNAL> reset)
        {
            return stdlib::component<WindowStrategy<Duration, Mean>>(w, "window-strategy", input, reset);
        }
    };

    EvalNodeRunOptions interval(Int begin, Int end)
    {
        return {.start_time = MIN_ST + MIN_TD * begin, .end_time = MIN_ST + MIN_TD * end};
    }

    template<typename T>
    std::vector<std::optional<T>> slice(const std::vector<std::optional<T>> &values, std::size_t begin, std::size_t end)
    {
        return {values.begin() + begin, values.begin() + end};
    }

    template<bool Duration, bool Mean>
    void compare_window_restarts(std::size_t split)
    {
        const auto input = values<Float>(1., 2., none, 4., 8., none, 16., 32., none, 64.);
        const auto reset = values<bool>(none, none, none, none, true, none, none, none, true, none);
        std::vector<std::optional<Float>> continuous;
        {
            GlobalContext context;
            continuous = eval_node_with_options<WindowComponent<Duration, Mean>>(interval(0, 10), input, reset);
        }
        std::optional<ComponentCheckpoint> completed;
        {
            GlobalContext context;
            configure_component_recovery(context.state().view(), {
                .component_id = "window-strategy",
                .commit = [&](const auto &image) { completed = image; }});
            CHECK_OUTPUT((eval_node_with_options<WindowComponent<Duration, Mean>>(
                interval(0, static_cast<Int>(split)), slice(input, 0, split), slice(reset, 0, split))),
                slice(continuous, 0, split));
        }
        REQUIRE(completed);
        {
            GlobalContext context;
            configure_component_recovery(context.state().view(), {
                .component_id = "window-strategy", .load = [&] { return completed; },
                .commit = [&](const auto &image) { completed = image; }});
            CHECK_OUTPUT((eval_node_with_options<WindowComponent<Duration, Mean>>(
                interval(static_cast<Int>(split), 10), slice(input, split, input.size()), slice(reset, split, reset.size()))),
                slice(continuous, split, continuous.size()));
        }
    }
}

TEST_CASE("TSW checkpoint: compact live samples preserve warmup and ring eviction", "[checkpoint][window]")
{
    auto &registry = TypeRegistry::instance();
    const auto *element = scalar_descriptor<Int>::value_meta();
    const auto *schema = registry.tsw(element, 3, 3);
    TSOutput source{schema};
    auto source_data = source.data_view();
    push(source_data, 10, 0);
    push(source_data, 20, 1);
    auto image = capture_ts_checkpoint(source_data);
    REQUIRE(image.children.empty());
    REQUIRE(image.payload.view().as_indexed_view().size() == 2);
    REQUIRE(image.window_times == std::vector<DateTime>{MIN_ST, MIN_ST + MIN_TD});
    TSOutput target{schema};
    auto target_data = target.data_view();
    WindowCheckpointObserver observer;
    target.subscribe(&observer);
    restore_ts_checkpoint(target_data, image);
    CHECK(target_data.has_current_value());
    CHECK_FALSE(target_data.all_valid());
    CHECK(observer.notifications == 0);
    CHECK_FALSE(target_data.modified(MIN_ST + MIN_TD * 2));
    push(target_data, 30, 2);
    CHECK(target_data.all_valid());
    push(target_data, 40, 3);
    auto window = target_data.as_window();
    CHECK(window.size() == 3);
    CHECK(window.front().checked_as<Int>() == 20);
    CHECK(window.back().checked_as<Int>() == 40);
    CHECK(window.time_at(0) == MIN_ST + MIN_TD);
    CHECK(window.removed_value(MIN_ST + MIN_TD * 3).checked_as<Int>() == 10);
    CHECK(observer.notifications == 2);
    target.unsubscribe(&observer);
    TSOutput rolled_target{schema};
    auto rolled_target_data = rolled_target.data_view();
    restore_ts_checkpoint(rolled_target_data, capture_ts_checkpoint(target_data));
    push(rolled_target_data, 50, 4);
    CHECK(rolled_target_data.as_window().front().checked_as<Int>() == 30);
}

TEST_CASE("TSW checkpoint: duration restore retains timestamps and push-only expiry", "[checkpoint][window]")
{
    auto &registry = TypeRegistry::instance();
    const auto *schema = registry.tsw_duration(scalar_descriptor<Int>::value_meta(), MIN_TD * 4, MIN_TD * 3);
    TSOutput source{schema};
    auto source_data = source.data_view();
    push(source_data, 10, 0);
    push(source_data, 20, 2);
    TSOutput target{schema};
    auto target_data = target.data_view();
    restore_ts_checkpoint(target_data, capture_ts_checkpoint(source_data));
    CHECK_FALSE(target_data.all_valid());
    push(target_data, 30, 4);
    CHECK(target_data.all_valid());
    CHECK(target_data.as_window().size() == 3); // inclusive lower boundary
    TSOutput quiet_target{schema};
    auto quiet_target_data = quiet_target.data_view();
    restore_ts_checkpoint(quiet_target_data, capture_ts_checkpoint(target_data));
    CHECK(quiet_target_data.as_window().size() == 3); // silence does not expire values
    push(quiet_target_data, 40, 10);
    CHECK(quiet_target_data.as_window().size() == 1);
    CHECK(quiet_target_data.as_window().front().checked_as<Int>() == 40);
    CHECK_FALSE(quiet_target_data.all_valid());
}

TEST_CASE("TSW checkpoint: empty reset and invalidated retained samples remain distinct", "[checkpoint][window]")
{
    const auto *schema = TypeRegistry::instance().tsw(scalar_descriptor<Int>::value_meta(), 3, 2);
    const auto mode = GENERATE(0, 1, 2);
    TSOutput source{schema};
    auto source_data = source.data_view();
    if (mode != 0)
    {
        push(source_data, 10, 0);
        if (mode == 1) { source_data.as_window().begin_mutation(MIN_ST + MIN_TD).clear(); }
        else { REQUIRE(source_data.begin_mutation(MIN_ST + MIN_TD).invalidate()); }
    }
    const auto image = capture_ts_checkpoint(source_data);
    TSOutput target{schema};
    auto target_data = target.data_view();
    restore_ts_checkpoint(target_data, image);
    CHECK(target_data.has_current_value() == (mode == 1));
    CHECK(target_data.as_window().size() == (mode == 2 ? 1 : 0));
    CHECK(target_data.last_modified_time() == source_data.last_modified_time());
    push(target_data, 20, 2);
    CHECK(target_data.as_window().size() == (mode == 2 ? 2 : 1));
}

TEST_CASE("TSW checkpoint: malformed samples are refused before mutation", "[checkpoint][window]")
{
    const auto *schema = TypeRegistry::instance().tsw(scalar_descriptor<Int>::value_meta(), 3, 2);
    TSOutput source{schema};
    auto source_data = source.data_view();
    push(source_data, 10, 0);
    push(source_data, 20, 1);
    auto image = capture_ts_checkpoint(source_data);
    const auto error = GENERATE(0, 1, 2, 3, 4, 5, 6, 7);
    if (error == 0) { image.window_times.pop_back(); }
    if (error == 1) { image.window_times[0] = MIN_DT; }
    if (error == 2) { image.window_times[0] = MIN_ST + MIN_TD * 2; }
    if (error == 3) { image.window_times.back() = MIN_ST + MIN_TD * 2; }
    if (error == 4) { image.payload = Value{Int{1}}; }
    if (error == 5) { image.keys.emplace_back(Int{1}); }
    if (error >= 6)
    {
        ListBuilder values{ValuePlanFactory::instance().type_for(scalar_descriptor<Int>::value_meta())};
        values.push_back(Int{10});
        if (error == 6) { values.push_back_unset(); }
        else
        {
            for (Int value : {20, 30, 40}) { values.push_back(value); }
            image.window_times = {MIN_ST, MIN_ST, MIN_ST, MIN_ST};
        }
        image.payload = values.build();
    }
    TSOutput target{schema};
    auto target_data = target.data_view();
    CHECK_THROWS_AS(restore_ts_checkpoint(target_data, image), std::invalid_argument);
    CHECK(target_data.as_window().empty());
    CHECK_FALSE(target_data.has_current_value());
}

TEST_CASE("TSW checkpoint: duration extent rejects malformed retained history", "[checkpoint][window]")
{
    const auto *schema = TypeRegistry::instance().tsw_duration(scalar_descriptor<Int>::value_meta(), MIN_TD * 2);
    TSOutput source{schema};
    auto source_data = source.data_view();
    push(source_data, 10, 4);
    push(source_data, 20, 5);
    auto image = capture_ts_checkpoint(source_data);
    image.window_times.front() = MIN_ST;
    TSOutput target{schema};
    auto target_data = target.data_view();
    CHECK_THROWS_AS(restore_ts_checkpoint(target_data, image), std::invalid_argument);
    CHECK(target_data.as_window().empty());
}

TEST_CASE("TSW checkpoint: nontrivial scalar samples are owned independently", "[checkpoint][window]")
{
    const auto *schema = TypeRegistry::instance().tsw(scalar_descriptor<Str>::value_meta(), 2, 1);
    TSOutput source{schema};
    auto source_data = source.data_view();
    const Value alpha{Str{"a long string requiring its own heap storage"}}, beta{Str{"replacement"}};
    source_data.as_window().begin_mutation(MIN_ST).push(alpha.view());
    const auto image = capture_ts_checkpoint(source_data);
    source_data.as_window().begin_mutation(MIN_ST + MIN_TD).clear();
    source_data.as_window().begin_mutation(MIN_ST + MIN_TD * 2).push(beta.view());
    TSOutput target{schema};
    auto target_data = target.data_view();
    restore_ts_checkpoint(target_data, image);
    CHECK(target_data.as_window().front().checked_as<Str>() == alpha.view().checked_as<Str>());
    CHECK(target_data.as_window().time_at(0) == MIN_ST);
}

TEST_CASE("TSW checkpoint: large configured capacity does not pad image", "[checkpoint][window]")
{
    const auto *schema = TypeRegistry::instance().tsw(scalar_descriptor<Int>::value_meta(), 100000, 100000);
    TSOutput source{schema};
    auto source_data = source.data_view();
    push(source_data, 10, 0);
    push(source_data, 20, 1);
    const auto image = capture_ts_checkpoint(source_data);
    CHECK(image.window_times.size() == 2);
    CHECK(image.payload.view().as_indexed_view().size() == 2);
    CHECK_FALSE(image.payload.schema()->is_fixed_size());
    TSOutput target{schema};
    auto target_data = target.data_view();
    restore_ts_checkpoint(target_data, image);
    CHECK(target_data.as_window().size() == 2);
    CHECK(target_data.as_window().capacity() == 100000);
}

TEST_CASE("TSW checkpoint: public aggregates match uninterrupted runs at every cut", "[checkpoint][window][component]")
{
    stdlib::register_standard_operators();
    const auto split = GENERATE(std::size_t{1}, 2, 3, 4, 5, 6, 7, 8, 9);
    compare_window_restarts<false, false>(split);
    compare_window_restarts<false, true>(split);
    compare_window_restarts<true, false>(split);
    compare_window_restarts<true, true>(split);
}

TEST_CASE("component checkpoint rejects future samples in an invalidated window image", "[checkpoint][window]")
{
    stdlib::register_standard_operators();
    GlobalContext context;
    std::optional<ComponentCheckpoint> saved;
    std::size_t commits = 0;
    configure_component_recovery(context.state().view(), {
        .component_id = "window-strategy", .load = [&] { return saved; },
        .commit = [&](const auto &image) { saved = image; ++commits; }});
    static_cast<void>(eval_node_with_options<WindowComponent<false, false>>(
        interval(0, 2), values<Float>(1., 2.), values<Bool>(none, none)));
    REQUIRE(saved);
    bool changed = false;
    for (auto &node : saved->graph.nodes)
    {
        if (node.output && !node.output->window_times.empty())
        {
            node.output->last_modified_time = MIN_DT;
            node.output->window_times.back() = saved->cut + MIN_TD;
            changed = true;
        }
    }
    REQUIRE(changed);
    REQUIRE_THROWS_WITH((eval_node_with_options<WindowComponent<false, false>>(
        interval(2, 3), values<Float>(3.), values<Bool>(none))),
        "component checkpoint: window sample timestamp exceeds the completed cut");
    CHECK(commits == 1);
}
