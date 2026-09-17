#include <hgraph/persistence/component_checkpoint_store.h>
#include <hgraph/lib/std/component.h>
#include <hgraph/lib/std/std_operators.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/metadata/type_registry.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <arrow/array.h>
#include <arrow/table.h>

#include <chrono>
#include <algorithm>
#include <filesystem>
#include <iostream>

namespace
{
    using namespace hgraph;
    using namespace hgraph::testing;
    using namespace hgraph::persistence;

    struct WindowCheckpointDirectory
    {
        std::filesystem::path path = std::filesystem::temp_directory_path() /
            ("hgraph-window-checkpoint-" + std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()));
        ~WindowCheckpointDirectory()
        {
            std::error_code ignored;
            std::filesystem::remove_all(path, ignored);
        }
        store::FrameStoreConfig config() const
        {
            store::FrameStoreConfig result;
            result.location = store::LocalLocation{path.string()};
            return result;
        }
    };

    template<bool Duration>
    struct DurableWindowStrategy
    {
        static Port<TS<Int>> compose(Wiring &w, NamedPort<"ts", TS<Int>> input)
        {
            auto window = [&] {
                if constexpr (Duration) { return wire<stdlib::to_window>(w, input, MIN_TD * 2, MIN_TD); }
                else { return wire<stdlib::to_window>(w, input, Int{3}, Int{1}); }
            }();
            return wire<stdlib::sum_>(w, window).template as<TS<Int>>();
        }
    };

    template<bool Duration>
    struct DurableWindowComponent
    {
        static Port<TS<Int>> compose(Wiring &w, Port<TS<Int>> input)
        {
            return stdlib::component<DurableWindowStrategy<Duration>>(w, "window", input);
        }
    };

    EvalNodeRunOptions interval(Int begin, Int end)
    {
        return {.start_time = MIN_ST + MIN_TD * begin, .end_time = MIN_ST + MIN_TD * end};
    }

    template<bool Duration>
    void durable_window_restart()
    {
        WindowCheckpointDirectory directory;
        {
            GlobalContext context;
            const ComponentCheckpointStore checkpoints{directory.config()};
            persistence::configure_component_recovery(context.state().view(), checkpoints, "window", "one");
            CHECK_OUTPUT(eval_node_with_options<DurableWindowComponent<Duration>>(
                interval(0, 2), values<Int>(1, 2)), values<Int>(1, 3));
        }
        {
            GlobalContext context;
            const ComponentCheckpointStore reopened{directory.config()};
            const auto saved = reopened.read("one");
            const TSCheckpointImage *window{};
            for (const auto &node : saved.graph.nodes)
                if (node.output && node.output->schema->kind == TSTypeKind::TSW) { window = &*node.output; }
            REQUIRE(window != nullptr);
            CHECK(window->payload.view().as_indexed_view().size() == 2);
            CHECK(window->window_times == std::vector<DateTime>{MIN_ST, MIN_ST + MIN_TD});
            CHECK(window->schema->is_duration_based() == Duration);
            persistence::configure_component_recovery(context.state().view(), reopened, "window", "two", "one");
            CHECK_OUTPUT(eval_node_with_options<DurableWindowComponent<Duration>>(
                interval(2, 5), values<Int>(none, 3, 4)),
                (Duration ? values<Int>(none, 5, 7) : values<Int>(none, 6, 9)));
            CHECK(reopened.contains("two"));
        }
    }

    ComponentCheckpoint window_image(std::size_t period)
    {
        const auto *schema = TypeRegistry::instance().tsw(scalar_descriptor<Int>::value_meta(), period, period);
        TSOutput output{schema};
        auto output_data = output.data_view();
        const Value first{Int{1}}, second{Int{2}};
        output_data.as_window().begin_mutation(MIN_ST).push(first.view());
        output_data.as_window().begin_mutation(MIN_ST + MIN_TD).push(second.view());
        ComponentCheckpoint checkpoint;
        checkpoint.component_id = "window";
        checkpoint.graph_signature = "compact-window-fixture";
        checkpoint.cut = MIN_ST + MIN_TD;
        checkpoint.completed_until = MIN_ST + MIN_TD * 2;
        NodeCheckpointImage node;
        node.id = "window";
        node.signature = "window-image-v1";
        node.output = capture_ts_checkpoint(output_data);
        node.custom.endpoints.push_back(*node.output);
        checkpoint.graph.nodes.push_back(std::move(node));
        return checkpoint;
    }
}

TEST_CASE("component checkpoint store: TSW native public graphs restart through durable codec")
{
    stdlib::register_standard_operators();
    durable_window_restart<false>();
    durable_window_restart<true>();
}

TEST_CASE("component checkpoint store: window image size depends on live samples rather than capacity")
{
    WindowCheckpointDirectory directory;
    const ComponentCheckpointStore checkpoints{directory.config()};
    checkpoints.write("small", window_image(3));
    checkpoints.write("large", window_image(100000));
    const auto frames = store::make_frame_store(directory.config());
    const auto bytes = [&](std::string_view key) {
        const auto frame = frames.read(key);
        const auto &array = static_cast<const arrow::LargeBinaryArray &>(*frame.table->column(0)->chunk(0));
        return array.value_length(0);
    };
    CHECK(bytes("large") < 4096);
    CHECK(bytes("large") - bytes("small") < 256);
    const auto restored = checkpoints.read("large");
    REQUIRE(restored.graph.nodes.front().custom.endpoints.size() == 1);
    const auto &window = restored.graph.nodes.front().custom.endpoints.front();
    CHECK(window.schema->period() == 100000);
    CHECK(window.payload.view().as_indexed_view().size() == 2);
    CHECK(window.window_times == std::vector<DateTime>{MIN_ST, MIN_ST + MIN_TD});
    TSOutput target{window.schema};
    auto target_data = target.data_view();
    restore_ts_checkpoint(target_data, window);
    CHECK(target_data.as_window().size() == 2);
    CHECK_FALSE(target_data.all_valid());
}

TEST_CASE("component checkpoint store: unsupported image versions cannot be published")
{
    const ComponentCheckpointStore checkpoints;
    auto image = window_image(3);
    const bool component = GENERATE(false, true);
    const auto version = GENERATE(0u, 2u, 3u);
    if (component) { image.version = version; }
    else { image.graph.nodes.front().output->version = version; }
    CHECK_THROWS(checkpoints.write("unsupported", image));
    CHECK_FALSE(checkpoints.contains("unsupported"));
}

// Explicitly selected; normal correctness gates do not run timing work.
// hgraph_persistence_tests '[checkpoint-window-benchmark]'
TEST_CASE("component checkpoint store: TSW capture and cold load scaling", "[.][checkpoint-window-benchmark]")
{
    constexpr std::size_t repetitions = 11;
    const auto median_us = [](auto &&action) {
        std::vector<double> timings;
        timings.reserve(repetitions);
        for (std::size_t repetition = 0; repetition < repetitions; ++repetition)
        {
            const auto start = std::chrono::steady_clock::now();
            action(repetition);
            const auto end = std::chrono::steady_clock::now();
            timings.push_back(std::chrono::duration<double, std::micro>{end - start}.count());
        }
        std::sort(timings.begin(), timings.end());
        return timings[timings.size() / 2];
    };
    for (const std::size_t live : {16, 256, 4096})
    {
        for (const std::size_t period : {live, std::size_t{100000}})
        {
            const auto *schema = TypeRegistry::instance().tsw(scalar_descriptor<Int>::value_meta(), period, period);
            TSOutput source{schema};
            auto source_data = source.data_view();
            auto source_window = source_data.as_window();
            for (std::size_t index = 0; index < live; ++index)
            {
                const Value value{static_cast<Int>(index)};
                source_window.begin_mutation(MIN_ST + MIN_TD * static_cast<Int>(index)).push(value.view());
            }
            const auto image = capture_ts_checkpoint(source_data);
            ComponentCheckpoint checkpoint;
            checkpoint.component_id = "window-benchmark";
            checkpoint.graph_signature = "integer-window-v1";
            checkpoint.cut = source_data.last_modified_time();
            checkpoint.completed_until = checkpoint.cut + MIN_TD;
            NodeCheckpointImage node;
            node.id = "window";
            node.signature = "window-image-v1";
            node.output = image;
            checkpoint.graph.nodes.push_back(std::move(node));

            // Obtain exact envelope size through the public durable store, off
            // the timed path. Timed persistence uses an in-memory backend so
            // filesystem latency does not obscure codec scaling.
            WindowCheckpointDirectory directory;
            const ComponentCheckpointStore disk{directory.config()};
            disk.write("image", checkpoint);
            const auto frames = store::make_frame_store(directory.config());
            const auto frame = frames.read("image");
            const auto &array = static_cast<const arrow::LargeBinaryArray &>(*frame.table->column(0)->chunk(0));
            const auto encoded_bytes = array.value_length(0);
            const ComponentCheckpointStore memory;
            memory.write("baseline", checkpoint);
            std::size_t observed{};
            const auto capture_us = median_us([&](std::size_t) {
                const auto captured = capture_ts_checkpoint(source_data);
                observed += captured.window_times.size();
            });
            const auto load_us = median_us([&](std::size_t) {
                TSOutput target{schema};
                auto target_data = target.data_view();
                restore_ts_checkpoint(target_data, image);
                observed += target_data.as_window().size();
            });
            const auto store_us = median_us([&](std::size_t repetition) {
                memory.write("sample-" + std::to_string(repetition), checkpoint);
            });
            const auto read_us = median_us([&](std::size_t) {
                const auto loaded = memory.read("baseline");
                observed += loaded.graph.nodes.front().output->window_times.size();
            });
            REQUIRE(observed == 3 * repetitions * live);
            std::cout << "{\"live_samples\":" << live << ",\"period\":" << period
                      << ",\"encoded_bytes\":" << encoded_bytes
                      << ",\"capture_us\":" << capture_us
                      << ",\"cold_load_us\":" << load_us
                      << ",\"encode_validate_memory_store_us\":" << store_us
                      << ",\"memory_read_decode_us\":" << read_us << "}\n";
        }
    }
}
