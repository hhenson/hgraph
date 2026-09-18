#include <hgraph/persistence/component_checkpoint_store.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/static_schema.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/value.h>

#include <catch2/catch_test_macros.hpp>

#include <arrow/array.h>
#include <arrow/table.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

// Explicitly selected; normal correctness gates do not run timing work.
//   hgraph_persistence_tests '[checkpoint-benchmark]'
//
// Every row compares the generic endpoint checkpoint against a hand-written
// record/recover of the same endpoint: the keys, values and modification
// times as three packed arrays, recovered through the ordinary mutation API.
// The hand-written form is the bar a generic image has to approach; it is not
// a correctness reference (it loses slots, free-list order and child clocks).
namespace
{
    using namespace hgraph;
    using namespace hgraph::persistence;

    constexpr std::size_t repetitions = 7;

    template <typename Action> double median_us(Action &&action)
    {
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
    }

    ComponentCheckpoint envelope(TSCheckpointImage image)
    {
        ComponentCheckpoint checkpoint;
        checkpoint.component_id = "benchmark";
        checkpoint.graph_signature = "benchmark";
        checkpoint.cut = MIN_ST + MIN_TD;
        checkpoint.completed_until = MIN_ST + MIN_TD * 2;
        NodeCheckpointImage node;
        node.id = "endpoint";
        node.signature = "endpoint";
        node.output = std::move(image);
        checkpoint.graph.nodes.push_back(std::move(node));
        return checkpoint;
    }

    /** Size of the published image, read back from a durable local object. */
    std::size_t encoded_size(const ComponentCheckpoint &checkpoint)
    {
        const auto path = std::filesystem::temp_directory_path() /
            ("hgraph-checkpoint-benchmark-" + std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()));
        store::FrameStoreConfig config;
        config.location = store::LocalLocation{path.string()};
        const ComponentCheckpointStore sized{config};
        sized.write("sized", checkpoint);
        const auto frame = store::make_frame_store(config).read("sized");
        const auto &array = static_cast<const arrow::LargeBinaryArray &>(*frame.table->column(0)->chunk(0));
        const auto size = static_cast<std::size_t>(array.value_length(0));
        std::error_code ignored;
        std::filesystem::remove_all(path, ignored);
        return size;
    }

    template <typename T> void append(std::string &out, const T &value)
    {
        out.append(reinterpret_cast<const char *>(&value), sizeof(T));
    }

    template <typename T> T take(const char *&cursor)
    {
        T value;
        std::memcpy(&value, cursor, sizeof(T));
        cursor += sizeof(T);
        return value;
    }
}

TEST_CASE("component checkpoint: keyed endpoint record and recover against a hand-written baseline",
          "[.][checkpoint-benchmark]")
{
    auto &registry = TypeRegistry::instance();
    const auto *key = scalar_descriptor<Int>::value_meta();
    const auto *value_ts = registry.ts(scalar_descriptor<Float>::value_meta());
    const auto *schema = registry.tsd(key, value_ts);

    std::cout << "{\"sizeof_ts_image\":" << sizeof(TSCheckpointImage) << ",\"sizeof_value\":" << sizeof(Value)
              << ",\"sizeof_node_image\":" << sizeof(NodeCheckpointImage) << "}\n";
    for (const std::size_t count : {1000, 10000, 100000})
    {
        TSOutput source{schema};
        auto source_view = source.data_view();
        {
            auto mutation = source_view.as_dict().begin_mutation(MIN_ST);
            for (std::size_t index = 0; index < count; ++index)
            {
                const Value entry_key{static_cast<Int>(index)};
                const Value entry_value{static_cast<Float>(index) * 0.5};
                auto child = mutation.at(entry_key.view());
                (void)child.begin_mutation(MIN_ST).copy_value_from(entry_value.view());
            }
        }

        std::vector<TSCheckpointImage> images(repetitions);
        const auto capture_us = median_us([&](std::size_t repetition) {
            images[repetition] = capture_ts_checkpoint(source_view);
        });

        const ComponentCheckpointStore checkpoints;
        const auto saved = envelope(images.front());
        const auto encode_us = median_us([&](std::size_t repetition) {
            checkpoints.write("image-" + std::to_string(repetition), saved);
        });
        std::vector<ComponentCheckpoint> decoded(repetitions);
        const auto decode_us = median_us([&](std::size_t repetition) {
            decoded[repetition] = checkpoints.read("image-" + std::to_string(repetition));
        });
        std::vector<std::unique_ptr<TSOutput>> targets;
        for (std::size_t repetition = 0; repetition < repetitions; ++repetition)
            targets.push_back(std::make_unique<TSOutput>(schema));
        const auto restore_us = median_us([&](std::size_t repetition) {
            restore_ts_checkpoint(targets[repetition]->data_view(), *decoded[repetition].graph.nodes.front().output);
        });
        auto restored_view = targets.front()->data_view();
        REQUIRE(restored_view.as_dict().size() == count);

        // Measure the encoded object rather than the in-memory image.
        const auto encoded_bytes = encoded_size(saved);

        std::vector<std::string> records(repetitions);
        const auto hand_record_us = median_us([&](std::size_t repetition) {
            auto &out = records[repetition];
            const auto dict = source_view.as_dict();
            out.reserve(count * 24 + 8);
            append(out, static_cast<std::uint64_t>(dict.size()));
            for (std::size_t slot = 0; slot < dict.slot_capacity(); ++slot)
            {
                if (!dict.slot_live(slot)) { continue; }
                const auto child = dict.at_slot(slot);
                append(out, dict.key_at_slot(slot).template checked_as<Int>());
                append(out, child.value().template checked_as<Float>());
                append(out, child.last_modified_time().time_since_epoch().count());
            }
        });
        std::vector<std::unique_ptr<TSOutput>> hand_targets;
        for (std::size_t repetition = 0; repetition < repetitions; ++repetition)
            hand_targets.push_back(std::make_unique<TSOutput>(schema));
        const auto hand_recover_us = median_us([&](std::size_t repetition) {
            const char *cursor = records[repetition].data();
            const auto entries = take<std::uint64_t>(cursor);
            auto target_view = hand_targets[repetition]->data_view();
            auto mutation = target_view.as_dict().begin_mutation(MIN_ST);
            for (std::uint64_t index = 0; index < entries; ++index)
            {
                const Value entry_key{take<Int>(cursor)};
                const Value entry_value{take<Float>(cursor)};
                const DateTime time{TimeDelta{take<std::int64_t>(cursor)}};
                (void)mutation.at(entry_key.view()).begin_mutation(time).copy_value_from(entry_value.view());
            }
        });
        auto recovered_view = hand_targets.front()->data_view();
        REQUIRE(recovered_view.as_dict().size() == count);

        std::cout << "{\"workload\":\"tsd_int_float\",\"count\":" << count
                  << ",\"encoded_bytes\":" << encoded_bytes
                  << ",\"bytes_per_entry\":" << static_cast<double>(encoded_bytes) / static_cast<double>(count)
                  << ",\"capture_us\":" << capture_us << ",\"encode_store_us\":" << encode_us
                  << ",\"read_decode_us\":" << decode_us << ",\"restore_us\":" << restore_us
                  << ",\"hand_bytes\":" << records.front().size()
                  << ",\"hand_record_us\":" << hand_record_us << ",\"hand_recover_us\":" << hand_recover_us
                  << "}\n";
    }
}
