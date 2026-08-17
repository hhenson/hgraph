#include <hgraph/persistence/recording_store.h>

#include <hgraph/runtime/global_state.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/table_config.h>
#include <hgraph/types/value/table_codec.h>

#include <arrow/array.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <chrono>
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

namespace hgraph::persistence
{
    namespace
    {
        /** The state key selecting the recording store. Extension-owned
            (RFC 0025 checkpoint 4); core never names it. */
        inline constexpr std::string_view FRAME_STORE_KEY{"__hgraph.record_replay.frame_store__"};
        inline constexpr std::string_view RECORDING_LAYOUT_KEY{"hgraph.recording.layout"};
        inline constexpr std::string_view SEGMENTED_LAYOUT{"segmented"};
        inline constexpr std::string_view RECORDING_VERSION_KEY{"hgraph.recording.version"};

        struct FrameStoreHolder
        {
            store::FrameStore store;
        };

        [[nodiscard]] Frame metadata_frame(std::vector<std::string> keys,
                                           std::vector<std::string> values)
        {
            arrow::BooleanBuilder         builder;
            std::shared_ptr<arrow::Array> marker;
            const auto                    status = builder.Finish(&marker);
            if (!status.ok())
            {
                throw std::runtime_error("build segmented-recording marker: " + status.ToString());
            }
            auto schema =
                arrow::schema({arrow::field("__hgraph_recording_marker__", arrow::boolean())},
                              arrow::key_value_metadata(std::move(keys), std::move(values)));
            return Frame{arrow::Table::Make(std::move(schema), {std::move(marker)}, 0)};
        }

        template <typename Fn>
        void for_each_recording_frame(GlobalStateView state, std::string_view key, Fn &&fn)
        {
            Frame frame = store_read(state, key);
            if (!frame.has_value())
            {
                return;
            }
            if (!is_segmented_recording(frame))
            {
                fn(frame);
                return;
            }
            for (std::size_t segment = 0;; ++segment)
            {
                frame = store_read(state, segment_key(key, segment));
                if (!frame.has_value())
                {
                    return;
                }
                fn(frame);
            }
        }
    }  // namespace

    void set_frame_store(GlobalStateView state, store::FrameStore frame_store)
    {
        if (!state.valid())
        {
            throw std::logic_error("installing a frame store requires GlobalState");
        }
        if (!frame_store)
        {
            throw std::invalid_argument("frame store must not be empty");
        }
        (void)TypeRegistry::instance().register_scalar<FrameStoreHolder>("__frame_store_holder__");
        state.set(FRAME_STORE_KEY, Value{FrameStoreHolder{std::move(frame_store)}});
    }

    void clear_frame_store(GlobalStateView state)
    {
        if (!state.valid())
        {
            throw std::logic_error("clearing a frame store requires GlobalState");
        }
        static_cast<void>(state.erase(FRAME_STORE_KEY));
    }

    store::FrameStore frame_store(GlobalStateView state)
    {
        if (!state.valid())
        {
            return {};
        }
        const ValueView value = state.get(FRAME_STORE_KEY);
        return value ? value.checked_as<FrameStoreHolder>().store : store::FrameStore{};
    }

    void install_fresh_frame_store(GlobalStateView state)
    {
        // The default store is memory-backed with immutable keys, matching
        // the configured file/S3 deployments' write-once behaviour.
        set_frame_store(state, store::make_frame_store(store::FrameStoreConfig{}));
    }

    store::FrameStore ensure_frame_store(GlobalStateView state)
    {
        if (auto selected = frame_store(state))
        {
            return selected;
        }
        install_fresh_frame_store(state);
        return frame_store(state);
    }

    void store_write(GlobalStateView state, std::string_view key, Frame frame)
    {
        ensure_frame_store(state).write(key, std::move(frame));
    }

    Frame store_read(GlobalStateView state, std::string_view key)
    {
        if (auto selected = frame_store(state))
        {
            return selected.read(key);
        }
        return {};
    }

    bool store_contains(GlobalStateView state, std::string_view key)
    {
        if (auto selected = frame_store(state))
        {
            return selected.contains(key);
        }
        return false;
    }

    Frame segmented_recording_marker()
    {
        return metadata_frame(
            {std::string{RECORDING_LAYOUT_KEY}, std::string{RECORDING_VERSION_KEY}},
            {std::string{SEGMENTED_LAYOUT}, "1"});
    }

    Frame segmented_recording_manifest(std::size_t segments, std::size_t rows)
    {
        return metadata_frame({std::string{RECORDING_LAYOUT_KEY},
                               std::string{RECORDING_VERSION_KEY}, "hgraph.recording.segments",
                               "hgraph.recording.rows"},
                              {"complete", "1", std::to_string(segments), std::to_string(rows)});
    }

    bool is_segmented_recording(const Frame &frame) noexcept
    {
        if (!frame.has_value() || frame.table->schema()->metadata() == nullptr)
        {
            return false;
        }
        return frame.table->schema()
                   ->metadata()
                   ->Get(std::string{RECORDING_LAYOUT_KEY})
                   .ValueOr("") == SEGMENTED_LAYOUT;
    }

    std::string segment_key(std::string_view key, std::size_t segment)
    {
        return std::string{key} + "." + std::to_string(segment);
    }

    std::string completion_key(std::string_view key) { return std::string{key} + ".complete"; }

    Value replay_const_value(GlobalStateView state, std::string_view fq_key,
                             const ValueTypeMetaData *meta, DateTime tm, DateTime as_of)
    {
        const table::TableConfig cfg = table::config(state);
        const auto              &converter = table_converter(meta, cfg.date_key, cfg.as_of_key);
        Value                    result;
        for_each_recording_frame(state, fq_key, [&](const Frame &frame) {
            const auto as_of_column = frame.table->GetColumnByName(converter.as_of_key);
            for (std::int64_t row = 0; row < frame_rows(frame); ++row)
            {
                if (frame_value_time(converter, frame, row) > tm)
                {
                    continue;
                }
                if (as_of_column != nullptr)
                {
                    const auto &array =
                        static_cast<const arrow::TimestampArray &>(*as_of_column->chunk(0));
                    if (DateTime{std::chrono::microseconds{array.Value(row)}} > as_of)
                    {
                        continue;
                    }
                }
                // Segments and their rows are monotonic, so assignment keeps
                // the last qualifying value across the whole recording.
                result = read_row(converter, frame, row);
            }
        });
        return result;
    }

    Value frame_seed_resolver(GlobalStateView state, std::string_view fq_key,
                              const TSValueTypeMetaData *schema, DateTime start_time)
    {
        if (schema == nullptr)
        {
            return {};
        }
        return replay_const_value(state, fq_key, schema->value_schema, start_time,
                                  table::config(state).as_of.value_or(MAX_DT));
    }
}  // namespace hgraph::persistence
