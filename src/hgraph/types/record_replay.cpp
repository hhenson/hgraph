#include <hgraph/types/record_replay.h>

#include <hgraph/runtime/graph.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/table_codec.h>

#include <arrow/array.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <stdexcept>
#include <vector>

namespace hgraph::record_replay
{
    namespace
    {
        inline constexpr std::string_view CONFIG_KEY{"__hgraph.record_replay.config__"};
        inline constexpr std::string_view FRAME_STORE_KEY{"__hgraph.record_replay.frame_store__"};
        inline constexpr std::string_view RECORDING_LAYOUT_KEY{"hgraph.recording.layout"};
        inline constexpr std::string_view SEGMENTED_LAYOUT{"segmented"};
        inline constexpr std::string_view RECORDING_VERSION_KEY{"hgraph.recording.version"};

        thread_local std::vector<ScopeState> g_scopes{};
        const ScopeState                     g_no_scope{};

        void ensure_config_type()
        {
            (void)TypeRegistry::instance().register_scalar<Config>("RecordReplayConfig");
        }

        struct FrameStoreHolder
        {
            store::FrameStore store;
        };

        struct ProcessFrameStores
        {
            ProcessFrameStores()
            {
                store::FrameStoreConfig config;
                config.immutable = false;
                fallback = store::make_frame_store(std::move(config));
                active = fallback;
            }

            store::FrameStore fallback{};
            store::FrameStore active{};
        };

        [[nodiscard]] ProcessFrameStores &process_frame_stores()
        {
            static ProcessFrameStores stores;
            return stores;
        }

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

    void set_config(GlobalStateView state, Config config)
    {
        if (!state.valid())
        {
            throw std::logic_error("record/replay configuration requires GlobalState");
        }
        if (config.model.empty())
        {
            throw std::invalid_argument("record/replay model must not be empty");
        }
        if (config.date_key.empty() || config.as_of_key.empty())
        {
            throw std::invalid_argument("record/replay table keys must not be empty");
        }
        ensure_config_type();
        if (config.model == DATA_FRAME && !frame_store(state))
        {
            // DATA_FRAME has a real graph-owned C++ store by default. Besides
            // matching the configured file/S3 lifetime, this gives the memory
            // backend the same immutable-key behaviour as those deployments.
            set_frame_store(state, store::make_frame_store(store::FrameStoreConfig{}));
        }
        state.set(CONFIG_KEY, Value{std::move(config)});
    }

    Config config(GlobalStateView state)
    {
        if (!state.valid())
        {
            return Config{};
        }
        const ValueView value = state.get(CONFIG_KEY);
        return value.valid() ? value.checked_as<Config>() : Config{};
    }

    bool model_is(GlobalStateView state, std::string_view model)
    {
        return config(state).model == model;
    }

    std::string call_model(const OperatorCallContext &context)
    {
        // A call-site ``model`` wins over the graph's configuration. Reading it
        // here rather than in each guard is what keeps the overloads mutually
        // exclusive - they all decide against the same answer.
        if (const Str *local = context.scalar_as<Str>("model");
            local != nullptr && !local->empty())
        {
            return *local;
        }
        // By value: ``config`` returns a Config by value, so a view into its
        // ``model`` would dangle the moment the temporary died.
        return config(context.global_state).model;
    }

    bool call_model_is(const OperatorCallContext &context, std::string_view model)
    {
        return call_model(context) == model;
    }

    const ScopeState &current_scope() noexcept
    {
        return g_scopes.empty() ? g_no_scope : g_scopes.back();
    }

    scope::scope(Mode mode, std::string recordable_id)
    {
        g_scopes.push_back(ScopeState{mode, std::move(recordable_id)});
    }

    scope::~scope()
    {
        if (!g_scopes.empty())
        {
            g_scopes.pop_back();
        }
    }

    void set_frame_store(store::FrameStore frame_store)
    {
        if (!frame_store)
        {
            throw std::invalid_argument("frame store must not be empty");
        }
        process_frame_stores().active = std::move(frame_store);
    }

    const store::FrameStore &frame_store() { return process_frame_stores().active; }

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

    void store_write(std::string_view key, Frame frame)
    {
        if (GlobalState *state = GlobalContext::active_state())
        {
            if (auto selected = frame_store(state->view()))
            {
                selected.write(key, std::move(frame));
                return;
            }
        }
        frame_store().write(key, std::move(frame));
    }

    Frame store_read(std::string_view key)
    {
        if (GlobalState *state = GlobalContext::active_state())
        {
            if (auto selected = frame_store(state->view()))
            {
                return selected.read(key);
            }
        }
        return frame_store().read(key);
    }

    bool store_contains(std::string_view key)
    {
        if (GlobalState *state = GlobalContext::active_state())
        {
            if (auto selected = frame_store(state->view()))
            {
                return selected.contains(key);
            }
        }
        return frame_store().contains(key);
    }

    void store_write(GlobalStateView state, std::string_view key, Frame frame)
    {
        if (auto selected = frame_store(state))
        {
            selected.write(key, std::move(frame));
        }
        else
        {
            frame_store().write(key, std::move(frame));
        }
    }

    Frame store_read(GlobalStateView state, std::string_view key)
    {
        if (auto selected = frame_store(state))
        {
            return selected.read(key);
        }
        return frame_store().read(key);
    }

    bool store_contains(GlobalStateView state, std::string_view key)
    {
        if (auto selected = frame_store(state))
        {
            return selected.contains(key);
        }
        return frame_store().contains(key);
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
        const Config cfg = config(state);
        const auto  &converter = table_converter(meta, cfg.date_key, cfg.as_of_key);
        Value        result;
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

    ComparisonSummary comparison_summary(GlobalStateView state, std::string_view fq_key)
    {
        if (!store_contains(state, fq_key))
        {
            throw std::runtime_error("no comparison recorded under '" + std::string{fq_key} + "'");
        }
        const Config cfg = config(state);
        const auto  &converter =
            table_converter(scalar_descriptor<Bool>::value_meta(), cfg.date_key, cfg.as_of_key);
        ComparisonSummary summary;
        for_each_recording_frame(state, fq_key, [&](const Frame &frame) {
            summary.compared += static_cast<std::size_t>(frame_rows(frame));
            for (std::int64_t row = 0; row < frame_rows(frame); ++row)
            {
                if (!read_row(converter, frame, row).view().checked_as<Bool>())
                {
                    ++summary.mismatches;
                }
            }
        });
        return summary;
    }

    Value recorded_seed_resolver(GlobalStateView state, std::string_view fq_key,
                                 const TSValueTypeMetaData *schema, DateTime start_time)
    {
        if (schema == nullptr)
        {
            return {};
        }
        if (!model_is(state, IN_MEMORY))
        {
            return replay_const_value(state, fq_key, schema->value_schema, start_time,
                                      config(state).as_of.value_or(MAX_DT));
        }

        const ValueView buffer = state.get(":memory:" + std::string{fq_key});
        if (!buffer.valid())
        {
            return {};
        }

        TSOutput   accumulated{schema};
        const auto entries = buffer.as_list();
        for (std::size_t i = 0; i < entries.size(); ++i)
        {
            const auto     entry = entries.at(i).as_indexed_view();
            const DateTime when = entry.at(0).checked_as<DateTime>();
            if (when > start_time)
            {
                break;
            }
            apply_delta(accumulated.view(when), entry.at(1));
        }
        const auto view = accumulated.view(start_time);
        return view.valid() ? Value{view.value()} : Value{};
    }

    void reset() noexcept
    {
        g_scopes.clear();
        auto &stores = process_frame_stores();
        stores.active.clear();
        stores.fallback.clear();
        stores.active = stores.fallback;
    }

    bool has_recordable_id(const GraphView &graph) noexcept
    {
        return graph.trait(RECORDABLE_ID_TRAIT).valid();
    }

    namespace
    {
        [[nodiscard]] std::string fq_from_parent(const ValueView &parent,
                                                 std::string_view recordable_id)
        {
            if (!parent.valid())
            {
                if (recordable_id.empty())
                {
                    throw std::runtime_error(
                        "no recordable id provided and no parent recordable id trait found");
                }
                return std::string{recordable_id};
            }
            const Str &parent_id = parent.checked_as<Str>();
            if (recordable_id.empty())
            {
                return parent_id;
            }
            return parent_id + "." + std::string{recordable_id};
        }
    }  // namespace

    std::string fq_recordable_id(const TraitsView &traits, std::string_view recordable_id)
    {
        return fq_from_parent(traits.trait(RECORDABLE_ID_TRAIT), recordable_id);
    }

    std::string fq_recordable_id(const GraphView &graph, std::string_view recordable_id)
    {
        return fq_from_parent(graph.trait(RECORDABLE_ID_TRAIT), recordable_id);
    }
}  // namespace hgraph::record_replay
