#include <hgraph/types/record_replay.h>

#include <hgraph/runtime/graph.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/time_series/ts_delta.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/value/table_codec.h>

#include <arrow/table.h>
#include <arrow/array.h>

#include <stdexcept>
#include <unordered_map>
#include <vector>

namespace hgraph::record_replay
{
    namespace
    {
        inline constexpr std::string_view CONFIG_KEY{"__hgraph.record_replay.config__"};

        thread_local std::vector<ScopeState> g_scopes{};
        const ScopeState                     g_no_scope{};

        void ensure_config_type()
        {
            (void)TypeRegistry::instance().register_scalar<Config>("RecordReplayConfig");
        }

        // ---- the default (in-memory) frame store ----
        std::unordered_map<std::string, Frame> g_default_store;

        void default_store_write(void *, std::string_view key, Frame frame)
        {
            g_default_store[std::string{key}] = std::move(frame);
        }

        Frame default_store_read(void *, std::string_view key)
        {
            const auto it = g_default_store.find(std::string{key});
            return it == g_default_store.end() ? Frame{} : it->second;
        }

        bool default_store_contains(void *, std::string_view key)
        {
            return g_default_store.contains(std::string{key});
        }

        void default_store_clear(void *) { g_default_store.clear(); }

        [[nodiscard]] FrameStoreOps default_store_ops() noexcept
        {
            return FrameStoreOps{nullptr, &default_store_write, &default_store_read, &default_store_contains,
                                 &default_store_clear};
        }

        FrameStoreOps g_store = default_store_ops();
    }  // namespace

    void set_config(GlobalStateView state, Config config)
    {
        if (!state.valid()) { throw std::logic_error("record/replay configuration requires GlobalState"); }
        if (config.model.empty()) { throw std::invalid_argument("record/replay model must not be empty"); }
        if (config.date_key.empty() || config.as_of_key.empty())
        {
            throw std::invalid_argument("record/replay table keys must not be empty");
        }
        ensure_config_type();
        state.set(CONFIG_KEY, Value{std::move(config)});
    }

    Config config(GlobalStateView state)
    {
        if (!state.valid()) { return Config{}; }
        const ValueView value = state.get(CONFIG_KEY);
        return value.valid() ? value.checked_as<Config>() : Config{};
    }

    bool model_is(GlobalStateView state, std::string_view model) { return config(state).model == model; }

    const ScopeState &current_scope() noexcept { return g_scopes.empty() ? g_no_scope : g_scopes.back(); }

    scope::scope(Mode mode, std::string recordable_id)
    {
        g_scopes.push_back(ScopeState{mode, std::move(recordable_id)});
    }

    scope::~scope()
    {
        if (!g_scopes.empty()) { g_scopes.pop_back(); }
    }

    void set_frame_store(FrameStoreOps ops)
    {
        if (ops.write == nullptr || ops.read == nullptr || ops.contains == nullptr)
        {
            throw std::invalid_argument("frame store registration requires write/read/contains ops");
        }
        g_store = ops;
    }

    const FrameStoreOps &frame_store() { return g_store; }

    void store_write(std::string_view key, Frame frame) { g_store.write(g_store.context, key, std::move(frame)); }

    Frame store_read(std::string_view key) { return g_store.read(g_store.context, key); }

    bool store_contains(std::string_view key) { return g_store.contains(g_store.context, key); }

    Value replay_const_value(GlobalStateView state, std::string_view fq_key, const ValueTypeMetaData *meta,
                             DateTime tm, DateTime as_of)
    {
        const Frame frame = store_read(fq_key);
        if (!frame.has_value()) { return Value{}; }
        const Config cfg = config(state);
        const auto  &converter = table_converter(meta, cfg.date_key, cfg.as_of_key);

        const auto as_of_column = frame.table->GetColumnByName(converter.as_of_key);
        std::int64_t best = -1;
        for (std::int64_t row = 0; row < frame_rows(frame); ++row)
        {
            if (frame_value_time(converter, frame, row) > tm) { continue; }
            if (as_of_column != nullptr)
            {
                const auto &array =
                    static_cast<const arrow::TimestampArray &>(*as_of_column->chunk(0));
                if (DateTime{std::chrono::microseconds{array.Value(row)}} > as_of) { continue; }
            }
            best = row;   // rows are recorded in time order; keep the last qualifying one
        }
        return best < 0 ? Value{} : read_row(converter, frame, best);
    }

    ComparisonSummary comparison_summary(GlobalStateView state, std::string_view fq_key)
    {
        const Frame frame = store_read(fq_key);
        if (!frame.has_value())
        {
            throw std::runtime_error("no comparison recorded under '" + std::string{fq_key} + "'");
        }
        const Config cfg = config(state);
        const auto  &converter =
            table_converter(scalar_descriptor<Bool>::value_meta(), cfg.date_key, cfg.as_of_key);
        ComparisonSummary summary;
        summary.compared = static_cast<std::size_t>(frame_rows(frame));
        for (std::int64_t row = 0; row < frame_rows(frame); ++row)
        {
            if (!read_row(converter, frame, row).view().checked_as<Bool>()) { ++summary.mismatches; }
        }
        return summary;
    }

    Value recorded_seed_resolver(GlobalStateView state, std::string_view fq_key,
                                 const TSValueTypeMetaData *schema,
                                 DateTime start_time)
    {
        if (schema == nullptr) { return {}; }
        if (!model_is(state, IN_MEMORY))
        {
            return replay_const_value(
                state, fq_key, schema->value_schema, start_time,
                config(state).as_of.value_or(MAX_DT));
        }

        const ValueView buffer = state.get(":memory:" + std::string{fq_key});
        if (!buffer.valid()) { return {}; }

        TSOutput accumulated{schema};
        const auto entries = buffer.as_list();
        for (std::size_t i = 0; i < entries.size(); ++i)
        {
            const auto entry = entries.at(i).as_indexed_view();
            const DateTime when = entry.at(0).checked_as<DateTime>();
            if (when > start_time) { break; }
            apply_delta(accumulated.view(when), entry.at(1));
        }
        const auto view = accumulated.view(start_time);
        return view.valid() ? Value{view.value()} : Value{};
    }

    void reset() noexcept
    {
        g_scopes.clear();
        if (g_store.clear != nullptr) { g_store.clear(g_store.context); }
        g_store = default_store_ops();
        g_default_store.clear();
    }

    bool has_recordable_id(const GraphView &graph) noexcept
    {
        return graph.trait(RECORDABLE_ID_TRAIT).valid();
    }

    namespace
    {
        [[nodiscard]] std::string fq_from_parent(const ValueView &parent, std::string_view recordable_id)
        {
            if (!parent.valid())
            {
                if (recordable_id.empty())
                {
                    throw std::runtime_error("no recordable id provided and no parent recordable id trait found");
                }
                return std::string{recordable_id};
            }
            const Str &parent_id = parent.checked_as<Str>();
            if (recordable_id.empty()) { return parent_id; }
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
