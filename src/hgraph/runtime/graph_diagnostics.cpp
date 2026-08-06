#include <hgraph/runtime/graph_diagnostics.h>

#include <hgraph/runtime/diagnostic_path.h>
#include <hgraph/runtime/executor.h>
#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/node.h>
#include <hgraph/types/series.h>
#include <hgraph/types/time_series_reference.h>
#include <hgraph/types/value/json_codec.h>
#include <hgraph/types/value/visitor.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <map>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace hgraph
{
    namespace
    {
        using DiagnosticsClock = std::chrono::steady_clock;
        using DiagnosticsTime = DiagnosticsClock::time_point;

        enum class DiagnosticsPhase : std::uint8_t
        {
            Start,
            Evaluation,
            Stop,
        };

        [[nodiscard]] TimeDelta elapsed(DiagnosticsTime start, DiagnosticsTime end) noexcept
        {
            return std::chrono::duration_cast<TimeDelta>(end - start);
        }

        [[nodiscard]] DateTime current_wall_time() noexcept
        {
            return std::chrono::time_point_cast<TimeDelta>(engine_clock::now());
        }

        [[nodiscard]] constexpr std::size_t phase_index(DiagnosticsPhase phase) noexcept
        {
            return static_cast<std::size_t>(phase);
        }

        [[nodiscard]] std::string schema_label(const TypeRecord *record)
        {
            return record == nullptr ? std::string{} : std::string{record->semantic_name()};
        }

        [[nodiscard]] std::string implementation_label(const TypeRecord *record)
        {
            return record == nullptr ? std::string{} : std::string{record->implementation_name()};
        }

        [[nodiscard]] std::string value_schema_label(const ValueView &value)
        {
            const ValueTypeMetaData *schema = value.schema();
            return schema == nullptr ? std::string{} : std::string{schema->name()};
        }

        using TargetResolver = std::uint64_t (*)(void *, const TSOutputView &);

        [[nodiscard]] std::uint64_t resolve_target_node(
            void *context, const TSOutputView &output)
        {
            auto &entities = *static_cast<
                std::unordered_map<const void *, std::uint64_t> *>(context);
            const NodeView owner = output.owner_node();
            if (!owner.valid()) { return 0; }
            const auto found = entities.find(owner.data());
            return found == entities.end() ? 0 : found->second;
        }

        void record_target(GraphDiagnosticValue *capture,
                           TargetResolver resolver, void *resolver_context,
                           const TSOutputView &output)
        {
            if (capture == nullptr || resolver == nullptr) { return; }
            const std::uint64_t id = resolver(resolver_context, output);
            if (id != 0 &&
                std::ranges::find(capture->target_node_ids, id) ==
                    capture->target_node_ids.end())
            {
                capture->target_node_ids.push_back(id);
            }
        }

        void append_json_string(std::string &target, std::string_view value)
        {
            constexpr char hex[] = "0123456789abcdef";
            target.push_back('"');
            for (const unsigned char character : value)
            {
                switch (character)
                {
                    case '"': target += "\\\""; break;
                    case '\\': target += "\\\\"; break;
                    case '\b': target += "\\b"; break;
                    case '\f': target += "\\f"; break;
                    case '\n': target += "\\n"; break;
                    case '\r': target += "\\r"; break;
                    case '\t': target += "\\t"; break;
                    default:
                        if (character < 0x20)
                        {
                            target += "\\u00";
                            target.push_back(hex[character >> 4U]);
                            target.push_back(hex[character & 0x0fU]);
                        }
                        else { target.push_back(static_cast<char>(character)); }
                }
            }
            target.push_back('"');
        }

        void append_diagnostic_json(std::string &target,
                                    const ValueView &value,
                                    DateTime evaluation_time,
                                    std::size_t depth,
                                    GraphDiagnosticValue *capture,
                                    TargetResolver resolver,
                                    void *resolver_context);

        void append_reference_json(std::string &target,
                                   const TimeSeriesReference &reference,
                                   DateTime evaluation_time,
                                   std::size_t depth,
                                   GraphDiagnosticValue *capture,
                                   TargetResolver resolver,
                                   void *resolver_context)
        {
            if (depth >= 32)
            {
                throw std::logic_error("reference rendering exceeded 32 levels");
            }
            if (reference.is_empty())
            {
                target += "null";
                return;
            }
            if (reference.is_peered())
            {
                TSOutputView output = reference.target_output().view(evaluation_time);
                record_target(capture, resolver, resolver_context, output);
                if (output.valid())
                {
                    const ValueView value = output.value();
                    append_diagnostic_json(target, value, evaluation_time,
                                           depth + 1, capture, resolver,
                                           resolver_context);
                }
                else { target += "null"; }
                return;
            }

            const auto &items = reference.items();
            const TSValueTypeMetaData *schema = reference.target_schema();
            const bool named = schema != nullptr &&
                               schema->kind == TSTypeKind::TSB &&
                               schema->data.tsb.field_count == items.size();
            target.push_back(named ? '{' : '[');
            for (std::size_t index = 0; index < items.size(); ++index)
            {
                if (index != 0) { target.push_back(','); }
                if (named)
                {
                    const char *name = schema->data.tsb.fields[index].name;
                    append_json_string(target, name == nullptr ? "" : name);
                    target.push_back(':');
                }
                append_reference_json(target, items[index], evaluation_time,
                                      depth + 1, capture, resolver,
                                      resolver_context);
            }
            target.push_back(named ? '}' : ']');
        }

        template <typename IndexedView>
        void append_sequence_json(std::string &target,
                                  const IndexedView &view,
                                  DateTime evaluation_time,
                                  std::size_t depth,
                                  GraphDiagnosticValue *capture,
                                  TargetResolver resolver,
                                  void *resolver_context)
        {
            target.push_back('[');
            for (std::size_t index = 0; index < view.size(); ++index)
            {
                if (index != 0) { target.push_back(','); }
                const ValueView child = view.at(index);
                append_diagnostic_json(target, child, evaluation_time,
                                       depth + 1, capture, resolver,
                                       resolver_context);
            }
            target.push_back(']');
        }

        void append_diagnostic_json(std::string &target,
                                    const ValueView &value,
                                    DateTime evaluation_time,
                                    std::size_t depth,
                                    GraphDiagnosticValue *capture,
                                    TargetResolver resolver,
                                    void *resolver_context)
        {
            if (!value.valid())
            {
                target += "null";
                return;
            }
            if (depth >= 32)
            {
                throw std::logic_error("value rendering exceeded 32 levels");
            }

            const ValueView concrete = value.concrete();
            if (!concrete.valid())
            {
                target += "null";
                return;
            }
            visit(
                concrete,
                [&](AtomicView atomic) {
                    if (atomic.holds_alternative<TimeSeriesReference>())
                    {
                        append_reference_json(
                            target,
                            atomic.checked_as<TimeSeriesReference>(),
                            evaluation_time, depth + 1, capture, resolver,
                            resolver_context);
                    }
                    else if (atomic.holds_alternative<Frame>())
                    {
                        std::ostringstream rendered;
                        rendered << atomic.checked_as<Frame>();
                        append_json_string(target, rendered.str());
                    }
                    else if (atomic.holds_alternative<Series>())
                    {
                        std::ostringstream rendered;
                        rendered << atomic.checked_as<Series>();
                        append_json_string(target, rendered.str());
                    }
                    else { target += to_json_string(atomic); }
                },
                [&](TupleView tuple) {
                    append_sequence_json(target, tuple, evaluation_time,
                                         depth, capture, resolver,
                                         resolver_context);
                },
                [&](BundleView bundle) {
                    const bool named = bundle.schema()->is_named_bundle();
                    target.push_back(named ? '{' : '[');
                    bool first = true;
                    for (std::size_t index = 0; index < bundle.size(); ++index)
                    {
                        const ValueView child = bundle.at(index);
                        if (!std::exchange(first, false)) { target.push_back(','); }
                        if (named)
                        {
                            const char *name = bundle.schema()->fields[index].name;
                            append_json_string(target, name == nullptr ? "" : name);
                            target.push_back(':');
                        }
                        append_diagnostic_json(target, child, evaluation_time,
                                               depth + 1, capture, resolver,
                                               resolver_context);
                    }
                    target.push_back(named ? '}' : ']');
                },
                [&](ListView list) {
                    append_sequence_json(target, list, evaluation_time,
                                         depth, capture, resolver,
                                         resolver_context);
                },
                [&](SetView set) {
                    target.push_back('[');
                    bool first = true;
                    for (const ValueView element : set)
                    {
                        if (!std::exchange(first, false)) { target.push_back(','); }
                        append_diagnostic_json(target, element,
                                               evaluation_time, depth + 1,
                                               capture, resolver,
                                               resolver_context);
                    }
                    target.push_back(']');
                },
                [&](MapView map) {
                    target.push_back('{');
                    bool first = true;
                    for (const auto [key, child] : map)
                    {
                        if (!std::exchange(first, false)) { target.push_back(','); }
                        const std::string key_text = to_json_string(key);
                        if (!key_text.empty() && key_text.front() == '"')
                        {
                            target += key_text;
                        }
                        else { append_json_string(target, key_text); }
                        target.push_back(':');
                        append_diagnostic_json(target, child,
                                               evaluation_time, depth + 1,
                                               capture, resolver,
                                               resolver_context);
                    }
                    target.push_back('}');
                },
                [&](CyclicBufferView buffer) {
                    append_sequence_json(target, buffer, evaluation_time,
                                         depth, capture, resolver,
                                         resolver_context);
                },
                [&](QueueView queue) {
                    append_sequence_json(target, queue, evaluation_time,
                                         depth, capture, resolver,
                                         resolver_context);
                });
        }

        [[nodiscard]] std::string diagnostic_json(const ValueView &value,
                                                   DateTime evaluation_time,
                                                   GraphDiagnosticValue *capture,
                                                   TargetResolver resolver,
                                                   void *resolver_context)
        {
            std::string target;
            append_diagnostic_json(target, value, evaluation_time, 0,
                                   capture, resolver, resolver_context);
            return target;
        }

        void capture_reference_targets(GraphDiagnosticValue &target,
                                       const TimeSeriesReference &reference,
                                       DateTime evaluation_time,
                                       TargetResolver resolver,
                                       void *resolver_context,
                                       std::size_t depth = 0)
        {
            if (depth >= 32 || reference.is_empty()) { return; }
            if (reference.is_peered())
            {
                TSOutputView output = reference.target_output().view(evaluation_time);
                record_target(&target, resolver, resolver_context, output);
                return;
            }
            for (const TimeSeriesReference &child : reference.items())
            {
                capture_reference_targets(target, child, evaluation_time,
                                          resolver, resolver_context,
                                          depth + 1);
            }
        }

        void capture_value(GraphDiagnosticValue &target, ValueView value,
                           bool valid, DateTime last_modified,
                           DateTime evaluation_time,
                           TargetResolver resolver,
                           void *resolver_context)
        {
            target.valid = valid;
            target.last_modified = last_modified;
            target.error.clear();
            target.frame = {};
            if (const std::string label = value_schema_label(value); !label.empty())
            {
                target.schema_label = label;
            }
            try
            {
                ValueView concrete = value.concrete();
                if (concrete.valid() && concrete.holds_alternative<Frame>())
                {
                    target.frame = concrete.checked_as<Frame>();
                }
                target.json = diagnostic_json(value, evaluation_time, &target,
                                              resolver, resolver_context);
            }
            catch (const std::exception &error)
            {
                target.json.clear();
                target.error = error.what();
            }
            catch (...)
            {
                target.json.clear();
                target.error = "value rendering failed";
            }
        }

        void capture_node_values(GraphDiagnosticEntry &entry,
                                 const NodeView &node,
                                 TargetResolver resolver,
                                 void *resolver_context)
        {
            const DateTime evaluation_time = node.graph().evaluation_time();
            if (entry.input.available)
            {
                try
                {
                    TSInputView input = node.input(evaluation_time);
                    entry.input.target_node_ids.clear();
                    capture_reference_targets(
                        entry.input, input.reference(), evaluation_time,
                        resolver, resolver_context);
                    capture_value(entry.input, input.value(), input.valid(),
                                  input.last_modified_time(), evaluation_time,
                                  resolver, resolver_context);
                }
                catch (const std::exception &error)
                {
                    entry.input.error = error.what();
                }
                catch (...)
                {
                    entry.input.error = "input rendering failed";
                }
            }
            if (entry.output.available)
            {
                try
                {
                    TSOutputView output = node.output(evaluation_time);
                    entry.output.target_node_ids.clear();
                    capture_value(entry.output, output.value(), output.valid(),
                                  output.last_modified_time(), evaluation_time,
                                  resolver, resolver_context);
                }
                catch (const std::exception &error)
                {
                    entry.output.error = error.what();
                }
                catch (...)
                {
                    entry.output.error = "output rendering failed";
                }
            }
            if (entry.scalars.available)
            {
                try
                {
                    ValueView scalars = node.scalars();
                    const bool valid = scalars.valid();
                    entry.scalars.target_node_ids.clear();
                    capture_value(entry.scalars, std::move(scalars), valid,
                                  MIN_DT, evaluation_time, resolver,
                                  resolver_context);
                }
                catch (const std::exception &error)
                {
                    entry.scalars.error = error.what();
                }
                catch (...)
                {
                    entry.scalars.error = "scalar rendering failed";
                }
            }
        }

        void max_storage(NodeStorageMetrics &target, const NodeStorageMetrics &value) noexcept
        {
            target.static_bytes = std::max(target.static_bytes, value.static_bytes);
            target.nested_graph_count = std::max(target.nested_graph_count, value.nested_graph_count);
            target.nested_graph_capacity = std::max(target.nested_graph_capacity, value.nested_graph_capacity);
            target.nested_graph_blocks = std::max(target.nested_graph_blocks, value.nested_graph_blocks);
            target.dynamic_live_bytes = std::max(target.dynamic_live_bytes, value.dynamic_live_bytes);
            target.dynamic_reserved_bytes = std::max(target.dynamic_reserved_bytes, value.dynamic_reserved_bytes);
        }
    }  // namespace

    struct GraphDiagnostics::State
    {
        struct PhaseState
        {
            EvaluationProfilePhase snapshot{};
            std::vector<TimeDelta> recent{};
            std::size_t recent_cursor{0};
        };

        struct EntryState
        {
            GraphDiagnosticEntry snapshot{};
            PhaseState start{};
            PhaseState evaluation{};
            PhaseState stop{};
            std::array<std::optional<DiagnosticsTime>, 3> active{};
        };

        mutable std::mutex mutex{};
        std::map<std::uint64_t, EntryState> entries{};
        std::unordered_map<const void *, std::uint64_t> graph_entities{};
        std::unordered_map<const void *, std::uint64_t> node_entities{};
        std::uint64_t next_id{1};
        std::optional<DiagnosticsTime> wall_started{};
        std::optional<DiagnosticsTime> root_evaluation_started{};
        TimeDelta wall_time{0};
        std::uint64_t graph_cycles{0};
        TimeDelta root_evaluation_time{0};
        TimeDelta scheduling_lag_total{0};
        TimeDelta scheduling_lag_max{0};
        std::uint64_t scheduling_lag_samples{0};
        std::size_t planned_bytes{0};
        std::size_t dynamic_live_bytes{0};
        std::size_t dynamic_reserved_bytes{0};
        std::size_t peak_dynamic_live_bytes{0};
        std::size_t peak_dynamic_reserved_bytes{0};
    };

    namespace
    {
        [[nodiscard]] GraphDiagnostics::State::PhaseState &phase_state(
            GraphDiagnostics::State::EntryState &entry, DiagnosticsPhase phase) noexcept
        {
            switch (phase)
            {
                case DiagnosticsPhase::Start: return entry.start;
                case DiagnosticsPhase::Evaluation: return entry.evaluation;
                case DiagnosticsPhase::Stop: return entry.stop;
            }
            std::terminate();
        }

        void record_duration(GraphDiagnostics::State::PhaseState &phase,
                             TimeDelta duration, bool failed,
                             std::size_t recent_window)
        {
            duration = std::max(duration, TimeDelta{0});
            ++phase.snapshot.count;
            if (failed) { ++phase.snapshot.failures; }
            phase.snapshot.total_time += duration;
            phase.snapshot.max_time = std::max(phase.snapshot.max_time, duration);
            if (recent_window == 0)
            {
                phase.recent.clear();
                phase.recent_cursor = 0;
                phase.snapshot.recent_time = TimeDelta{0};
                return;
            }
            if (phase.recent.capacity() < recent_window) { phase.recent.reserve(recent_window); }
            if (phase.recent.size() < recent_window)
            {
                phase.recent.push_back(duration);
                phase.snapshot.recent_time += duration;
                return;
            }
            phase.snapshot.recent_time -= phase.recent[phase.recent_cursor];
            phase.recent[phase.recent_cursor] = duration;
            phase.snapshot.recent_time += duration;
            phase.recent_cursor = (phase.recent_cursor + 1) % recent_window;
        }

        void begin_phase(GraphDiagnostics::State::EntryState &entry,
                         DiagnosticsPhase phase) noexcept
        {
            entry.active[phase_index(phase)] = DiagnosticsClock::now();
        }

        void end_phase(GraphDiagnostics::State::EntryState &entry,
                       DiagnosticsPhase phase, bool failed,
                       std::size_t recent_window)
        {
            auto &started = entry.active[phase_index(phase)];
            if (!started.has_value()) { return; }
            record_duration(phase_state(entry, phase),
                            elapsed(*started, DiagnosticsClock::now()),
                            failed, recent_window);
            started.reset();
        }

        [[nodiscard]] GraphDiagnostics::State::EntryState *find_entry(
            GraphDiagnostics::State &state,
            const std::unordered_map<const void *, std::uint64_t> &entities,
            const void *identity) noexcept
        {
            const auto entity = entities.find(identity);
            if (entity == entities.end()) { return nullptr; }
            const auto entry = state.entries.find(entity->second);
            return entry == state.entries.end() ? nullptr : &entry->second;
        }

        [[nodiscard]] std::uint64_t parent_id_for_graph(
            GraphDiagnostics::State &state, const GraphView &graph) noexcept
        {
            if (!graph.is_nested()) { return 0; }
            NodeView parent = graph.as_nested().parent_node();
            const auto found = state.node_entities.find(parent.data());
            return found == state.node_entities.end() ? 0 : found->second;
        }

        [[nodiscard]] std::uint64_t parent_id_for_node(
            GraphDiagnostics::State &state, const NodeView &node) noexcept
        {
            GraphView graph = node.graph();
            const auto found = state.graph_entities.find(graph.data());
            return found == state.graph_entities.end() ? 0 : found->second;
        }

        [[nodiscard]] std::string child_path(const GraphDiagnostics::State &state,
                                             std::uint64_t parent_id,
                                             std::string_view suffix)
        {
            const auto parent = state.entries.find(parent_id);
            std::string result = parent == state.entries.end()
                                     ? std::string{"[]"}
                                     : parent->second.snapshot.path;
            result += suffix;
            return result;
        }

        void attach_to_parent(GraphDiagnostics::State &state, std::uint64_t parent_id,
                              std::uint64_t child_id)
        {
            if (parent_id == 0) { return; }
            const auto parent = state.entries.find(parent_id);
            if (parent != state.entries.end())
            {
                parent->second.snapshot.children.push_back(child_id);
            }
        }

        void update_totals(GraphDiagnostics::State &state,
                           GraphDiagnostics::State::EntryState &entry,
                           NodeStorageMetrics metrics)
        {
            state.dynamic_live_bytes -= entry.snapshot.storage.dynamic_live_bytes;
            state.dynamic_reserved_bytes -= entry.snapshot.storage.dynamic_reserved_bytes;
            state.dynamic_live_bytes += metrics.dynamic_live_bytes;
            state.dynamic_reserved_bytes += metrics.dynamic_reserved_bytes;
            state.peak_dynamic_live_bytes = std::max(
                state.peak_dynamic_live_bytes, state.dynamic_live_bytes);
            state.peak_dynamic_reserved_bytes = std::max(
                state.peak_dynamic_reserved_bytes, state.dynamic_reserved_bytes);
            entry.snapshot.storage = metrics;
            max_storage(entry.snapshot.peak_storage, metrics);
        }

        void clear_dynamic_totals(GraphDiagnostics::State &state,
                                  GraphDiagnostics::State::EntryState &entry) noexcept
        {
            state.dynamic_live_bytes -= entry.snapshot.storage.dynamic_live_bytes;
            state.dynamic_reserved_bytes -= entry.snapshot.storage.dynamic_reserved_bytes;
            entry.snapshot.storage.nested_graph_count = 0;
            entry.snapshot.storage.nested_graph_capacity = 0;
            entry.snapshot.storage.nested_graph_blocks = 0;
            entry.snapshot.storage.dynamic_live_bytes = 0;
            entry.snapshot.storage.dynamic_reserved_bytes = 0;
        }

        [[nodiscard]] NodeStorageMetrics graph_storage_metrics(
            const GraphView &graph) noexcept
        {
            NodeStorageMetrics result{};
            if (!graph.valid()) { return result; }
            result.static_bytes = graph.type().checked_plan().layout.size;
            if (graph.is_root())
            {
                const DynamicStorageMetrics pooled =
                    graph.compound_scalar_storage().metrics();
                result.dynamic_live_bytes = pooled.live_bytes;
                result.dynamic_reserved_bytes = pooled.reserved_bytes;
            }
            return result;
        }

        void refresh_graph(GraphDiagnostics::State &state, const GraphView &graph)
        {
            auto *entry = find_entry(state, state.graph_entities, graph.data());
            if (entry == nullptr) { return; }
            entry->snapshot.started = graph.started();
            entry->snapshot.evaluation_time = graph.evaluation_time();
            entry->snapshot.scheduled_time = graph.next_scheduled_time();
            update_totals(state, *entry, graph_storage_metrics(graph));

            for (std::size_t index = 0; index < graph.node_count(); ++index)
            {
                NodeView node = graph.node_at(index);
                auto *node_entry = find_entry(state, state.node_entities, node.data());
                if (node_entry == nullptr) { continue; }
                node_entry->snapshot.scheduled_time = graph.node_scheduled_time(index);
                update_totals(state, *node_entry, node.storage_metrics());
            }
        }

        [[nodiscard]] bool failed_node_is(const NodeView &node)
        {
            NodeView failed = node.graph().failed_node();
            return failed.valid() && failed.pointer() == node.pointer();
        }
    }  // namespace

    GraphDiagnostics::GraphDiagnostics(GraphDiagnosticsOptions options)
        : options_(options), state_(std::make_shared<State>())
    {
    }

    GraphDiagnostics::GraphDiagnostics(std::size_t recent_window)
        : GraphDiagnostics(GraphDiagnosticsOptions{.recent_window = recent_window})
    {
    }

    GraphDiagnosticsSnapshot GraphDiagnostics::snapshot() const
    {
        std::scoped_lock lock{state_->mutex};
        GraphDiagnosticsSnapshot result;
        result.graph_cycles = state_->graph_cycles;
        result.wall_time = state_->wall_started.has_value()
                               ? elapsed(*state_->wall_started, DiagnosticsClock::now())
                               : state_->wall_time;
        result.root_evaluation_time = state_->root_evaluation_time;
        result.scheduling_lag_total = state_->scheduling_lag_total;
        result.scheduling_lag_max = state_->scheduling_lag_max;
        result.scheduling_lag_samples = state_->scheduling_lag_samples;
        result.runtime_load = result.wall_time > TimeDelta{0}
                                  ? static_cast<double>(result.root_evaluation_time.count()) /
                                        static_cast<double>(result.wall_time.count())
                                  : 0.0;
        result.planned_bytes = state_->planned_bytes;
        result.dynamic_live_bytes = state_->dynamic_live_bytes;
        result.dynamic_reserved_bytes = state_->dynamic_reserved_bytes;
        result.peak_dynamic_live_bytes = state_->peak_dynamic_live_bytes;
        result.peak_dynamic_reserved_bytes = state_->peak_dynamic_reserved_bytes;
        result.entries.reserve(state_->entries.size());
        for (const auto &[id, entry] : state_->entries)
        {
            static_cast<void>(id);
            GraphDiagnosticEntry copy = entry.snapshot;
            copy.start = entry.start.snapshot;
            copy.evaluation = entry.evaluation.snapshot;
            copy.stop = entry.stop.snapshot;
            result.entries.push_back(std::move(copy));
        }
        return result;
    }

    void GraphDiagnostics::reset()
    {
        std::scoped_lock lock{state_->mutex};
        if (!state_->graph_entities.empty())
        {
            throw std::logic_error{"graph diagnostics cannot be reset while a graph is active"};
        }
        state_->entries.clear();
        state_->graph_entities.clear();
        state_->node_entities.clear();
        state_->next_id = 1;
        state_->wall_started.reset();
        state_->root_evaluation_started.reset();
        state_->wall_time = TimeDelta{0};
        state_->graph_cycles = 0;
        state_->root_evaluation_time = TimeDelta{0};
        state_->scheduling_lag_total = TimeDelta{0};
        state_->scheduling_lag_max = TimeDelta{0};
        state_->scheduling_lag_samples = 0;
        state_->planned_bytes = 0;
        state_->dynamic_live_bytes = 0;
        state_->dynamic_reserved_bytes = 0;
        state_->peak_dynamic_live_bytes = 0;
        state_->peak_dynamic_reserved_bytes = 0;
    }

    void GraphDiagnostics::on_before_start_graph(const GraphView &graph)
    {
        std::scoped_lock lock{state_->mutex};
        if (graph.is_root()) { state_->wall_started = DiagnosticsClock::now(); }
        const std::uint64_t id = state_->next_id++;
        const std::uint64_t parent_id = parent_id_for_graph(*state_, graph);
        GraphDiagnosticEntry snapshot{
            .id = id,
            .parent_id = parent_id,
            .path = graph.is_root()
                        ? std::string{"[]"}
                        : child_path(*state_, parent_id,
                                     "/" + diagnostic::graph_label(graph) +
                                         "#" + std::to_string(id)),
            .label = diagnostic::graph_label(graph),
            .schema_label = schema_label(graph.type().record()),
            .implementation_label = implementation_label(graph.type().record()),
            .kind = GraphDiagnosticEntityKind::Graph,
            .started = graph.started(),
            .evaluation_time = graph.evaluation_time(),
            .scheduled_time = graph.next_scheduled_time(),
        };
        auto [entry, inserted] = state_->entries.emplace(
            id, State::EntryState{.snapshot = std::move(snapshot)});
        static_cast<void>(inserted);
        update_totals(*state_, entry->second, graph_storage_metrics(graph));
        state_->graph_entities[graph.data()] = id;
        attach_to_parent(*state_, parent_id, id);
        if (graph.is_root())
        {
            state_->planned_bytes = std::max(
                state_->planned_bytes, graph.type().checked_plan().layout.size);
        }
        begin_phase(state_->entries.at(id), DiagnosticsPhase::Start);
    }

    void GraphDiagnostics::on_after_start_graph(const GraphView &graph)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->graph_entities, graph.data()))
        {
            end_phase(*entry, DiagnosticsPhase::Start, false, options_.recent_window);
            entry->snapshot.started = true;
            refresh_graph(*state_, graph);
        }
    }

    void GraphDiagnostics::on_start_graph_failed(const GraphView &graph)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->graph_entities, graph.data()))
        {
            end_phase(*entry, DiagnosticsPhase::Start, true, options_.recent_window);
            entry->snapshot.stopped = true;
            entry->snapshot.started = false;
            clear_dynamic_totals(*state_, *entry);
        }
        state_->graph_entities.erase(graph.data());
        if (graph.is_root() && state_->wall_started.has_value())
        {
            state_->wall_time = elapsed(*state_->wall_started, DiagnosticsClock::now());
            state_->wall_started.reset();
        }
    }

    void GraphDiagnostics::on_before_start_node(const NodeView &node)
    {
        std::scoped_lock lock{state_->mutex};
        const std::uint64_t id = state_->next_id++;
        const std::uint64_t parent_id = parent_id_for_node(*state_, node);
        const NodeStorageMetrics storage = node.storage_metrics();
        GraphDiagnosticEntry snapshot{
            .id = id,
            .parent_id = parent_id,
            .path = child_path(*state_, parent_id,
                               "." + diagnostic::node_label(node) +
                                   "<" + std::to_string(node.node_index()) + ">"),
            .label = diagnostic::node_label(node),
            .schema_label = schema_label(node.type().record()),
            .implementation_label = implementation_label(node.type().record()),
            .kind = GraphDiagnosticEntityKind::Node,
            .node_kind = node.node_kind(),
            .node_index = node.node_index(),
            .started = node.started(),
            .evaluation_time = node.graph().evaluation_time(),
            .scheduled_time = node.graph().node_scheduled_time(node.node_index()),
            .input = GraphDiagnosticValue{
                .available = node.has_input(),
                .schema_label = node.has_input()
                                    ? std::string{node.schema()->input_schema->name()}
                                    : std::string{},
            },
            .output = GraphDiagnosticValue{
                .available = node.has_output(),
                .schema_label = node.has_output()
                                    ? std::string{node.schema()->output_schema->name()}
                                    : std::string{},
            },
            .scalars = GraphDiagnosticValue{
                .available = node.has_scalars(),
                .schema_label = node.has_scalars()
                                    ? std::string{node.schema()->scalar_schema->name()}
                                    : std::string{},
            },
        };
        auto [entry, inserted] = state_->entries.emplace(
            id, State::EntryState{.snapshot = std::move(snapshot)});
        static_cast<void>(inserted);
        state_->node_entities[node.data()] = id;
        attach_to_parent(*state_, parent_id, id);
        update_totals(*state_, entry->second, storage);
        begin_phase(entry->second, DiagnosticsPhase::Start);
    }

    void GraphDiagnostics::on_after_start_node(const NodeView &node)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->node_entities, node.data()))
        {
            end_phase(*entry, DiagnosticsPhase::Start, false, options_.recent_window);
            entry->snapshot.started = true;
            update_totals(*state_, *entry, node.storage_metrics());
            if (options_.capture_values)
            {
                capture_node_values(entry->snapshot, node,
                                    resolve_target_node,
                                    &state_->node_entities);
            }
        }
    }

    void GraphDiagnostics::on_start_node_failed(const NodeView &node)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->node_entities, node.data()))
        {
            end_phase(*entry, DiagnosticsPhase::Start, true, options_.recent_window);
            entry->snapshot.started = false;
            entry->snapshot.stopped = true;
            clear_dynamic_totals(*state_, *entry);
        }
        state_->node_entities.erase(node.data());
    }

    void GraphDiagnostics::on_before_graph_evaluation(const GraphView &graph)
    {
        std::scoped_lock lock{state_->mutex};
        if (graph.is_root())
        {
            state_->root_evaluation_started = DiagnosticsClock::now();
            if (graph.executor().schema()->mode == GraphExecutorMode::RealTime)
            {
                const TimeDelta lag = std::max(
                    current_wall_time() - graph.evaluation_time(), TimeDelta{0});
                state_->scheduling_lag_total += lag;
                state_->scheduling_lag_max = std::max(state_->scheduling_lag_max, lag);
                ++state_->scheduling_lag_samples;
            }
        }
        if (auto *entry = find_entry(*state_, state_->graph_entities, graph.data()))
        {
            refresh_graph(*state_, graph);
            begin_phase(*entry, DiagnosticsPhase::Evaluation);
        }
    }

    void GraphDiagnostics::on_after_graph_evaluation(const GraphView &graph)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->graph_entities, graph.data()))
        {
            end_phase(*entry, DiagnosticsPhase::Evaluation,
                      graph.failed_node().valid(), options_.recent_window);
            refresh_graph(*state_, graph);
        }
        if (graph.is_root())
        {
            ++state_->graph_cycles;
            if (state_->root_evaluation_started.has_value())
            {
                state_->root_evaluation_time += elapsed(
                    *state_->root_evaluation_started, DiagnosticsClock::now());
                state_->root_evaluation_started.reset();
            }
        }
    }

    void GraphDiagnostics::on_before_node_evaluation(const NodeView &node)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->node_entities, node.data()))
        {
            begin_phase(*entry, DiagnosticsPhase::Evaluation);
        }
    }

    void GraphDiagnostics::on_after_node_evaluation(const NodeView &node)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->node_entities, node.data()))
        {
            end_phase(*entry, DiagnosticsPhase::Evaluation, failed_node_is(node),
                      options_.recent_window);
            entry->snapshot.evaluation_time = node.graph().evaluation_time();
            entry->snapshot.scheduled_time =
                node.graph().node_scheduled_time(node.node_index());
            update_totals(*state_, *entry, node.storage_metrics());
            if (options_.capture_values)
            {
                capture_node_values(entry->snapshot, node,
                                    resolve_target_node,
                                    &state_->node_entities);
            }
        }
    }

    void GraphDiagnostics::on_after_graph_push_nodes_evaluation(const GraphView &graph)
    {
        std::scoped_lock lock{state_->mutex};
        refresh_graph(*state_, graph);
    }

    void GraphDiagnostics::on_before_stop_node(const NodeView &node)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->node_entities, node.data()))
        {
            update_totals(*state_, *entry, node.storage_metrics());
            if (options_.capture_values)
            {
                capture_node_values(entry->snapshot, node,
                                    resolve_target_node,
                                    &state_->node_entities);
            }
            begin_phase(*entry, DiagnosticsPhase::Stop);
        }
    }

    void GraphDiagnostics::on_after_stop_node(const NodeView &node)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->node_entities, node.data()))
        {
            end_phase(*entry, DiagnosticsPhase::Stop, false, options_.recent_window);
            entry->snapshot.started = false;
            entry->snapshot.stopped = true;
            entry->snapshot.scheduled_time = MIN_DT;
            clear_dynamic_totals(*state_, *entry);
        }
        state_->node_entities.erase(node.data());
    }

    void GraphDiagnostics::on_stop_node_failed(const NodeView &node)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->node_entities, node.data()))
        {
            end_phase(*entry, DiagnosticsPhase::Stop, true, options_.recent_window);
            entry->snapshot.started = false;
            entry->snapshot.stopped = true;
            entry->snapshot.scheduled_time = MIN_DT;
            clear_dynamic_totals(*state_, *entry);
        }
        state_->node_entities.erase(node.data());
    }

    void GraphDiagnostics::on_before_stop_graph(const GraphView &graph)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->graph_entities, graph.data()))
        {
            refresh_graph(*state_, graph);
            begin_phase(*entry, DiagnosticsPhase::Stop);
        }
    }

    void GraphDiagnostics::on_after_stop_graph(const GraphView &graph)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->graph_entities, graph.data()))
        {
            end_phase(*entry, DiagnosticsPhase::Stop, false, options_.recent_window);
            entry->snapshot.started = false;
            entry->snapshot.stopped = true;
            entry->snapshot.scheduled_time = MIN_DT;
            clear_dynamic_totals(*state_, *entry);
        }
        state_->graph_entities.erase(graph.data());
        if (graph.is_root() && state_->wall_started.has_value())
        {
            state_->wall_time = elapsed(*state_->wall_started, DiagnosticsClock::now());
            state_->wall_started.reset();
        }
    }

    void GraphDiagnostics::on_stop_graph_failed(const GraphView &graph)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->graph_entities, graph.data()))
        {
            end_phase(*entry, DiagnosticsPhase::Stop, true, options_.recent_window);
            entry->snapshot.started = false;
            entry->snapshot.stopped = true;
            entry->snapshot.scheduled_time = MIN_DT;
            clear_dynamic_totals(*state_, *entry);
        }
        state_->graph_entities.erase(graph.data());
        if (graph.is_root() && state_->wall_started.has_value())
        {
            state_->wall_time = elapsed(*state_->wall_started, DiagnosticsClock::now());
            state_->wall_started.reset();
        }
    }
}  // namespace hgraph
