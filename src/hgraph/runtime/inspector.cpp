#include <hgraph/runtime/inspector.h>

#include <hgraph/runtime/diagnostic_path.h>
#include <hgraph/runtime/executor.h>
#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/node.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <map>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace hgraph
{
    namespace
    {
        using InspectionClock = std::chrono::steady_clock;
        using InspectionTime = InspectionClock::time_point;

        enum class InspectionPhase : std::uint8_t
        {
            Start,
            Evaluation,
            Stop,
        };

        [[nodiscard]] TimeDelta elapsed(InspectionTime start, InspectionTime end) noexcept
        {
            return std::chrono::duration_cast<TimeDelta>(end - start);
        }

        [[nodiscard]] DateTime current_wall_time() noexcept
        {
            return std::chrono::time_point_cast<TimeDelta>(engine_clock::now());
        }

        [[nodiscard]] constexpr std::size_t phase_index(InspectionPhase phase) noexcept
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

    struct Inspector::State
    {
        struct PhaseState
        {
            EvaluationProfilePhase snapshot{};
            std::vector<TimeDelta> recent{};
            std::size_t recent_cursor{0};
        };

        struct EntryState
        {
            InspectionEntry snapshot{};
            PhaseState start{};
            PhaseState evaluation{};
            PhaseState stop{};
            std::array<std::optional<InspectionTime>, 3> active{};
        };

        mutable std::mutex mutex{};
        std::map<std::uint64_t, EntryState> entries{};
        std::unordered_map<const void *, std::uint64_t> graph_entities{};
        std::unordered_map<const void *, std::uint64_t> node_entities{};
        std::uint64_t next_id{1};
        std::optional<InspectionTime> wall_started{};
        std::optional<InspectionTime> root_evaluation_started{};
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
        [[nodiscard]] Inspector::State::PhaseState &phase_state(
            Inspector::State::EntryState &entry, InspectionPhase phase) noexcept
        {
            switch (phase)
            {
                case InspectionPhase::Start: return entry.start;
                case InspectionPhase::Evaluation: return entry.evaluation;
                case InspectionPhase::Stop: return entry.stop;
            }
            std::terminate();
        }

        void record_duration(Inspector::State::PhaseState &phase,
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

        void begin_phase(Inspector::State::EntryState &entry,
                         InspectionPhase phase) noexcept
        {
            entry.active[phase_index(phase)] = InspectionClock::now();
        }

        void end_phase(Inspector::State::EntryState &entry,
                       InspectionPhase phase, bool failed,
                       std::size_t recent_window)
        {
            auto &started = entry.active[phase_index(phase)];
            if (!started.has_value()) { return; }
            record_duration(phase_state(entry, phase),
                            elapsed(*started, InspectionClock::now()),
                            failed, recent_window);
            started.reset();
        }

        [[nodiscard]] Inspector::State::EntryState *find_entry(
            Inspector::State &state,
            const std::unordered_map<const void *, std::uint64_t> &entities,
            const void *identity) noexcept
        {
            const auto entity = entities.find(identity);
            if (entity == entities.end()) { return nullptr; }
            const auto entry = state.entries.find(entity->second);
            return entry == state.entries.end() ? nullptr : &entry->second;
        }

        [[nodiscard]] std::uint64_t parent_id_for_graph(
            Inspector::State &state, const GraphView &graph) noexcept
        {
            if (!graph.is_nested()) { return 0; }
            NodeView parent = graph.as_nested().parent_node();
            const auto found = state.node_entities.find(parent.data());
            return found == state.node_entities.end() ? 0 : found->second;
        }

        [[nodiscard]] std::uint64_t parent_id_for_node(
            Inspector::State &state, const NodeView &node) noexcept
        {
            GraphView graph = node.graph();
            const auto found = state.graph_entities.find(graph.data());
            return found == state.graph_entities.end() ? 0 : found->second;
        }

        [[nodiscard]] std::string child_path(const Inspector::State &state,
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

        void attach_to_parent(Inspector::State &state, std::uint64_t parent_id,
                              std::uint64_t child_id)
        {
            if (parent_id == 0) { return; }
            const auto parent = state.entries.find(parent_id);
            if (parent != state.entries.end())
            {
                parent->second.snapshot.children.push_back(child_id);
            }
        }

        void update_totals(Inspector::State &state,
                           Inspector::State::EntryState &entry,
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

        void clear_dynamic_totals(Inspector::State &state,
                                  Inspector::State::EntryState &entry) noexcept
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

        void refresh_graph(Inspector::State &state, const GraphView &graph)
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

    Inspector::Inspector(InspectorOptions options)
        : options_(options), state_(std::make_shared<State>())
    {
    }

    Inspector::Inspector(std::size_t recent_window)
        : Inspector(InspectorOptions{.recent_window = recent_window})
    {
    }

    InspectionSnapshot Inspector::snapshot() const
    {
        std::scoped_lock lock{state_->mutex};
        InspectionSnapshot result;
        result.graph_cycles = state_->graph_cycles;
        result.wall_time = state_->wall_started.has_value()
                               ? elapsed(*state_->wall_started, InspectionClock::now())
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
            InspectionEntry copy = entry.snapshot;
            copy.start = entry.start.snapshot;
            copy.evaluation = entry.evaluation.snapshot;
            copy.stop = entry.stop.snapshot;
            result.entries.push_back(std::move(copy));
        }
        return result;
    }

    void Inspector::reset()
    {
        std::scoped_lock lock{state_->mutex};
        if (!state_->graph_entities.empty())
        {
            throw std::logic_error{"inspector cannot be reset while a graph is active"};
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

    void Inspector::on_before_start_graph(const GraphView &graph)
    {
        std::scoped_lock lock{state_->mutex};
        if (graph.is_root()) { state_->wall_started = InspectionClock::now(); }
        const std::uint64_t id = state_->next_id++;
        const std::uint64_t parent_id = parent_id_for_graph(*state_, graph);
        InspectionEntry snapshot{
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
            .kind = InspectionEntityKind::Graph,
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
        begin_phase(state_->entries.at(id), InspectionPhase::Start);
    }

    void Inspector::on_after_start_graph(const GraphView &graph)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->graph_entities, graph.data()))
        {
            end_phase(*entry, InspectionPhase::Start, false, options_.recent_window);
            entry->snapshot.started = true;
            refresh_graph(*state_, graph);
        }
    }

    void Inspector::on_start_graph_failed(const GraphView &graph)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->graph_entities, graph.data()))
        {
            end_phase(*entry, InspectionPhase::Start, true, options_.recent_window);
            entry->snapshot.stopped = true;
            entry->snapshot.started = false;
            clear_dynamic_totals(*state_, *entry);
        }
        state_->graph_entities.erase(graph.data());
        if (graph.is_root() && state_->wall_started.has_value())
        {
            state_->wall_time = elapsed(*state_->wall_started, InspectionClock::now());
            state_->wall_started.reset();
        }
    }

    void Inspector::on_before_start_node(const NodeView &node)
    {
        std::scoped_lock lock{state_->mutex};
        const std::uint64_t id = state_->next_id++;
        const std::uint64_t parent_id = parent_id_for_node(*state_, node);
        const NodeStorageMetrics storage = node.storage_metrics();
        InspectionEntry snapshot{
            .id = id,
            .parent_id = parent_id,
            .path = child_path(*state_, parent_id,
                               "." + diagnostic::node_label(node) +
                                   "<" + std::to_string(node.node_index()) + ">"),
            .label = diagnostic::node_label(node),
            .schema_label = schema_label(node.type().record()),
            .implementation_label = implementation_label(node.type().record()),
            .kind = InspectionEntityKind::Node,
            .node_kind = node.node_kind(),
            .started = node.started(),
            .evaluation_time = node.graph().evaluation_time(),
            .scheduled_time = node.graph().node_scheduled_time(node.node_index()),
        };
        auto [entry, inserted] = state_->entries.emplace(
            id, State::EntryState{.snapshot = std::move(snapshot)});
        static_cast<void>(inserted);
        state_->node_entities[node.data()] = id;
        attach_to_parent(*state_, parent_id, id);
        update_totals(*state_, entry->second, storage);
        begin_phase(entry->second, InspectionPhase::Start);
    }

    void Inspector::on_after_start_node(const NodeView &node)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->node_entities, node.data()))
        {
            end_phase(*entry, InspectionPhase::Start, false, options_.recent_window);
            entry->snapshot.started = true;
            update_totals(*state_, *entry, node.storage_metrics());
        }
    }

    void Inspector::on_start_node_failed(const NodeView &node)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->node_entities, node.data()))
        {
            end_phase(*entry, InspectionPhase::Start, true, options_.recent_window);
            entry->snapshot.started = false;
            entry->snapshot.stopped = true;
            clear_dynamic_totals(*state_, *entry);
        }
        state_->node_entities.erase(node.data());
    }

    void Inspector::on_before_graph_evaluation(const GraphView &graph)
    {
        std::scoped_lock lock{state_->mutex};
        if (graph.is_root())
        {
            state_->root_evaluation_started = InspectionClock::now();
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
            begin_phase(*entry, InspectionPhase::Evaluation);
        }
    }

    void Inspector::on_after_graph_evaluation(const GraphView &graph)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->graph_entities, graph.data()))
        {
            end_phase(*entry, InspectionPhase::Evaluation,
                      graph.failed_node().valid(), options_.recent_window);
            refresh_graph(*state_, graph);
        }
        if (graph.is_root())
        {
            ++state_->graph_cycles;
            if (state_->root_evaluation_started.has_value())
            {
                state_->root_evaluation_time += elapsed(
                    *state_->root_evaluation_started, InspectionClock::now());
                state_->root_evaluation_started.reset();
            }
        }
    }

    void Inspector::on_before_node_evaluation(const NodeView &node)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->node_entities, node.data()))
        {
            begin_phase(*entry, InspectionPhase::Evaluation);
        }
    }

    void Inspector::on_after_node_evaluation(const NodeView &node)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->node_entities, node.data()))
        {
            end_phase(*entry, InspectionPhase::Evaluation, failed_node_is(node),
                      options_.recent_window);
            entry->snapshot.evaluation_time = node.graph().evaluation_time();
            entry->snapshot.scheduled_time =
                node.graph().node_scheduled_time(node.node_index());
            update_totals(*state_, *entry, node.storage_metrics());
        }
    }

    void Inspector::on_after_graph_push_nodes_evaluation(const GraphView &graph)
    {
        std::scoped_lock lock{state_->mutex};
        refresh_graph(*state_, graph);
    }

    void Inspector::on_before_stop_node(const NodeView &node)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->node_entities, node.data()))
        {
            update_totals(*state_, *entry, node.storage_metrics());
            begin_phase(*entry, InspectionPhase::Stop);
        }
    }

    void Inspector::on_after_stop_node(const NodeView &node)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->node_entities, node.data()))
        {
            end_phase(*entry, InspectionPhase::Stop, false, options_.recent_window);
            entry->snapshot.started = false;
            entry->snapshot.stopped = true;
            entry->snapshot.scheduled_time = MIN_DT;
            clear_dynamic_totals(*state_, *entry);
        }
        state_->node_entities.erase(node.data());
    }

    void Inspector::on_stop_node_failed(const NodeView &node)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->node_entities, node.data()))
        {
            end_phase(*entry, InspectionPhase::Stop, true, options_.recent_window);
            entry->snapshot.started = false;
            entry->snapshot.stopped = true;
            entry->snapshot.scheduled_time = MIN_DT;
            clear_dynamic_totals(*state_, *entry);
        }
        state_->node_entities.erase(node.data());
    }

    void Inspector::on_before_stop_graph(const GraphView &graph)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->graph_entities, graph.data()))
        {
            refresh_graph(*state_, graph);
            begin_phase(*entry, InspectionPhase::Stop);
        }
    }

    void Inspector::on_after_stop_graph(const GraphView &graph)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->graph_entities, graph.data()))
        {
            end_phase(*entry, InspectionPhase::Stop, false, options_.recent_window);
            entry->snapshot.started = false;
            entry->snapshot.stopped = true;
            entry->snapshot.scheduled_time = MIN_DT;
            clear_dynamic_totals(*state_, *entry);
        }
        state_->graph_entities.erase(graph.data());
        if (graph.is_root() && state_->wall_started.has_value())
        {
            state_->wall_time = elapsed(*state_->wall_started, InspectionClock::now());
            state_->wall_started.reset();
        }
    }

    void Inspector::on_stop_graph_failed(const GraphView &graph)
    {
        std::scoped_lock lock{state_->mutex};
        if (auto *entry = find_entry(*state_, state_->graph_entities, graph.data()))
        {
            end_phase(*entry, InspectionPhase::Stop, true, options_.recent_window);
            entry->snapshot.started = false;
            entry->snapshot.stopped = true;
            entry->snapshot.scheduled_time = MIN_DT;
            clear_dynamic_totals(*state_, *entry);
        }
        state_->graph_entities.erase(graph.data());
        if (graph.is_root() && state_->wall_started.has_value())
        {
            state_->wall_time = elapsed(*state_->wall_started, InspectionClock::now());
            state_->wall_started.reset();
        }
    }
}  // namespace hgraph
