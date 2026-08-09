#include <hgraph/runtime/evaluation_profiler.h>

#include <hgraph/runtime/diagnostic_path.h>
#include <hgraph/runtime/executor.h>
#include <hgraph/runtime/graph.h>
#include <hgraph/runtime/node.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>

namespace hgraph {
namespace {
using ProfileClock = std::chrono::steady_clock;
using ProfileTime = ProfileClock::time_point;

enum class ProfilePhase : std::uint8_t {
  Start,
  Evaluation,
  Stop,
};

[[nodiscard]] TimeDelta elapsed(ProfileTime start, ProfileTime end) noexcept {
  return std::chrono::duration_cast<TimeDelta>(end - start);
}

[[nodiscard]] DateTime current_wall_time() noexcept {
  return std::chrono::time_point_cast<TimeDelta>(engine_clock::now());
}
} // namespace

struct EvaluationProfiler::State {
  struct PhaseState {
    EvaluationProfilePhase snapshot{};
    std::vector<TimeDelta> recent{};
    std::size_t recent_cursor{0};
  };

  struct EntryState {
    EvaluationProfileEntry identity{};
    PhaseState start{};
    PhaseState evaluation{};
    PhaseState stop{};
  };

  struct EntityState {
    EntryState *entry{nullptr};
    std::array<std::optional<ProfileTime>, 3> active{};
  };

  mutable std::mutex mutex{};
  std::unordered_map<std::string, EntryState> entries{};
  std::unordered_map<const void *, EntityState> graph_entities{};
  std::unordered_map<const void *, EntityState> node_entities{};
  std::optional<ProfileTime> wall_started{};
  std::optional<ProfileTime> root_evaluation_started{};
  TimeDelta wall_time{0};
  std::uint64_t graph_cycles{0};
  TimeDelta root_evaluation_time{0};
  TimeDelta scheduling_lag_total{0};
  TimeDelta scheduling_lag_max{0};
  std::uint64_t scheduling_lag_samples{0};
};

namespace {
[[nodiscard]] constexpr std::size_t phase_index(ProfilePhase phase) noexcept {
  return static_cast<std::size_t>(phase);
}

EvaluationProfiler::State::PhaseState &
phase_state(EvaluationProfiler::State::EntryState &entry, ProfilePhase phase) {
  switch (phase) {
  case ProfilePhase::Start:
    return entry.start;
  case ProfilePhase::Evaluation:
    return entry.evaluation;
  case ProfilePhase::Stop:
    return entry.stop;
  }
  std::terminate();
}

void record_duration(EvaluationProfiler::State::PhaseState &phase,
                     TimeDelta duration, bool failed,
                     std::size_t recent_window) {
  if (duration < TimeDelta{0}) {
    duration = TimeDelta{0};
  }
  ++phase.snapshot.count;
  if (failed) {
    ++phase.snapshot.failures;
  }
  phase.snapshot.total_time += duration;
  phase.snapshot.max_time = std::max(phase.snapshot.max_time, duration);
  if (recent_window == 0) {
    phase.recent.clear();
    phase.recent_cursor = 0;
    phase.snapshot.recent_time = TimeDelta{0};
    return;
  }
  if (phase.recent.capacity() < recent_window) {
    phase.recent.reserve(recent_window);
  }
  if (phase.recent.size() < recent_window) {
    phase.recent.push_back(duration);
    phase.snapshot.recent_time += duration;
    return;
  }
  phase.snapshot.recent_time -= phase.recent[phase.recent_cursor];
  phase.recent[phase.recent_cursor] = duration;
  phase.snapshot.recent_time += duration;
  phase.recent_cursor = (phase.recent_cursor + 1) % recent_window;
}

EvaluationProfiler::State::EntryState &
ensure_entry(EvaluationProfiler::State &state, std::string path,
             std::string label, bool graph) {
  auto [it, inserted] = state.entries.try_emplace(path);
  if (inserted) {
    it->second.identity.path = std::move(path);
    it->second.identity.label = std::move(label);
    it->second.identity.graph = graph;
  }
  return it->second;
}

EvaluationProfiler::State::EntityState &register_entity(
    EvaluationProfiler::State &state,
    std::unordered_map<const void *, EvaluationProfiler::State::EntityState>
        &entities,
    const void *identity, std::string path, std::string label, bool graph) {
  auto [entity, inserted] = entities.try_emplace(identity);
  if (inserted || entity->second.entry == nullptr) {
    entity->second.entry =
        &ensure_entry(state, std::move(path), std::move(label), graph);
  }
  return entity->second;
}

void begin_phase(EvaluationProfiler::State::EntityState &entity,
                 ProfilePhase phase) {
  entity.active[phase_index(phase)] = ProfileClock::now();
}

TimeDelta end_phase(EvaluationProfiler::State::EntityState &entity,
                    ProfilePhase phase, bool failed,
                    std::size_t recent_window) {
  auto &started = entity.active[phase_index(phase)];
  if (!started.has_value() || entity.entry == nullptr) {
    return TimeDelta{0};
  }
  const TimeDelta duration = elapsed(*started, ProfileClock::now());
  record_duration(phase_state(*entity.entry, phase), duration, failed,
                  recent_window);
  started.reset();
  return duration;
}

EvaluationProfiler::State::EntityState *find_entity(
    std::unordered_map<const void *, EvaluationProfiler::State::EntityState>
        &entities,
    const void *identity) noexcept {
  const auto found = entities.find(identity);
  return found != entities.end() ? &found->second : nullptr;
}

[[nodiscard]] bool failed_node_is(const NodeView &node) {
  NodeView failed = node.graph().failed_node();
  return failed.valid() && failed.pointer() == node.pointer();
}
} // namespace

EvaluationProfiler::EvaluationProfiler(EvaluationProfilerOptions options)
    : options_(options), state_(std::make_shared<State>()) {}

EvaluationProfiler::EvaluationProfiler(bool start, bool eval, bool stop,
                                       bool node, bool graph,
                                       std::size_t recent_window)
    : EvaluationProfiler(EvaluationProfilerOptions{
          .start = start,
          .eval = eval,
          .stop = stop,
          .node = node,
          .graph = graph,
          .recent_window = recent_window,
      }) {}

EvaluationProfileSnapshot EvaluationProfiler::snapshot() const {
  std::scoped_lock lock{state_->mutex};
  EvaluationProfileSnapshot result;
  result.graph_cycles = state_->graph_cycles;
  result.wall_time = state_->wall_started.has_value()
                         ? elapsed(*state_->wall_started, ProfileClock::now())
                         : state_->wall_time;
  result.root_evaluation_time = state_->root_evaluation_time;
  result.scheduling_lag_total = state_->scheduling_lag_total;
  result.scheduling_lag_max = state_->scheduling_lag_max;
  result.scheduling_lag_samples = state_->scheduling_lag_samples;
  if (result.wall_time > TimeDelta{0}) {
    result.runtime_load =
        static_cast<double>(result.root_evaluation_time.count()) /
        static_cast<double>(result.wall_time.count());
  }

  result.entries.reserve(state_->entries.size());
  for (const auto &[path, entry] : state_->entries) {
    static_cast<void>(path);
    EvaluationProfileEntry copy = entry.identity;
    copy.start = entry.start.snapshot;
    copy.evaluation = entry.evaluation.snapshot;
    copy.stop = entry.stop.snapshot;
    result.entries.push_back(std::move(copy));
  }

  std::ranges::sort(result.entries, {}, &EvaluationProfileEntry::path);
  return result;
}

void EvaluationProfiler::reset() {
  std::scoped_lock lock{state_->mutex};
  state_->entries.clear();
  state_->graph_entities.clear();
  state_->node_entities.clear();
  state_->wall_started.reset();
  state_->root_evaluation_started.reset();
  state_->wall_time = TimeDelta{0};
  state_->graph_cycles = 0;
  state_->root_evaluation_time = TimeDelta{0};
  state_->scheduling_lag_total = TimeDelta{0};
  state_->scheduling_lag_max = TimeDelta{0};
  state_->scheduling_lag_samples = 0;
}

void EvaluationProfiler::on_before_start_graph(const GraphView &graph) {
  std::scoped_lock lock{state_->mutex};
  if (graph.is_root()) {
    state_->wall_started = ProfileClock::now();
  }
  if (!options_.graph) {
    return;
  }
  auto &entity = register_entity(*state_, state_->graph_entities, graph.data(),
                                 diagnostic::graph_path(graph),
                                 diagnostic::graph_label(graph), true);
  if (options_.start) {
    begin_phase(entity, ProfilePhase::Start);
  }
}

void EvaluationProfiler::on_after_start_graph(const GraphView &graph) {
  if (!options_.start || !options_.graph) {
    return;
  }
  std::scoped_lock lock{state_->mutex};
  if (auto *entity = find_entity(state_->graph_entities, graph.data())) {
    end_phase(*entity, ProfilePhase::Start, false, options_.recent_window);
  }
}

void EvaluationProfiler::on_start_graph_failed(const GraphView &graph) {
  std::scoped_lock lock{state_->mutex};
  if (auto *entity = find_entity(state_->graph_entities, graph.data())) {
    if (options_.start) {
      end_phase(*entity, ProfilePhase::Start, true, options_.recent_window);
    }
    state_->graph_entities.erase(graph.data());
  }
  if (graph.is_root() && state_->wall_started.has_value()) {
    state_->wall_time = elapsed(*state_->wall_started, ProfileClock::now());
    state_->wall_started.reset();
  }
}

void EvaluationProfiler::on_before_start_node(const NodeView &node) {
  if (!options_.node) {
    return;
  }
  std::scoped_lock lock{state_->mutex};
  auto &entity = register_entity(*state_, state_->node_entities, node.data(),
                                 diagnostic::node_path(node),
                                 diagnostic::node_label(node), false);
  if (options_.start) {
    begin_phase(entity, ProfilePhase::Start);
  }
}

void EvaluationProfiler::on_after_start_node(const NodeView &node) {
  if (!options_.start || !options_.node) {
    return;
  }
  std::scoped_lock lock{state_->mutex};
  if (auto *entity = find_entity(state_->node_entities, node.data())) {
    end_phase(*entity, ProfilePhase::Start, false, options_.recent_window);
  }
}

void EvaluationProfiler::on_start_node_failed(const NodeView &node) {
  if (!options_.node) {
    return;
  }
  std::scoped_lock lock{state_->mutex};
  if (auto *entity = find_entity(state_->node_entities, node.data())) {
    if (options_.start) {
      end_phase(*entity, ProfilePhase::Start, true, options_.recent_window);
    }
    state_->node_entities.erase(node.data());
  }
}

void EvaluationProfiler::on_before_graph_evaluation(const GraphView &graph) {
  if (!options_.eval) {
    return;
  }
  std::scoped_lock lock{state_->mutex};
  if (graph.is_root()) {
    state_->root_evaluation_started = ProfileClock::now();
    if (graph.executor().schema()->mode == GraphExecutorMode::RealTime) {
      const TimeDelta lag =
          std::max(current_wall_time() - graph.evaluation_time(), TimeDelta{0});
      state_->scheduling_lag_total += lag;
      state_->scheduling_lag_max = std::max(state_->scheduling_lag_max, lag);
      ++state_->scheduling_lag_samples;
    }
  }
  if (options_.graph) {
    if (auto *entity = find_entity(state_->graph_entities, graph.data())) {
      begin_phase(*entity, ProfilePhase::Evaluation);
    }
  }
}

void EvaluationProfiler::on_after_graph_evaluation(const GraphView &graph) {
  if (!options_.eval) {
    return;
  }
  std::scoped_lock lock{state_->mutex};
  if (options_.graph) {
    if (auto *entity = find_entity(state_->graph_entities, graph.data())) {
      end_phase(*entity, ProfilePhase::Evaluation, graph.failed_node().valid(),
                options_.recent_window);
    }
  }
  if (graph.is_root()) {
    ++state_->graph_cycles;
    if (state_->root_evaluation_started.has_value()) {
      state_->root_evaluation_time +=
          elapsed(*state_->root_evaluation_started, ProfileClock::now());
      state_->root_evaluation_started.reset();
    }
  }
}

void EvaluationProfiler::on_before_node_evaluation(const NodeView &node) {
  if (!options_.eval || !options_.node) {
    return;
  }
  std::scoped_lock lock{state_->mutex};
  if (auto *entity = find_entity(state_->node_entities, node.data())) {
    begin_phase(*entity, ProfilePhase::Evaluation);
  }
}

void EvaluationProfiler::on_after_node_evaluation(const NodeView &node) {
  if (!options_.eval || !options_.node) {
    return;
  }
  std::scoped_lock lock{state_->mutex};
  if (auto *entity = find_entity(state_->node_entities, node.data())) {
    end_phase(*entity, ProfilePhase::Evaluation, failed_node_is(node),
              options_.recent_window);
  }
}

void EvaluationProfiler::on_before_stop_node(const NodeView &node) {
  if (!options_.stop || !options_.node) {
    return;
  }
  std::scoped_lock lock{state_->mutex};
  if (auto *entity = find_entity(state_->node_entities, node.data())) {
    begin_phase(*entity, ProfilePhase::Stop);
  }
}

void EvaluationProfiler::on_after_stop_node(const NodeView &node) {
  if (!options_.node) {
    return;
  }
  std::scoped_lock lock{state_->mutex};
  if (auto *entity = find_entity(state_->node_entities, node.data())) {
    if (options_.stop) {
      end_phase(*entity, ProfilePhase::Stop, false, options_.recent_window);
    }
    state_->node_entities.erase(node.data());
  }
}

void EvaluationProfiler::on_stop_node_failed(const NodeView &node) {
  if (!options_.node) {
    return;
  }
  std::scoped_lock lock{state_->mutex};
  if (auto *entity = find_entity(state_->node_entities, node.data())) {
    if (options_.stop) {
      end_phase(*entity, ProfilePhase::Stop, true, options_.recent_window);
    }
    state_->node_entities.erase(node.data());
  }
}

void EvaluationProfiler::on_before_stop_graph(const GraphView &graph) {
  if (!options_.stop || !options_.graph) {
    return;
  }
  std::scoped_lock lock{state_->mutex};
  if (auto *entity = find_entity(state_->graph_entities, graph.data())) {
    begin_phase(*entity, ProfilePhase::Stop);
  }
}

void EvaluationProfiler::on_after_stop_graph(const GraphView &graph) {
  std::scoped_lock lock{state_->mutex};
  if (options_.stop && options_.graph) {
    if (auto *entity = find_entity(state_->graph_entities, graph.data())) {
      end_phase(*entity, ProfilePhase::Stop, false, options_.recent_window);
    }
  }
  if (graph.is_root() && state_->wall_started.has_value()) {
    state_->wall_time = elapsed(*state_->wall_started, ProfileClock::now());
    state_->wall_started.reset();
  }
  if (options_.graph) {
    state_->graph_entities.erase(graph.data());
  }
}

void EvaluationProfiler::on_stop_graph_failed(const GraphView &graph) {
  std::scoped_lock lock{state_->mutex};
  if (auto *entity = find_entity(state_->graph_entities, graph.data())) {
    if (options_.stop) {
      end_phase(*entity, ProfilePhase::Stop, true, options_.recent_window);
    }
    state_->graph_entities.erase(graph.data());
  }
  if (graph.is_root() && state_->wall_started.has_value()) {
    state_->wall_time = elapsed(*state_->wall_started, ProfileClock::now());
    state_->wall_started.reset();
  }
}
} // namespace hgraph
