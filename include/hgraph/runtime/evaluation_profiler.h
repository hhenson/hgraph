#ifndef HGRAPH_RUNTIME_EVALUATION_PROFILER_H
#define HGRAPH_RUNTIME_EVALUATION_PROFILER_H

#include <hgraph/hgraph_export.h>
#include <hgraph/runtime/lifecycle_observer.h>
#include <hgraph/util/date_time.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace hgraph {
/** Aggregate timings for one lifecycle phase of one graph or node. */
struct HGRAPH_EXPORT EvaluationProfilePhase {
  std::uint64_t count{0};
  std::uint64_t failures{0};
  TimeDelta total_time{0};
  TimeDelta max_time{0};
  TimeDelta recent_time{0};
};

/** Owned profile entry. No runtime graph/node pointer escapes into a snapshot.
 */
struct HGRAPH_EXPORT EvaluationProfileEntry {
  std::string path{};
  std::string label{};
  bool graph{false};
  EvaluationProfilePhase start{};
  EvaluationProfilePhase evaluation{};
  EvaluationProfilePhase stop{};
};

/** Immutable, self-contained profile captured from an EvaluationProfiler. */
struct HGRAPH_EXPORT EvaluationProfileSnapshot {
  std::uint64_t graph_cycles{0};
  TimeDelta wall_time{0};
  TimeDelta root_evaluation_time{0};
  TimeDelta scheduling_lag_total{0};
  TimeDelta scheduling_lag_max{0};
  std::uint64_t scheduling_lag_samples{0};
  double runtime_load{0.0};
  std::vector<EvaluationProfileEntry> entries{};
};

/** Select which lifecycle phases and entity families are measured. */
struct HGRAPH_EXPORT EvaluationProfilerOptions {
  bool start{true};
  bool eval{true};
  bool stop{true};
  bool node{true};
  bool graph{true};
  std::size_t recent_window{100};
};

/**
 * Native aggregate evaluation profiler.
 *
 * The observer is entirely inactive unless explicitly registered on an
 * executor. Enabled profiling uses a monotonic clock and owns every path
 * and label retained in a snapshot, so keyed nested graph erase cannot
 * leave borrowed diagnostic pointers behind. Copies share one measurement
 * state; this lets a Python-facing profiler remain inspectable when the run
 * owns its observer copy.
 */
class HGRAPH_EXPORT EvaluationProfiler final : public LifecycleObserver {
public:
  struct State;

  explicit EvaluationProfiler(EvaluationProfilerOptions options = {});
  explicit EvaluationProfiler(bool start, bool eval = true, bool stop = true,
                              bool node = true, bool graph = true,
                              std::size_t recent_window = 100);

  [[nodiscard]] EvaluationProfileSnapshot snapshot() const;
  void reset();

  void on_before_start_graph(const GraphView &graph) override;
  void on_after_start_graph(const GraphView &graph) override;
  void on_start_graph_failed(const GraphView &graph) override;
  void on_before_start_node(const NodeView &node) override;
  void on_after_start_node(const NodeView &node) override;
  void on_start_node_failed(const NodeView &node) override;
  void on_before_graph_evaluation(const GraphView &graph) override;
  void on_after_graph_evaluation(const GraphView &graph) override;
  void on_before_node_evaluation(const NodeView &node) override;
  void on_after_node_evaluation(const NodeView &node) override;
  void on_before_stop_node(const NodeView &node) override;
  void on_after_stop_node(const NodeView &node) override;
  void on_stop_node_failed(const NodeView &node) override;
  void on_before_stop_graph(const GraphView &graph) override;
  void on_after_stop_graph(const GraphView &graph) override;
  void on_stop_graph_failed(const GraphView &graph) override;

private:
  EvaluationProfilerOptions options_{};
  std::shared_ptr<State> state_{};
};
} // namespace hgraph

#endif // HGRAPH_RUNTIME_EVALUATION_PROFILER_H
