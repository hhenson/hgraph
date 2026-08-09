#include <hgraph/lib/std/std_operators.h>
#include <hgraph/runtime/evaluation_profiler.h>
#include <hgraph/runtime/executor.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/static_node.h>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <stdexcept>
#include <string_view>

namespace {
using namespace hgraph;

struct ProfileAddOne {
  static constexpr auto name = "profile_add_one";

  static void eval(In<"ts", TS<Int>> ts, Out<TS<Int>> out) {
    out.set(ts.value() + 1);
  }
};

struct ProfileThrow {
  static constexpr auto name = "profile_throw";

  static void eval(In<"ts", TS<Int>>) {
    throw std::runtime_error("profile failure");
  }
};

struct ProfileStartThrow {
  static constexpr auto name = "profile_start_throw";

  static void start() { throw std::runtime_error("profile start failure"); }
  static void eval(In<"ts", TS<Int>>) {}
};

struct ProfileStopThrow {
  static constexpr auto name = "profile_stop_throw";

  static void eval(In<"ts", TS<Int>>) {}
  static void stop() { throw std::runtime_error("profile stop failure"); }
};

template <typename Node>
EvaluationProfileSnapshot run_profile(bool expect_failure = false) {
  stdlib::register_standard_operators();
  EvaluationProfiler profiler;

  Wiring wiring;
  auto input = wire<stdlib::const_>(wiring, Int{41}).as<TS<Int>>();
  if constexpr (std::same_as<Node, ProfileAddOne>) {
    auto output = wire<Node>(wiring, input);
    static_cast<void>(wire<stdlib::null_sink>(wiring, output));
  } else {
    static_cast<void>(wire<Node>(wiring, input));
  }

  GraphBuilder graph = std::move(wiring).finish();
  graph.label("profile_graph");

  GraphExecutorBuilder builder;
  builder.graph_builder(std::move(graph)).add_lifecycle_observer(&profiler);
  GraphExecutorValue executor = builder.make_executor();
  if (expect_failure) {
    CHECK_THROWS(executor.view().run());
  } else {
    executor.view().run();
  }
  return profiler.snapshot();
}

const EvaluationProfileEntry &
entry_containing(const EvaluationProfileSnapshot &snapshot,
                 std::string_view text) {
  const auto found = std::ranges::find_if(
      snapshot.entries, [&](const EvaluationProfileEntry &entry) {
        return entry.path.contains(text);
      });
  REQUIRE(found != snapshot.entries.end());
  return *found;
}
} // namespace

TEST_CASE(
    "evaluation profiler: native snapshots aggregate graph and node phases") {
  const EvaluationProfileSnapshot snapshot = run_profile<ProfileAddOne>();

  CHECK(snapshot.graph_cycles == 1);
  CHECK(snapshot.wall_time >= TimeDelta{0});
  CHECK(snapshot.root_evaluation_time >= TimeDelta{0});
  CHECK(snapshot.runtime_load >= 0.0);
  CHECK(std::ranges::is_sorted(snapshot.entries, {},
                               &EvaluationProfileEntry::path));

  const EvaluationProfileEntry &graph = entry_containing(snapshot, "[]");
  CHECK(graph.graph);
  CHECK(graph.start.count == 1);
  CHECK(graph.evaluation.count == 1);
  CHECK(graph.stop.count == 1);

  const EvaluationProfileEntry &node =
      entry_containing(snapshot, "profile_add_one");
  CHECK_FALSE(node.graph);
  CHECK(node.start.count == 1);
  CHECK(node.evaluation.count == 1);
  CHECK(node.evaluation.failures == 0);
  CHECK(node.stop.count == 1);
  CHECK(node.evaluation.total_time >= TimeDelta{0});
  CHECK(node.evaluation.max_time >= TimeDelta{0});
  CHECK(node.evaluation.recent_time >= TimeDelta{0});
}

TEST_CASE(
    "evaluation profiler: failed node and graph evaluations are recorded") {
  const EvaluationProfileSnapshot snapshot = run_profile<ProfileThrow>(true);

  const EvaluationProfileEntry &graph = entry_containing(snapshot, "[]");
  const EvaluationProfileEntry &node =
      entry_containing(snapshot, "profile_throw");
  CHECK(graph.evaluation.count == 1);
  CHECK(graph.evaluation.failures == 1);
  CHECK(node.evaluation.count == 1);
  CHECK(node.evaluation.failures == 1);
}

TEST_CASE("evaluation profiler: start failures finalize active phase timers") {
  const EvaluationProfileSnapshot snapshot =
      run_profile<ProfileStartThrow>(true);

  const EvaluationProfileEntry &graph = entry_containing(snapshot, "[]");
  const EvaluationProfileEntry &node =
      entry_containing(snapshot, "profile_start_throw");
  CHECK(graph.start.count == 1);
  CHECK(graph.start.failures == 1);
  CHECK(node.start.count == 1);
  CHECK(node.start.failures == 1);
}

TEST_CASE("evaluation profiler: stop failures finalize active phase timers") {
  const EvaluationProfileSnapshot snapshot =
      run_profile<ProfileStopThrow>(true);

  const EvaluationProfileEntry &graph = entry_containing(snapshot, "[]");
  const EvaluationProfileEntry &node =
      entry_containing(snapshot, "profile_stop_throw");
  CHECK(graph.stop.count == 1);
  CHECK(graph.stop.failures == 1);
  CHECK(node.stop.count == 1);
  CHECK(node.stop.failures == 1);
}

TEST_CASE(
    "evaluation profiler: disabled entity families do not create entries") {
  stdlib::register_standard_operators();
  EvaluationProfiler profiler{EvaluationProfilerOptions{
      .start = false,
      .eval = true,
      .stop = false,
      .node = false,
      .graph = true,
  }};

  Wiring wiring;
  auto value = wire<stdlib::const_>(wiring, Int{1}).as<TS<Int>>();
  static_cast<void>(wire<stdlib::null_sink>(wiring, value));

  GraphExecutorBuilder builder;
  builder.graph_builder(std::move(wiring).finish())
      .add_lifecycle_observer(&profiler);
  GraphExecutorValue executor = builder.make_executor();
  executor.view().run();

  const EvaluationProfileSnapshot snapshot = profiler.snapshot();
  REQUIRE(snapshot.entries.size() == 1);
  CHECK(snapshot.entries.front().graph);
  CHECK(snapshot.entries.front().start.count == 0);
  CHECK(snapshot.entries.front().evaluation.count == 1);
  CHECK(snapshot.entries.front().stop.count == 0);
}
