#include "fabric_extension_probe.h"

#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/runtime/runtime.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/static_node.h>
#include <hgraph/types/subgraph_wiring.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <typeindex>
#include <unordered_set>
#include <utility>
#include <vector>

namespace hgraph_install_consumer {
namespace {
// RFC 0026 checkpoint 0: an installed extension can defer a source,
// discover that marked source through public wiring edges from a sink,
// bind the source after composition, and select its runtime mode once in
// start. This deliberately remains a consumer-side proof rather than
// introducing fabric policy or a new hook in core.

constexpr std::string_view kDependencyCount{":fabric_probe:dependency_count"};
constexpr std::string_view kSinkValue{":fabric_probe:sink_value"};

class FabricProbeDependencyHandle {
public:
  [[nodiscard]] std::uint64_t root_identity() const noexcept {
    return root_identity_;
  }

  [[nodiscard]] const hgraph::WiringPortRef &source() const noexcept {
    return source_;
  }

private:
  friend struct FabricProbeSubscription;

  FabricProbeDependencyHandle(std::uint64_t root_identity,
                              hgraph::WiringPortRef source)
      : root_identity_{root_identity}, source_{std::move(source)} {}

  std::uint64_t root_identity_{};
  hgraph::WiringPortRef source_{};
};

/** A subscription exposes its data port and an opaque dependency handle.
    The handle is constructed only from this subscription result; it is
    not a data-id string which can claim lineage the graph did not load. */
struct FabricProbeSubscription {
  FabricProbeSubscription(std::uint64_t root_identity,
                          hgraph::Port<hgraph::TS<hgraph::Int>> source)
      : value{source}, dependency{root_identity, source.erased()} {}

  hgraph::Port<hgraph::TS<hgraph::Int>> value;
  FabricProbeDependencyHandle dependency;
};

/** Installed proof source: one startup schedule, one output tick, and no
    runtime policy branch. ``start`` selects the immutable executor mode;
    ``eval`` only publishes that already-selected value. */
struct FabricProbeModeSource {
  static constexpr auto name = "fabric_probe_mode_source";
  static constexpr bool schedule_on_start = true;

  static void start(hgraph::EngineControlView engine,
                    hgraph::State<hgraph::Int> mode) {
    mode.set(engine.mode() == hgraph::GraphExecutorMode::Simulation
                 ? hgraph::Int{11}
                 : hgraph::Int{22});
  }

  static void eval(hgraph::State<hgraph::Int> mode,
                   hgraph::Out<hgraph::TS<hgraph::Int>> out) {
    out.set(mode.get());
  }
};

/** Installed proof sink: capture the selected source value in run-scoped
    state so the consumer can verify the deferred edge evaluated. */
struct FabricProbeSink {
  static constexpr auto name = "fabric_probe_sink";

  static void eval(hgraph::In<"value", hgraph::TS<hgraph::Int>> value,
                   hgraph::GlobalStateView state) {
    state.set(kSinkValue, hgraph::Value{value.value()});
  }
};

/** Independent proof value used to show that an explicit subscription
    handle supplies lineage even when the data edge is intentionally
    hidden from ordinary upstream traversal. */
struct FabricProbeIndependentSource {
  static constexpr auto name = "fabric_probe_independent_source";
  static constexpr bool schedule_on_start = true;

  static void eval(hgraph::Out<hgraph::TS<hgraph::Int>> out) {
    out.set(hgraph::Int{33});
  }
};

struct FabricProbeNestedPassThrough {
  static hgraph::Port<hgraph::TS<hgraph::Int>>
  compose(hgraph::Wiring &wiring, hgraph::Port<hgraph::TS<hgraph::Int>> value) {
    return hgraph::wire<hgraph::stdlib::pass_through_node>(wiring, value)
        .template as<hgraph::TS<hgraph::Int>>();
  }
};

struct FabricProbePublisher {
  hgraph::WiringPortRef value;
  std::vector<FabricProbeDependencyHandle> explicit_dependencies;
};

struct FabricProbePlanner {
  std::vector<hgraph::DelayedBindingWiringPort<hgraph::TS<hgraph::Int>>>
      sources;
  std::vector<FabricProbePublisher> publishers;
  bool finalizer_registered{false};

  static void collect_marked_sources(
      const hgraph::WiringPortRef &source,
      std::unordered_set<const hgraph::WiringInstance *> &visited,
      std::size_t &marked_sources) {
    using SourceKind = hgraph::WiringPortRef::SourceKind;

    switch (source.source_kind()) {
    case SourceKind::Null:
      return;
    case SourceKind::Structural:
      for (const auto &child : source.structural_children()) {
        collect_marked_sources(child, visited, marked_sources);
      }
      return;
    case SourceKind::Delayed: {
      const auto &resolved = source.delayed_state()->source;
      if (!resolved) {
        throw std::logic_error(
            "fabric probe encountered an unbound deferred source");
      }
      collect_marked_sources(*resolved, visited, marked_sources);
      return;
    }
    case SourceKind::Peered: {
      const hgraph::WiringInstance *node = source.peered_node();
      if (!visited.insert(node).second) {
        return;
      }
      if (node->definition == std::type_index(typeid(FabricProbeModeSource))) {
        ++marked_sources;
      }
      for (const auto &input : node->inputs) {
        collect_marked_sources(input.source, visited, marked_sources);
      }
      return;
    }
    case SourceKind::Boundary:
      throw std::logic_error(
          "fabric probe expected a top-level source, not a child boundary");
    case SourceKind::Unbound:
      throw std::logic_error(
          "fabric probe encountered an unbound wiring source");
    }
  }

  void finalize(hgraph::Wiring &wiring) {
    for (auto &source : sources) {
      if (!source.bound()) {
        source(hgraph::wire<FabricProbeModeSource>(wiring));
      }
    }

    for (const auto &publisher : publishers) {
      std::unordered_set<const hgraph::WiringInstance *> visited;
      std::size_t marked_sources{0};
      if (publisher.explicit_dependencies.empty()) {
        collect_marked_sources(publisher.value, visited, marked_sources);
      } else {
        for (const auto &dependency : publisher.explicit_dependencies) {
          if (dependency.root_identity() != wiring.identity()) {
            throw std::logic_error("fabric probe explicit dependency belongs "
                                   "to another wired root");
          }
          collect_marked_sources(dependency.source(), visited, marked_sources);
        }
      }
      if (marked_sources != 1) {
        throw std::logic_error(
            "fabric probe did not discover exactly one upstream marked source");
      }
      wiring.global_state().set(
          kDependencyCount, hgraph::Value{hgraph::Int{
                                static_cast<std::int64_t>(marked_sources)}});
    }
  }
};

[[nodiscard]] std::shared_ptr<FabricProbePlanner>
planner_for(hgraph::Wiring &wiring) {
  auto planner = std::static_pointer_cast<FabricProbePlanner>(
      wiring.acquire_extension_state(
          std::type_index(typeid(FabricProbePlanner)),
          [] { return std::make_shared<FabricProbePlanner>(); }));
  if (!planner->finalizer_registered) {
    planner->finalizer_registered = true;
    std::weak_ptr<FabricProbePlanner> weak = planner;
    wiring.register_pre_rank_finalizer([weak](hgraph::Wiring &target) {
      if (const auto locked = weak.lock()) {
        locked->finalize(target);
      }
    });
  }
  return planner;
}

[[nodiscard]] FabricProbeSubscription subscribe(hgraph::Wiring &wiring) {
  auto delayed = hgraph::delayed_binding<hgraph::TS<hgraph::Int>>(wiring);
  planner_for(wiring)->sources.push_back(delayed);
  auto value = delayed();
  return FabricProbeSubscription{wiring.identity(), value};
}

void publish(
    hgraph::Wiring &wiring, hgraph::Port<hgraph::TS<hgraph::Int>> value,
    std::vector<FabricProbeDependencyHandle> explicit_dependencies = {}) {
  planner_for(wiring)->publishers.push_back(
      FabricProbePublisher{value.erased(), std::move(explicit_dependencies)});
  hgraph::wire<FabricProbeSink>(wiring, value);
}

struct FabricNestedExtensionProbeGraph {
  static void compose(hgraph::Wiring &wiring) {
    auto subscribed = subscribe(wiring);
    auto forwarded =
        hgraph::nested_<FabricProbeNestedPassThrough>(wiring, subscribed.value);
    publish(wiring, forwarded);
  }
};

struct FabricExplicitExtensionProbeGraph {
  static void compose(hgraph::Wiring &wiring) {
    auto subscribed = subscribe(wiring);
    auto value = hgraph::wire<FabricProbeIndependentSource>(wiring);
    publish(wiring, value, {subscribed.dependency});
  }
};
} // namespace

void check_fabric_core_extension_seam() {
  using namespace hgraph;

  const auto run = []<typename Graph>(GraphExecutorMode mode, Int expected) {
    Wiring wiring;
    Graph::compose(wiring);
    // A pre-rank finalizer must tolerate notebook-style snapshots and
    // the final consuming build without rebinding its delayed source.
    GraphBuilder snapshot = wiring.snapshot();
    if (snapshot.global_state().get(kDependencyCount).checked_as<Int>() !=
        Int{1}) {
      throw std::runtime_error(
          "installed fabric probe did not finalize an intermediate snapshot");
    }
    GraphBuilder graph = std::move(wiring).finish();
    if (graph.global_state().get(kDependencyCount).checked_as<Int>() !=
        Int{1}) {
      throw std::runtime_error(
          "installed fabric probe did not retain its discovered dependency");
    }

    GraphExecutorBuilder executor_builder;
    executor_builder.graph_builder(std::move(graph))
        .mode(mode)
        .start_time(MIN_ST)
        .end_time(MIN_ST + MIN_TD * 2);
    GraphExecutorValue executor = executor_builder.make_executor();
    auto view = executor.view();
    view.run();

    const auto result = view.graph().global_state().get(kSinkValue);
    if (!result.valid() || result.checked_as<Int>() != expected) {
      throw std::runtime_error("installed fabric probe did not evaluate its "
                               "deferred source and sink");
    }
  };

  run.template operator()<FabricNestedExtensionProbeGraph>(
      GraphExecutorMode::Simulation, Int{11});
  run.template operator()<FabricNestedExtensionProbeGraph>(
      GraphExecutorMode::RealTime, Int{22});
  run.template operator()<FabricExplicitExtensionProbeGraph>(
      GraphExecutorMode::Simulation, Int{33});

  Wiring first_root;
  auto first_subscription = subscribe(first_root);
  Wiring second_root;
  auto value = wire<FabricProbeIndependentSource>(second_root);
  publish(second_root, value, {first_subscription.dependency});
  try {
    static_cast<void>(std::move(second_root).finish());
    throw std::runtime_error(
        "installed fabric probe accepted a dependency from another wired root");
  } catch (const std::logic_error &error) {
    if (std::string_view{error.what()} !=
        "fabric probe explicit dependency belongs to another wired root") {
      throw;
    }
  }
}
} // namespace hgraph_install_consumer
