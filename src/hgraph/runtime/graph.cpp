#include <hgraph/runtime/graph.h>

#include "registry_snapshot_detail.h"

#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/metadata/type_registry.h>

#include <hgraph/runtime/executor.h>
#include <hgraph/runtime/lifecycle_observer.h>
#include <hgraph/types/metadata/debug_descriptor.h>
#include <hgraph/types/metadata/type_record_registry.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace hgraph {
namespace {
constexpr std::size_t invalid_cursor = std::numeric_limits<std::size_t>::max();

[[nodiscard]] DateTime current_wall_time() noexcept {
  return std::chrono::time_point_cast<std::chrono::microseconds>(
      engine_clock::now());
}

/** "node[3 'my_label']" — the identity prefix used in diagnostics. */
[[nodiscard]] std::string node_identity(const NodeView &node,
                                        std::size_t index) {
  std::string id = "node[" + std::to_string(index);
  std::string_view name{};
  if (node.valid()) {
    name = node.label();
    if (name.empty() && node.schema() != nullptr &&
        node.schema()->display_name != nullptr) {
      name = node.schema()->display_name;
    }
  }
  if (!name.empty()) {
    id += " '";
    id.append(name);
    id += '\'';
  }
  id += ']';
  return id;
}

/**
 * Re-throw the in-flight exception annotated with the throwing node's
 * identity. Applied only at the ROOT graph boundary: exceptions inside
 * nested graphs must reach ``try_except_`` / per-node error capture
 * unmodified (``NodeError.error_msg`` is the original ``what()``), so
 * the annotation happens exactly once — where an exception would
 * otherwise leave the runtime with no clue which node threw.
 */
[[noreturn]] void rethrow_with_node_identity(const NodeView &node,
                                             std::size_t index,
                                             const char *phase) {
  const std::string prefix =
      node_identity(node, index) + ' ' + phase + " failed: ";
  try {
    throw;
  } catch (const std::exception &e) {
    std::string message = prefix + e.what();
    static_cast<void>(fallback_on_exception(false, [&] {
      const ErrorCaptureOptions options =
          node.graph().root().executor().error_capture_options();
      const NodeErrorFields details = capture_node_error(
          node, node.graph().evaluation_time(), e.what(), options);
      if (!details.activation_back_trace.empty()) {
        message += "\nActivation Back Trace:\n";
        message += details.activation_back_trace;
      }
      return true;
    }));
    throw std::runtime_error(message);
  } catch (...) {
    throw std::runtime_error(prefix + "unknown error");
  }
}

/** Render a graph-schedule entry for dump(): sentinels by name, else raw ticks.
 */
[[nodiscard]] std::string schedule_to_string(DateTime when) {
  if (when == MIN_DT) {
    return "-";
  }
  if (when == MAX_DT) {
    return "MAX_DT";
  }
  return std::to_string(when.time_since_epoch().count());
}

[[nodiscard]] TSOutputView
output_at_path(TSOutputView view, const std::vector<std::size_t> &path) {
  for (const std::size_t component : path) {
    if (view.schema() == nullptr) {
      throw std::logic_error("Graph output path requires a typed output view");
    }
    if (view.schema()->kind == TSTypeKind::REF) {
      // Any structural hop through a REF source resolves via
      // its from-REF alternative first (key-set hops, field
      // projections of REF[TSB], ...).
      const auto *target = TypeRegistry::instance().dereference(view.schema());
      view = view.binding_for(*target).view(view.evaluation_time());
    }
    if (component == ts_key_set_path_component) {
      view = view.as_dict().key_set();
      continue;
    }
    view = view.indexed_child_at(component);
  }
  return view;
}

[[nodiscard]] TSOutputView edge_source_root(NodeView view,
                                            DateTime evaluation_time,
                                            GraphEdgeSourceKind source_kind) {
  switch (source_kind) {
  case GraphEdgeSourceKind::Output:
    return view.output(evaluation_time);
  case GraphEdgeSourceKind::ErrorOutput:
    return view.error_output(evaluation_time);
  case GraphEdgeSourceKind::RecordableState:
    return view.recordable_state(evaluation_time);
  }
  throw std::logic_error("Graph edge source kind is invalid");
}

[[nodiscard]] TSInputView input_at_path(TSInputView view,
                                        const std::vector<std::size_t> &path) {
  for (const std::size_t component : path) {
    if (view.schema() == nullptr) {
      throw std::logic_error("Graph input path requires a typed input view");
    }
    if (component == ts_key_set_path_component) {
      throw std::invalid_argument(
          "Graph input path cannot address a TSD key set");
    }
    view = view.indexed_child_at(component);
  }
  return view;
}

struct GraphRuntimeBaseStorage {
  GraphRuntimeBaseStorage() = default;

  GraphRuntimeBaseStorage(const GraphRuntimeBaseStorage &) = delete;
  GraphRuntimeBaseStorage &operator=(const GraphRuntimeBaseStorage &) = delete;
  GraphRuntimeBaseStorage(GraphRuntimeBaseStorage &&) noexcept = default;
  GraphRuntimeBaseStorage &
  operator=(GraphRuntimeBaseStorage &&) noexcept = default;
  ~GraphRuntimeBaseStorage() = default;

  DateTime next_scheduled_time{MAX_DT};
  DateTime evaluation_time{MIN_DT};
  DateTime cycle_wall_start{current_wall_time()};
  std::size_t evaluation_cursor{invalid_cursor};
  bool started{false};
  bool evaluating{false};
  bool evaluation_failed{false};
  /** Graph traits (parent-chained key-value metadata; GraphView::trait_or).
      The same value-layer ``Map<string, Any>`` store as GlobalState. */
  GlobalState traits{};
  /** The executor-owned lifecycle observer list, cached once at construction
      (root: from the root executor; nested: one hop to the parent graph's own
      cached pointer) so the hot path never walks the nested-parent chain. */
  LifecycleObserverList *lifecycle_observers{nullptr};
  /** Borrowed from executor storage; nested graphs copy their parent's pointer. */
  spdlog::logger *logger{nullptr};
  const TypeRealizationSnapshot *type_realization{nullptr};
};

struct RootGraphRuntimeStorage : GraphRuntimeBaseStorage {
  RootGraphRuntimeStorage() = default;

  [[nodiscard]] GraphExecutorView root_executor() noexcept {
    return GraphExecutorView{root_executor_ptr};
  }

  GlobalState global_state{};
  ExecutorPtr root_executor_ptr{};
};

struct NestedGraphRuntimeStorage : GraphRuntimeBaseStorage {
  NestedGraphRuntimeStorage() = default;

  [[nodiscard]] NodeView parent_node() { return NodeView{parent_node_ptr}; }

  NodePtr parent_node_ptr{};
  /** Keyed-parent hook: out-of-band child schedules report (context, when)
      so the parent can track WHICH child is due without scanning slots. */
  void (*child_schedule_observer)(void *, DateTime) = nullptr;
  void *child_schedule_observer_context = nullptr;
};

inline constexpr std::string_view graph_header_field_name{"header"};
inline constexpr std::string_view graph_nodes_field_name{"nodes"};
inline constexpr std::string_view graph_schedule_field_name{"schedule"};

struct GraphNodeRuntimeLocation {
  NodeTypeRef type{};
  std::size_t offset{0};
};

struct GraphRuntimeStorageLayout {
  std::size_t node_count{0};
  std::size_t header_offset{0};
  std::size_t schedule_offset{0};
  std::size_t schedule_stride{0};
};

struct GraphRuntimeContext {
  GraphRuntimeStorageLayout layout{};
  std::vector<GraphNodeRuntimeLocation> node_locations{};
};

[[nodiscard]] const GraphRuntimeContext &graph_context(const void *context) {
  if (context == nullptr) {
    throw std::logic_error("Graph runtime context is null");
  }
  return *static_cast<const GraphRuntimeContext *>(context);
}

[[nodiscard]] const MemoryUtils::StoragePlan &
graph_nodes_plan_for(const std::vector<NodeBuilder> &nodes) {
  auto builder = MemoryUtils::tuple();
  builder.reserve(nodes.size());
  for (const NodeBuilder &node : nodes) {
    builder.add_plan(node.type().checked_plan());
  }
  return builder.build();
}

[[nodiscard]] const MemoryUtils::StoragePlan &
graph_storage_plan_for(const MemoryUtils::StoragePlan &header_plan,
                       const std::vector<NodeBuilder> &nodes) {
  auto builder = MemoryUtils::named_tuple();
  builder.reserve(3);
  builder.add_field(graph_header_field_name, header_plan);
  builder.add_field(graph_nodes_field_name, graph_nodes_plan_for(nodes));
  builder.add_field(graph_schedule_field_name,
                    MemoryUtils::array_plan<DateTime>(nodes.size()));
  return builder.build();
}

template <typename Header>
[[nodiscard]] const MemoryUtils::StoragePlan &
graph_storage_plan_for(const std::vector<NodeBuilder> &nodes) {
  return graph_storage_plan_for(MemoryUtils::plan_for<Header>(), nodes);
}

[[nodiscard]] GraphRuntimeContext
graph_runtime_context_for(const MemoryUtils::StoragePlan &plan,
                          const std::vector<NodeBuilder> &node_builders) {
  const auto &header = plan.component(graph_header_field_name);
  const auto &node_storage = plan.component(graph_nodes_field_name);
  const auto &schedule = plan.component(graph_schedule_field_name);
  if (!node_storage.plan->is_tuple() ||
      node_storage.plan->component_count() != node_builders.size()) {
    throw std::logic_error(
        "Graph storage plan has an invalid node storage tuple");
  }
  if (!schedule.plan->is_array() ||
      schedule.plan->array_count() != node_builders.size()) {
    throw std::logic_error("Graph storage plan has an invalid schedule array");
  }

  GraphRuntimeContext context;
  context.layout = GraphRuntimeStorageLayout{
      .node_count = node_builders.size(),
      .header_offset = header.offset,
      .schedule_offset = schedule.offset,
      .schedule_stride = schedule.plan->array_stride(),
  };
  context.node_locations.reserve(node_builders.size());
  for (std::size_t index = 0; index < node_builders.size(); ++index) {
    const auto node_type = node_builders[index].type();
    const auto &node_component = node_storage.plan->component(index);
    if (node_component.plan != node_type.plan()) {
      throw std::logic_error(
          "Graph storage plan node component does not match node type");
    }
    context.node_locations.push_back(GraphNodeRuntimeLocation{
        .type = node_type,
        .offset = node_storage.offset + node_component.offset,
    });
  }
  return context;
}

template <typename Header>
[[nodiscard]] Header &graph_header(const GraphRuntimeContext &context,
                                   void *memory) {
  return *MemoryUtils::cast<Header>(
      MemoryUtils::advance(memory, context.layout.header_offset));
}

template <typename Header>
[[nodiscard]] const Header &graph_header(const GraphRuntimeContext &context,
                                         const void *memory) {
  return *MemoryUtils::cast<Header>(
      MemoryUtils::advance(memory, context.layout.header_offset));
}

[[nodiscard]] void *graph_node_memory(const GraphRuntimeContext &context,
                                      void *memory, std::size_t index) {
  return MemoryUtils::advance(memory, context.node_locations[index].offset);
}

[[nodiscard]] NodeView graph_node_view(const GraphRuntimeContext &context,
                                       void *memory, std::size_t index) {
  const auto &location = context.node_locations[index];
  return NodeView{location.type, MemoryUtils::advance(memory, location.offset)};
}

[[nodiscard]] DateTime &graph_schedule(const GraphRuntimeContext &context,
                                       void *memory, std::size_t index) {
  return *MemoryUtils::cast<DateTime>(
      MemoryUtils::advance(memory, context.layout.schedule_offset +
                                       index * context.layout.schedule_stride));
}

void bind_edges(const GraphRuntimeContext &context, void *memory,
                const std::vector<GraphEdge> &edges) {
  const auto path_label = [](const std::vector<std::size_t> &path) {
    std::string label;
    for (const auto component : path) {
      if (!label.empty()) {
        label += '.';
      }
      label += std::to_string(component);
    }
    return label;
  };
  for (const auto &edge : edges) {
    const std::size_t source_node = graph_edge_source_node(edge.source_node);
    if (source_node >= context.layout.node_count ||
        edge.target_node >= context.layout.node_count) {
      throw std::out_of_range("Graph edge references a missing node");
    }

    auto source_view = graph_node_view(context, memory, source_node);
    auto target_view = graph_node_view(context, memory, edge.target_node);
    TSOutputView source;
    TSInputView target;
    try {
      auto source_root =
          edge_source_root(graph_node_view(context, memory, source_node),
                           MIN_DT, graph_edge_source_kind(edge.source_node));
      source = output_at_path(std::move(source_root), edge.source_path);
      target = input_at_path(
          graph_node_view(context, memory, edge.target_node).input(MIN_DT),
          edge.target_path);
    } catch (const std::exception &error) {
      throw std::invalid_argument(
          "Graph edge from " + node_identity(source_view, source_node) +
          " path [" + path_label(edge.source_path) + "] to " +
          node_identity(target_view, edge.target_node) + " path [" +
          path_label(edge.target_path) + "] is invalid: " + error.what());
    }
    target.bind_output(source);
  }
}

/**
 * Stop-time subscription teardown — the dual of ``bind_edges``.
 *
 * Lifecycle contract: ``stop`` tears the subscriptions down while every
 * producer's storage is still alive; by dispose time no references may
 * remain. This matters because boundary machinery (services/adaptors)
 * retargets edge-established links at runtime to outputs the ranker
 * never saw, so a link may point at a HIGHER-ranked node — and storage
 * destruction runs in reverse rank, freeing that producer before the
 * consumer's link storage is destroyed. Unbinding here (all storage
 * alive, ``MIN_DT`` so no notifier side effects) leaves the destructor
 * unbind as a no-op backstop. Best-effort per edge: teardown must not
 * throw.
 */
void unbind_edges(const GraphRuntimeContext &context, void *memory,
                  const std::vector<GraphEdge> &edges) noexcept {
  for (const auto &edge : edges) {
    static_cast<void>(fallback_on_exception(false, [&] {
      if (edge.target_node >= context.layout.node_count) {
        return false;
      }
      auto target = input_at_path(
          graph_node_view(context, memory, edge.target_node).input(MIN_DT),
          edge.target_path);
      if (target.is_bindable() && target.bound()) {
        target.unbind_output();
      }
      return true;
    }));
  }
}

/**
 * Stop-time release of output alternative-store subscriptions — the
 * second half of the teardown contract (see unbind_edges). REF
 * alternatives subscribe to their source output and hold links to the
 * currently referenced output, which teardown order may free first;
 * releasing them at stop leaves their destructors nothing to touch.
 */
void release_alternative_subscriptions(const GraphRuntimeContext &context,
                                       void *memory,
                                       DateTime release_time) noexcept {
  for (std::size_t index = 0; index < context.layout.node_count; ++index) {
    auto node = graph_node_view(context, memory, index);
    const auto release = [&](TSOutputView view) {
      if (view.output() != nullptr) {
        view.output()->release_alternative_subscriptions(release_time);
      }
    };

    // These are optional node surfaces, not exceptional conditions.
    // Probe their explicit capabilities before constructing a view;
    // throwing once per absent surface made nested graph stop
    // disproportionately expensive under keyed churn.
    if (node.has_output()) {
      static_cast<void>(fallback_on_exception(false, [&] {
        release(node.output(MIN_DT));
        return true;
      }));
    }
    if (node.has_error_output()) {
      static_cast<void>(fallback_on_exception(false, [&] {
        release(node.error_output(MIN_DT));
        return true;
      }));
    }
    if (node.has_recordable_state()) {
      static_cast<void>(fallback_on_exception(false, [&] {
        release(node.recordable_state(MIN_DT));
        return true;
      }));
    }
  }
}

template <typename Header>
void destroy_constructed_graph_parts(
    const GraphRuntimeContext &context, void *memory, bool graph_complete,
    bool header_constructed, std::size_t constructed_nodes,
    std::size_t constructed_schedule,
    const MemoryUtils::StoragePlan &graph_plan) noexcept {
  if (graph_complete) {
    graph_plan.destroy(memory);
    return;
  }

  const auto &date_plan = MemoryUtils::plan_for<DateTime>();
  for (std::size_t index = constructed_schedule; index > 0; --index) {
    date_plan.destroy(&graph_schedule(context, memory, index - 1));
  }

  for (std::size_t index = constructed_nodes; index > 0; --index) {
    const auto &location = context.node_locations[index - 1];
    location.type.destroy_at(MemoryUtils::advance(memory, location.offset));
  }

  if (header_constructed) {
    MemoryUtils::plan_for<Header>().destroy(
        MemoryUtils::advance(memory, context.layout.header_offset));
  }
}

template <typename Header, typename InitHeader>
void construct_graph_storage(GraphTypeRef type, const GraphBuilder &builder,
                             void *memory, InitHeader &&init_header) {
  const auto &context = graph_context(type.ops_ref().context);
  const auto &plan = type.checked_plan();
  bool graph_complete = false;
  bool header_constructed = false;
  std::size_t constructed_nodes = 0;
  std::size_t constructed_schedule = 0;
  auto rollback = make_scope_exit([&]() noexcept {
    destroy_constructed_graph_parts<Header>(
        context, memory, graph_complete, header_constructed, constructed_nodes,
        constructed_schedule, plan);
  });

  std::construct_at(&graph_header<Header>(context, memory));
  header_constructed = true;
  std::forward<InitHeader>(init_header)(graph_header<Header>(context, memory));
  for (std::size_t index = 0; index < context.layout.node_count; ++index) {
    builder.nodes()[index].construct_node_storage(
        graph_node_memory(context, memory, index), index);
    ++constructed_nodes;
  }
  for (std::size_t index = 0; index < context.layout.node_count; ++index) {
    std::construct_at(&graph_schedule(context, memory, index), MIN_DT);
    ++constructed_schedule;
  }
  graph_complete = true;
  bind_edges(context, memory, builder.edges());
  rollback.release();
}

void propagate_nested_parent_schedule(NestedGraphRuntimeStorage &state) {
  const DateTime next = state.next_scheduled_time;
  if (next >= MAX_DT) {
    return;
  }

  auto parent = state.parent_node();
  parent.graph().schedule_node(parent.node_index(), next);
}

[[nodiscard]] std::size_t
compute_push_source_nodes_end(const GraphBuilder &builder) {
  std::size_t prefix = 0;
  bool seen_non_push_source = false;

  for (const NodeBuilder &node : builder.nodes()) {
    const NodeKind kind = node.type().schema()->node_kind;
    if (kind == NodeKind::PushSource) {
      if (seen_non_push_source) {
        throw std::invalid_argument(
            "Push source nodes must occupy the graph node prefix");
      }
      ++prefix;
    } else {
      seen_non_push_source = true;
    }
  }

  return prefix;
}

template <typename Storage>
void attach_nodes_impl(const void *context, void *memory, GraphValue *graph) {
  static_cast<void>(sizeof(Storage));
  const auto &runtime = graph_context(context);
  for (std::size_t index = 0; index < runtime.layout.node_count; ++index) {
    auto node = graph_node_view(runtime, memory, index);
    const auto node_type = node.type();
    const auto &ops = node_type.ops_ref();
    ops.attach_graph_impl(ops.context, node.data(), graph, index);
  }
}

template <typename Storage>
bool started_impl(const void *context, const void *memory) noexcept {
  return graph_header<Storage>(graph_context(context), memory).started;
}

template <typename Storage>
bool evaluating_impl(const void *context, const void *memory) noexcept {
  return graph_header<Storage>(graph_context(context), memory).evaluating;
}

template <typename Storage>
DateTime evaluation_time_impl(const void *context,
                              const void *memory) noexcept {
  return graph_header<Storage>(graph_context(context), memory).evaluation_time;
}

template <typename Storage>
DateTime next_scheduled_time_impl(const void *context,
                                  const void *memory) noexcept {
  return graph_header<Storage>(graph_context(context), memory)
      .next_scheduled_time;
}

template <typename Storage>
std::size_t node_count_impl(const void *context, const void *memory) noexcept {
  static_cast<void>(sizeof(Storage));
  static_cast<void>(memory);
  return graph_context(context).layout.node_count;
}

template <typename Storage>
NodeView node_at_impl(const void *context, void *memory, std::size_t index) {
  static_cast<void>(sizeof(Storage));
  const auto &runtime = graph_context(context);
  if (index >= runtime.layout.node_count) {
    throw std::out_of_range("Graph node index is out of range");
  }
  return graph_node_view(runtime, memory, index);
}

template <typename Storage>
NodeView failed_node_impl(const void *context, void *memory) noexcept {
  const auto &runtime = graph_context(context);
  const auto &state = graph_header<Storage>(runtime, memory);
  return state.evaluation_failed &&
                 state.evaluation_cursor < runtime.layout.node_count
             ? graph_node_view(runtime, memory, state.evaluation_cursor)
             : NodeView{};
}

template <typename Storage>
ValueView graph_trait_impl(const void *context, void *memory,
                           std::string_view name) noexcept {
  auto &traits = graph_header<Storage>(graph_context(context), memory).traits;
  return traits.view().get(name);
}

GlobalStateView root_global_state_impl(const void *context, void *memory) {
  return graph_header<RootGraphRuntimeStorage>(graph_context(context), memory)
      .global_state.view();
}

GlobalStateView nested_global_state_impl(const void *context, void *memory) {
  auto parent =
      graph_header<NestedGraphRuntimeStorage>(graph_context(context), memory)
          .parent_node();
  if (!parent.valid()) {
    throw std::logic_error("Nested graph is missing its parent node");
  }
  return parent.graph().root().global_state();
}

GraphExecutorView root_graph_executor_impl(const void *context, void *memory) {
  auto executor =
      graph_header<RootGraphRuntimeStorage>(graph_context(context), memory)
          .root_executor();
  if (!executor.valid()) {
    throw std::logic_error("Root graph is missing its graph executor");
  }
  return executor;
}

GraphExecutorView nested_graph_executor_impl(const void *context,
                                             void *memory) {
  auto parent =
      graph_header<NestedGraphRuntimeStorage>(graph_context(context), memory)
          .parent_node();
  if (!parent.valid()) {
    throw std::logic_error("Nested graph is missing its parent node");
  }
  return parent.graph().executor();
}

NodeView root_parent_node_impl(const void *, void *) {
  throw std::logic_error("GraphView::as_nested requires a nested graph");
}

NodeView nested_parent_node_impl(const void *context, void *memory) {
  auto parent =
      graph_header<NestedGraphRuntimeStorage>(graph_context(context), memory)
          .parent_node();
  if (!parent.valid()) {
    throw std::logic_error("Nested graph is missing its parent node");
  }
  return parent;
}

RootGraphView root_graph_root_impl(const void *, const GraphView &graph) {
  return RootGraphView{GraphView{graph.pointer()}};
}

RootGraphView nested_graph_root_impl(const void *context,
                                     const GraphView &graph) {
  auto parent = graph_header<NestedGraphRuntimeStorage>(graph_context(context),
                                                        graph.data())
                    .parent_node();
  if (!parent.valid()) {
    throw std::logic_error("Nested graph is missing its parent node");
  }
  return parent.graph().root();
}

template <typename Storage>
void schedule_node_impl(const void *context, const GraphView &graph,
                        std::size_t node_index, DateTime when) {
  const auto &runtime = graph_context(context);
  auto &state = graph_header<Storage>(runtime, graph.data());
  if (node_index >= runtime.layout.node_count) {
    throw std::out_of_range("Graph schedule node index is out of range");
  }

  const DateTime current = state.evaluation_time;
  if (when < current) {
    throw std::runtime_error("Graph cannot schedule a node in the past");
  }

  auto &scheduled = graph_schedule(runtime, graph.data(), node_index);
  if (scheduled <= current || when < scheduled) {
    scheduled = when;
    if (when > current && when < state.next_scheduled_time) {
      state.next_scheduled_time = when;
    }
  }
}

// The **push** half of nested scheduling delegation (the RFC clock
// invariant, executor-ops style): any schedule recorded on a child graph
// immediately wakes the parent node no later than that time. The **pull**
// half (``single_nested_graph_propagate_schedule`` after start/evaluate)
// covers schedules created while the parent is already driving the child,
// so the push is gated to out-of-band calls only — a notification or
// scheduler arriving while the child is idle (``started && !evaluating``).
// Multi-level nesting recurses up to the root naturally.
void nested_schedule_node_impl(const void *context, const GraphView &graph,
                               std::size_t node_index, DateTime when) {
  const auto &runtime = graph_context(context);
  auto &state = graph_header<NestedGraphRuntimeStorage>(runtime, graph.data());
  auto parent = state.parent_node();

  // An idle child retains the time of its last evaluation while its
  // parent may already be processing a later cycle. A cross-boundary
  // notification (notably a REF rebind that samples an older target)
  // must run in the parent's current cycle, never schedule either
  // graph back at the child's stale clock.
  when = std::max(when, parent.graph().evaluation_time());
  schedule_node_impl<NestedGraphRuntimeStorage>(context, graph, node_index,
                                                when);

  // An idle child can receive a current-cycle schedule immediately
  // after startup. The per-node entry is authoritative, but keyed
  // parents use this cache to decide which children are due. Graph
  // startup normally rebuilds the cache after node start hooks; this
  // post-start path must maintain the same invariant explicitly.
  if (state.started && !state.evaluating && when < state.next_scheduled_time) {
    state.next_scheduled_time = when;
  }

  if (!state.started || state.evaluating) {
    return;
  }
  if (state.child_schedule_observer != nullptr) {
    state.child_schedule_observer(state.child_schedule_observer_context, when);
  }
  parent.graph().schedule_node(parent.node_index(), when);
}

void nested_set_child_schedule_observer_impl(const void *context, void *memory,
                                             void (*observer)(void *,
                                                              DateTime),
                                             void *observer_context) {
  auto &state = graph_header<NestedGraphRuntimeStorage>(graph_context(context),
                                                        memory);
  state.child_schedule_observer = observer;
  state.child_schedule_observer_context = observer_context;
}

template <typename Storage>
void start_impl(const void *context, const GraphView &graph,
                DateTime start_time) {
  const auto &runtime = graph_context(context);
  auto &state = graph_header<Storage>(runtime, graph.data());
  if (state.started) {
    return;
  }

  auto graph_start_failed = UnwindCleanupGuard([&] {
    state.lifecycle_observers->notify_start_graph_failed(graph);
  });
  state.lifecycle_observers->notify_before_start_graph(graph);

  state.evaluation_time = start_time;
  state.cycle_wall_start = current_wall_time();
  std::size_t started_nodes = 0;
  auto rollback = UnwindCleanupGuard([&] {
    for (std::size_t index = started_nodes; index > 0; --index) {
      NodeView node_view = graph_node_view(runtime, graph.data(), index - 1);
      bool notify_after = false;
      auto after_notify = make_scope_exit<true>([&] {
        if (notify_after) {
          state.lifecycle_observers->notify_after_stop_node(node_view);
        }
      });
      auto failed_notify = UnwindCleanupGuard([&] {
        state.lifecycle_observers->notify_stop_node_failed(node_view);
      });
      state.lifecycle_observers->notify_before_stop_node(node_view);
      // HideExceptions: a buggy observer must not mask the rollback itself,
      // nor terminate() by throwing a second exception during unwind.
      notify_after = true;
      node_view.stop(state.evaluation_time);
      failed_notify.release();
    }
    state.next_scheduled_time = MAX_DT;
    state.started = false;
  });

  // NOTE: edge subscriptions are established at construction and torn
  // down at stop (see unbind_edges). Restart is NOT supported by
  // design: stop is a step toward erase (cleanup before disposal),
  // so no rebind pass exists here — a blanket bind_edges() would
  // reset REF-adapted bindings that construction set up.

  // Nodes are NOT scheduled by default. A node that needs an initial
  // evaluation schedules itself in its ``start`` (a source does
  // ``schedule(now())``); compute/sink nodes are driven by input
  // notifications. This mirrors 2603, where the node-kind ``start``
  // (e.g. GeneratorNodeImpl.start) does the initial scheduling rather
  // than the graph blanket-scheduling everything.
  for (std::size_t index = 0; index < runtime.layout.node_count; ++index) {
    NodeView node_view = graph_node_view(runtime, graph.data(), index);
    auto node_start_failed = UnwindCleanupGuard([&] {
      state.lifecycle_observers->notify_start_node_failed(node_view);
    });
    state.lifecycle_observers->notify_before_start_node(node_view);
    // Plain sequential (no "after" on throw): a node that fails to start
    // never really started, so no matching after-start notification fires.
    // The rollback above still stops (and notifies) whatever DID start.
    if constexpr (std::is_same_v<Storage, RootGraphRuntimeStorage>) {
      annotate_on_exception(
          [&] { node_view.start(state.evaluation_time); },
          [&] { rethrow_with_node_identity(node_view, index, "start"); });
    } else {
      node_view.start(state.evaluation_time);
    }
    state.lifecycle_observers->notify_after_start_node(node_view);
    node_start_failed.release();
    ++started_nodes;
  }

  state.next_scheduled_time = MAX_DT;
  for (std::size_t index = 0; index < runtime.layout.node_count; ++index) {
    const DateTime scheduled = graph_schedule(runtime, graph.data(), index);
    if (scheduled >= state.evaluation_time &&
        scheduled < state.next_scheduled_time) {
      state.next_scheduled_time = scheduled;
    }
  }
  state.started = true;
  rollback.release();
  state.lifecycle_observers->notify_after_start_graph(graph);
  graph_start_failed.release();
}

template <typename Storage>
void stop_impl(const void *context, const GraphView &graph, DateTime stop_time) {
  const auto &runtime = graph_context(context);
  auto &state = graph_header<Storage>(runtime, graph.data());
  if (!state.started) {
    return;
  }
  if (stop_time < state.evaluation_time) {
    throw std::invalid_argument("Graph cannot stop in the past");
  }
  state.evaluation_time = stop_time;

  auto graph_stop_failed = UnwindCleanupGuard([&] {
    state.lifecycle_observers->notify_stop_graph_failed(graph);
  });
  state.lifecycle_observers->notify_before_stop_graph(graph);

  FirstExceptionRecorder exceptions;
  for (std::size_t index = runtime.layout.node_count; index > 0; --index) {
    exceptions.capture([&] {
      NodeView node_view = graph_node_view(runtime, graph.data(), index - 1);
      bool notify_after = false;
      auto after_notify = make_scope_exit<true>([&] {
        if (notify_after) {
          state.lifecycle_observers->notify_after_stop_node(node_view);
        }
      });
      auto failed_notify = UnwindCleanupGuard([&] {
        state.lifecycle_observers->notify_stop_node_failed(node_view);
      });
      state.lifecycle_observers->notify_before_stop_node(node_view);
      // Best-effort: every node gets a stop attempt (FirstExceptionRecorder
      // defers the throw), so "after" fires even when stop() itself throws.
      // HideExceptions guards against a buggy observer masking that throw or
      // terminate()-ing during its unwind.
      notify_after = true;
      if constexpr (std::is_same_v<Storage, RootGraphRuntimeStorage>) {
        annotate_on_exception(
            [&] { node_view.stop(state.evaluation_time); },
            [&] { rethrow_with_node_identity(node_view, index - 1, "stop"); });
      } else {
        node_view.stop(state.evaluation_time);
      }
      failed_notify.release();
    });
  }
  // Tear down the edge subscriptions and alternative-store links
  // while every node's storage is still alive (see unbind_edges /
  // release_alternative_subscriptions); dispose must find no
  // references.
  if (graph.schema() != nullptr) {
    unbind_edges(runtime, graph.data(), graph.schema()->edges);
  }
  release_alternative_subscriptions(runtime, graph.data(),
                                    state.evaluation_time);
  state.started = false;
  if (exceptions.has_exception()) {
    state.lifecycle_observers->notify_stop_graph_failed(graph);
  }
  // Fires even when one or more nodes failed to stop: the graph as a whole
  // completed its (best-effort) stop attempt before the deferred throw below.
  state.lifecycle_observers->notify_after_stop_graph(graph);
  graph_stop_failed.release();
  exceptions.rethrow_if_any();
}

template <typename Storage>
DateTime node_scheduled_time_impl(const void *context, const void *memory,
                                  std::size_t index) noexcept {
  const auto &runtime = graph_context(context);
  if (index >= runtime.layout.node_count) {
    return MIN_DT;
  }
  return graph_schedule(runtime, const_cast<void *>(memory), index);
}

template <typename Storage>
LifecycleObserverList *lifecycle_observers_impl(const void *context,
                                                const void *memory) noexcept {
  const auto &runtime = graph_context(context);
  return graph_header<Storage>(runtime, memory).lifecycle_observers;
}

template <typename Storage>
spdlog::logger *logger_impl(const void *context,
                            const void *memory) noexcept {
  const auto &runtime = graph_context(context);
  return graph_header<Storage>(runtime, memory).logger;
}

template <typename Storage>
const TypeRealizationSnapshot *
type_realization_impl(const void *context, const void *memory) noexcept {
  return graph_header<Storage>(graph_context(context), memory).type_realization;
}

template <typename Storage>
bool evaluate_impl(const void *context, const GraphView &graph,
                   DateTime evaluation_time) {
  const auto &runtime = graph_context(context);
  auto &state = graph_header<Storage>(runtime, graph.data());
  if (!state.started) {
    throw std::logic_error("Graph must be started before evaluation");
  }

  // The node loop drives state.evaluation_cursor (the node_id) directly so a
  // pause can resume mid-cycle. When a node returns false it has requested a
  // pause: the cursor is already sitting on that node, we return false, and the
  // next evaluate at the same time continues from there WITHOUT redoing the
  // per-cycle setup (next_scheduled accumulation / push-source pass). A
  // completed cycle resets the cursor to 0. (A cursor of 0 or the initial
  // invalid sentinel means "fresh".)
  const bool resuming =
      state.evaluation_cursor != 0 && state.evaluation_cursor != invalid_cursor;

  state.evaluation_time = evaluation_time;
  state.evaluation_failed = false;
  state.evaluating = true;
  auto reset = make_scope_exit([&] noexcept { state.evaluating = false; });

  // Fires once this cycle either genuinely completes or an exception escapes
  // the node loop below — never on a pause (return false mid-cycle, suppressed
  // via `paused`). HideExceptions: a buggy observer must not mask the real
  // evaluation failure or terminate() by throwing during its unwind.
  bool paused = false;
  auto after_eval_notify = make_scope_exit<true>([&] {
    if (!paused) {
      state.lifecycle_observers->notify_after_graph_evaluation(graph);
    }
  });

  std::size_t first_normal_node = 0;
  if constexpr (std::is_same_v<Storage, RootGraphRuntimeStorage>) {
    first_normal_node = graph.schema()->push_source_nodes_end;
  }

  if (!resuming) {
    state.lifecycle_observers->notify_before_graph_evaluation(graph);
    state.cycle_wall_start = current_wall_time();
    state.next_scheduled_time = MAX_DT;

    if constexpr (std::is_same_v<Storage, RootGraphRuntimeStorage>) {
      if (first_normal_node > 0) {
        PushQueueEngineView push_queue =
            graph.root().executor().push_queue_engine();
        const bool push_update_pending = push_queue.reset_push_update_pending();
        bool push_phase_evaluated = push_update_pending;
        for (std::size_t index = 0; index < first_normal_node; ++index) {
          auto &scheduled = graph_schedule(runtime, graph.data(), index);
          const bool scheduled_now = scheduled == evaluation_time;
          if (push_update_pending || scheduled_now) {
            push_phase_evaluated = true;
            if (scheduled_now) {
              scheduled = MIN_DT;
            }
            state.evaluation_cursor = index;
            NodeView node_view = graph_node_view(runtime, graph.data(), index);
            state.lifecycle_observers->notify_before_node_evaluation(node_view);
            auto node_after_notify = make_scope_exit<true>([&] {
              state.lifecycle_observers->notify_after_node_evaluation(
                  node_view);
            });
            annotate_on_exception([&] { node_view.evaluate(evaluation_time); },
                                  [&] {
                                    state.evaluation_cursor = index;
                                    state.evaluation_failed = true;
                                    rethrow_with_node_identity(node_view, index,
                                                               "evaluate");
                                  });
          }
          if (scheduled > evaluation_time &&
              scheduled < state.next_scheduled_time) {
            state.next_scheduled_time = scheduled;
          }
        }
        if (push_phase_evaluated) {
          state.lifecycle_observers
              ->notify_after_graph_push_nodes_evaluation(graph);
        }
      }
    }
    state.evaluation_cursor = first_normal_node;
  }

  for (; state.evaluation_cursor < runtime.layout.node_count;
       ++state.evaluation_cursor) {
    auto &scheduled =
        graph_schedule(runtime, graph.data(), state.evaluation_cursor);
    if (scheduled == evaluation_time) {
      // post-eval MIN_DT stamp removed (see lazy-cleanup invariant)
      bool completed = true;
      NodeView node_view =
          graph_node_view(runtime, graph.data(), state.evaluation_cursor);
      state.lifecycle_observers->notify_before_node_evaluation(node_view);
      // Best-effort: the node did run once, so a matching "after" fires
      // regardless of a pause or a thrown exception (unlike the graph-level
      // notification above, which is about the whole CYCLE completing).
      auto node_after_notify = make_scope_exit<true>([&] {
        state.lifecycle_observers->notify_after_node_evaluation(node_view);
      });
      if constexpr (std::is_same_v<Storage, RootGraphRuntimeStorage>) {
        completed = annotate_on_exception(
            [&] { return node_view.evaluate(state.evaluation_time); },
            [&] {
              state.evaluation_failed = true;
              rethrow_with_node_identity(node_view, state.evaluation_cursor,
                                         "evaluate");
            });
      } else {
        completed = annotate_on_exception(
            [&] { return node_view.evaluate(state.evaluation_time); },
            [&] { state.evaluation_failed = true; });
      }
      if (!completed) {
        // Pause requested: hold the cursor on this node and propagate upward
        // (the enclosing mesh node resolves the dependency and resumes us).
        return false;
      }
    } else if (scheduled > evaluation_time) {
      if (scheduled < state.next_scheduled_time) {
        state.next_scheduled_time = scheduled;
      }
    }
  }

  state.evaluation_cursor = 0; // completed: reset the cursor

  if constexpr (std::is_same_v<Storage, NestedGraphRuntimeStorage>) {
    propagate_nested_parent_schedule(
        graph_header<NestedGraphRuntimeStorage>(runtime, graph.data()));
  }
  return true;
}

struct GraphRuntimeRegistry {
  struct Entry {
    GraphTypeMetaData schema{};
    GraphRuntimeContext root_context{};
    GraphRuntimeContext nested_context{};
    GraphOps root_ops{};
    GraphOps nested_ops{};
    GraphTypeRef root_type{};
    GraphTypeRef nested_type{};
  };

  struct Types {
    GraphTypeRef root{};
    GraphTypeRef nested{};
  };

  GraphTypeMetaData make_meta(const GraphBuilder &builder) {
    GraphTypeMetaData meta;
    names.push_back(
        std::make_unique<std::string>(std::string{builder.label()}));
    if (!names.back()->empty()) {
      meta.display_name = names.back()->c_str();
    }
    meta.header = SchemaHeader{TypeFamily::Graph, TYPE_KIND_NONE,
                               meta.display_name != nullptr &&
                                       meta.display_name[0] != '\0'
                                   ? meta.display_name
                                   : "graph"};

    meta.nodes.reserve(builder.nodes().size());
    for (std::size_t index = 0; index < builder.nodes().size(); ++index) {
      meta.nodes.push_back(
          GraphNodeEntry{builder.nodes()[index].type().schema(), index});
    }
    meta.edges = builder.edges();
    meta.push_source_nodes_end = compute_push_source_nodes_end(builder);

    return meta;
  }

  Types make_types(const GraphBuilder &builder) {
    entries.push_back({});
    auto &entry = entries.back();
    entry.schema = make_meta(builder);

    const auto &root_plan =
        graph_storage_plan_for<RootGraphRuntimeStorage>(builder.nodes());
    entry.root_context = graph_runtime_context_for(root_plan, builder.nodes());
    entry.root_ops = make_root_ops(&entry.root_context);
    entry.root_type = intern_graph_type(entry.schema, root_plan, entry.root_ops,
                                        "hgraph.graph.root");

    if (entry.schema.push_source_nodes_end == 0) {
      const auto &nested_plan =
          graph_storage_plan_for<NestedGraphRuntimeStorage>(builder.nodes());
      entry.nested_context =
          graph_runtime_context_for(nested_plan, builder.nodes());
      entry.nested_ops = make_nested_ops(&entry.nested_context);
      entry.nested_type = intern_graph_type(
          entry.schema, nested_plan, entry.nested_ops, "hgraph.graph.nested");
    }
    return Types{entry.root_type, entry.nested_type};
  }

  static GraphOps make_root_ops(const GraphRuntimeContext *context) {
    return GraphOps{
        .context = context,
        .parent_kind = GraphParentKind::Root,
        .attach_nodes_impl = &attach_nodes_impl<RootGraphRuntimeStorage>,
        .start_impl = &start_impl<RootGraphRuntimeStorage>,
        .stop_impl = &stop_impl<RootGraphRuntimeStorage>,
        .evaluate_impl = &evaluate_impl<RootGraphRuntimeStorage>,
        .schedule_node_impl = &schedule_node_impl<RootGraphRuntimeStorage>,
        .started_impl = &started_impl<RootGraphRuntimeStorage>,
        .evaluating_impl = &evaluating_impl<RootGraphRuntimeStorage>,
        .evaluation_time_impl = &evaluation_time_impl<RootGraphRuntimeStorage>,
        .next_scheduled_time_impl =
            &next_scheduled_time_impl<RootGraphRuntimeStorage>,
        .node_count_impl = &node_count_impl<RootGraphRuntimeStorage>,
        .node_at_impl = &node_at_impl<RootGraphRuntimeStorage>,
        .failed_node_impl = &failed_node_impl<RootGraphRuntimeStorage>,
        .node_scheduled_time_impl =
            &node_scheduled_time_impl<RootGraphRuntimeStorage>,
        .global_state_impl = &root_global_state_impl,
        .trait_impl = &graph_trait_impl<RootGraphRuntimeStorage>,
        .root_impl = &root_graph_root_impl,
        .graph_executor_impl = &root_graph_executor_impl,
        .parent_node_impl = &root_parent_node_impl,
        .lifecycle_observers_impl =
            &lifecycle_observers_impl<RootGraphRuntimeStorage>,
        .type_realization_impl =
            &type_realization_impl<RootGraphRuntimeStorage>,
        .logger_impl = &logger_impl<RootGraphRuntimeStorage>,
    };
  }

  static GraphOps make_nested_ops(const GraphRuntimeContext *context) {
    return GraphOps{
        .context = context,
        .parent_kind = GraphParentKind::Nested,
        .attach_nodes_impl = &attach_nodes_impl<NestedGraphRuntimeStorage>,
        .start_impl = &start_impl<NestedGraphRuntimeStorage>,
        .stop_impl = &stop_impl<NestedGraphRuntimeStorage>,
        .evaluate_impl = &evaluate_impl<NestedGraphRuntimeStorage>,
        .schedule_node_impl = &nested_schedule_node_impl,
        .started_impl = &started_impl<NestedGraphRuntimeStorage>,
        .evaluating_impl = &evaluating_impl<NestedGraphRuntimeStorage>,
        .evaluation_time_impl =
            &evaluation_time_impl<NestedGraphRuntimeStorage>,
        .next_scheduled_time_impl =
            &next_scheduled_time_impl<NestedGraphRuntimeStorage>,
        .node_count_impl = &node_count_impl<NestedGraphRuntimeStorage>,
        .node_at_impl = &node_at_impl<NestedGraphRuntimeStorage>,
        .failed_node_impl = &failed_node_impl<NestedGraphRuntimeStorage>,
        .node_scheduled_time_impl =
            &node_scheduled_time_impl<NestedGraphRuntimeStorage>,
        .global_state_impl = &nested_global_state_impl,
        .trait_impl = &graph_trait_impl<NestedGraphRuntimeStorage>,
        .root_impl = &nested_graph_root_impl,
        .graph_executor_impl = &nested_graph_executor_impl,
        .parent_node_impl = &nested_parent_node_impl,
        .set_child_schedule_observer_impl =
            &nested_set_child_schedule_observer_impl,
        .lifecycle_observers_impl =
            &lifecycle_observers_impl<NestedGraphRuntimeStorage>,
        .type_realization_impl =
            &type_realization_impl<NestedGraphRuntimeStorage>,
        .logger_impl = &logger_impl<NestedGraphRuntimeStorage>,
    };
  }

  std::deque<Entry> entries{};
  std::vector<std::unique_ptr<std::string>> names{};

  void clear() noexcept {
    entries.clear();
    names.clear();
  }
};

GraphRuntimeRegistry &graph_runtime_registry() {
  static GraphRuntimeRegistry registry;
  return registry;
}

} // namespace

namespace {
void validate_graph_record(const TypeRecord &record) {
  if (!record.valid() || record.schema->family != TypeFamily::Graph ||
      record.role != TypeRole::Runtime ||
      record.schema->kind != TYPE_KIND_NONE) {
    throw std::invalid_argument(
        "GraphTypeRef requires a Graph/Runtime TypeRecord");
  }
  if (record.ops_abi_version != GRAPH_OPS_ABI_VERSION ||
      record.ops == nullptr) {
    throw std::invalid_argument("GraphTypeRef requires graph ops ABI version " +
                                std::to_string(GRAPH_OPS_ABI_VERSION));
  }
  if (record.capabilities != graph_type_capabilities(*record.plan)) {
    throw std::invalid_argument(
        "GraphTypeRef capabilities do not match its storage plan");
  }
}
} // namespace

TypeCapabilities graph_type_capabilities(const MemoryUtils::StoragePlan &plan) {
  TypeCapabilities result =
      TypeCapabilities::Viewable | TypeCapabilities::Mutable;
  if (plan.can_default_construct())
    result |= TypeCapabilities::Constructible;
  if (plan.trivially_destructible || plan.lifecycle.can_destroy())
    result |= TypeCapabilities::Destructible;
  if (plan.can_copy_construct())
    result |= TypeCapabilities::Copyable;
  if (plan.can_move_construct())
    result |= TypeCapabilities::Movable;
  return result;
}

GraphTypeRef intern_graph_type(const GraphTypeMetaData &schema,
                               const MemoryUtils::StoragePlan &plan,
                               const GraphOps &ops,
                               std::string_view implementation_label) {
  if (!schema.header.valid() || schema.header.family != TypeFamily::Graph ||
      schema.header.kind != TYPE_KIND_NONE) {
    throw std::invalid_argument(
        "intern_graph_type requires a valid graph schema header");
  }
  const auto &runtime = graph_context(ops.context);
  std::vector<DebugField> debug_fields;
  debug_fields.reserve(runtime.node_locations.size());
  for (const GraphNodeRuntimeLocation &node : runtime.node_locations) {
    debug_fields.push_back(DebugField{
        .offset = node.offset,
        .type = node.type.record(),
    });
  }
  const auto &debug = intern_structured_debug_descriptor(
      schema.header, plan, DebugLayoutKind::Graph,
      debug_fields.empty() ? nullptr : debug_fields.data(),
      debug_fields.size());
  const TypeRecordDefinition definition{
      .key = TypeRecordKey{.schema = &schema.header,
                           .role = TypeRole::Runtime,
                           .plan = &plan,
                           .ops = &ops,
                           .debug = &debug},
      .ops_abi_version = GRAPH_OPS_ABI_VERSION,
      .capabilities = graph_type_capabilities(plan),
      .implementation_label = implementation_label,
  };
  return GraphTypeRef{&TypeRecordRegistry::instance().intern(definition)};
}

GraphTypeRef GraphTypeRef::checked(AnyPtr pointer) {
  if (pointer.is_unbound())
    return {};
  if (!pointer.well_formed() || pointer.record() == nullptr)
    throw std::invalid_argument("GraphTypeRef requires a well-formed pointer");
  validate_graph_record(*pointer.record());
  return GraphTypeRef{pointer.record()};
}

bool GraphTypeRef::valid() const noexcept {
  if (record_ == nullptr)
    return false;
  return fallback_on_exception(false, [&] {
    validate_graph_record(*record_);
    return true;
  });
}

const GraphTypeMetaData *GraphTypeRef::schema() const noexcept {
  return record_ != nullptr
             ? reinterpret_cast<const GraphTypeMetaData *>(record_->schema)
             : nullptr;
}

const MemoryUtils::StoragePlan &GraphTypeRef::checked_plan() const {
  if (plan() == nullptr)
    throw std::logic_error("GraphTypeRef is unbound");
  return *plan();
}

GraphPtr GraphTypeRef::typed_null() const noexcept {
  return GraphPtr{AnyPtr{record_, nullptr, AccessMode::ReadOnly},
                  GraphPtr::UncheckedTag{}};
}

GraphPtr GraphTypeRef::read_only(const void *data) const noexcept {
  return GraphPtr{AnyPtr{record_, data, AccessMode::ReadOnly},
                  GraphPtr::UncheckedTag{}};
}

GraphPtr GraphTypeRef::writable(void *data) const noexcept {
  return GraphPtr{AnyPtr{record_, data, AccessMode::Writable},
                  GraphPtr::UncheckedTag{}};
}

std::string_view GraphTypeMetaData::name() const noexcept {
  return display_name != nullptr ? std::string_view{display_name}
                                 : std::string_view{};
}

GraphView::GraphView() noexcept = default;

GraphView::GraphView(GraphPtr pointer) noexcept : pointer_(pointer) {}

GraphView::GraphView(GraphTypeRef type, void *memory) noexcept
    : pointer_(type && memory != nullptr ? type.writable(memory) : GraphPtr{}) {
}

bool GraphView::valid() const noexcept { return pointer_.valid(); }
GraphTypeRef GraphView::type() const noexcept {
  return GraphTypeRef{pointer_.record()};
}
GraphPtr GraphView::pointer() const noexcept { return pointer_; }
const GraphTypeMetaData *GraphView::schema() const noexcept {
  return type().schema();
}
void *GraphView::data() const noexcept {
  return const_cast<void *>(pointer_.data());
}

bool GraphView::started() const noexcept {
  return valid() && ops().started_impl(ops().context, data());
}
bool GraphView::evaluating() const noexcept {
  return valid() && ops().evaluating_impl(ops().context, data());
}
DateTime GraphView::evaluation_time() const noexcept {
  return valid() ? ops().evaluation_time_impl(ops().context, data()) : MIN_DT;
}
DateTime GraphView::next_scheduled_time() const noexcept {
  return valid() ? ops().next_scheduled_time_impl(ops().context, data())
                 : MAX_DT;
}
std::size_t GraphView::node_count() const noexcept {
  return valid() ? ops().node_count_impl(ops().context, data()) : 0;
}

NodeView GraphView::node_at(std::size_t index) const {
  return ops().node_at_impl(ops().context, data(), index);
}

NodeView GraphView::failed_node() const noexcept {
  if (!valid() || ops().failed_node_impl == nullptr) {
    return NodeView{};
  }
  return ops().failed_node_impl(ops().context, data());
}

DateTime GraphView::node_scheduled_time(std::size_t node_index) const noexcept {
  if (!valid() || ops().node_scheduled_time_impl == nullptr) {
    return MIN_DT;
  }
  return ops().node_scheduled_time_impl(ops().context, data(), node_index);
}

std::string GraphView::dump() const {
  if (!valid()) {
    return "<invalid graph>";
  }

  std::string out = "graph";
  const auto *meta = schema();
  if (meta != nullptr && meta->display_name != nullptr &&
      *meta->display_name != '\0') {
    out += " '";
    out += meta->display_name;
    out += '\'';
  }
  const std::size_t count = node_count();
  out += started() ? " [started" : " [stopped";
  out += " nodes=" + std::to_string(count);
  out += " evaluation_time=" + schedule_to_string(evaluation_time());
  out += " next_scheduled=" + schedule_to_string(next_scheduled_time());
  out += "]\n";

  for (std::size_t index = 0; index < count; ++index) {
    out += "  " + node_identity(node_at(index), index);
    out += " scheduled=" + schedule_to_string(node_scheduled_time(index));
    out += '\n';
  }
  return out;
}

GlobalStateView GraphView::global_state() const {
  return ops().global_state_impl(ops().context, data());
}

ValueView GraphView::trait_or(std::string_view name) const noexcept {
  // This graph's OWN entry only (Python get_trait_or) — no bubbling.
  if (!valid() || ops().trait_impl == nullptr) {
    return ValueView{};
  }
  return ops().trait_impl(ops().context, data(), name);
}

ValueView GraphView::trait(std::string_view name) const noexcept {
  // Chained lookup (Python get_trait): own entry first, then BUBBLE UP
  // the nested parent chain (each hop is one graph; terminates at root).
  const GraphView *graph = this;
  GraphView parent_holder;
  while (graph != nullptr && graph->valid()) {
    if (graph->ops().trait_impl != nullptr) {
      if (ValueView entry = graph->ops().trait_impl(graph->ops().context,
                                                    graph->data(), name);
          entry.valid()) {
        return entry;
      }
    }
    if (!graph->is_nested()) {
      break;
    }
    GraphValue *parent = graph->as_nested().parent_node().graph_value();
    if (parent == nullptr) {
      break;
    }
    parent_holder = parent->view();
    graph = &parent_holder;
  }
  return ValueView{};
}

GraphParentKind GraphView::parent_kind() const { return ops().parent_kind; }

bool GraphView::is_root() const {
  return parent_kind() == GraphParentKind::Root;
}

bool GraphView::is_nested() const {
  return parent_kind() == GraphParentKind::Nested;
}

RootGraphView GraphView::as_root() const {
  return RootGraphView{GraphView{pointer()}};
}

NestedGraphView GraphView::as_nested() const {
  return NestedGraphView{GraphView{pointer()}};
}

GraphExecutorView GraphView::executor() const {
  return ops().graph_executor_impl(ops().context, data());
}

RootGraphView GraphView::root() const {
  return ops().root_impl(ops().context, *this);
}

LifecycleObserverList &GraphView::lifecycle_observers() const {
  auto *list = ops().lifecycle_observers_impl == nullptr
                   ? nullptr
                   : ops().lifecycle_observers_impl(ops().context, data());
  if (list == nullptr) {
    throw std::logic_error("Graph is missing its lifecycle observer list");
  }
  return *list;
}

spdlog::logger *GraphView::logger() const noexcept {
  if (!valid()) {
    return nullptr;
  }
  const auto &table = ops();
  return table.logger_impl != nullptr ? table.logger_impl(table.context, data())
                                      : nullptr;
}

const TypeRealizationSnapshot *GraphView::type_realization() const noexcept {
  if (!valid()) {
    return nullptr;
  }
  const auto &table = ops();
  return table.type_realization_impl != nullptr
             ? table.type_realization_impl(table.context, data())
             : nullptr;
}

void GraphView::start(DateTime start_time) const {
  TypeRealizationScope scope{type_realization()};
  ops().start_impl(ops().context, *this, start_time);
}
void GraphView::stop() const {
  TypeRealizationScope scope{type_realization()};
  ops().stop_impl(ops().context, *this, evaluation_time());
}
void GraphView::stop(DateTime stop_time) const {
  TypeRealizationScope scope{type_realization()};
  ops().stop_impl(ops().context, *this, stop_time);
}
bool GraphView::evaluate(DateTime evaluation_time) const {
  TypeRealizationScope scope{type_realization()};
  return ops().evaluate_impl(ops().context, *this, evaluation_time);
}
void GraphView::schedule_node(std::size_t node_index, DateTime when) const {
  ops().schedule_node_impl(ops().context, *this, node_index, when);
}

void GraphView::set_child_schedule_observer(void (*observer)(void *, DateTime),
                                            void *observer_context) const {
  if (ops().set_child_schedule_observer_impl == nullptr) {
    throw std::logic_error(
        "set_child_schedule_observer requires a nested child graph");
  }
  ops().set_child_schedule_observer_impl(ops().context, data(), observer,
                                         observer_context);
}

const GraphOps &GraphView::ops() const { return type().ops_ref(); }

RootGraphView::RootGraphView() noexcept : GraphView() {}

RootGraphView::RootGraphView(GraphView graph) : GraphView(graph.pointer()) {
  if (!graph.valid()) {
    throw std::logic_error("GraphView::as_root requires a live graph");
  }
  if (graph.parent_kind() != GraphParentKind::Root) {
    throw std::logic_error("GraphView::as_root requires a root graph");
  }
}

GraphExecutorView RootGraphView::executor() const {
  auto executor = GraphView::executor();
  if (!executor.valid()) {
    throw std::logic_error("Root graph is missing its graph executor");
  }
  return executor;
}

NestedGraphView::NestedGraphView() noexcept : GraphView() {}

NestedGraphView::NestedGraphView(GraphView graph) : GraphView(graph.pointer()) {
  if (!graph.valid()) {
    throw std::logic_error("GraphView::as_nested requires a live graph");
  }
  if (graph.parent_kind() != GraphParentKind::Nested) {
    throw std::logic_error("GraphView::as_nested requires a nested graph");
  }
}

NodeView NestedGraphView::parent_node() const {
  return ops().parent_node_impl(ops().context, data());
}

GraphValue::GraphValue() noexcept = default;

GraphValue::GraphValue(const GraphBuilder &builder, ExecutorPtr root_executor) {
  const auto snapshot = builder.type_realization();
  TypeRealizationScope realization_scope{snapshot.get()};
  const auto type = builder.root_type();
  storage_ = storage_type::owning_constructed(*type.record(), [&](void *dst) {
    // GraphValue is a friend of GraphBuilder, so we read the owning
    // GlobalState directly to seed this graph's copy.
    construct_graph_storage<RootGraphRuntimeStorage>(
        type, builder, dst, [&](RootGraphRuntimeStorage &state) {
          if (!root_executor.has_value()) {
            throw std::invalid_argument(
                "Root graph construction requires a live executor parent");
          }
          state.global_state = builder.global_state_;
          state.traits = builder.traits_;
          state.root_executor_ptr = root_executor;
          state.lifecycle_observers =
              &GraphExecutorView{root_executor}.lifecycle_observers();
          state.logger = GraphExecutorView{root_executor}.logger();
          state.type_realization = snapshot.get();
        });
  });
  pointer_ = type.writable(storage_.data());
  attach_nodes();
}

GraphValue::GraphValue(const GraphBuilder &builder, NodePtr parent_node) {
  const auto *active_snapshot = active_type_realization();
  const auto snapshot =
      active_snapshot == nullptr ? builder.type_realization() : nullptr;
  const auto *effective_snapshot =
      active_snapshot != nullptr ? active_snapshot : snapshot.get();
  TypeRealizationScope realization_scope{effective_snapshot};
  const auto type = builder.nested_type();
  storage_ = storage_type::owning_constructed(*type.record(), [&](void *dst) {
    construct_graph_storage<NestedGraphRuntimeStorage>(
        type, builder, dst, [&](NestedGraphRuntimeStorage &state) {
          state.traits = builder.traits_;
          if (!parent_node.has_value()) {
            throw std::invalid_argument(
                "Nested graph construction requires a live node parent");
          }
          state.parent_node_ptr = parent_node;
          // One hop to the parent graph's own cached pointer (already populated
          // during its construction) — O(1) regardless of nesting depth.
          state.lifecycle_observers =
              &NodeView{parent_node}.graph().lifecycle_observers();
          state.logger = NodeView{parent_node}.graph().logger();
          state.type_realization = effective_snapshot;
        });
  });
  pointer_ = type.writable(storage_.data());
  attach_nodes();
}

GraphValue::GraphValue(const GraphBuilder &builder, NodePtr parent_node,
                       void *external_memory,
                       MemoryUtils::StorageLayout available_layout) {
  const auto *active_snapshot = active_type_realization();
  const auto snapshot =
      active_snapshot == nullptr ? builder.type_realization() : nullptr;
  const auto *effective_snapshot =
      active_snapshot != nullptr ? active_snapshot : snapshot.get();
  TypeRealizationScope realization_scope{effective_snapshot};
  const auto type = builder.nested_type();
  const auto required = type.checked_plan().layout;
  if (external_memory == nullptr) {
    throw std::invalid_argument(
        "Nested graph external storage requires live memory");
  }
  if (available_layout.size < required.size ||
      available_layout.alignment < required.alignment ||
      reinterpret_cast<std::uintptr_t>(external_memory) % required.alignment !=
          0) {
    throw std::invalid_argument(
        "Nested graph external storage does not satisfy its graph layout");
  }

  construct_graph_storage<NestedGraphRuntimeStorage>(
      type, builder, external_memory, [&](NestedGraphRuntimeStorage &state) {
        state.traits = builder.traits_;
        if (!parent_node.has_value()) {
          throw std::invalid_argument(
              "Nested graph construction requires a live node parent");
        }
        state.parent_node_ptr = parent_node;
        state.lifecycle_observers =
            &NodeView{parent_node}.graph().lifecycle_observers();
        state.logger = NodeView{parent_node}.graph().logger();
        state.type_realization = effective_snapshot;
      });
  auto rollback =
      make_scope_exit([&]() noexcept { type.destroy_at(external_memory); });
  pointer_ = type.writable(external_memory);
  attach_nodes();
  rollback.release();
}

GraphValue::~GraphValue() { reset(); }

void GraphValue::reset() noexcept {
  // Lifecycle contract: subscriptions are torn down at stop, while every
  // producer's storage is alive; disposal must find no references. A
  // graph destroyed while still started would skip that teardown, so
  // stop it here (best-effort — a destructor must not throw).
  if (pointer_.has_value()) {
    const GraphView graph = view();
    if (graph.valid() && graph.started()) {
      static_cast<void>(fallback_on_exception(false, [&] {
        graph.stop();
        return true;
      }));
    }
  }
  if (uses_external_storage()) {
    type().destroy_at(const_cast<void *>(pointer_.data()));
  } else {
    storage_.reset();
  }
  pointer_ = {};
}

GraphValue::GraphValue(GraphValue &&other) noexcept
    : pointer_(std::exchange(other.pointer_, {})),
      storage_(std::move(other.storage_)) {
  if (storage_.has_value()) {
    pointer_ = GraphTypeRef{storage_.binding()}.writable(storage_.data());
  }
  attach_nodes();
}

GraphValue &GraphValue::operator=(GraphValue &&other) noexcept {
  if (this != &other) {
    reset();
    pointer_ = std::exchange(other.pointer_, {});
    storage_ = std::move(other.storage_);
    if (storage_.has_value()) {
      pointer_ = GraphTypeRef{storage_.binding()}.writable(storage_.data());
    }
    attach_nodes();
  }
  return *this;
}

bool GraphValue::has_value() const noexcept { return pointer_.has_value(); }
bool GraphValue::uses_external_storage() const noexcept {
  return pointer_.has_value() && !storage_.has_value();
}
GraphTypeRef GraphValue::type() const noexcept {
  return GraphTypeRef{pointer_.record()};
}
const GraphTypeMetaData *GraphValue::schema() const noexcept {
  return type().schema();
}

GraphView GraphValue::view() { return GraphView{pointer_}; }

GraphView GraphValue::view() const { return GraphView{pointer_}; }

void GraphValue::schedule_node(std::size_t node_index, DateTime when) {
  view().schedule_node(node_index, when);
}

void GraphValue::attach_nodes() {
  if (!has_value()) {
    return;
  }
  const auto graph_type = type();
  const auto &table = graph_type.ops_ref();
  table.attach_nodes_impl(table.context, const_cast<void *>(pointer_.data()),
                          this);
}

GraphBuilder::GraphBuilder() {
  if (const GlobalState *state = GlobalContext::active_state()) {
    global_state_ = *state;
  }
}

GraphBuilder &GraphBuilder::label(std::string label) {
  label_ = std::move(label);
  invalidate_types();
  return *this;
}

GraphBuilder &GraphBuilder::add_node(NodeBuilder node) {
  nodes_.push_back(std::move(node));
  invalidate_types();
  return *this;
}

GraphBuilder &GraphBuilder::add_edge(GraphEdge edge) {
  edges_.push_back(std::move(edge));
  invalidate_types();
  return *this;
}

GlobalStateView GraphBuilder::global_state() noexcept {
  return global_state_.view();
}

ValueView TraitsView::trait(std::string_view name) const noexcept {
  if (!graph_.bound()) {
    return ValueView{};
  }
  return GraphView{graph_}.trait(name);
}

ValueView TraitsView::trait_or(std::string_view name) const noexcept {
  if (!graph_.bound()) {
    return ValueView{};
  }
  return GraphView{graph_}.trait_or(name);
}

GraphBuilder &GraphBuilder::trait(std::string_view name,
                                  const ValueView &value) {
  if (name.empty()) {
    throw std::invalid_argument("graph trait name must not be empty");
  }
  traits_.view().set(name, value);
  return *this;
}

GraphBuilder &GraphBuilder::trait(std::string_view name, Value &&value) {
  if (name.empty()) {
    throw std::invalid_argument("graph trait name must not be empty");
  }
  traits_.view().set(name, std::move(value));
  return *this;
}

GlobalStateView GraphBuilder::traits() noexcept { return traits_.view(); }

GraphBuilder &GraphBuilder::global_state(GlobalState state) {
  global_state_ = std::move(state);
  return *this;
}

GraphBuilder &GraphBuilder::type_realization(
    std::shared_ptr<const TypeRealizationSnapshot> snapshot) {
  if (!snapshot) {
    throw std::invalid_argument(
        "graph type realization snapshot must not be null");
  }
  type_realization_ = std::move(snapshot);
  return *this;
}

std::shared_ptr<const TypeRealizationSnapshot>
GraphBuilder::type_realization() const {
  if (!type_realization_) {
    type_realization_ =
        TypeRealizationSnapshot::capture(TypeRegistry::instance());
  }
  return type_realization_;
}

std::string_view GraphBuilder::label() const noexcept { return label_; }
std::size_t GraphBuilder::node_count() const noexcept { return nodes_.size(); }
const std::vector<NodeBuilder> &GraphBuilder::nodes() const noexcept {
  return nodes_;
}
const std::vector<GraphEdge> &GraphBuilder::edges() const noexcept {
  return edges_;
}

NodeBuilder &GraphBuilder::node_at(std::size_t index) {
  if (index >= nodes_.size()) {
    throw std::out_of_range("GraphBuilder node index is out of range");
  }
  invalidate_types();
  return nodes_[index];
}

GraphTypeRef GraphBuilder::type() const { return root_type(); }

MemoryUtils::StorageLayout GraphBuilder::nested_storage_layout() const {
  return nested_type().checked_plan().layout;
}

GraphTypeRef GraphBuilder::root_type() const {
  if (!types_compiled_) {
    const auto types = graph_runtime_registry().make_types(*this);
    root_type_ = types.root;
    nested_type_ = types.nested;
    types_compiled_ = true;
  }
  return root_type_;
}

GraphTypeRef GraphBuilder::nested_type() const {
  static_cast<void>(root_type());
  if (!nested_type_) {
    throw std::invalid_argument(
        "Nested graphs do not support push source nodes");
  }
  return nested_type_;
}

void GraphBuilder::invalidate_types() noexcept {
  root_type_ = {};
  nested_type_ = {};
  types_compiled_ = false;
  type_realization_.reset();
}

GraphValue GraphBuilder::make_root_graph(ExecutorPtr root_executor) const {
  return GraphValue{*this, root_executor};
}

GraphValue GraphBuilder::make_nested_graph(NodePtr parent_node) const {
  return GraphValue{*this, parent_node};
}

GraphValue GraphBuilder::make_nested_graph(
    NodePtr parent_node, void *external_memory,
    MemoryUtils::StorageLayout available_layout) const {
  return GraphValue{*this, parent_node, external_memory, available_layout};
}

void clear_graph_runtime_types() noexcept {
  clear_debug_descriptors(TypeFamily::Graph);
  graph_runtime_registry().clear();
}

std::size_t detail::graph_program_count() noexcept {
  return graph_runtime_registry().entries.size();
}

std::size_t detail::graph_runtime_type_count() noexcept {
  std::size_t count = 0;
  for (const auto &entry : graph_runtime_registry().entries) {
    count += entry.root_type ? 1U : 0U;
    count += entry.nested_type ? 1U : 0U;
  }
  return count;
}

} // namespace hgraph
