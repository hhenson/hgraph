#include <hgraph/runtime/nested_bindings.h>
#include <hgraph/runtime/switch_node.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <array>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace hgraph {
namespace {
constexpr std::string_view switch_storage_field_name{"switch"};
constexpr std::string_view switch_graph_memory_field_name{
    "switch_graph_memory"};

struct SwitchNodeStorage {
  std::array<GraphValue, 2> graphs{};
  std::optional<std::size_t> active_slot{};
  std::optional<std::size_t> previous_slot{};
  Value active_key{};
  const SingleNestedGraphNodeSpec *active_spec{nullptr};

  [[nodiscard]] GraphValue *active_graph() noexcept {
    return active_slot.has_value() ? &graphs[*active_slot] : nullptr;
  }
};

struct SwitchNodeContext {
  SwitchNodeSpec spec{};
  std::size_t storage_offset{0};
  std::size_t graph_memory_offset{0};
  std::size_t graph_slot_stride{0};
  MemoryUtils::StorageLayout graph_slot_layout{};
};

[[nodiscard]] MemoryUtils::StorageLayout
switch_graph_slot_layout(const SwitchNodeSpec &spec) {
  MemoryUtils::StorageLayout layout{.size = 1, .alignment = 1};
  const auto include_layout = [&](const SingleNestedGraphNodeSpec &branch) {
    const auto branch_layout = branch.graph_builder.nested_storage_layout();
    layout.size = std::max(layout.size, branch_layout.size);
    layout.alignment = std::max(layout.alignment, branch_layout.alignment);
  };
  for (const SwitchBranch &branch : spec.branches) {
    include_layout(branch.spec);
  }
  if (spec.default_branch.has_value()) {
    include_layout(*spec.default_branch);
  }
  return layout;
}

// Program-lifetime, intentionally-leaked context storage — same rationale
// as single_nested_graph_contexts (see nested_graph_node.cpp): contexts
// own GraphBuilders referencing interned registries, and a static
// container's destruction order against those registries is undefined.
[[nodiscard]] std::vector<std::unique_ptr<SwitchNodeContext>> &
switch_node_contexts() noexcept {
  static auto *contexts = new std::vector<std::unique_ptr<SwitchNodeContext>>;
  return *contexts;
}

[[nodiscard]] const SwitchNodeContext *
register_switch_node_context(SwitchNodeSpec spec, std::size_t storage_offset,
                             std::size_t graph_memory_offset,
                             std::size_t graph_slot_stride,
                             MemoryUtils::StorageLayout graph_slot_layout) {
  auto context = std::make_unique<SwitchNodeContext>(SwitchNodeContext{
      .spec = std::move(spec),
      .storage_offset = storage_offset,
      .graph_memory_offset = graph_memory_offset,
      .graph_slot_stride = graph_slot_stride,
      .graph_slot_layout = graph_slot_layout,
  });
  const auto *result = context.get();
  switch_node_contexts().push_back(std::move(context));
  return result;
}

[[nodiscard]] NodeStorageMetrics switch_storage_metrics(
    const void *raw_context, const void *memory) noexcept {
  const auto &context = *static_cast<const SwitchNodeContext *>(raw_context);
  const auto &storage = *MemoryUtils::cast<const SwitchNodeStorage>(
      MemoryUtils::advance(memory, context.storage_offset));
  std::size_t count = 0;
  for (const GraphValue &graph : storage.graphs) {
    if (graph.has_value()) {
      ++count;
    }
  }
  return NodeStorageMetrics{
      .nested_graph_count = count,
      .nested_graph_capacity = storage.graphs.size(),
  };
}

void visit_switch_children(const void *raw_context, const NodeBuilder &,
                           void *visitor_context, ChildGraphVisitor visitor) {
  const auto &context = *static_cast<const SwitchNodeContext *>(raw_context);
  for (const SwitchBranch &branch : context.spec.branches) {
    visitor(visitor_context, ChildGraphInspectionView{
                                 .graph = &branch.spec.graph_builder,
                                 .output_binding = branch.spec.output_binding
                                                       ? &*branch.spec.output_binding
                                                       : nullptr,
                             });
  }
  if (context.spec.default_branch.has_value()) {
    const auto &branch = *context.spec.default_branch;
    visitor(visitor_context, ChildGraphInspectionView{
                                 .graph = &branch.graph_builder,
                                 .output_binding = branch.output_binding
                                                       ? &*branch.output_binding
                                                       : nullptr,
                             });
  }
}

[[nodiscard]] void *switch_graph_memory(const NodeView &view,
                                        const SwitchNodeContext &context,
                                        std::size_t slot) noexcept {
  return MemoryUtils::advance(view.data(),
                              context.graph_memory_offset +
                                  slot * context.graph_slot_stride);
}

void bind_branch_inputs(const NodeView &view,
                        const SingleNestedGraphNodeSpec &spec,
                        const GraphView &child, DateTime evaluation_time,
                        bool sampled = false) {
  if (spec.input_bindings.empty()) {
    return;
  }
  auto root_input = view.input(evaluation_time);
  for (const NestedGraphInputBinding &binding : spec.input_bindings) {
    auto target =
        walk_ts_path(child.node_at(binding.target.node).input(evaluation_time),
                     binding.target.path);
    if (!binding.source_path.empty() &&
        binding.source_path.back() == ts_key_set_path_component) {
      auto source =
          walk_source_to_output(root_input.borrowed_ref(), binding.source_path);
      if (sampled) {
        bind_sampled_input_to_source(std::move(target), source,
                                     evaluation_time);
      } else {
        bind_input_to_source(std::move(target), source);
      }
    } else {
      TSInputView source = annotate_on_exception<std::out_of_range>(
          [&] {
            return walk_ts_path(root_input.borrowed_ref(), binding.source_path);
          },
          [&](const std::out_of_range &error) {
            std::string path;
            for (const auto component : binding.source_path) {
              if (!path.empty()) {
                path += ".";
              }
              path += std::to_string(component);
            }
            throw std::out_of_range(
                "switch_: failed to bind branch input source path [" + path +
                "] to child node " + std::to_string(binding.target.node) +
                ": " + error.what());
          });
      bind_nested_input_to_source(std::move(target), std::move(source),
                                  evaluation_time, sampled);
    }
  }
}

void bind_branch_output(const NodeView &view, const SwitchNodeContext &context,
                        const SingleNestedGraphNodeSpec &spec,
                        const GraphView &child, DateTime evaluation_time,
                        bool sampled = false) {
  if (!spec.output_binding.has_value()) {
    return;
  }

  const NestedGraphOutputBinding &binding = *spec.output_binding;

  if (binding.kind != NestedGraphOutputBinding::Kind::ChildOutput) {
    throw std::logic_error("switch_ branches must terminate at a child output");
  }

  auto branch_terminal =
      walk_ts_path(child.node_at(binding.source.node).output(evaluation_time),
                   binding.source.path);
  auto switch_output =
      walk_ts_path(view.output(evaluation_time), binding.target_path);

  if (switch_output.schema() != nullptr &&
      switch_output.schema()->kind == TSTypeKind::REF &&
      branch_terminal.schema() != nullptr) {
    if (branch_terminal.schema()->kind == TSTypeKind::REF &&
        !branch_terminal.valid()) {
      return;
    }
    const TimeSeriesReference reference =
        branch_terminal.schema()->kind == TSTypeKind::REF
            ? branch_terminal.value().checked_as<TimeSeriesReference>()
            : TimeSeriesReference::peered(branch_terminal);
    if (switch_output.valid() &&
        switch_output.value().checked_as<TimeSeriesReference>() == reference) {
      return;
    }

    Value value{reference};
    static_cast<void>(switch_output.begin_mutation(evaluation_time)
                          .move_value_from(std::move(value)));
    return;
  }

  if (context.spec.output_forwards_to_child_terminal) {
    static_cast<void>(bind_forwarding_output_tree_to_source(
        std::move(switch_output), branch_terminal, sampled,
        ForwardingSourceMode::PreserveEndpoint));
    return;
  }

  bind_forwarding_output_to_source(branch_terminal, switch_output);
}

void clear_branch_output(const NodeView &view, const SwitchNodeContext &context,
                         const SingleNestedGraphNodeSpec &spec,
                         DateTime evaluation_time) {
  if (!spec.output_binding.has_value()) {
    return;
  }

  const NestedGraphOutputBinding &binding = *spec.output_binding;
  if (binding.kind != NestedGraphOutputBinding::Kind::ChildOutput) {
    return;
  }

  if (context.spec.output_forwards_to_child_terminal) {
    auto output =
        walk_ts_path(view.output(evaluation_time), binding.target_path);
    if (output.schema() != nullptr &&
        output.schema()->kind == TSTypeKind::REF) {
      // A REF switch owns the reference token that preserves the selected
      // terminal. The terminal itself retains responsibility for its
      // forwarding lifecycle.
      return;
    }
    static_cast<void>(clear_forwarding_output_tree(std::move(output)));
    return;
  }

  auto switch_view = view.as<SwitchNodeView>();
  auto &storage =
      *MemoryUtils::cast<SwitchNodeStorage>(switch_view.internal_storage());
  GraphValue *active = storage.active_graph();
  if (active == nullptr || !active->has_value()) {
    return;
  }

  auto terminal = walk_ts_path(
      active->view().node_at(binding.source.node).output(evaluation_time),
      binding.source.path);
  if (terminal.forwarding_bound()) {
    terminal.clear_forwarding_target();
  }
}

void reset_switch_output(const NodeView &view, DateTime evaluation_time) {
  if (!view.has_output()) {
    return;
  }
  auto output = view.output(evaluation_time);
  if (!output.bound()) {
    return;
  }

  if (output.schema() != nullptr && output.schema()->kind == TSTypeKind::REF) {
    Value empty{TimeSeriesReference{}};
    auto mutation = output.begin_mutation(evaluation_time);
    static_cast<void>(mutation.move_value_from(std::move(empty)));
    return;
  }
  static_cast<void>(output.data_view().clear_collection(evaluation_time));
}

[[nodiscard]] const SingleNestedGraphNodeSpec *
select_branch(const SwitchNodeContext &context, const Value &key) {
  for (const SwitchBranch &branch : context.spec.branches) {
    if (branch.key.equals(key)) {
      return &branch.spec;
    }
  }
  if (context.spec.default_branch.has_value()) {
    return &*context.spec.default_branch;
  }
  return nullptr;
}

void switch_teardown(const NodeView &view, const SwitchNodeContext &context,
                     SwitchNodeStorage &storage, DateTime evaluation_time,
                     bool reset_output = true) {
  GraphValue *active = storage.active_graph();
  if (active == nullptr || !active->has_value()) {
    return;
  }
  if (storage.active_spec != nullptr) {
    clear_branch_output(view, context, *storage.active_spec, evaluation_time);
  }
  active->view().stop(evaluation_time);
  if (reset_output) {
    reset_switch_output(view, evaluation_time);
  }
  storage.previous_slot = storage.active_slot;
  storage.active_slot.reset();
  storage.active_key = Value{};
  storage.active_spec = nullptr;
}

void activate_branch(const NodeView &view, const SwitchNodeContext &context,
                     SwitchNodeStorage &storage,
                     const SingleNestedGraphNodeSpec &spec, Value key,
                     DateTime evaluation_time) {
  const std::size_t next_slot =
      storage.active_slot.has_value() ? 1U - *storage.active_slot : 0U;

  // This slot contains the graph retired on the previous switch. Its
  // stop phase has already completed, so it can now be destructed and
  // the same fixed memory reused for the new branch.
  if (storage.previous_slot.has_value() &&
      *storage.previous_slot != next_slot) {
    throw std::logic_error(
        "switch_ previous graph does not occupy the reusable slot");
  }
  storage.graphs[next_slot] = GraphValue{};
  storage.previous_slot.reset();
  storage.graphs[next_slot] = spec.graph_builder.make_nested_graph(
      view.pointer(), switch_graph_memory(view, context, next_slot),
      context.graph_slot_layout);

  auto next = storage.graphs[next_slot].view();
  auto construction_rollback =
      UnwindCleanupGuard([&] { storage.graphs[next_slot] = GraphValue{}; });
  bind_branch_inputs(view, spec, next, evaluation_time, true);
  construction_rollback.release();

  if (context.spec.output_forwards_to_child_terminal) {
    GraphValue *active = storage.active_graph();
    bind_branch_output(view, context, spec, next, evaluation_time, true);
    if (active != nullptr && active->has_value()) {
      active->view().stop(evaluation_time);
    }
    storage.previous_slot = storage.active_slot;
    storage.active_slot.reset();
    storage.active_key = Value{};
    storage.active_spec = nullptr;
  } else {
    switch_teardown(view, context, storage, evaluation_time);
  }
  storage.active_slot = next_slot;
  storage.active_key = std::move(key);
  storage.active_spec = &spec;

  if (!context.spec.output_forwards_to_child_terminal) {
    bind_branch_output(view, context, spec, next, evaluation_time);
  }
  // Selecting a branch samples the current boundary values once.
  // This is an explicit switch_ lifecycle operation: activating an
  // input only subscribes it and must never synthesize scheduling or
  // modification state.
  next.start(evaluation_time);
  schedule_sampled_input_consumers(next, evaluation_time, spec.input_bindings);
}

// Single active child: propagate its pause directly. On resume, re-binding is
// idempotent and the active child resumes from its own cursor.
bool switch_evaluate(const NodeView &view, DateTime evaluation_time) {
  if (!view.started()) {
    return true;
  }

  auto switch_view = view.as<SwitchNodeView>();
  const auto &context =
      *static_cast<const SwitchNodeContext *>(switch_view.internal_context());
  auto &storage =
      *MemoryUtils::cast<SwitchNodeStorage>(switch_view.internal_storage());

  auto root_input = view.input(evaluation_time);
  auto root_bundle = root_input.as_bundle();
  auto key_input = root_bundle[0];

  if (key_input.valid() &&
      (key_input.modified() || !storage.active_slot.has_value())) {
    Value key_value{key_input.value()};
    const bool same_key = storage.active_slot.has_value() &&
                          storage.active_key.has_value() &&
                          key_value.equals(storage.active_key);

    if (!storage.active_slot.has_value() || context.spec.reload_on_ticked ||
        !same_key) {
      const SingleNestedGraphNodeSpec *spec = select_branch(context, key_value);
      if (spec == nullptr) {
        throw std::runtime_error(
            "switch_: no branch is registered for key " +
            key_value.to_string() + " (and no default branch)");
      }

      activate_branch(view, context, storage, *spec, std::move(key_value),
                      evaluation_time);
    }
  }

  if (GraphValue *active = storage.active_graph();
      active != nullptr && active->has_value()) {
    // Re-bind boundaries each cycle (cheap no-op when stable; absorbs
    // upstream REF re-points). The nested graph evaluator propagates
    // its cached next scheduled time back to this node before returning.
    const SingleNestedGraphNodeSpec &spec = *storage.active_spec;
    bind_branch_inputs(view, spec, active->view(), evaluation_time);
    bind_branch_output(view, context, spec, active->view(), evaluation_time);
    const bool complete = active->view().evaluate(evaluation_time);
    // A REF terminal can become valid or repoint while evaluating the child.
    // Refresh the copied switch boundary after that mutation; value terminals
    // propagate through their forwarding endpoints without this extra sample.
    bind_branch_output(view, context, spec, active->view(), evaluation_time);
    return complete;
  }
  return true;
}

bool switch_evaluate_impl(const void *, const NodeView &view,
                          DateTime evaluation_time) {
  return switch_evaluate(view, evaluation_time);
}

void switch_node_stop(const NodeView &view, DateTime evaluation_time) {
  auto switch_view = view.as<SwitchNodeView>();
  const auto &context =
      *static_cast<const SwitchNodeContext *>(switch_view.internal_context());
  auto &storage =
      *MemoryUtils::cast<SwitchNodeStorage>(switch_view.internal_storage());
  switch_teardown(view, context, storage, evaluation_time, false);
}
} // namespace

const void *SwitchNodeView::node_view_type_id() noexcept {
  static const char token{};
  return &token;
}

SwitchNodeView SwitchNodeView::from_node(NodeView view, const void *context) {
  if (context == nullptr) {
    throw std::logic_error("SwitchNodeView requires a typed view context");
  }
  const auto &typed_context = *static_cast<const SwitchNodeContext *>(context);
  void *storage =
      MemoryUtils::advance(view.data(), typed_context.storage_offset);
  return SwitchNodeView{std::move(view), context, storage};
}

const NodeView &SwitchNodeView::node() const noexcept { return view_; }

bool SwitchNodeView::has_active_branch() const noexcept {
  return MemoryUtils::cast<SwitchNodeStorage>(storage_)
      ->active_slot.has_value();
}

GraphValue &SwitchNodeView::active_graph_value() const noexcept {
  auto &storage = *MemoryUtils::cast<SwitchNodeStorage>(storage_);
  return storage.graphs[*storage.active_slot];
}

const Value &SwitchNodeView::active_key() const noexcept {
  return MemoryUtils::cast<SwitchNodeStorage>(storage_)->active_key;
}

std::size_t SwitchNodeView::stored_graph_count() const noexcept {
  const auto &storage = *MemoryUtils::cast<SwitchNodeStorage>(storage_);
  return static_cast<std::size_t>(storage.graphs[0].has_value()) +
         static_cast<std::size_t>(storage.graphs[1].has_value());
}

bool SwitchNodeView::child_graphs_use_in_place_storage() const noexcept {
  const auto &storage = *MemoryUtils::cast<SwitchNodeStorage>(storage_);
  for (const GraphValue &graph : storage.graphs) {
    if (graph.has_value() && !graph.uses_external_storage()) {
      return false;
    }
  }
  return true;
}

SwitchNodeView::SwitchNodeView(NodeView view, const void *context,
                               void *storage) noexcept
    : view_(std::move(view)), context_(context), storage_(storage) {}

NodeBuilder switch_node(NodeTypeMetaData meta, SwitchNodeSpec spec) {
  if (spec.branches.empty() && !spec.default_branch.has_value()) {
    throw std::invalid_argument("switch_node requires at least one branch");
  }

  const auto validate_branch = [&](const SingleNestedGraphNodeSpec &branch) {
    if (branch.output_binding.has_value() != (meta.output_schema != nullptr)) {
      throw std::invalid_argument(
          "switch_node branches must all produce an output exactly when the "
          "node has an output schema");
    }
    if (branch.output_binding.has_value() &&
        !branch.output_binding->target_path.empty()) {
      throw std::invalid_argument(
          "switch_node currently supports forwarding only at the output root");
    }
  };
  for (const SwitchBranch &branch : spec.branches) {
    validate_branch(branch.spec);
  }
  if (spec.default_branch.has_value()) {
    validate_branch(*spec.default_branch);
  }

  meta.requires_phase_runner =
      meta.requires_phase_runner ||
      std::ranges::any_of(spec.branches, [](const SwitchBranch &branch) {
        return branch.spec.graph_builder.requires_phase_runner();
      }) ||
      (spec.default_branch.has_value() &&
       spec.default_branch->graph_builder.requires_phase_runner());
  meta.node_kind = NodeKind::Nested;
  if (meta.output_schema != nullptr) {
    const bool forwards_output =
        spec.output_forwards_to_child_terminal &&
        meta.output_schema->kind != TSTypeKind::REF;
    meta.output_endpoint_schema =
        forwards_output
            ? forwarding_output_endpoint_schema(meta.output_schema)
            : TSEndpointSchema::local(meta.output_schema);
  }

  NodeTypeDescriptor descriptor;
  descriptor.schema = std::move(meta);

  const MemoryUtils::StorageLayout graph_slot_layout =
      switch_graph_slot_layout(spec);
  const auto &graph_slot_plan =
      MemoryUtils::raw_storage_plan(graph_slot_layout);
  const auto &graph_memory_plan = MemoryUtils::array_plan(graph_slot_plan, 2);
  const std::array fields{
      NodeStorageField{
          .name = switch_graph_memory_field_name,
          .plan = &graph_memory_plan,
      },
      NodeStorageField{
          .name = switch_storage_field_name,
          .plan = &MemoryUtils::plan_for<SwitchNodeStorage>(),
      },
  };
  descriptor.storage_plan = &node_storage_plan_for(descriptor.schema, fields);
  descriptor.debug_fields.push_back(DebugField{
      .name = "graph[0]",
      .offset =
          descriptor.storage_plan->component(switch_storage_field_name).offset +
          offsetof(SwitchNodeStorage, graphs) +
          GraphValue::debug_pointer_offset(),
      .flags = DebugFieldFlags::EmbeddedPointer,
  });
  descriptor.debug_fields.push_back(DebugField{
      .name = "graph[1]",
      .offset =
          descriptor.storage_plan->component(switch_storage_field_name).offset +
          offsetof(SwitchNodeStorage, graphs) + sizeof(GraphValue) +
          GraphValue::debug_pointer_offset(),
      .flags = DebugFieldFlags::EmbeddedPointer,
  });

  descriptor.callbacks.stop = &switch_node_stop;
  descriptor.ops.evaluate_impl = &switch_evaluate_impl;
  descriptor.ops.storage_metrics_impl = &switch_storage_metrics;
  descriptor.ops.extended_view_type_id = SwitchNodeView::node_view_type_id();
  const auto *context = register_switch_node_context(
      std::move(spec),
      descriptor.storage_plan->component(switch_storage_field_name).offset,
      descriptor.storage_plan->component(switch_graph_memory_field_name).offset,
      graph_memory_plan.array_stride(), graph_slot_layout);
  descriptor.ops.extended_view_context = context;
  descriptor.ops.child_graph_inspection = ChildGraphInspectionOps{
      .context = context,
      .visit_impl = &visit_switch_children,
  };

  return NodeBuilder::from_descriptor(std::move(descriptor));
}
} // namespace hgraph
