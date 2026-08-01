#include <hgraph/runtime/map_node.h>
#include <hgraph/types/graph_wiring.h>
#include <hgraph/types/metadata/type_realization.h>
#include <hgraph/types/operator_dispatch.h> // context scope stack (OperatorRegistry)
#include <hgraph/types/subgraph_wiring.h>
#include <hgraph/util/scope.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <deque>
#include <stdexcept>
#include <typeindex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace hgraph {
namespace {
// Interning key: node definition identity (typeid of the static node type)
// + input edges by (producing instance, source path, target path) + the scalar
// configuration values. The output schema is implied by the node + path, so it
// is not part of the key. ``scalars`` is empty for a node with no scalar
// inputs. The node's resolved schema identity (``WiringNodeSchema``) enters the
// key because for a GENERIC node two wirings of one definition resolve to
// different schemas (e.g. const_ over int vs double) and must not dedup.
// (The NodeTypeMetaData object itself is freshly built per builder, so it
// cannot be used as identity.)
[[nodiscard]] WiringNodeSchema resolved_schema_of(const NodeBuilder &builder) {
  const auto *tm = builder.type().schema();
  if (tm == nullptr) {
    return WiringNodeSchema{};
  }
  return WiringNodeSchema{tm->input_schema,        tm->output_schema,
                          tm->error_output_schema, tm->recordable_state_schema,
                          tm->scalar_schema,       tm->state_schema};
}

struct SourceKey {
  WiringPortRef::SourceKind kind{WiringPortRef::SourceKind::Unbound};
  GraphEdgeSourceKind peered_output_kind{GraphEdgeSourceKind::Output};
  const WiringInstance *peered_node{nullptr};
  std::vector<std::size_t> peered_path{};
  const TSValueTypeMetaData *schema{nullptr};
  std::vector<SourceKey> structural_children{};
  std::size_t boundary_arg{static_cast<std::size_t>(-1)};
  std::vector<std::size_t> boundary_path{};
  bool captured_boundary{false};
  const WiringDelayedBindingState *delayed_state{nullptr};
  std::vector<std::size_t> delayed_path{};

  bool operator==(const SourceKey &) const noexcept = default;
};

struct InputKey {
  SourceKey source{};
  std::vector<std::size_t> target_path{};
  bool rank_dependency{true};

  bool operator==(const InputKey &) const noexcept = default;
};

struct InstanceKey {
  std::type_index def;
  WiringNodeSchema schema;
  std::vector<InputKey> inputs;
  Value scalars;

  bool operator==(const InstanceKey &other) const noexcept {
    if (def != other.def || !(schema == other.schema) ||
        inputs != other.inputs) {
      return false;
    }
    if (scalars.has_value() != other.scalars.has_value()) {
      return false;
    }
    if (!scalars.has_value()) {
      return true;
    }
    return scalars.equals(other.scalars);
  }
};

struct InstanceKeyHash {
  std::size_t operator()(const InstanceKey &key) const noexcept {
    std::size_t h = std::hash<std::type_index>{}(key.def);
    combine(h, std::hash<const void *>{}(key.schema.input));
    combine(h, std::hash<const void *>{}(key.schema.output));
    combine(h, std::hash<const void *>{}(key.schema.error_output));
    combine(h, std::hash<const void *>{}(key.schema.recordable_state));
    combine(h, std::hash<const void *>{}(key.schema.scalar));
    combine(h, std::hash<const void *>{}(key.schema.state));
    for (const auto &input : key.inputs) {
      hash_source(input.source, h);
      for (std::size_t p : input.target_path) {
        combine(h, std::hash<std::size_t>{}(p));
      }
      combine(h, std::hash<bool>{}(input.rank_dependency));
      combine(h, 0xA7A7A7A7ULL); // target-path separator
    }
    combine(h, key.scalars.has_value() ? key.scalars.hash() : std::size_t{0});
    return h;
  }

private:
  static void combine(std::size_t &h, std::size_t v) noexcept {
    h ^= v + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
  }

  static void hash_source(const SourceKey &source, std::size_t &h) noexcept {
    combine(h, std::hash<int>{}(static_cast<int>(source.kind)));
    combine(h, std::hash<int>{}(static_cast<int>(source.peered_output_kind)));
    combine(h, std::hash<const void *>{}(source.peered_node));
    combine(h, std::hash<const void *>{}(source.schema));
    for (std::size_t p : source.peered_path) {
      combine(h, std::hash<std::size_t>{}(p));
    }
    combine(h, 0xF1F1F1F1ULL); // path separator
    for (const SourceKey &child : source.structural_children) {
      hash_source(child, h);
    }
    combine(h, 0xC8C8C8C8ULL); // children separator
    combine(h, std::hash<std::size_t>{}(source.boundary_arg));
    for (std::size_t p : source.boundary_path) {
      combine(h, std::hash<std::size_t>{}(p));
    }
    combine(h, std::hash<bool>{}(source.captured_boundary));
    combine(h, 0xB0B0B0B0ULL); // boundary separator
    combine(h, std::hash<const void *>{}(source.delayed_state));
    for (std::size_t p : source.delayed_path) {
      combine(h, std::hash<std::size_t>{}(p));
    }
    combine(h, 0xD3D3D3D3ULL); // delayed-path separator
  }
};

[[nodiscard]] SourceKey source_key_for(const WiringPortRef &source) {
  SourceKey key{.kind = source.source_kind(), .schema = source.schema};
  if (source.is_peered_source()) {
    key.peered_node = source.peered_node();
    key.peered_path = source.peered_path();
    key.peered_output_kind = source.peered_output_kind();
  } else if (source.is_structural_source()) {
    const auto &children = source.structural_children();
    key.structural_children.reserve(children.size());
    for (const WiringPortRef &child : children) {
      key.structural_children.push_back(source_key_for(child));
    }
  } else if (source.is_boundary_source()) {
    key.boundary_arg = source.is_captured_boundary_source()
                           ? source.boundary_capture_index()
                           : source.boundary_arg_index();
    key.boundary_path = source.boundary_path();
    key.captured_boundary = source.is_captured_boundary_source();
  } else if (source.is_delayed_source()) {
    key.delayed_state = source.delayed_state().get();
    key.delayed_path = source.delayed_path();
  }
  return key;
}

[[nodiscard]] InstanceKey make_key(std::type_index def, WiringNodeSchema schema,
                                   std::span<const WiringInputRef> inputs,
                                   const Value &scalars) {
  InstanceKey key{def, schema, {}, scalars};
  key.inputs.reserve(inputs.size());
  for (std::size_t index = 0; index < inputs.size(); ++index) {
    const WiringInputRef &input = inputs[index];
    key.inputs.push_back(InputKey{
        .source = source_key_for(input.source),
        .target_path = input.target_path.empty()
                           ? std::vector<std::size_t>{index}
                           : input.target_path,
        .rank_dependency = input.rank_dependency,
    });
  }
  return key;
}

[[nodiscard]] const TSValueTypeMetaData *
output_schema_of(const WiringInstance &instance) {
  const auto *meta = instance.builder.type().schema();
  return meta != nullptr ? meta->output_schema : nullptr;
}

struct StructuralRefNodeTag {};

void evaluate_structural_ref_node(const NodeView &view,
                                  DateTime evaluation_time) {
  auto root = view.input(evaluation_time);
  auto bundle = root.as_bundle();
  auto ts = bundle.field("ts");

  Value reference{ts.reference()};
  auto output = view.output(evaluation_time);
  if (output.valid() &&
      output.value().checked_as<TimeSeriesReference>() ==
          reference.view().checked_as<TimeSeriesReference>()) {
    return;
  }
  auto mutation = output.begin_mutation(evaluation_time);
  // move_value_from returns NEWLY-MODIFIED, not success: publishing
  // a reference that lands as no new modification (e.g. an emptied
  // reference over an invalid output while a switch/map cascade
  // tears a branch down) is a legitimate no-op — UNBIND IS SILENT,
  // consumers observe invalidity by reading, not by tick.
  static_cast<void>(mutation.move_value_from(std::move(reference)));
}

/** The ts-input schema preserving the SOURCE children's REF-ness.
    A REF-typed child keeps its REF schema so an emptied reference is
    an ordinary VALUE tick on this node's input (hgraph parity:
    UNBIND IS SILENT, so notification must not depend on the deref'd
    write-through - linking_strategies.rst). Falls back to the deref'd
    target schema when the source shape cannot be preserved. */
[[nodiscard]] const TSValueTypeMetaData *
structural_ref_input_ts_schema(const TSValueTypeMetaData *target_schema,
                               const WiringPortRef &source) {
  if (!source.is_structural_source()) {
    return target_schema;
  }
  const auto &children = source.structural_children();
  if (children.empty()) {
    return target_schema;
  }
  const TSValueTypeMetaData *element = children[0].schema;
  for (const WiringPortRef &child : children) {
    if (child.schema == nullptr) {
      return target_schema;
    }
    if (child.schema != element) {
      element = nullptr;
    }
  }
  auto &registry = TypeRegistry::instance();
  if (target_schema->kind == TSTypeKind::TSL && element != nullptr) {
    return registry.tsl(element, children.size());
  }
  if (target_schema->kind == TSTypeKind::TSB &&
      children.size() == target_schema->field_count()) {
    std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
    fields.reserve(children.size());
    for (std::size_t index = 0; index < children.size(); ++index) {
      fields.emplace_back(target_schema->fields()[index].name,
                          children[index].schema);
    }
    return registry.un_named_tsb(fields);
  }
  return target_schema;
}

[[nodiscard]] NodeBuilder
structural_ref_node_builder(const TSValueTypeMetaData *target_schema,
                            const WiringPortRef &source) {
  if (target_schema == nullptr) {
    throw std::logic_error("structural REF node requires a target schema");
  }

  auto &registry = TypeRegistry::instance();
  const auto *ts_schema = structural_ref_input_ts_schema(target_schema, source);
  const auto *input_schema = registry.un_named_tsb({{"ts", ts_schema}});
  const auto *output_schema = registry.ref(target_schema);

  NodeTypeMetaData schema;
  schema.display_name = "structural_ref";
  schema.input_schema = input_schema;
  schema.output_schema = output_schema;
  schema.node_kind = NodeKind::Compute;
  // NO validity gating (an explicitly EMPTY required set - nullopt
  // would mean "all fields"): the node must re-evaluate when the
  // source goes INVALID too, so consumers observe the emptied
  // reference (race re-races on winner invalidation).
  schema.valid_inputs.emplace();

  NodeCallbacks callbacks;
  callbacks.evaluate = &evaluate_structural_ref_node;

  std::array<WiringPortRef, 1> inputs{source};
  NodeBuilder builder =
      NodeBuilder::native(std::move(schema), std::move(callbacks));
  builder.input_endpoint(graph_wiring_detail::input_endpoint_for_sources(
      input_schema,
      std::span<const WiringPortRef>{inputs.data(), inputs.size()}));
  builder.label("structural_ref");
  return builder;
}

[[nodiscard]] WiringPortRef make_delayed_port_tree(
    Wiring *wiring, const TSValueTypeMetaData *schema,
    std::vector<std::shared_ptr<WiringDelayedBindingState>> &leaves) {
  if (schema->kind == TSTypeKind::TSL && schema->fixed_size() > 0) {
    std::vector<WiringPortRef> children;
    children.reserve(schema->fixed_size());
    for (std::size_t index = 0; index < schema->fixed_size(); ++index) {
      children.push_back(
          make_delayed_port_tree(wiring, schema->element_ts(), leaves));
    }
    return WiringPortRef::structural_source(schema, std::move(children));
  }
  if (schema->kind == TSTypeKind::TSB) {
    std::vector<WiringPortRef> children;
    children.reserve(schema->field_count());
    for (std::size_t index = 0; index < schema->field_count(); ++index) {
      children.push_back(
          make_delayed_port_tree(wiring, schema->fields()[index].type, leaves));
    }
    return WiringPortRef::structural_source(schema, std::move(children));
  }

  auto state =
      std::make_shared<WiringDelayedBindingState>(WiringDelayedBindingState{
          .wiring = wiring,
          .schema = schema,
      });
  leaves.push_back(state);
  return WiringPortRef::delayed_source(std::move(state), {}, schema);
}

[[nodiscard]] WiringPortRef
project_source_component(const WiringPortRef &source, std::size_t index,
                         const TSValueTypeMetaData *schema) {
  if (source.is_structural_source()) {
    if (index >= source.structural_children().size()) {
      throw std::invalid_argument(
          "delayed_binding structural source has the wrong number of children");
    }
    return source.structural_children()[index];
  }
  if (source.is_peered_source()) {
    std::vector<std::size_t> path = source.peered_path();
    path.push_back(index);
    return WiringPortRef::peered_source(source.peered_node(), std::move(path),
                                        schema, source.peered_output_kind());
  }
  if (source.is_boundary_source()) {
    std::vector<std::size_t> path = source.boundary_path();
    path.push_back(index);
    return source.projected_boundary_source(std::move(path), schema);
  }
  if (source.is_delayed_source()) {
    std::vector<std::size_t> path = source.delayed_path();
    path.push_back(index);
    return source.projected_delayed_source(std::move(path), schema);
  }
  if (source.is_null_source()) {
    return WiringPortRef::null_source(schema);
  }
  throw std::invalid_argument("delayed_binding cannot bind an unbound source");
}

struct DelayedLeafBinding {
  std::shared_ptr<WiringDelayedBindingState> state{};
  WiringPortRef source{};
};

void collect_delayed_leaf_bindings(const WiringPortRef &placeholder,
                                   const WiringPortRef &source,
                                   std::vector<DelayedLeafBinding> &bindings) {
  if (placeholder.is_structural_source()) {
    const auto &children = placeholder.structural_children();
    if (source.is_structural_source() &&
        source.structural_children().size() != children.size()) {
      throw std::invalid_argument(
          "delayed_binding structural source has the wrong number of children");
    }
    for (std::size_t index = 0; index < children.size(); ++index) {
      collect_delayed_leaf_bindings(
          children[index],
          project_source_component(source, index, children[index].schema),
          bindings);
    }
    return;
  }

  if (!placeholder.is_delayed_source()) {
    throw std::logic_error(
        "delayed_binding placeholder tree contains a non-delayed leaf");
  }
  if (source.is_structural_source()) {
    throw std::invalid_argument(
        "a dynamic structural source must be materialized before "
        "delayed_binding can bind it");
  }
  if (source.is_delayed_source() &&
      source.delayed_state() == placeholder.delayed_state()) {
    throw std::runtime_error("delayed_binding cannot bind directly to itself");
  }
  bindings.push_back(DelayedLeafBinding{placeholder.delayed_state(), source});
}

[[nodiscard]] WiringPortRef
project_delayed_source(WiringPortRef source, const WiringPortRef &delayed) {
  const auto &path = delayed.delayed_path();
  if (source.is_structural_source()) {
    for (const std::size_t component : path) {
      if (!source.is_structural_source() ||
          component >= source.structural_children().size()) {
        throw std::logic_error("delayed_binding projection does not match the "
                               "bound structural source");
      }
      source = source.structural_children()[component];
    }
  } else if (source.is_peered_source()) {
    std::vector<std::size_t> source_path = source.peered_path();
    source_path.insert(source_path.end(), path.begin(), path.end());
    source = WiringPortRef::peered_source(
        source.peered_node(), std::move(source_path), delayed.schema,
        source.peered_output_kind());
  } else if (source.is_boundary_source()) {
    std::vector<std::size_t> source_path = source.boundary_path();
    source_path.insert(source_path.end(), path.begin(), path.end());
    source = source.projected_boundary_source(std::move(source_path),
                                              delayed.schema);
  } else if (source.is_null_source()) {
    source = WiringPortRef::null_source(delayed.schema);
  } else {
    throw std::logic_error(
        "delayed_binding resolved to an unsupported wiring source");
  }

  source.schema = delayed.schema;
  source.arg_tag = delayed.arg_tag;
  return source;
}

[[nodiscard]] WiringPortRef resolve_delayed_source_impl(
    const WiringPortRef &source,
    std::vector<const WiringDelayedBindingState *> &resolving) {
  if (!source.is_delayed_source()) {
    return source;
  }

  const auto &state = source.delayed_state();
  if (state == nullptr || !state->source.has_value()) {
    throw std::logic_error(
        "Wiring::finish encountered an unbound delayed_binding");
  }
  if (std::ranges::find(resolving, state.get()) != resolving.end()) {
    throw std::runtime_error(
        "Wiring::finish detected a cycle between delayed_binding placeholders");
  }

  resolving.push_back(state.get());
  auto pop = make_scope_exit([&] { resolving.pop_back(); });
  WiringPortRef resolved =
      resolve_delayed_source_impl(*state->source, resolving);
  return project_delayed_source(std::move(resolved), source);
}

[[nodiscard]] WiringPortRef
resolve_delayed_source(const WiringPortRef &source) {
  std::vector<const WiringDelayedBindingState *> resolving;
  return resolve_delayed_source_impl(source, resolving);
}

void collect_producers(
    const WiringPortRef &source, std::vector<const WiringInstance *> &producers,
    const std::unordered_set<const WiringInstance *> &owned) {
  if (source.is_delayed_source()) {
    collect_producers(resolve_delayed_source(source), producers, owned);
    return;
  }
  if (source.is_peered_source()) {
    // Foreign producers (outer captures) rank in the OUTER graph;
    // inside the child they behave as boundary inputs.
    if (owned.contains(source.peered_node())) {
      producers.push_back(source.peered_node());
    }
    return;
  }
  if (source.is_null_source() || source.is_boundary_source()) {
    return;
  }
  if (source.is_unbound_source()) {
    throw std::logic_error(
        "Wiring::finish encountered an unbound wiring source");
  }
  for (const WiringPortRef &child : source.structural_children()) {
    collect_producers(child, producers, owned);
  }
}

// One edge emitter for both finish flavours. A boundary source is only
// legal when compiling a sub-graph (``boundary_bindings`` supplied): it
// becomes a nested-graph input binding (outer input root path =
// {arg} + boundary path) instead of an edge.
/**
 * Outer-port CAPTURES (nested_graphs.rst, "Outer-port capture"): a
 * peered source whose producer is not part of this wiring converts
 * to a fresh boundary argument appended after the declared inputs;
 * the outer refs are reported for the caller to bind.
 */
struct OuterCaptureCollector {
  std::size_t base_index{0};
  std::vector<WiringPortRef> captured{};
  bool frozen{false};

  [[nodiscard]] std::size_t index_for(const WiringPortRef &outer) {
    for (std::size_t index = 0; index < captured.size(); ++index) {
      if (captured[index].same_source_as(outer)) {
        return index;
      }
    }
    if (frozen) {
      throw std::logic_error("sub-graph discovered an outer capture after "
                             "boundary ordinals were finalized");
    }
    captured.push_back(outer);
    return captured.size() - 1;
  }

  [[nodiscard]] std::size_t
  boundary_ordinal(const WiringPortRef &source) const {
    if (!source.is_captured_boundary_source()) {
      return source.boundary_arg_index();
    }
    const std::size_t capture_index = source.boundary_capture_index();
    if (capture_index >= captured.size()) {
      throw std::logic_error(
          "captured sub-graph boundary index is out of range");
    }
    return base_index + capture_index;
  }
};

[[nodiscard]] std::optional<std::size_t> structural_boundary_ordinal(
    const WiringPortRef &source, const OuterCaptureCollector &captures) {
  std::optional<std::size_t> ordinal;
  std::vector<std::size_t> expected_path;

  const auto matches_boundary = [&](const auto &self,
                                    const WiringPortRef &part) -> bool {
    if (part.is_null_source()) {
      return true;
    }
    if (part.is_boundary_source()) {
      if (part.boundary_path() != expected_path) {
        return false;
      }
      const std::size_t part_ordinal = captures.boundary_ordinal(part);
      if (ordinal.has_value() && *ordinal != part_ordinal) {
        return false;
      }
      ordinal = part_ordinal;
      return true;
    }
    if (!part.is_structural_source()) {
      return false;
    }

    const auto &children = part.structural_children();
    for (std::size_t index = 0; index < children.size(); ++index) {
      expected_path.push_back(index);
      const bool matches = self(self, children[index]);
      expected_path.pop_back();
      if (!matches) {
        return false;
      }
    }
    return true;
  };

  if (!source.is_structural_source() ||
      !matches_boundary(matches_boundary, source)) {
    return std::nullopt;
  }
  return ordinal;
}

void collect_outer_captures(
    const WiringPortRef &source,
    const std::unordered_set<const WiringInstance *> &owned,
    const std::unordered_set<const WiringInstance *> &external,
    OuterCaptureCollector &captures) {
  if (source.is_delayed_source()) {
    collect_outer_captures(resolve_delayed_source(source), owned, external,
                           captures);
    return;
  }
  if (source.is_peered_source()) {
    const auto *producer = source.peered_node();
    if (!owned.contains(producer) && !external.contains(producer)) {
      static_cast<void>(captures.index_for(source));
    }
    return;
  }
  if (!source.is_structural_source()) {
    return;
  }
  for (const WiringPortRef &child : source.structural_children()) {
    collect_outer_captures(child, owned, external, captures);
  }
}

void emit_edges(
    const WiringPortRef &source, const std::vector<std::size_t> &target_path,
    const std::unordered_map<const WiringInstance *, std::size_t> &index_of,
    const std::unordered_map<const WiringInstance *, std::size_t>
        *external_sources,
    GraphBuilder &graph_builder, std::size_t target_node,
    std::vector<NestedGraphInputBinding> *boundary_bindings,
    OuterCaptureCollector *captures) {
  if (source.is_delayed_source()) {
    emit_edges(resolve_delayed_source(source), target_path, index_of,
               external_sources, graph_builder, target_node, boundary_bindings,
               captures);
    return;
  }
  if (source.is_peered_source()) {
    if (external_sources != nullptr) {
      const auto external = external_sources->find(source.peered_node());
      if (external != external_sources->end()) {
        boundary_bindings->push_back(NestedGraphInputBinding{
            .source_path = {external->second},
            .target =
                NestedGraphEndpoint{.node = target_node, .path = target_path},
        });
        return;
      }
    }
    const auto it = index_of.find(source.peered_node());
    if (it == index_of.end()) {
      // A FOREIGN producer: an outer-wiring port referenced
      // inside a sub-graph compose (closure capture).
      if (boundary_bindings == nullptr || captures == nullptr) {
        throw std::logic_error(
            "Wiring::finish encountered a port from a different wiring; "
            "outer-port capture "
            "applies only inside a sub-graph compile (map_/switch_ child)");
      }
      boundary_bindings->push_back(NestedGraphInputBinding{
          .source_path = {captures->base_index + captures->index_for(source)},
          .target =
              NestedGraphEndpoint{.node = target_node, .path = target_path},
      });
      return;
    }
    graph_builder.add_edge(GraphEdge{
        .source_node =
            make_graph_edge_source(it->second, source.peered_output_kind()),
        .source_path = source.peered_path(),
        .target_node = target_node,
        .target_path = target_path,
    });
    return;
  }
  if (source.is_null_source()) {
    return;
  }
  if (source.is_boundary_source()) {
    if (boundary_bindings == nullptr ||
        (source.is_captured_boundary_source() && captures == nullptr)) {
      throw std::logic_error("Wiring::finish encountered a sub-graph boundary "
                             "source; compile with finish_subgraph "
                             "instead");
    }
    std::vector<std::size_t> source_path;
    source_path.reserve(1 + source.boundary_path().size());
    source_path.push_back(captures != nullptr
                              ? captures->boundary_ordinal(source)
                              : source.boundary_arg_index());
    source_path.insert(source_path.end(), source.boundary_path().begin(),
                       source.boundary_path().end());
    boundary_bindings->push_back(NestedGraphInputBinding{
        .source_path = std::move(source_path),
        .target = NestedGraphEndpoint{.node = target_node, .path = target_path},
    });
    return;
  }
  if (source.is_unbound_source()) {
    throw std::logic_error(
        "Wiring::finish encountered an unbound wiring source");
  }
  const auto &children = source.structural_children();
  for (std::size_t index = 0; index < children.size(); ++index) {
    std::vector<std::size_t> child_target_path = target_path;
    child_target_path.push_back(index);
    emit_edges(children[index], child_target_path, index_of, external_sources,
               graph_builder, target_node, boundary_bindings, captures);
  }
}

struct RankedGraphBuild {
  GraphBuilder graph_builder{};
  std::unordered_map<const WiringInstance *, std::size_t> index_of{};
};

struct OutputReaders {
  bool python{false};
  bool native{false};
};

void collect_output_reader(
    const WiringPortRef &source, bool python_reader,
    const std::unordered_set<const WiringInstance *> &owned,
    std::unordered_map<const WiringInstance *, OutputReaders> &readers) {
  if (source.is_peered_source()) {
    const WiringInstance *producer = source.peered_node();
    if (!owned.contains(producer) ||
        source.peered_output_kind() != GraphEdgeSourceKind::Output) {
      return;
    }
    auto &entry = readers[producer];
    // Child projections and other structural access can require native
    // value views even when the terminal callable is Python.
    const bool plain_root = source.peered_path().empty();
    if (python_reader && plain_root) {
      entry.python = true;
    } else {
      entry.native = true;
    }
    return;
  }
  if (!source.is_structural_source()) {
    return;
  }
  for (const WiringPortRef &child : source.structural_children()) {
    collect_output_reader(child, python_reader, owned, readers);
  }
}

void collect_escaped_output_producers(
    const WiringPortRef &source,
    const std::unordered_map<const WiringInstance *, std::size_t>
        &external_sources,
    std::unordered_set<const WiringInstance *> &escaped_outputs) {
  if (source.is_peered_source()) {
    if (source.peered_output_kind() == GraphEdgeSourceKind::Output &&
        !external_sources.contains(source.peered_node())) {
      escaped_outputs.insert(source.peered_node());
    }
    return;
  }
  if (!source.is_structural_source()) {
    return;
  }
  for (const WiringPortRef &child : source.structural_children()) {
    collect_escaped_output_producers(child, external_sources,
                                     escaped_outputs);
  }
}

void select_output_value_storage(
    std::deque<WiringInstance> &instances,
    const std::unordered_set<const WiringInstance *> &owned,
    const std::unordered_set<const WiringInstance *> *escaped_outputs) {
  std::unordered_map<const WiringInstance *, OutputReaders> readers;
  readers.reserve(owned.size());
  for (WiringInstance &instance : instances) {
    instance.builder.output_value_storage(ValueStorageVariant::Native);
    if (!owned.contains(&instance)) {
      continue;
    }
    const auto *consumer_schema = instance.builder.type().schema();
    const bool python_reader =
        consumer_schema != nullptr && consumer_schema->uses_python_values;
    for (const WiringInputRef &input : instance.inputs) {
      collect_output_reader(input.source, python_reader, owned, readers);
    }
  }
  if (escaped_outputs != nullptr) {
    for (const WiringInstance *producer : *escaped_outputs) {
      if (owned.contains(producer)) {
        readers[producer].native = true;
      }
    }
  }

  for (WiringInstance &instance : instances) {
    if (!owned.contains(&instance)) {
      continue;
    }
    const auto *producer_schema = instance.builder.type().schema();
    const auto found = readers.find(&instance);
    if (producer_schema == nullptr || !producer_schema->has_output() ||
        producer_schema->output_schema->kind != TSTypeKind::TS ||
        !instance.builder.output_endpoint().empty() ||
        found == readers.end() || !found->second.python) {
      continue;
    }
    const bool python_only =
        producer_schema->uses_python_values && !found->second.native;
    instance.builder.output_value_storage(
        python_only ? ValueStorageVariant::PythonOnly
                    : ValueStorageVariant::NativeWithPythonCache);
  }
}

// The one rank-and-build pass behind both ``finish`` flavours: Kahn
// topological sort (an input edge is producer -> consumer; insertion
// order breaks ties), then nodes + edges into a GraphBuilder.
[[nodiscard]] RankedGraphBuild
build_ranked_graph(std::deque<WiringInstance> &instances,
                   std::vector<NestedGraphInputBinding> *boundary_bindings,
                   OuterCaptureCollector *captures = nullptr,
                   const std::unordered_map<const WiringInstance *, std::size_t>
                       *external_sources = nullptr,
                   const std::unordered_set<const WiringInstance *>
                       *escaped_outputs = nullptr) {
  std::vector<const WiringInstance *> all;
  all.reserve(instances.size());
  for (const auto &instance : instances) {
    if (external_sources == nullptr || !external_sources->contains(&instance)) {
      all.push_back(&instance);
    }
  }
  std::unordered_set<const WiringInstance *> owned{all.begin(), all.end()};
  select_output_value_storage(instances, owned, escaped_outputs);

  std::unordered_map<const WiringInstance *, std::size_t> indegree;
  std::unordered_map<const WiringInstance *,
                     std::vector<const WiringInstance *>>
      consumers;
  for (const WiringInstance *instance : all) {
    indegree.try_emplace(instance, 0);
  }
  for (const WiringInstance *instance : all) {
    for (const auto &input : instance->inputs) {
      if (!input.rank_dependency) {
        continue;
      }
      std::vector<const WiringInstance *> producers;
      collect_producers(input.source, producers, owned);
      for (const WiringInstance *producer : producers) {
        ++indegree[instance];
        consumers[producer].push_back(instance);
      }
    }
    for (const WiringInstance *producer : instance->rank_dependencies) {
      if (producer == nullptr || !owned.contains(producer)) {
        continue;
      }
      ++indegree[instance];
      consumers[producer].push_back(instance);
    }
  }

  const auto is_push_source = [](const WiringInstance *instance) {
    const auto *schema = instance->builder.type().schema();
    return schema != nullptr && schema->node_kind == NodeKind::PushSource;
  };

  std::deque<const WiringInstance *> ready_push_sources;
  std::deque<const WiringInstance *> ready;
  for (const WiringInstance *instance :
       all) // insertion order → stable tie-break
  {
    if (is_push_source(instance) && indegree[instance] != 0) {
      throw std::invalid_argument(
          "Push source nodes cannot have rank dependencies");
    }
    if (indegree[instance] == 0) {
      (is_push_source(instance) ? ready_push_sources : ready)
          .push_back(instance);
    }
  }

  std::vector<const WiringInstance *> ranked;
  ranked.reserve(all.size());
  while (!ready_push_sources.empty() || !ready.empty()) {
    auto &next = !ready_push_sources.empty() ? ready_push_sources : ready;
    const WiringInstance *instance = next.front();
    next.pop_front();
    ranked.push_back(instance);
    for (const WiringInstance *consumer : consumers[instance]) {
      if (--indegree[consumer] == 0) {
        (is_push_source(consumer) ? ready_push_sources : ready)
            .push_back(consumer);
      }
    }
  }

  if (ranked.size() != all.size()) {
    auto name_of = [](const WiringInstance *instance) {
      if (instance == nullptr) {
        return std::string{"<null>"};
      }
      const auto &builder = instance->builder;
      std::string label = std::string{builder.label()};
      if (!label.empty()) {
        return label;
      }
      const auto *meta = builder.type().schema();
      return meta != nullptr && meta->display_name != nullptr
                 ? std::string{meta->display_name}
                 : std::string{"<unnamed>"};
    };

    std::string message = "Wiring::finish detected a cycle in the wiring graph";
    std::size_t shown = 0;
    for (const auto &[instance, degree] : indegree) {
      if (degree == 0) {
        continue;
      }
      message += shown == 0 ? ": " : ", ";
      message += name_of(instance);
      if (++shown == 8) {
        break;
      }
    }
    shown = 0;
    for (const auto &[instance, degree] : indegree) {
      if (degree == 0 || shown == 4) {
        continue;
      }
      message += "; ";
      message += name_of(instance);
      message += " waits on ";
      bool first = true;
      for (const auto &input : instance->inputs) {
        if (!input.rank_dependency) {
          continue;
        }
        std::vector<const WiringInstance *> producers;
        collect_producers(input.source, producers, owned);
        for (const WiringInstance *producer : producers) {
          if (indegree[producer] == 0) {
            continue;
          }
          if (!first) {
            message += ", ";
          }
          message += name_of(producer);
          first = false;
        }
      }
      for (const WiringInstance *producer : instance->rank_dependencies) {
        if (producer == nullptr || indegree[producer] == 0) {
          continue;
        }
        if (!first) {
          message += ", ";
        }
        message += name_of(producer);
        first = false;
      }
      if (first) {
        message += "<none in cycle>";
      }
      ++shown;
    }
    throw std::runtime_error(message);
  }

  RankedGraphBuild build;
  build.index_of.reserve(ranked.size());
  for (std::size_t i = 0; i < ranked.size(); ++i) {
    build.index_of.emplace(ranked[i], i);
  }

  for (const WiringInstance *instance : ranked) {
    build.graph_builder.add_node(instance->builder);
  }
  for (std::size_t i = 0; i < ranked.size(); ++i) {
    const WiringInstance *instance = ranked[i];
    for (std::size_t input_index = 0; input_index < instance->inputs.size();
         ++input_index) {
      const WiringInputRef &input = instance->inputs[input_index];
      std::vector<std::size_t> target_path =
          input.target_path.empty() ? std::vector<std::size_t>{input_index}
                                    : input.target_path;
      emit_edges(input.source, target_path, build.index_of, external_sources,
                 build.graph_builder, i, boundary_bindings, captures);
    }
  }
  return build;
}
} // namespace

WiringPortRef graph_wiring_detail::adapt_source_for_input(
    Wiring &w, const TSValueTypeMetaData *input_schema, WiringPortRef source) {
  if (input_schema == nullptr) {
    return source;
  }

  auto &registry = TypeRegistry::instance();
  const auto *input = registry.dereference(input_schema);
  const auto *output = registry.dereference(source.schema);
  const bool bundle_upcast =
      input_schema->kind == TSTypeKind::TS && source.schema != nullptr &&
      source.schema->kind == TSTypeKind::TS && input != nullptr &&
      output != nullptr && input->kind == TSTypeKind::TS &&
      output->kind == TSTypeKind::TS && input->value_schema != nullptr &&
      output->value_schema != nullptr &&
      input->value_schema->is_named_bundle() &&
      output->value_schema->is_named_bundle() &&
      TypeRegistry::instance().bundle_is_a(output->value_schema,
                                           input->value_schema);
  if (bundle_upcast && !time_series_schema_equivalent(input, output)) {
    WiringArg arg;
    arg.kind = WiringArg::Kind::TimeSeries;
    arg.port = std::move(source);
    std::array<WiringArg, 1> args{std::move(arg)};
    return wire_operator(w, "convert", std::span<const WiringArg>{args}, true,
                         input_schema)
        .output.erased();
  }

  if (!source.is_structural_source()) {
    return source;
  }

  if (input_schema->kind == TSTypeKind::REF) {
    const TSValueTypeMetaData *target_schema = input_schema->referenced_ts();
    if (!input_accepts_output_schema(input_schema, source.schema)) {
      throw std::logic_error(
          "wire<T>: structural source schema does not match REF input target");
    }

    std::array<WiringPortRef, 1> inputs{std::move(source)};
    NodeBuilder builder = structural_ref_node_builder(target_schema, inputs[0]);
    return w.add_node(
        std::type_index(typeid(StructuralRefNodeTag)), std::move(builder),
        std::span<const WiringPortRef>{inputs.data(), inputs.size()}, Value{});
  }

  const auto &source_children = source.structural_children();
  std::vector<WiringPortRef> adapted;
  switch (input_schema->kind) {
  case TSTypeKind::TSL:
    if (input_schema->fixed_size() == 0 ||
        source_children.size() != input_schema->fixed_size()) {
      return source;
    }
    adapted.reserve(source_children.size());
    for (const WiringPortRef &child : source_children) {
      adapted.push_back(
          adapt_source_for_input(w, input_schema->element_ts(), child));
    }
    return WiringPortRef::structural_source(input_schema, std::move(adapted));

  case TSTypeKind::TSB:
    if (source_children.size() != input_schema->field_count()) {
      return source;
    }
    adapted.reserve(source_children.size());
    for (std::size_t index = 0; index < source_children.size(); ++index) {
      adapted.push_back(adapt_source_for_input(
          w, input_schema->fields()[index].type, source_children[index]));
    }
    return WiringPortRef::structural_source(input_schema, std::move(adapted));

  default:
    return source;
  }
}

WiringPortRef
graph_wiring_detail::resolve_context_source(Wiring &w, std::string_view name) {
  const auto *entry = OperatorRegistry::instance().resolve_context_scope(name);
  if (entry == nullptr) {
    throw std::logic_error("no enclosing context::scope publishes context '" +
                           std::string{name} + "' (required by a Context<\"" +
                           std::string{name} + "\", ...> input)");
  }
  if (entry->wiring != static_cast<const void *>(&w)) {
    return w.capture_outer_source(entry->port);
  }
  return entry->port;
}

bool graph_wiring_detail::has_context_source(const Wiring &w,
                                             std::string_view name) noexcept {
  (void)w;
  const auto *entry = OperatorRegistry::instance().resolve_context_scope(name);
  return entry != nullptr;
}

void graph_wiring_detail::push_context_source(const Wiring &w,
                                              std::string_view name,
                                              WiringPortRef port) {
  OperatorRegistry::instance().push_context_scope(
      name, std::move(port), static_cast<const void *>(&w));
}

void graph_wiring_detail::pop_context_source() noexcept {
  OperatorRegistry::instance().pop_context_scope();
}

struct WiringObserverRegistry {
  std::vector<WiringObserver *> observers{};
};

struct Wiring::Impl {
  std::unordered_set<std::string> component_ids; // claimed recordable ids

  explicit Impl(WiringKind wiring_kind,
                std::shared_ptr<WiringObserverRegistry> observer_registry = {},
                std::vector<std::string> observer_path = {})
      : observers(std::move(observer_registry)),
        wiring_path(std::move(observer_path)), kind(wiring_kind) {
    if (kind == WiringKind::TopLevel) {
      // The LIVE selected state, not a copy (ruling 2026-07-27): setters
      // invoked during wiring (set_record_replay_model, set_as_of, ...)
      // write into the selected GlobalState, and wiring-time reads must see
      // the same store — a construction-time copy silently ignored them.
      // NOTHING is retained: reads resolve through the active context per
      // call, the seed is FIXED by copy at wiring end (graph build), and
      // the selected state must span the wiring process (finish fails
      // loudly if it exited early). Only whether this wiring was live-
      // seeded is remembered.
      live_seeded = GlobalContext::active_state() != nullptr;
    }
  }

  struct ServiceImplementationScopeState {
    std::string description{};
    std::unordered_set<std::string> required_endpoints{};
    std::unordered_map<std::string, ResolutionMap> endpoint_resolutions{};
    std::unordered_set<std::string> used_endpoints{};
    bool require_all{true};
  };

  struct ServiceClientRank {
    std::string path{};
    std::string kind{};
    const WiringInstance *node{nullptr};
    bool receive{true};
  };

  struct ServiceClientIdentity {
    std::string kind{};
    std::string interface_name{};
    std::string specialization{};
  };

  struct ServiceImplementationCandidate {
    std::vector<std::string> paths{};
    std::string description{};
    std::function<void(Wiring &)> materialize{};
    std::function<void(Wiring &, std::string_view)> materialize_default{};
    std::vector<std::pair<std::string, std::string>> path_selectors{};
    bool catch_all{false};
    bool default_fallback{false};
    bool materialized{false};
    std::unordered_set<std::string> materialized_paths{};
  };

  /** A same-cycle boundary pairing: the capture must rank before the source. */
  struct SameCyclePair {
    const WiringInstance *capture{nullptr};
    const WiringInstance *source{nullptr};
  };

  std::deque<WiringInstance> instances{};
  std::unordered_map<InstanceKey, WiringInstance *, InstanceKeyHash> interned{};
  std::unordered_map<std::string, std::string> built_service_paths{};
  std::unordered_map<std::string, ServiceClientIdentity> client_service_paths{};
  std::unordered_map<std::string, const WiringInstance *>
      service_rank_anchors{};
  std::vector<ServiceClientRank> service_client_ranks{};
  std::vector<ServiceImplementationCandidate> service_candidates{};
  std::unordered_map<std::string, std::size_t> service_candidate_paths{};
  std::unordered_map<std::string, std::size_t> default_service_candidates{};
  std::vector<SameCyclePair> same_cycle_pairs{};
  std::vector<WiringPortRef> captured_inputs{};
  GlobalState traits{}; // value-layer Map<string, Any>
  std::vector<ServiceImplementationScopeState> implementation_scopes{};
  bool building_services{false};
  std::string service_materialization_path{};
  GlobalState global_state{};  // stateless-wiring fallback (no live context)
  bool live_seeded{false};
  std::shared_ptr<WiringObserverRegistry> observers{};
  std::vector<std::string> wiring_path{};
  WiringKind kind{WiringKind::TopLevel};
};

Wiring::Wiring(WiringKind kind) : impl_(std::make_unique<Impl>(kind)) {}
Wiring::Wiring(WiringKind kind,
               std::shared_ptr<WiringObserverRegistry> observers,
               std::vector<std::string> path)
    : impl_(std::make_unique<Impl>(kind, std::move(observers),
                                  std::move(path))) {}

WiringObservationScope::WiringObservationScope(Wiring &wiring,
                                               WiringScopeEvent event) {
  if (!wiring.has_wiring_observers()) {
    return;
  }
  wiring_ = &wiring;
  event_ = wiring.begin_observation(std::move(event));
  active_ = true;
}

WiringObservationScope::WiringObservationScope(
    WiringObservationScope &&other) noexcept
    : wiring_(std::exchange(other.wiring_, nullptr)),
      event_(std::move(other.event_)),
      active_(std::exchange(other.active_, false)) {}

WiringObservationScope &WiringObservationScope::operator=(
    WiringObservationScope &&other) noexcept {
  if (this == &other) {
    return *this;
  }
  cancel_noexcept();
  wiring_ = std::exchange(other.wiring_, nullptr);
  event_ = std::move(other.event_);
  active_ = std::exchange(other.active_, false);
  return *this;
}

WiringObservationScope::~WiringObservationScope() noexcept { cancel_noexcept(); }

void WiringObservationScope::complete() {
  if (!active_) {
    return;
  }
  active_ = false;
  wiring_->end_observation(event_, {});
}

void WiringObservationScope::fail(std::string_view error) {
  if (!active_) {
    return;
  }
  active_ = false;
  wiring_->end_observation(event_, error);
}

void WiringObservationScope::cancel_noexcept() noexcept {
  if (!active_) {
    return;
  }
  active_ = false;
  static_cast<void>(fallback_on_exception(false, [&] {
    wiring_->end_observation(event_, "wiring scope abandoned");
    return true;
  }));
}

void Wiring::add_wiring_observer(WiringObserver *observer) {
  if (observer == nullptr) {
    throw std::invalid_argument("wiring observer cannot be null");
  }
  if (impl_->observers == nullptr) {
    impl_->observers = std::make_shared<WiringObserverRegistry>();
  }
  auto &observers = impl_->observers->observers;
  if (std::find(observers.begin(), observers.end(), observer) == observers.end()) {
    observers.push_back(observer);
  }
}

bool Wiring::has_wiring_observers() const noexcept {
  return impl_->observers != nullptr && !impl_->observers->observers.empty();
}

Wiring Wiring::child_wiring() const {
  return Wiring{WiringKind::SubGraph, impl_->observers, impl_->wiring_path};
}

std::vector<std::string> Wiring::current_wiring_path() const {
  return impl_->wiring_path;
}

WiringScopeEvent Wiring::begin_observation(WiringScopeEvent event) {
  impl_->wiring_path.push_back(event.label.empty() ? "<unnamed>" : event.label);
  event.path = impl_->wiring_path;
  auto rollback_path = make_scope_exit([&] { impl_->wiring_path.pop_back(); });
  const auto observers = impl_->observers->observers;
  for (WiringObserver *observer : observers) {
    if (observer == nullptr) {
      continue;
    }
    switch (event.kind) {
    case WiringScopeKind::Graph:
      observer->on_enter_graph_wiring(event);
      break;
    case WiringScopeKind::NestedGraph:
      observer->on_enter_nested_graph_wiring(event);
      break;
    case WiringScopeKind::Node:
      observer->on_enter_node_wiring(event);
      break;
    }
  }
  rollback_path.release();
  return event;
}

void Wiring::end_observation(const WiringScopeEvent &event,
                             std::string_view error) {
  auto pop_path = make_scope_exit([&] {
    if (!impl_->wiring_path.empty()) {
      impl_->wiring_path.pop_back();
    }
  });
  const auto observers = impl_->observers->observers;
  for (WiringObserver *observer : observers) {
    if (observer == nullptr) {
      continue;
    }
    switch (event.kind) {
    case WiringScopeKind::Graph:
      observer->on_exit_graph_wiring(event, error);
      break;
    case WiringScopeKind::NestedGraph:
      observer->on_exit_nested_graph_wiring(event, error);
      break;
    case WiringScopeKind::Node:
      observer->on_exit_node_wiring(event, error);
      break;
    }
  }
}

void Wiring::notify_overload_resolution(
    const WiringResolutionEvent &event) const {
  const auto observers = impl_->observers->observers;
  for (WiringObserver *observer : observers) {
    if (observer != nullptr) {
      observer->on_overload_resolution(event);
    }
  }
}

void Wiring::claim_component_id(std::string_view fq_recordable_id) {
  if (!impl_->component_ids.emplace(std::string{fq_recordable_id}).second) {
    throw std::invalid_argument("component: duplicate recordable id '" +
                                std::string{fq_recordable_id} +
                                "' in one wiring");
  }
}

Wiring::~Wiring() = default;
Wiring::Wiring(Wiring &&) noexcept = default;
Wiring &Wiring::operator=(Wiring &&) noexcept = default;

ErasedDelayedBindingWiringPort::ErasedDelayedBindingWiringPort(
    Wiring &wiring, const TSValueTypeMetaData *schema) {
  if (schema == nullptr) {
    throw std::invalid_argument(
        "delayed_binding requires a concrete time-series schema");
  }
  control_ = std::make_shared<WiringDelayedBindingControl>();
  control_->wiring = &wiring;
  control_->schema = schema;
  control_->port = make_delayed_port_tree(&wiring, schema, control_->leaves);
}

WiringDelayedBindingControl &ErasedDelayedBindingWiringPort::control() const {
  if (control_ == nullptr) {
    throw std::logic_error("delayed_binding is not initialized");
  }
  return *control_;
}

WiringPortRef ErasedDelayedBindingWiringPort::port() const {
  return control().port;
}

ErasedDelayedBindingWiringPort &
ErasedDelayedBindingWiringPort::bind(WiringPortRef source) {
  auto &c = control();
  if (c.bound) {
    throw std::logic_error("delayed_binding is already bound");
  }
  if (!graph_wiring_detail::input_accepts_output_schema(c.schema,
                                                        source.schema)) {
    throw std::invalid_argument(
        "delayed_binding output type does not match the port being bound");
  }

  std::vector<DelayedLeafBinding> bindings;
  bindings.reserve(c.leaves.size());
  collect_delayed_leaf_bindings(c.port, source, bindings);
  for (const auto &binding : bindings) {
    if (binding.state->source.has_value()) {
      throw std::logic_error("delayed_binding leaf is already bound");
    }
  }

  std::size_t assigned = 0;
  bool committed = false;
  auto rollback = make_scope_exit([&] {
    if (!committed) {
      for (std::size_t index = 0; index < assigned; ++index) {
        bindings[index].state->source.reset();
      }
    }
  });
  for (auto &binding : bindings) {
    binding.source.arg_tag = WiringPortRef::ArgTag::None;
    binding.state->source = std::move(binding.source);
    ++assigned;
  }
  c.bound = true;
  committed = true;
  return *this;
}

bool ErasedDelayedBindingWiringPort::bound() const { return control().bound; }

const TSValueTypeMetaData *ErasedDelayedBindingWiringPort::schema() const {
  return control().schema;
}

Wiring *ErasedDelayedBindingWiringPort::wiring() const {
  return control().wiring;
}

WiringPortRef Wiring::capture_outer_source(WiringPortRef source) {
  if (impl_->kind != WiringKind::SubGraph) {
    throw std::logic_error(
        "only a compiled sub-graph may capture an enclosing wiring source");
  }
  if (source.schema == nullptr) {
    throw std::logic_error("cannot capture an unbound enclosing wiring source");
  }

  const auto captured_shape = [](const WiringPortRef &outer,
                                 std::size_t capture_index) {
    const auto build = [&](auto &&self, const WiringPortRef &part,
                           std::vector<std::size_t> path) -> WiringPortRef {
      if (part.is_structural_source()) {
        std::vector<WiringPortRef> children;
        children.reserve(part.structural_children().size());
        for (std::size_t child_index = 0;
             child_index < part.structural_children().size(); ++child_index) {
          std::vector<std::size_t> child_path = path;
          child_path.push_back(child_index);
          children.push_back(self(self, part.structural_children()[child_index],
                                  std::move(child_path)));
        }
        return WiringPortRef::structural_source(part.schema,
                                                std::move(children));
      }
      if (part.is_null_source()) {
        return part;
      }
      return WiringPortRef::captured_boundary_source(
          capture_index, std::move(path), part.schema);
    };
    return build(build, outer, {});
  };

  for (std::size_t index = 0; index < impl_->captured_inputs.size(); ++index) {
    if (impl_->captured_inputs[index].same_source_as(source)) {
      return captured_shape(impl_->captured_inputs[index], index);
    }
  }

  const std::size_t index = impl_->captured_inputs.size();
  impl_->captured_inputs.push_back(std::move(source));
  return captured_shape(impl_->captured_inputs.back(), index);
}

namespace {
WiringScopeEvent make_node_wiring_event(
    std::type_index def, const NodeBuilder &builder,
    std::span<const WiringInputRef> inputs) {
  const NodeTypeMetaData *node_type = builder.type().schema();
  std::vector<WiringTypeHandle> input_types;
  std::vector<std::string> input_schemas;
  input_types.reserve(inputs.size());
  input_schemas.reserve(inputs.size());
  for (const WiringInputRef &input : inputs) {
    input_types.emplace_back(input.source.schema);
    input_schemas.push_back(input.source.schema != nullptr
                                ? std::string{input.source.schema->name()}
                                : std::string{"<unwired>"});
  }
  const TSValueTypeMetaData *output_type =
      node_type != nullptr ? node_type->output_schema : nullptr;
  return WiringScopeEvent{
      .kind = WiringScopeKind::Node,
      .label = !builder.label().empty()
                   ? std::string{builder.label()}
                   : (node_type != nullptr ? std::string{node_type->name()}
                                           : std::string{def.name()}),
      .signature = node_type != nullptr ? std::string{node_type->name()}
                                        : std::string{def.name()},
      .input_types = std::move(input_types),
      .input_schemas = std::move(input_schemas),
      .output_type = WiringTypeHandle{output_type},
      .output_schema = output_type != nullptr
                           ? std::string{output_type->name()}
                           : std::string{},
  };
}
} // namespace

WiringPortRef Wiring::add_node(std::type_index def, NodeBuilder builder,
                               std::span<const WiringInputRef> inputs,
                               Value scalars) {
  auto add = [&]() -> WiringPortRef {
  // The passive marker (Python's ``passive(ts)``): a Passive-tagged
  // source removes the receiving slot from THIS node's active list.
  // Adjusted before schema resolution so node identity reflects it.
  std::vector<std::size_t> passive_slots;
  for (std::size_t slot = 0; slot < inputs.size(); ++slot) {
    if (inputs[slot].source.arg_tag == WiringPortRef::ArgTag::Passive) {
      passive_slots.push_back(slot);
    }
  }
  if (!passive_slots.empty()) {
    builder = builder.with_passive_inputs(
        {passive_slots.data(), passive_slots.size()});
  }

  const WiringNodeSchema schema = resolved_schema_of(builder);

  // Output-less (sink / side-effecting) nodes are never deduped: two identical
  // sinks must stay distinct runtime nodes so each performs its side effect,
  // matching Python where node calls are not common-subexpression-eliminated.
  // Only value-producing nodes are interned, where an identical
  // (def, schema, inputs, scalars) genuinely is one shared subexpression.
  const bool interns = schema.output != nullptr;

  InstanceKey key = make_key(def, schema, inputs, scalars);
  if (interns) {
    if (auto it = impl_->interned.find(key); it != impl_->interned.end()) {
      const WiringInstance *existing = it->second;
      return WiringPortRef::peered_source(existing, {},
                                          output_schema_of(*existing));
    }
  }

  builder.scalars(std::move(
      scalars)); // record the scalar configuration on the build artifact

  WiringInstance &instance = impl_->instances.emplace_back();
  instance.definition = def;
  instance.builder = std::move(builder);
  instance.inputs.assign(inputs.begin(), inputs.end());
  if (interns) {
    impl_->interned.emplace(std::move(key), &instance);
  }

  return WiringPortRef::peered_source(&instance, {},
                                      output_schema_of(instance));
  };
  if (!has_wiring_observers()) {
    return add();
  }
  return observe(make_node_wiring_event(def, builder, inputs), add);
}

WiringPortRef Wiring::add_node(std::type_index def, NodeBuilder builder,
                               std::span<const WiringPortRef> inputs,
                               Value scalars) {
  std::vector<WiringInputRef> input_refs;
  input_refs.reserve(inputs.size());
  for (const WiringPortRef &input : inputs) {
    input_refs.push_back(WiringInputRef{.source = input});
  }
  return add_node(
      def, std::move(builder),
      std::span<const WiringInputRef>{input_refs.data(), input_refs.size()},
      std::move(scalars));
}

WiringPortRef Wiring::add_unique_node(std::type_index def, NodeBuilder builder,
                                      std::span<const WiringInputRef> inputs,
                                      Value scalars) {
  auto add = [&]() -> WiringPortRef {
    builder.scalars(std::move(scalars));

    WiringInstance &instance = impl_->instances.emplace_back();
    instance.definition = def;
    instance.builder = std::move(builder);
    instance.inputs.assign(inputs.begin(), inputs.end());

    return WiringPortRef::peered_source(&instance, {},
                                        output_schema_of(instance));
  };
  if (!has_wiring_observers()) {
    return add();
  }
  return observe(make_node_wiring_event(def, builder, inputs), add);
}

WiringPortRef Wiring::add_unique_node(std::type_index def, NodeBuilder builder,
                                      std::span<const WiringPortRef> inputs,
                                      Value scalars) {
  std::vector<WiringInputRef> input_refs;
  input_refs.reserve(inputs.size());
  for (const WiringPortRef &input : inputs) {
    input_refs.push_back(WiringInputRef{.source = input});
  }
  return add_unique_node(
      def, std::move(builder),
      std::span<const WiringInputRef>{input_refs.data(), input_refs.size()},
      std::move(scalars));
}

void Wiring::add_rank_dependency(const WiringInstance *node,
                                 const WiringInstance *depends_on) {
  if (node == nullptr || depends_on == nullptr) {
    throw std::invalid_argument("rank dependency requires nodes");
  }
  if (node == depends_on) {
    throw std::invalid_argument("rank dependency cannot target the same node");
  }

  auto &dependencies = const_cast<WiringInstance *>(node)->rank_dependencies;
  if (std::find(dependencies.begin(), dependencies.end(), depends_on) ==
      dependencies.end()) {
    dependencies.push_back(depends_on);
  }
}

void Wiring::set_trait(std::string_view name, const ValueView &value) {
  if (name.empty()) {
    throw std::invalid_argument("graph trait name must not be empty");
  }
  impl_->traits.view().set(name, value);
}

void Wiring::set_trait(std::string_view name, Value &&value) {
  if (name.empty()) {
    throw std::invalid_argument("graph trait name must not be empty");
  }
  impl_->traits.view().set(name, std::move(value));
}

void Wiring::add_same_cycle_pair(const WiringInstance *capture,
                                 const WiringInstance *source) {
  // A same-cycle boundary pairing (shared-output relays): the source is
  // rank-constrained after the capture, and finish() validates the final
  // order so the RUNTIME can trust it unconditionally — the capture
  // schedules the source for the current evaluation time with no checks
  // on the hot path (wiring-time validation over run-time cost).
  add_rank_dependency(source, capture);
  impl_->same_cycle_pairs.push_back(Impl::SameCyclePair{capture, source});
}

void Wiring::validate_same_cycle_pairs(
    const std::unordered_map<const WiringInstance *, std::size_t> &index_of)
    const {
  auto name_of = [](const WiringInstance *instance) {
    if (instance == nullptr) {
      return std::string{"<null>"};
    }
    std::string label = std::string{instance->builder.label()};
    if (!label.empty()) {
      return label;
    }
    const auto *meta = instance->builder.type().schema();
    return meta != nullptr && meta->display_name != nullptr
               ? std::string{meta->display_name}
               : std::string{"<unnamed>"};
  };

  for (const auto &pair : impl_->same_cycle_pairs) {
    const auto capture = index_of.find(pair.capture);
    const auto source = index_of.find(pair.source);
    if (capture == index_of.end() || source == index_of.end()) {
      throw std::logic_error(
          "Wiring::finish lost a same-cycle boundary pair node ('" +
          name_of(pair.capture) + "' / '" + name_of(pair.source) + "')");
    }
    if (capture->second >= source->second) {
      throw std::logic_error(
          "Wiring::finish rank violation: same-cycle boundary capture '" +
          name_of(pair.capture) + "' (rank " + std::to_string(capture->second) +
          ") must rank before its paired source '" + name_of(pair.source) +
          "' (rank " + std::to_string(source->second) + ")");
    }
  }
}

void Wiring::register_built_service_path(std::string path,
                                         std::string_view kind) {
  if (path.empty()) {
    throw std::invalid_argument(
        "service/adaptor implementation path must not be empty");
  }

  auto [it, inserted] =
      impl_->built_service_paths.try_emplace(path, std::string{kind});
  if (!inserted) {
    throw std::invalid_argument("duplicate " + std::string{kind} +
                                " implementation registration for '" + path +
                                "'; already registered as " + it->second);
  }
}

void Wiring::register_service_client_path(std::string path,
                                          std::string_view kind,
                                          std::string_view interface_name,
                                          std::string_view specialization) {
  if (path.empty()) {
    throw std::invalid_argument(
        "service/adaptor client path must not be empty");
  }
  auto [it, inserted] = impl_->client_service_paths.try_emplace(
      std::move(path), Impl::ServiceClientIdentity{
          .kind = std::string{kind},
          .interface_name = std::string{interface_name},
          .specialization = std::string{specialization},
      });
  if (!inserted) {
    if ((!kind.empty() && it->second.kind != kind) ||
        (!interface_name.empty() && !it->second.interface_name.empty() &&
         it->second.interface_name != interface_name) ||
        (!specialization.empty() && !it->second.specialization.empty() &&
         it->second.specialization != specialization)) {
      throw std::invalid_argument(
          "conflicting service/adaptor client identity for '" + it->first + "'");
    }
    if (!interface_name.empty() && it->second.interface_name.empty()) {
      it->second.interface_name = interface_name;
    }
    if (!specialization.empty() && it->second.specialization.empty()) {
      it->second.specialization = specialization;
    }
  }
}

void Wiring::register_service_rank_anchor(std::string path,
                                          const WiringInstance *node) {
  if (path.empty()) {
    throw std::invalid_argument(
        "service/adaptor rank anchor path must not be empty");
  }
  if (node == nullptr) {
    return;
  }

  auto [it, inserted] =
      impl_->service_rank_anchors.try_emplace(std::move(path), node);
  if (!inserted && it->second != node) {
    throw std::invalid_argument(
        "conflicting service/adaptor rank anchor for '" + it->first + "'");
  }
}

void Wiring::register_service_client_rank(std::string path,
                                          std::string_view kind,
                                          const WiringInstance *node,
                                          bool receive) {
  if (path.empty()) {
    throw std::invalid_argument(
        "service/adaptor client rank path must not be empty");
  }
  if (node == nullptr) {
    return;
  }

  impl_->service_client_ranks.push_back(Impl::ServiceClientRank{
      .path = std::move(path),
      .kind = std::string{kind},
      .node = node,
      .receive = receive,
  });
}

void Wiring::register_service_implementation_candidate(
    std::vector<std::string> paths, std::string description,
    std::function<void(Wiring &)> materialize) {
  if (impl_->kind == WiringKind::SubGraph) {
    throw std::logic_error(
        "service/adaptor implementations cannot be registered inside an isolated sub-graph");
  }
  if (paths.empty()) {
    throw std::invalid_argument(
        "service/adaptor implementation candidate requires at least one path");
  }
  if (!materialize) {
    throw std::invalid_argument(
        "service/adaptor implementation candidate is not materializable");
  }
  std::sort(paths.begin(), paths.end());
  paths.erase(std::unique(paths.begin(), paths.end()), paths.end());
  for (const auto &path : paths) {
    if (path.empty()) {
      throw std::invalid_argument(
          "service/adaptor implementation candidate path must not be empty");
    }
    if (const auto found = impl_->service_candidate_paths.find(path);
        found != impl_->service_candidate_paths.end()) {
      throw std::invalid_argument(
          "duplicate service/adaptor implementation candidate for '" + path +
          "'; already registered by '" +
          impl_->service_candidates[found->second].description + "'");
    }
  }
  const std::size_t index = impl_->service_candidates.size();
  impl_->service_candidates.push_back(Impl::ServiceImplementationCandidate{
      .paths = std::move(paths),
      .description = std::move(description),
      .materialize = std::move(materialize),
  });
  for (const auto &path : impl_->service_candidates.back().paths) {
    impl_->service_candidate_paths.emplace(path, index);
  }
}

void Wiring::register_default_service_implementation_candidate(
    std::string path_prefix, std::string path_suffix,
    std::string description,
    std::function<void(Wiring &, std::string_view)> materialize) {
  register_default_service_implementation_candidate(
      {{std::move(path_prefix), std::move(path_suffix)}},
      std::move(description), std::move(materialize));
}

void Wiring::register_default_service_implementation_candidate(
    std::vector<std::pair<std::string, std::string>> path_selectors,
    std::string description,
    std::function<void(Wiring &, std::string_view)> materialize) {
  if (impl_->kind == WiringKind::SubGraph) {
    throw std::logic_error(
        "service/adaptor implementations cannot be registered inside an isolated sub-graph");
  }
  if (path_selectors.empty()) {
    throw std::invalid_argument(
        "default service/adaptor implementation candidate requires at least one path selector");
  }
  if (!materialize) {
    throw std::invalid_argument(
        "default service/adaptor implementation candidate is not materializable");
  }
  std::vector<std::string> selector_keys;
  selector_keys.reserve(path_selectors.size());
  for (const auto &[path_prefix, path_suffix] : path_selectors) {
    if (path_prefix.empty() || path_suffix.empty()) {
      throw std::invalid_argument(
          "default service/adaptor implementation candidate requires a path prefix and suffix");
    }
    std::string selector = path_prefix;
    selector.push_back('\0');
    selector.append(path_suffix);
    if (const auto found = impl_->default_service_candidates.find(selector);
        found != impl_->default_service_candidates.end()) {
      throw std::invalid_argument(
          "duplicate default service/adaptor implementation candidate for '" +
          path_prefix + "*" + path_suffix + "'; already registered by '" +
          impl_->service_candidates[found->second].description + "'");
    }
    selector_keys.push_back(std::move(selector));
  }
  const std::size_t index = impl_->service_candidates.size();
  impl_->service_candidates.push_back(Impl::ServiceImplementationCandidate{
      .description = std::move(description),
      .materialize_default = std::move(materialize),
      .path_selectors = std::move(path_selectors),
      .default_fallback = true,
  });
  for (auto &selector : selector_keys) {
    impl_->default_service_candidates.emplace(std::move(selector), index);
  }
}

void Wiring::register_catch_all_service_implementation_candidate(
    std::string description, std::function<void(Wiring &)> materialize) {
  if (impl_->kind == WiringKind::SubGraph) {
    throw std::logic_error(
        "service/adaptor implementations cannot be registered inside an isolated sub-graph");
  }
  if (!materialize) {
    throw std::invalid_argument(
        "catch-all service/adaptor implementation candidate is not materializable");
  }
  impl_->service_candidates.push_back(Impl::ServiceImplementationCandidate{
      .description = std::move(description),
      .materialize = std::move(materialize),
      .catch_all = true,
  });
}

void Wiring::build_services() {
  if (impl_->building_services) {
    return;
  }
  impl_->building_services = true;
  const auto reset_building = [this] { impl_->building_services = false; };
  try {
  bool progress = true;
  while (progress) {
    progress = false;

    // Ordinary exact-path candidates form the recursive fixed point. Mark a
    // candidate before invoking it so a recursively requested sibling cannot
    // materialize the same multi-interface implementation twice.
    for (std::size_t i = 0; i < impl_->service_candidates.size(); ++i) {
      auto &candidate = impl_->service_candidates[i];
      if (candidate.materialized || candidate.catch_all || candidate.default_fallback) {
        continue;
      }
      const bool requested = std::ranges::any_of(
          candidate.paths, [&](const std::string &path) {
            return impl_->client_service_paths.contains(path) &&
                   !impl_->built_service_paths.contains(path);
          });
      if (!requested) {
        continue;
      }
      candidate.materialized = true;
      auto materialize = candidate.materialize;
      materialize(*this);
      progress = true;
    }

    // Exact registrations take precedence. For every remaining concrete
    // request, materialize the matching interface-default candidate at that
    // requested path, once per path/specialization.
    const auto clients = service_client_paths();
    for (const auto &[path, _kind] : clients) {
      if (impl_->built_service_paths.contains(path) ||
          impl_->service_candidate_paths.contains(path)) {
        continue;
      }
      for (std::size_t i = 0; i < impl_->service_candidates.size(); ++i) {
        auto &candidate = impl_->service_candidates[i];
        if (!candidate.default_fallback) {
          continue;
        }
        const auto selector = std::ranges::find_if(
            candidate.path_selectors, [&](const auto &value) {
              return path.starts_with(value.first) && path.ends_with(value.second) &&
                     path.size() >= value.first.size() + value.second.size();
            });
        if (selector == candidate.path_selectors.end()) { continue; }
        const std::string concrete_user_path = path.substr(
            selector->first.size(),
            path.size() - selector->first.size() - selector->second.size());
        if (candidate.materialized_paths.contains(concrete_user_path)) { continue; }
        for (const auto &[prefix, suffix] : candidate.path_selectors) {
          const std::string sibling_path = prefix + concrete_user_path + suffix;
          if (const auto exact = impl_->service_candidate_paths.find(sibling_path);
              exact != impl_->service_candidate_paths.end()) {
            throw std::invalid_argument(
                "default multi-interface implementation '" + candidate.description +
                "' overlaps exact implementation '" +
                impl_->service_candidates[exact->second].description +
                "' at '" + sibling_path + "'");
          }
        }
        candidate.materialized_paths.insert(concrete_user_path);
        auto materialize = candidate.materialize_default;
        impl_->service_materialization_path = path;
        try {
          materialize(*this, path);
          impl_->service_materialization_path.clear();
        } catch (...) {
          impl_->service_materialization_path.clear();
          throw;
        }
        progress = true;
        break;
      }
    }

    // Catch-all registrations are ordinary implementation graphs which inspect
    // the complete demand ledger themselves. Compose each once, matching the
    // reference engine; rewiring the same graph for later clients is invalid.
    for (std::size_t i = 0; i < impl_->service_candidates.size(); ++i) {
      auto &candidate = impl_->service_candidates[i];
      if (!candidate.catch_all || candidate.materialized) {
        continue;
      }
      candidate.materialized = true;
      auto materialize = candidate.materialize;
      materialize(*this);
      progress = true;
    }
  }
  reset_building();
  } catch (...) {
    reset_building();
    throw;
  }
}

std::vector<std::pair<std::string, std::string>>
Wiring::service_client_paths() const {
  std::vector<std::pair<std::string, std::string>> result;
  result.reserve(impl_->client_service_paths.size());
  for (const auto &[path, identity] : impl_->client_service_paths) {
    result.emplace_back(path, identity.kind);
  }
  std::ranges::sort(result);
  return result;
}

std::vector<WiringServiceClientRecord> Wiring::service_client_records() const {
  std::vector<WiringServiceClientRecord> result;
  for (const auto &[base_path, identity] : impl_->client_service_paths) {
    bool has_rank = false;
    for (const auto &rank : impl_->service_client_ranks) {
      if (rank.path != base_path &&
          !(rank.path.size() > base_path.size() && rank.path.starts_with(base_path) &&
            rank.path[base_path.size()] == '/')) {
        continue;
      }
      has_rank = true;
      result.push_back(WiringServiceClientRecord{
          .base_path = base_path,
          .endpoint_path = rank.path,
          .kind = identity.kind,
          .interface_name = identity.interface_name,
          .specialization = identity.specialization,
          .receive = rank.receive,
      });
    }
    if (!has_rank) {
      result.push_back(WiringServiceClientRecord{
          .base_path = base_path,
          .endpoint_path = base_path,
          .kind = identity.kind,
          .interface_name = identity.interface_name,
          .specialization = identity.specialization,
          .receive = true,
      });
    }
  }
  std::ranges::sort(result, {}, [](const auto &record) {
    return std::tuple{record.base_path, record.endpoint_path, record.receive};
  });
  return result;
}

std::vector<std::pair<std::string, std::string>>
Wiring::built_service_paths() const {
  std::vector<std::pair<std::string, std::string>> result;
  result.reserve(impl_->built_service_paths.size());
  for (const auto &[path, kind] : impl_->built_service_paths) {
    result.emplace_back(path, kind);
  }
  std::ranges::sort(result);
  return result;
}

std::string_view Wiring::service_materialization_path() const noexcept {
  return impl_->service_materialization_path;
}

Wiring::ServiceImplementationScope::ServiceImplementationScope(
    Wiring &wiring, std::string description,
    std::vector<WiringServiceImplementationEndpoint> required_endpoints,
    bool require_all)
    : wiring_{&wiring} {
  wiring_->begin_service_implementation(std::move(description),
                                        std::move(required_endpoints), require_all);
  active_ = true;
}

Wiring::ServiceImplementationScope::ServiceImplementationScope(
    Wiring &wiring, std::string description,
    std::vector<std::string> required_endpoints)
    : wiring_{&wiring} {
  wiring_->begin_service_implementation(std::move(description),
                                        std::move(required_endpoints));
  active_ = true;
}

Wiring::ServiceImplementationScope::ServiceImplementationScope(
    ServiceImplementationScope &&other) noexcept
    : wiring_{std::exchange(other.wiring_, nullptr)},
      active_{std::exchange(other.active_, false)} {}

Wiring::ServiceImplementationScope &
Wiring::ServiceImplementationScope::operator=(
    ServiceImplementationScope &&other) noexcept {
  if (this != &other) {
    cancel_if_active();
    wiring_ = std::exchange(other.wiring_, nullptr);
    active_ = std::exchange(other.active_, false);
  }
  return *this;
}

Wiring::ServiceImplementationScope::~ServiceImplementationScope() noexcept {
  cancel_if_active();
}

void Wiring::ServiceImplementationScope::complete() {
  if (!active_) {
    return;
  }
  Wiring *wiring = wiring_;
  active_ = false;
  wiring_ = nullptr;
  wiring->end_service_implementation();
}

void Wiring::ServiceImplementationScope::cancel_if_active() noexcept {
  if (active_ && wiring_ != nullptr) {
    wiring_->cancel_service_implementation();
  }
  active_ = false;
  wiring_ = nullptr;
}

Wiring::ServiceImplementationScope Wiring::service_implementation_scope(
    std::string description,
    std::vector<WiringServiceImplementationEndpoint> required_endpoints,
    bool require_all) {
  return ServiceImplementationScope{*this, std::move(description),
                                    std::move(required_endpoints), require_all};
}

Wiring::ServiceImplementationScope Wiring::service_implementation_scope(
    std::string description, std::vector<std::string> required_endpoints) {
  return ServiceImplementationScope{*this, std::move(description),
                                    std::move(required_endpoints)};
}

void Wiring::begin_service_implementation(
    std::string description, std::vector<std::string> required_endpoints) {
  std::vector<WiringServiceImplementationEndpoint> endpoints;
  endpoints.reserve(required_endpoints.size());
  for (auto &endpoint : required_endpoints) {
    endpoints.push_back(WiringServiceImplementationEndpoint{std::move(endpoint),
                                                            ResolutionMap{}});
  }
  begin_service_implementation(std::move(description), std::move(endpoints), true);
}

void Wiring::begin_service_implementation(
    std::string description,
    std::vector<WiringServiceImplementationEndpoint> required_endpoints,
    bool require_all) {
  Impl::ServiceImplementationScopeState scope;
  scope.description = std::move(description);
  scope.require_all = require_all;
  scope.required_endpoints.reserve(required_endpoints.size());
  for (auto &endpoint : required_endpoints) {
    if (endpoint.endpoint.empty()) {
      throw std::invalid_argument(
          "service/adaptor implementation endpoint must not be empty");
    }
    scope.required_endpoints.insert(endpoint.endpoint);
    scope.endpoint_resolutions.emplace(std::move(endpoint.endpoint),
                                       std::move(endpoint.resolution));
  }
  impl_->implementation_scopes.push_back(std::move(scope));
}

std::vector<std::string> Wiring::service_implementation_used_endpoints() const {
  if (impl_->implementation_scopes.empty()) { return {}; }
  const auto &used = impl_->implementation_scopes.back().used_endpoints;
  return {used.begin(), used.end()};
}

void Wiring::register_service_implementation_stub(std::string endpoint,
                                                  std::string_view kind) {
  if (impl_->implementation_scopes.empty()) {
    throw std::invalid_argument(std::string{kind} +
                                " implementation stub may only be used inside "
                                "a registered implementation graph");
  }

  auto &scope = impl_->implementation_scopes.back();
  if (!scope.required_endpoints.empty() &&
      !scope.required_endpoints.contains(endpoint)) {
    throw std::invalid_argument(
        std::string{kind} + " implementation stub for '" + endpoint +
        "' is not part of active implementation '" + scope.description + "'");
  }
  scope.used_endpoints.insert(std::move(endpoint));
}

ResolutionMap Wiring::service_implementation_stub_resolution(
    const std::string &endpoint) const {
  if (impl_->implementation_scopes.empty()) {
    return ResolutionMap{};
  }
  const auto &scope = impl_->implementation_scopes.back();
  auto it = scope.endpoint_resolutions.find(endpoint);
  return it != scope.endpoint_resolutions.end() ? it->second : ResolutionMap{};
}

void Wiring::end_service_implementation() {
  if (impl_->implementation_scopes.empty()) {
    throw std::logic_error(
        "end_service_implementation called without an active implementation");
  }

  auto scope = std::move(impl_->implementation_scopes.back());
  impl_->implementation_scopes.pop_back();

  std::vector<std::string> missing;
  for (const auto &endpoint : scope.required_endpoints) {
    if (!scope.require_all) { break; }
    if (!scope.used_endpoints.contains(endpoint)) {
      missing.push_back(endpoint);
    }
  }
  if (!missing.empty()) {
    std::sort(missing.begin(), missing.end());
    std::string message =
        "implementation '" + scope.description + "' did not wire required stub";
    if (missing.size() != 1) {
      message.push_back('s');
    }
    message.append(": ");
    for (std::size_t i = 0; i < missing.size(); ++i) {
      if (i != 0) {
        message.append(", ");
      }
      message.append(missing[i]);
    }
    throw std::invalid_argument(message);
  }
}

void Wiring::cancel_service_implementation() noexcept {
  if (!impl_->implementation_scopes.empty()) {
    impl_->implementation_scopes.pop_back();
  }
}

WiringPortRef Wiring::add_node(std::type_index def,
                               const WiringNodeSchema &schema,
                               std::span<const WiringInputRef> inputs,
                               Value scalars,
                               std::function<NodeBuilder()> make_builder) {
  auto add = [&]() -> WiringPortRef {
    const bool interns = schema.output != nullptr;
    InstanceKey key = make_key(def, schema, inputs, scalars);
    if (interns) {
      if (auto it = impl_->interned.find(key); it != impl_->interned.end()) {
        const WiringInstance *existing = it->second;
        return WiringPortRef::peered_source(existing, {},
                                            output_schema_of(*existing));
      }
    }

    NodeBuilder builder = make_builder(); // intern miss: only now pay for (and
                                          // register) the builder
    // The supplied scalar participates in wiring interning. A specialised
    // builder may carry a different runtime-only value (for example a nested
    // graph context); preserve that explicit per-instance configuration.
    if (!builder.scalars().has_value()) {
      builder.scalars(std::move(scalars));
    }

    WiringInstance &instance = impl_->instances.emplace_back();
    instance.definition = def;
    instance.builder = std::move(builder);
    instance.inputs.assign(inputs.begin(), inputs.end());
    if (interns) {
      impl_->interned.emplace(std::move(key), &instance);
    }

    return WiringPortRef::peered_source(&instance, {},
                                        output_schema_of(instance));
  };
  if (!has_wiring_observers()) {
    return add();
  }

  std::vector<WiringTypeHandle> input_types;
  std::vector<std::string> input_schemas;
  input_types.reserve(inputs.size());
  input_schemas.reserve(inputs.size());
  for (const WiringInputRef &input : inputs) {
    input_types.emplace_back(input.source.schema);
    input_schemas.push_back(input.source.schema != nullptr
                                ? std::string{input.source.schema->name()}
                                : std::string{"<unwired>"});
  }
  WiringScopeEvent event{
      .kind = WiringScopeKind::Node,
      .label = std::string{def.name()},
      .signature = std::string{def.name()},
      .input_types = std::move(input_types),
      .input_schemas = std::move(input_schemas),
      .output_type = WiringTypeHandle{schema.output},
      .output_schema = schema.output != nullptr
                           ? std::string{schema.output->name()}
                           : std::string{},
  };
  return observe(std::move(event), add);
}

WiringPortRef Wiring::add_node(std::type_index def,
                               const WiringNodeSchema &schema,
                               std::span<const WiringPortRef> inputs,
                               Value scalars,
                               std::function<NodeBuilder()> make_builder) {
  std::vector<WiringInputRef> input_refs;
  input_refs.reserve(inputs.size());
  for (const WiringPortRef &input : inputs) {
    input_refs.push_back(WiringInputRef{.source = input});
  }
  return add_node(
      def, schema,
      std::span<const WiringInputRef>{input_refs.data(), input_refs.size()},
      std::move(scalars), std::move(make_builder));
}

const TSValueTypeMetaData *
Wiring::activate_error_capture(const WiringInstance *node,
                               const TSValueTypeMetaData *error_schema,
                               ErrorCaptureOptions options) {
  if (node == nullptr) {
    throw std::invalid_argument("activate_error_capture: null node");
  }
  // The deque owns the instances (stable addresses); the const port ref
  // names one we own and may amend before finish.
  WiringInstance &instance = const_cast<WiringInstance &>(*node);
  const NodeTypeMetaData *meta = instance.builder.type().schema();
  ErrorCaptureOptions merged = options;
  if (meta != nullptr && meta->captures_errors) {
    merged.trace_back_depth =
        std::max(merged.trace_back_depth, meta->error_capture.trace_back_depth);
    merged.capture_values =
        merged.capture_values || meta->error_capture.capture_values;
  }
  if (meta == nullptr || meta->error_output_schema == nullptr ||
      merged != meta->error_capture) {
    const NodeTypeRef node_type = instance.builder.type();
    const bool is_map_node = node_type.ops_ref().extended_view_type_id ==
                             MapNodeView::node_view_type_id();
    instance.builder =
        is_map_node ? map_node_with_error_capture(instance.builder,
                                                  error_schema, merged)
                    : instance.builder.with_error_capture(error_schema, merged);
  }
  return instance.builder.type().schema()->error_output_schema;
}

GlobalStateView Wiring::global_state() noexcept {
  // Resolved LIVE through the wiring context on every call — never a
  // retained pointer (which would dangle when a short-lived GlobalContext
  // exits before the wiring is done).
  if (impl_->kind == WiringKind::TopLevel && impl_->live_seeded) {
    if (GlobalState *state = GlobalContext::active_state()) {
      return state->view();
    }
  }
  return impl_->global_state.view();
}

GlobalStateView Wiring::operator_state() noexcept {
  if (impl_->kind == WiringKind::SubGraph) {
    if (GlobalState *state = GlobalContext::active_state()) {
      return state->view();
    }
  }
  return global_state();
}

void Wiring::apply_service_rank_dependencies() {
  std::unordered_set<std::string> ranked_senders;
  for (const auto &client : impl_->service_client_ranks) {
    auto anchor_it = impl_->service_rank_anchors.find(client.path);
    if (anchor_it == impl_->service_rank_anchors.end() ||
        anchor_it->second == nullptr) {
      continue;
    }

    const WiringInstance *anchor = anchor_it->second;
    if (anchor == client.node) {
      continue;
    }

    if (client.receive) {
      add_rank_dependency(client.node, anchor);
    } else if (ranked_senders.insert(client.path).second) {
      add_rank_dependency(anchor, client.node);
    } else {
      add_rank_dependency(client.node, anchor);
    }
  }
}

GraphBuilder Wiring::finish_top_level(bool consume_state) {
  if (!impl_->implementation_scopes.empty()) {
    throw std::logic_error("Wiring::finish encountered an unterminated "
                           "service/adaptor implementation scope");
  }
  build_services();
  for (const auto &[path, identity] : impl_->client_service_paths) {
    if (!impl_->built_service_paths.contains(path)) {
      throw std::invalid_argument("missing implementation for " + identity.kind + " '" +
                                  path + "'");
    }
  }

  apply_service_rank_dependencies(); // add_rank_dependency de-dupes: idempotent
  const auto realization =
      TypeRealizationSnapshot::capture(TypeRegistry::instance());
  TypeRealizationScope realization_scope{realization.get()};
  RankedGraphBuild build = build_ranked_graph(impl_->instances, nullptr);
  validate_same_cycle_pairs(build.index_of);
  build.graph_builder.type_realization(realization);
  // The wiring end FIXES the seed (ruling 2026-07-27): a live-seeded wiring
  // copies the selected GlobalState as it stands NOW — wiring-time
  // configuration and entries included — into the graph as its initial
  // state; the user's state object stays theirs (results copy back at run
  // end). The selected state must span the wiring process. A stateless
  // wiring keeps the internal store: moved on the consuming finish() path,
  // copied on the snapshot() path so the wiring stays live.
  GlobalState *live = impl_->kind == WiringKind::TopLevel && impl_->live_seeded
                          ? GlobalContext::active_state()
                          : nullptr;
  if (impl_->live_seeded && live == nullptr) {
    throw std::logic_error(
        "Wiring: the selected GlobalState exited before wiring finished — "
        "the global state must span the wiring process");
  }
  build.graph_builder.global_state(
      live != nullptr    ? GlobalState{*live}
      : consume_state    ? std::move(impl_->global_state)
                         : GlobalState{impl_->global_state});
  const ValueView traits_value = impl_->traits.as_value().view();
  const auto traits_map = traits_value.as_map();
  for (const auto [key, boxed] : traits_map) {
    build.graph_builder.trait(key.checked_as<Str>(), boxed.as_any().get());
  }
  return std::move(build.graph_builder);
}

GraphBuilder Wiring::finish() && { return finish_top_level(true); }

GraphBuilder Wiring::snapshot() & { return finish_top_level(false); }

CompiledSubGraph Wiring::finish_subgraph(
    std::optional<WiringPortRef> output,
    std::vector<const TSValueTypeMetaData *> input_schemas) && {
  if (!impl_->implementation_scopes.empty()) {
    throw std::logic_error("Wiring::finish_subgraph encountered an "
                           "unterminated service/adaptor implementation scope");
  }
  apply_service_rank_dependencies();
  CompiledSubGraph compiled;
  compiled.input_schemas = std::move(input_schemas);

  OuterCaptureCollector captures{
      .base_index = compiled.input_schemas.size(),
      .captured = std::move(impl_->captured_inputs),
  };

  struct ExternalServiceEndpoint {
    std::string_view path{};
    WiringPortRef::ArgTag arg_tag{WiringPortRef::ArgTag::None};
  };
  const auto endpoint_for =
      [](std::string_view label) -> std::optional<ExternalServiceEndpoint> {
    constexpr std::array prefixes{
        std::pair{std::string_view{"shared_output_source:"},
                  WiringPortRef::ArgTag::PassThrough},
        std::pair{std::string_view{"subscription_key_source:"},
                  WiringPortRef::ArgTag::Passive},
        std::pair{std::string_view{"request_input_source:"},
                  WiringPortRef::ArgTag::Passive},
    };
    for (const auto &[prefix, arg_tag] : prefixes) {
      if (label.starts_with(prefix)) {
        return ExternalServiceEndpoint{label.substr(prefix.size()), arg_tag};
      }
    }
    return std::nullopt;
  };
  const auto endpoint_belongs_to = [](std::string_view endpoint,
                                      std::string_view service_path) {
    return endpoint == service_path || (endpoint.size() > service_path.size() &&
                                        endpoint.starts_with(service_path) &&
                                        endpoint[service_path.size()] == '/');
  };

  std::vector<const WiringInstance *> external_instances;
  for (const auto &[service_path, service_identity] : impl_->client_service_paths) {
    if (impl_->built_service_paths.contains(service_path)) {
      continue;
    }

    bool found_endpoint = false;
    for (const WiringInstance &instance : impl_->instances) {
      const auto endpoint = endpoint_for(instance.builder.label());
      if (!endpoint.has_value() ||
          !endpoint_belongs_to(endpoint->path, service_path)) {
        continue;
      }

      found_endpoint = true;
      if (std::ranges::find(external_instances, &instance) !=
          external_instances.end()) {
        continue;
      }
      const auto *source_schema = output_schema_of(instance);
      if (source_schema == nullptr) {
        throw std::logic_error(
            "external service transport source has no output schema");
      }
      std::vector<NestedServiceRank> ranks;
      for (const auto &rank : impl_->service_client_ranks) {
        if (rank.node != &instance) {
          continue;
        }
        const auto duplicate = std::ranges::find_if(
            ranks, [&](const NestedServiceRank &existing) {
              return existing.path == rank.path && existing.receive == rank.receive;
            });
        if (duplicate == ranks.end()) {
          ranks.push_back(NestedServiceRank{
              .path = rank.path,
              .kind = rank.kind,
              .receive = rank.receive,
          });
        }
      }
      compiled.external_service_inputs.push_back(NestedServiceInput{
          .service_path = service_path,
          .service_kind = service_identity.kind,
          .interface_name = service_identity.interface_name,
          .specialization = service_identity.specialization,
          .definition = instance.definition,
          .builder = instance.builder,
          .source_schema = source_schema,
          .arg_tag = endpoint->arg_tag,
          .ranks = std::move(ranks),
      });
      external_instances.push_back(&instance);
    }
    if (!found_endpoint) {
      throw std::invalid_argument(
          "nested graph cannot externalize " + service_identity.kind + " '" +
          service_path +
          "': no supported service transport endpoint was wired");
    }
  }

  std::unordered_map<const WiringInstance *, std::size_t> external_sources;
  std::unordered_set<const WiringInstance *> owned_instances;
  owned_instances.reserve(impl_->instances.size());
  for (const WiringInstance &instance : impl_->instances) {
    owned_instances.insert(&instance);
  }
  std::unordered_set<const WiringInstance *> external_instance_set{
      external_instances.begin(), external_instances.end()};
  for (const WiringInstance &instance : impl_->instances) {
    if (external_instance_set.contains(&instance)) {
      continue;
    }
    for (const WiringInputRef &input : instance.inputs) {
      collect_outer_captures(input.source, owned_instances,
                             external_instance_set, captures);
    }
  }
  captures.frozen = true;
  for (std::size_t index = 0; index < external_instances.size(); ++index) {
    external_sources.emplace(external_instances[index],
                             captures.base_index + captures.captured.size() +
                                 index);
  }

  if (output.has_value() && output->is_delayed_source()) {
    output = resolve_delayed_source(*output);
  }
  std::unordered_set<const WiringInstance *> escaped_outputs;
  if (output.has_value()) {
    collect_escaped_output_producers(*output, external_sources,
                                     escaped_outputs);
  }

  RankedGraphBuild build = build_ranked_graph(
      impl_->instances, &compiled.input_bindings, &captures, &external_sources,
      &escaped_outputs);
  validate_same_cycle_pairs(build.index_of);
  // GraphBuilder's default construction honours an active top-level
  // GlobalContext. A compiled child must instead share its root graph's
  // runtime state, so discard that constructor-time seed here.
  build.graph_builder.global_state(GlobalState{});
  const ValueView traits_value = impl_->traits.as_value().view();
  const auto traits_map = traits_value.as_map();
  for (const auto [key, boxed] : traits_map) {
    build.graph_builder.trait(key.checked_as<Str>(), boxed.as_any().get());
  }
  compiled.graph_builder = std::move(build.graph_builder);
  const auto &index_of = build.index_of;

  if (output.has_value()) {
    if (output->is_boundary_source()) {
      // Pass-through: the sub-graph returns a boundary input directly
      // (alias_parent_input) — the outer output aliases the upstream
      // output the outer input is bound to.
      std::vector<std::size_t> parent_path;
      parent_path.reserve(1 + output->boundary_path().size());
      parent_path.push_back(captures.boundary_ordinal(*output));
      parent_path.insert(parent_path.end(), output->boundary_path().begin(),
                         output->boundary_path().end());
      compiled.output_binding = NestedGraphOutputBinding{
          .kind = NestedGraphOutputBinding::Kind::ParentInput,
          .parent_source_path = std::move(parent_path),
      };
      compiled.output_schema = output->schema;
    } else if (output->is_peered_source()) {
      const auto external = external_sources.find(output->peered_node());
      if (external != external_sources.end()) {
        compiled.output_binding = NestedGraphOutputBinding{
            .kind = NestedGraphOutputBinding::Kind::ParentInput,
            .parent_source_path = {external->second},
        };
        compiled.output_schema = output->schema;
      } else {
        if (output->peered_output_kind() != GraphEdgeSourceKind::Output) {
          throw std::invalid_argument(
              "Wiring::finish_subgraph: error/recordable-state outputs cannot "
              "be a sub-graph output");
        }
        const auto it = index_of.find(output->peered_node());
        if (it == index_of.end()) {
          compiled.output_binding = NestedGraphOutputBinding{
              .kind = NestedGraphOutputBinding::Kind::ParentInput,
              .parent_source_path = {captures.base_index +
                                     captures.index_for(*output)},
          };
          compiled.output_schema = output->schema;
        } else {
          compiled.output_binding = NestedGraphOutputBinding{
              .source = NestedGraphEndpoint{.node = it->second,
                                            .path = output->peered_path()},
          };
          compiled.output_schema = output->schema;
          compiled.output_is_structural_reference =
              output->peered_path().empty() &&
              output->peered_node()->definition ==
                  std::type_index(typeid(StructuralRefNodeTag));
        }
      }
    } else if (const auto ordinal =
                   structural_boundary_ordinal(*output, captures);
               ordinal.has_value() && *ordinal < compiled.input_schemas.size() &&
               compiled.input_schemas[*ordinal] == output->schema) {
      // A fixed TSB/TSL argument is represented by a structural root whose
      // leaves are boundary projections. Returning that argument directly is
      // the structural form of an ordinary parent-input pass-through.
      compiled.output_binding = NestedGraphOutputBinding{
          .kind = NestedGraphOutputBinding::Kind::ParentInput,
          .parent_source_path = {*ordinal},
      };
      compiled.output_schema = output->schema;
    } else {
      throw std::invalid_argument(
          "Wiring::finish_subgraph: the sub-graph output must be a node output "
          "or an unchanged boundary input");
    }
  }

  for (const WiringPortRef &outer : captures.captured) {
    compiled.input_schemas.push_back(outer.schema);
  }
  compiled.captured_inputs = std::move(captures.captured);
  for (const NestedServiceInput &input : compiled.external_service_inputs) {
    compiled.input_schemas.push_back(input.source_schema);
  }

  // Wiring-time GlobalState entries cannot be carried by a sub-graph:
  // nested graphs delegate global state to the root graph at runtime, so
  // the seeded store would be silently discarded. Seed it on the OUTER
  // wiring instead.
  if (impl_->global_state.as_value().view().as_map().size() != 0) {
    throw std::invalid_argument(
        "Wiring::finish_subgraph: a sub-graph compose must not seed "
        "GlobalState (nested graphs share the "
        "root graph's state); seed it on the outer wiring");
  }

  return compiled;
}
} // namespace hgraph
