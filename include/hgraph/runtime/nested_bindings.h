#ifndef HGRAPH_RUNTIME_NESTED_BINDINGS_H
#define HGRAPH_RUNTIME_NESTED_BINDINGS_H

#include <hgraph/runtime/nested_graph_node.h>
#include <hgraph/types/metadata/type_registry.h>
#include <hgraph/types/time_series/ts_input.h>
#include <hgraph/types/time_series/ts_input/detail.h>
#include <hgraph/types/time_series/ts_output.h>
#include <hgraph/types/time_series/ts_output/forwarding.h>

#include <cstddef>
#include <span>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace hgraph {
/**
 * Shared boundary-binding helpers for nested-graph node implementations
 * (``single_nested_graph_node`` today; ``switch_`` / ``map_`` build on the
 * same model — no bespoke bind/unbind logic per operator). See the
 * developer guide *Nested Graphs*.
 */

/**
 * Walk an indexed time-series view (TSB field index or TSL index) along a
 * path. Inputs and outputs traverse identically, so one template serves
 * both; the ``ts_key_set_path_component`` sentinel (a TSD's key-set
 * projection) is output-side only — use ``walk_source_to_output`` when a
 * binding path may carry it.
 */
template <typename View>
[[nodiscard]] View walk_ts_path(View view, std::span<const std::size_t> path) {
  for (const std::size_t component : path) {
    if (view.schema() == nullptr) {
      throw std::logic_error(
          "Nested graph path requires a typed time-series view");
    }
    if constexpr (std::is_same_v<View, TSOutputView>) {
      if (component == ts_key_set_path_component) {
        view = view.as_dict().key_set();
        continue;
      }
    } else if (component == ts_key_set_path_component) {
      throw std::invalid_argument("Nested graph path through a TSD addresses "
                                  "only its key set (output side)");
    }
    view = view.indexed_child_at(component);
  }
  return view;
}

template <typename View>
[[nodiscard]] View walk_ts_path(View view,
                                const std::vector<std::size_t> &path) {
  return walk_ts_path(std::move(view), std::span<const std::size_t>{path});
}

/**
 * Walk an output endpoint path intended to be a forwarding target. Unlike
 * normal output projection, peered children are returned as the raw
 * TargetLink storage so callers can bind or clear the forwarding target.
 */
[[nodiscard]] inline TSOutputView
walk_forwarding_target_path(TSOutputView view,
                            std::span<const std::size_t> path) {
  for (const std::size_t component : path) {
    if (view.schema() == nullptr) {
      throw std::logic_error(
          "Nested graph forwarding target path requires a typed output view");
    }
    if (component == ts_key_set_path_component) {
      view = view.as_dict().key_set();
      continue;
    }

    auto projection =
        detail::input_child_projection(view.data_view(), component);
    TSDataView child = projection.target_link.valid()
                           ? std::move(projection.target_link)
                           : std::move(projection.visible);
    if (!child.valid()) {
      throw std::logic_error(
          "Nested graph forwarding target path projection failed");
    }
    view = TSOutputView{view.output(), child, view.evaluation_time()};
  }
  return view;
}

[[nodiscard]] inline TSOutputView
walk_forwarding_target_path(TSOutputView view,
                            const std::vector<std::size_t> &path) {
  return walk_forwarding_target_path(std::move(view),
                                     std::span<const std::size_t>{path});
}

/**
 * Bind a peered child input endpoint to the *same upstream output* the
 * outer boundary is bound to (re-resolved each cycle; a no-op when the
 * endpoint already references the same output, and unbinds while the
 * upstream is unbound).
 */
inline void bind_input_to_source(TSInputView target,
                                 const TSOutputView &source) {
  if (!target.is_bindable()) {
    throw std::logic_error("Nested graph input binding target must be a peered "
                           "child input endpoint");
  }

  if (!source.bound()) {
    if (target.bound()) {
      target.unbind_output();
    }
    return;
  }

  auto current = target.bound_output();
  if (!current.handle().same_as(source.handle())) {
    target.bind_output(source);
  }
}

/** Migrate an unchanged runtime-owned route without sampling its consumer. */
inline void rebind_input_to_source_silent(TSInputView target,
                                          const TSOutputView &source) {
  if (!target.is_bindable()) {
    throw std::logic_error("Silent nested input rebind target must be a peered "
                           "child input endpoint");
  }
  if (!source.bound()) {
    if (target.bound()) {
      target.unbind_output();
    }
    return;
  }

  auto current = target.bound_output();
  if (!current.handle().same_as(source.handle())) {
    target.rebind_output_silent(source);
  }
}

/** Bind a newly constructed child boundary and expose the source's current
    value as a sampled delta for this lifecycle transition. */
inline void bind_sampled_input_to_source(TSInputView target,
                                         const TSOutputView &source,
                                         DateTime evaluation_time) {
  if (!target.is_bindable()) {
    throw std::logic_error("Sampled nested graph input binding target must be "
                           "a peered child input endpoint");
  }

  if (!source.bound()) {
    if (target.bound()) {
      target.unbind_output();
    }
    return;
  }

  target.bind_output_sampled(source, evaluation_time);
}

/**
 * Bind a nested child input from an outer input position.
 *
 * A peered outer position has one bound output and takes the normal fast
 * path. A fixed TSB/TSL assembled structurally has no root output; recurse
 * into its children so each peered leaf is carried across the boundary.
 */
inline void bind_nested_input_to_source(TSInputView target, TSInputView source,
                                        DateTime evaluation_time,
                                        bool sampled) {
  const auto source_output = source.bound_output();
  if (source_output.bound()) {
    if (sampled) {
      bind_sampled_input_to_source(std::move(target), source_output,
                                   evaluation_time);
    } else {
      bind_input_to_source(std::move(target), source_output);
    }
    return;
  }

  if (source.is_bindable()) {
    if (sampled) {
      bind_sampled_input_to_source(std::move(target), source_output,
                                   evaluation_time);
    } else {
      bind_input_to_source(std::move(target), source_output);
    }
    return;
  }

  const auto *source_schema = source.schema();
  const auto *target_schema = target.schema();
  const auto fixed_child_count =
      [](const TSValueTypeMetaData *schema) -> std::size_t {
    if (schema == nullptr) {
      return 0;
    }
    if (schema->kind == TSTypeKind::TSB) {
      return schema->field_count();
    }
    if (schema->kind == TSTypeKind::TSL) {
      return schema->fixed_size();
    }
    return 0;
  };
  const std::size_t source_children = fixed_child_count(source_schema);
  const std::size_t target_children = fixed_child_count(target_schema);
  if (source_schema == nullptr || target_schema == nullptr ||
      source_schema->kind != target_schema->kind ||
      source_children != target_children || target.is_bindable()) {
    throw std::logic_error(
        "Nested structural input binding requires matching non-peered fixed "
        "structures (source '" +
        (source_schema != nullptr ? std::string{source_schema->name()}
                                  : std::string{"<untyped>"}) +
        "', " + (source.is_bindable() ? "peered" : "non-peered") +
        "; target '" +
        (target_schema != nullptr ? std::string{target_schema->name()}
                                  : std::string{"<untyped>"}) +
        "', " + (target.is_bindable() ? "peered" : "non-peered") + ")");
  }

  for (std::size_t index = 0; index < source_children; ++index) {
    bind_nested_input_to_source(target.indexed_child_at(index),
                                source.indexed_child_at(index), evaluation_time,
                                sampled);
  }
}

/** Re-point a forwarding output endpoint at ``source`` (no-op when already
 * there). */
inline void bind_forwarding_output_to_source(const TSOutputView &target,
                                             const TSOutputView &source) {
  if (!target.forwarding()) {
    throw std::logic_error("Nested graph output binding target must be a "
                           "forwarding output endpoint");
  }

  TSOutputView resolved_source =
      resolve_forwarding_source(source.borrowed_ref());
  if (!resolved_source.bound()) {
    if (target.forwarding_bound()) {
      target.clear_forwarding_target();
    }
    return;
  }

  const auto current = target.forwarding_target();
  if (!current.same_as(resolved_source.handle())) {
    target.bind_forwarding_target(resolved_source);
  }
}

/** Build a stable forwarding tree for fixed structural outputs. */
[[nodiscard]] inline bool
requires_structural_forwarding(const TSValueTypeMetaData &schema) noexcept
{
  return schema.kind == TSTypeKind::TSD || schema.kind == TSTypeKind::TSL ||
         schema.kind == TSTypeKind::TSB;
}

[[nodiscard]] inline TSEndpointSchema
forwarding_output_endpoint_schema(const TSValueTypeMetaData *schema) {
  if (schema == nullptr) {
    throw std::invalid_argument("Forwarding output endpoint requires a schema");
  }

  std::vector<TSEndpointSchema> children;
  if (schema->kind == TSTypeKind::TSB) {
    children.reserve(schema->field_count());
    for (std::size_t index = 0; index < schema->field_count(); ++index) {
      children.push_back(
          forwarding_output_endpoint_schema(schema->fields()[index].type));
    }
    return TSEndpointSchema::non_peered(schema, std::move(children));
  }
  // A fixed list's owner constructs every element, so each stable element can
  // carry its own forwarding tree. A dynamic list's size belongs to its
  // producer; before that producer grows the list there are no target
  // elements to walk, so its generic forwarding endpoint must remain peered
  // as one whole output. Owner-managed dynamic containers (for example the
  // outer result of dynamic TSL map_) opt into non_peered_list explicitly.
  if (schema->kind == TSTypeKind::TSL && schema->fixed_size() != 0) {
    return TSEndpointSchema::non_peered_list(
        schema, forwarding_output_endpoint_schema(schema->element_ts()));
  }
  return TSEndpointSchema::peered(schema);
}

/** Clear every forwarding leaf in a fixed structural output tree. */
inline bool clear_forwarding_output_tree(TSOutputView target,
                                         bool sampled = false) {
  if (target.forwarding()) {
    if (!target.forwarding_bound()) {
      return false;
    }
    if (sampled) {
      target.clear_forwarding_target_sampled();
    } else {
      target.clear_forwarding_target();
    }
    return true;
  }

  const auto *schema = target.schema();
  const std::size_t child_count =
      schema != nullptr && schema->kind == TSTypeKind::TSB
          ? schema->field_count()
      : schema != nullptr && schema->kind == TSTypeKind::TSL
          ? (schema->fixed_size() != 0 ? schema->fixed_size()
                                       : target.data_view().indexed_child_count())
          : 0;
  if (child_count == 0) {
    throw std::logic_error("Forwarding output tree has a non-forwarding leaf");
  }

  bool changed = false;
  for (std::size_t index = 0; index < child_count; ++index) {
    changed =
        clear_forwarding_output_tree(target.indexed_child_at(index), sampled) ||
        changed;
  }
  return changed;
}

/** Bind every forwarding leaf in a fixed structural output tree. */
enum class ForwardingSourceMode
{
  ResolveCurrentTarget,
  PreserveEndpoint,
};

inline bool bind_forwarding_output_tree_to_source(TSOutputView target,
                                                  const TSOutputView &source,
                                                  bool sampled = false,
                                                  ForwardingSourceMode source_mode =
                                                      ForwardingSourceMode::ResolveCurrentTarget) {
  TSOutputView effective_source = source.borrowed_ref();
  // Adapt only the terminal's root REF. Interior REF fields are part of the
  // structural endpoint contract and must remain visible to the forwarding
  // tree (REF[TSB[REF[...]]] -> TSB[REF[...]]).
  if (source.bound() && source.schema() != nullptr &&
      source.schema()->kind == TSTypeKind::REF && target.schema() != nullptr &&
      time_series_schema_equivalent(
          source.schema()->referenced_ts(),
          target.schema())) {
    effective_source =
        source.binding_for(*target.schema()).view(source.evaluation_time());
  }

  if (target.forwarding()) {
    TSOutputView binding_source =
        source_mode == ForwardingSourceMode::ResolveCurrentTarget
            ? resolve_forwarding_source(std::move(effective_source))
            : std::move(effective_source);
    if (!binding_source.bound()) {
      return clear_forwarding_output_tree(std::move(target), sampled);
    }

    const TSOutputHandle before = target.forwarding_target();
    if (before.same_as(binding_source.handle())) {
      return false;
    }
    if (sampled) {
      target.bind_forwarding_target_sampled(binding_source);
    } else {
      target.bind_forwarding_target(binding_source);
    }
    return true;
  }

  if (!effective_source.bound()) {
    return clear_forwarding_output_tree(std::move(target), sampled);
  }
  auto &registry = TypeRegistry::instance();
  if (!time_series_schema_equivalent(
          registry.dereference(target.schema()),
          registry.dereference(effective_source.schema()))) {
    throw std::logic_error("Forwarding output tree source schema '" +
                           (effective_source.schema() != nullptr
                                ? std::string{effective_source.schema()->name()}
                                : std::string{"<untyped>"}) +
                           "' does not match endpoint schema '" +
                           (target.schema() != nullptr
                                ? std::string{target.schema()->name()}
                                : std::string{"<untyped>"}) +
                           "'");
  }

  const auto *schema = target.schema();
  const std::size_t child_count =
      schema != nullptr && schema->kind == TSTypeKind::TSB
          ? schema->field_count()
      : schema != nullptr && schema->kind == TSTypeKind::TSL
          ? (schema->fixed_size() != 0 ? schema->fixed_size()
                                       : target.data_view().indexed_child_count())
          : 0;
  if (child_count == 0) {
    throw std::logic_error("Forwarding output tree has a non-forwarding leaf");
  }

  bool changed = false;
  for (std::size_t index = 0; index < child_count; ++index) {
    changed = bind_forwarding_output_tree_to_source(
                  target.indexed_child_at(index), effective_source.indexed_child_at(index),
                  sampled, source_mode) ||
              changed;
  }
  return changed;
}
/**
 * Resolve a binding source path from an outer INPUT root to the bound
 * OUTPUT it addresses. A trailing ``ts_key_set_path_component`` applies on
 * the output side after the bound-output hop (the input layer has no
 * key-set surface).
 */
[[nodiscard]] inline TSOutputView
walk_source_to_output(TSInputView root, std::span<const std::size_t> path) {
  if (!path.empty() && path.back() == ts_key_set_path_component) {
    auto bound = walk_ts_path(std::move(root), path.first(path.size() - 1))
                     .bound_output();
    if (!bound.bound()) {
      return {};
    }
    if (bound.schema() != nullptr &&
        bound.schema()->kind == TSTypeKind::REF) {
      const auto *target = TypeRegistry::instance().dereference(bound.schema());
      if (target == nullptr || target->kind != TSTypeKind::TSD) {
        throw std::logic_error(
            "Nested graph key-set source must resolve to a TSD");
      }
      bound = bound.binding_for(*target).view(bound.evaluation_time());
    }
    return bound.as_dict().key_set();
  }
  return walk_ts_path(std::move(root), path).bound_output();
}

[[nodiscard]] inline TSOutputView
walk_source_to_output(TSInputView root, const std::vector<std::size_t> &path) {
  return walk_source_to_output(std::move(root),
                               std::span<const std::size_t>{path});
}

[[nodiscard]] inline bool nested_input_binding_has_sampled_active_target(
    const NodeView &node, DateTime evaluation_time,
    std::span<const std::size_t> target_path) {
  const auto *schema = node.schema();
  if (schema == nullptr || schema->input_schema == nullptr) {
    return false;
  }
  const bool accepts_invalid =
      schema->valid_inputs.has_value() && schema->valid_inputs->empty();

  auto input = node.input(evaluation_time);
  if (!target_path.empty()) {
    bool active = input.active();
    for (const std::size_t component : target_path) {
      input = input.indexed_child_at(component);
      active = active || input.active();
    }
    return active && (input.valid() || accepts_invalid);
  }
  if (schema->input_schema->kind != TSTypeKind::TSB) {
    return input.active() && (input.valid() || accepts_invalid);
  }

  const auto bundle = input.as_bundle();
  for (std::size_t slot = 0; slot < schema->input_schema->field_count();
       ++slot) {
    const auto child = bundle[slot];
    if (child.active() && (child.valid() || accepts_invalid)) {
      return true;
    }
  }
  return false;
}

/**
 * Schedule active consumers of sampled boundary inputs after a newly
 * constructed child graph starts. A valid source supplies its current
 * value; a node with an explicit empty validity gate also samples an unset
 * source so it can publish an identity such as an empty collection.
 * Startup first establishes the actual endpoint activity, including custom
 * nodes whose activity is more precise than their root schema.
 *
 * This is the explicit sampled-initialization step owned by nested graph
 * lifecycle code. Input activation only establishes subscriptions; it does
 * not schedule a node or fabricate modified state.
 */
inline void schedule_sampled_input_consumers(
    const GraphView &child, DateTime evaluation_time,
    std::span<const NestedGraphInputBinding> bindings) {
  for (const NestedGraphInputBinding &binding : bindings) {
    const auto node = child.node_at(binding.target.node);
    if (nested_input_binding_has_sampled_active_target(node, evaluation_time,
                                                       binding.target.path)) {
      child.schedule_node(binding.target.node, evaluation_time);
    }
  }
}

inline void schedule_sampled_input_consumers(
    const GraphView &child, DateTime evaluation_time,
    const std::vector<NestedGraphInputBinding> &bindings) {
  schedule_sampled_input_consumers(
      child, evaluation_time,
      std::span<const NestedGraphInputBinding>{bindings});
}
} // namespace hgraph

#endif // HGRAPH_RUNTIME_NESTED_BINDINGS_H
