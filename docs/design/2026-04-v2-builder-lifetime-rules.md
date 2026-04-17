# v2 Builder And Template Lifetime Rules

## Summary

`v2::NodeBuilder` is a build-time object. It sizes, aligns, and constructs node
storage, but a constructed `v2::Graph` does not depend on the builder for normal
runtime operation or destruction.

Long-lived immutable artifacts that must outlive built graphs are handled
through registries, following the same general model as the type registries.

## Rules

1. `NodeBuilder` state is build-time state.
2. A built `Graph` must be self-describing and self-destructing through data
   embedded in the node slab, especially `BuiltNodeSpec`.
3. Graph-specific code must not use `std::shared_ptr` for builder lifetime
   management. Copyable builders should clone their build-time state explicitly.
4. If an artifact must outlive the builder and any graph built from it, it must
   be owned by a registry that provides stable addresses.
5. Registry reset hooks are allowed for tests, but only when no live graph can
   still reference the registered artifact.

## Current Application

### NodeBuilder

`NodeBuilder` stores its family-specific state as uniquely owned type-erased
data. Copies clone that state. Moves transfer ownership. This keeps builder
ownership explicit and avoids shared ownership in graph code.

### ChildGraphTemplate

`ChildGraphTemplate` is a long-lived immutable nested-graph artifact. It is
owned by `ChildGraphTemplateRegistry`, and nested builders / child instances
store only a pointer to the registry-owned template.

This means:

1. `ChildGraphTemplate::create(...)` registers the template and returns a stable
   pointer.
2. `nested_graph_implementation(...)` expects a registry-owned template
   pointer.
3. `ChildGraphInstance` keeps only a borrowed pointer to that registry-owned
   template.

## Rationale

This split keeps ordinary builders simple and ephemeral while giving nested
graph compilation a stable home for artifacts that genuinely need process-level
lifetime.
