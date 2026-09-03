# hgraph language documentation

The documentation is split by audience:

- The [User Guide](user-guide/README.md) shows how language functions and
  canonical temporal types look.
- The [Developer Guide](developer-guide/README.md) explains how the compiler
  parses, checks, lowers, builds, and tests those programs.
- The design records capture architectural decisions, project boundaries, and
  the delivery roadmap.

The language is still a design preview. Examples describe the target first
vertical slice; the current `hgl` parses them (`hgl check`) but does not yet
resolve, check, or run them. Provisional syntax is called out in the guides so
that examples do not imply an implemented compatibility promise.

## Design records

1. [Architecture](design/architecture.md) — ownership, compiler pipeline,
   the two backends, and execution modes.
2. [Language model](design/language-model.md) — functions, nominal operators,
   generics, generic constraints and substitution, exports, canonical and
   rolling temporal types, nominal structs, optional fields, sparse deltas,
   runtime state, lifecycle, activation, output, tests and running, and syntax
   decisions.
3. [Modules and native extensions](design/modules.md) — how C++ packages become
   importable, contribute overloads, and participate in generated module
   initialization and deinitialization without exposing a general FFI.
4. [Roadmap](design/roadmap.md) — vertical slices and their acceptance gates.

An accepted change should update the relevant guide and its owning design
record together. The user guide is the source of truth for observable language
behavior; the developer guide is the source of truth for implementation
constraints.
