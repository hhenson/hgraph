# hgraph language documentation

The documentation is split by audience:

- The [User Guide](user-guide/README.md) shows how language functions and
  canonical temporal types look.
- The [Developer Guide](developer-guide/README.md) explains how the compiler
  parses, checks, lowers, builds, and tests those programs.
- The design records capture architectural decisions, project boundaries, and
  the delivery roadmap.

The language is still a design preview. Examples describe the target first
vertical slice; the current `hgl` checks them (`hgl check`), runs composition
functions directly, compiles and loads the documented scalar runtime-node
subset for file-based `hgl test` and `hgl run`, and emits the same C++ through
`hgl emit-cpp`. Compiled file commands use a content-addressed native cache;
on Unix the REPL uses the same cache and transactionally replaces generated
runtime images as declarations join the session.
First-pass limits are listed in the user guide. Provisional syntax is called
out so examples do not imply an implemented compatibility promise.

## Design records

1. [Architecture](design/architecture.md) — ownership, compiler pipeline,
   the two backends, and execution modes.
2. [Language model](design/language-model.md) — functions, nominal operators,
   generics, generic constraints and substitution, exports, canonical and
   rolling temporal types, nominal and generic structs, abstract data families,
   generic construction, inherited defaults, optional fields, sparse deltas,
   runtime state, lifecycle, activation, output, tests and running, and syntax
   decisions.
3. [Compiler architecture](design/compiler-architecture.md) — source fidelity,
   pass contracts, typed HIR, hgraph IR, backend boundaries, and the parser
   migration.
4. [Modules and native extensions](design/modules.md) — how C++ packages become
   importable, contribute overloads, and participate in generated module
   initialization and deinitialization without exposing a general FFI.
5. [Native interface](design/native-interface.md) — constrained scalar kernels,
   opaque state, phase/effect/ownership metadata, and why ordinary HGL has no
   inline C++ escape.
6. [Documentation architecture](design/documentation.md) — audience boundaries,
   feature status, executable examples, and code documentation.
7. [Architecture decisions](design/decisions/README.md) — numbered decisions
   that constrain several compiler passes or artifacts.
8. [Roadmap](design/roadmap.md) — vertical slices, the compiler architecture
   stack, core-library migration, and acceptance gates.
9. [Distribution and deployment](design/distribution.md) — release train,
   package channels (Homebrew first), the relocatable native context, and
   what a host needs to run an HGL program.

An accepted change should update the relevant guide and its owning design
record together. The user guide is the source of truth for observable language
behavior; the developer guide is the source of truth for implementation
constraints.
