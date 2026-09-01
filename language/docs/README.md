# Design documentation

These records live with the language project because they describe the
language toolchain rather than the hgraph core runtime. They are the starting
point for an RFC process local to this project.

The initial records are:

1. [Architecture](design/architecture.md) — ownership, compiler pipeline, and
   execution modes.
2. [Language model](design/language-model.md) — phase rules, types, graphs,
   nodes, and provisional syntax.
3. [Modules and native extensions](design/modules.md) — how C++ packages become
   importable without exposing a general FFI.
4. [Roadmap](design/roadmap.md) — vertical slices and their acceptance gates.

Each document identifies settled constraints separately from provisional
syntax or packaging choices. A later accepted change should update the owning
record instead of creating a conflicting description elsewhere in the hgraph
repository.
