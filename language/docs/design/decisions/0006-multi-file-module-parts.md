# ADR 0006: Explicit source parts form one logical module

Status: accepted and implemented

## Context

Large HGL libraries need to be split into reviewable source files without
turning each file into a new public module. Treating filenames as implicit
module fragments would make builds depend on directory discovery and would
leave declaration ownership invisible in source. Treating each file as a
module would instead create artificial import routes, descriptors, provider
identities, and registration lifecycles for one library.

## Decision

A source file may give its contribution to a module an explicit part name:

```hgl
module hgraph.std part arithmetic
```

A multi-file compilation lists one anchor file and one or more additional
parts. Every listed file must declare the same module identity and a unique
`part` name. The `part` keyword is hard reserved and its following name is an
ordinary identifier local to the source set; that name is not a namespace,
declaration, export, or nominal identity.

All declarations and imports in the listed files form one logical module and
are resolved and lowered once. Private declarations are therefore visible
across parts, and duplicate declarations are diagnosed across the complete
set. Each file is parsed independently before assembly, so its `use`
declarations must still precede its ordinary declarations. Imports contribute
to the shared module scope; they are not re-exports.

The compiler orders inputs lexically by part name to make diagnostics and
generated artifacts reproducible. Source semantics must not depend on that
order. Duplicate part names are errors, so filesystem paths are used only as a
stable diagnostic tie-breaker. Diagnostics and generated source annotations
retain the original filename, line, and column.

The command line names additional files explicitly with repeatable `--part`
options. `hgl_add_module()` exposes the same contract with one anchor in `HGL`
and the remaining files in `PARTS`. The anchor filename determines generated
artifact names; it does not receive special source visibility or affect the
module identity.

A file carrying `part` can still be checked or compiled alone. The label has
no semantic effect until the driver is given more than one source file.

## Consequences

- A large library can have one module descriptor, generated C++ namespace,
  registration provider, and public import path while retaining small files.
- Source ownership is explicit and independent of directory layout.
- A build system or future package manifest owns the complete part list; the
  compiler does not scan directories or infer missing parts.
- Moving a declaration between parts does not change its canonical identity.
- Parts do not introduce re-export or partial-module lifecycle semantics.

Automatic part discovery and a package manifest remain separate design work.
