# hgraph Debugger Printers

This directory contains opt-in debugger summaries and expandable navigation for
hgraph's type-erased runtime data structures.

The printers currently cover the common type-erasure ABI:

- `hgraph::SchemaHeader`
- `hgraph::TypeRecord`
- `hgraph::AnyPtr`
- every `hgraph::TypedPtr<Family, Role>` specialization, including the public
  `ValuePtr`, time-series pointer, `NodePtr`, `GraphPtr`, `ExecutorPtr`, and
  `ClockPtr` aliases

They are intentionally read-only. They inspect fields from debug info and
inferior memory but never call methods in the debugged process. A pointer
summary includes its state and access mode, family/role/kind, semantic and
implementation labels, record address, and data address. Expanding it exposes
the canonical `TypeRecord`, then its `SchemaHeader`, plan, ops, and optional
debug descriptor pointers.

Records carrying stable data-only debug descriptors expose bool,
signed/unsigned integer, and 32/64-bit floating-point payloads directly. Fixed
tuples and bundles expand into child `AnyPtr` values using descriptor offsets;
unset fields appear typed-null through the published validity bitmap. Supported
sequence and stable-slot layouts expand lists, sets, mutable maps, graph nodes,
node state/scalars, and nested child graphs. Slot state comes from the public
pointer table and bitmap ABI, so the adapters do not decode standard-library
container internals. Nullable dynamic validity, compact maps with value holes,
endpoint owners, and unsupported atomics remain explicitly opaque. No payload
is inferred from semantic labels, C++ template names, or private container
layouts.

## LLDB

From the repository:

```lldb
(lldb) command script import /path/to/hgraph/tools/debugger/hgraph_lldb.py
```

From an installed CMake package:

```lldb
(lldb) command script import /install/prefix/share/hgraph/debugger/hgraph_lldb.py
```

For a per-checkout setup, add the import line to `.lldbinit` and enable local
LLDB init loading if your LLDB configuration requires it.

## GDB

From the repository:

```gdb
(gdb) source /path/to/hgraph/tools/debugger/hgraph_gdb.py
```

From an installed CMake package:

```gdb
(gdb) source /install/prefix/share/hgraph/debugger/hgraph_gdb.py
```

## Notes

- Build with debug information enabled. Optimized builds may remove or fold
  fields that the printers inspect.
- Family-specific kind values are decoded only after reading the family from
  the common schema header, so overlapping numeric kind values are unambiguous.
- Invalid magic, ABI, family, role, access, and required pointers are reported
  as invalid or malformed instead of being cast to a guessed representation.
