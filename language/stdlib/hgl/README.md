# HGL standard-library migration prototypes

Status: design-review corpus; not part of the compiler or shipped library

This folder separates the first compiled HGL library slice from the broader
inputs needed to migrate hgraph's existing C++ nodes and graphs.

- [`hgraph/standard.hgl`](hgraph/standard.hgl) is real HGL source. It is built,
  registered, installed, and runtime-tested as the parallel `hgraph.std`
  prototype described by [`hgraph/README.md`](hgraph/README.md).
- [`hgraph/native.hgl`](hgraph/native.hgl) is the compiled C++ value/view
  substrate used by that source.
- [`hgraph/std`](hgraph/std) contains recovered operator-family designs for
  review. Those files are deliberately excluded from the build.

The family prototypes preserve the breadth of the earlier standard-library
extraction without implying compiler or runtime support. Every file begins
with `DESIGN PROTOTYPE`, and every unresolved form names an entry in the
[`HGL-MIG` requirements ledger](../requirements.md). `HGL-MIG-*` identifiers
belong only to this migration-design corpus; they are intentionally distinct
from the `HGL-LIB-*` implementation blockers beside the compiled
`standard.hgl` slice.

## Reading the prototypes

The prototypes mix three kinds of input:

1. ordinary syntax and semantics already accepted by the language;
2. candidate HGL implementations that still need behavioral and parity
   review; and
3. visibly provisional spellings used to expose an unresolved design need.

They are not passing examples. Unsupported forms must continue to be rejected
by the compiler until their associated requirement is agreed and implemented.
The executable compiler corpus remains under [`language/examples`](../../examples),
while design fixtures for individual language decisions remain under
[`language/stdlib/examples`](../examples).

`when { ... }` uses the implemented default handler policy: any temporal input
may activate the handler and every temporal input must be top-level valid.
The superseded design-only `when-defaults` fixture has not been restored;
[`language/examples/when-defaults.hgl`](../../examples/when-defaults.hgl) is
now the executable source of truth.

## Proposed module layout

The review corpus mirrors the public C++ operator families:

- [`arithmetic.hgl`](hgraph/std/arithmetic.hgl)
- [`comparison.hgl`](hgraph/std/comparison.hgl)
- [`collection.hgl`](hgraph/std/collection.hgl)
- [`control.hgl`](hgraph/std/control.hgl)
- [`conversion.hgl`](hgraph/std/conversion.hgl)
- [`stream.hgl`](hgraph/std/stream.hgl)
- [`temporal.hgl`](hgraph/std/temporal.hgl)
- [`text-io.hgl`](hgraph/std/text-io.hgl)
- [`frames.hgl`](hgraph/std/frames.hgl)

The illustrative `module hgraph.std part ...` header is itself unresolved. It
records the need for maintainable multi-file ownership of one operator module;
it is not accepted HGL syntax. A package manifest may ultimately solve that
need without adding a `part` clause.

## Migration boundary

Moving a candidate from this corpus into compiled library source requires:

- agreement and implementation of every referenced `HGL-MIG` requirement;
- a declaration-by-declaration audit against the authoritative C++ operator;
- formatted, readable generated C++ using public hgraph contracts;
- native and Python behavioral parity, including lifecycle and delta behavior;
- operator registration under the existing identity; and
- removal or explicit native-kernel classification of the former C++
  implementation.

The recovered [`inventory`](../inventory.md) is a historical checkpoint, not a
claim that its counts match the current headers. It should be refreshed from
the authoritative public C++ surface before migration planning begins.
