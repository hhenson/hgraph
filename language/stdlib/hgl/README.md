# HGL standard-library migration prototypes

Status: compiled modules plus a separate, non-compiled design-review corpus

This folder separates the first compiled HGL library slice from the broader
inputs needed to migrate hgraph's existing C++ nodes and graphs.

- [`hgraph/standard.hgl`](hgraph/standard.hgl) is real HGL source. It is built,
  registered, installed, and runtime-tested as the parallel `hgraph.std`
  prototype described by [`hgraph/README.md`](hgraph/README.md).
- [`hgraph/native.hgl`](hgraph/native.hgl) is the compiled C++ value/view
  substrate used by that source.
- [`hgraph/control.hgl`](hgraph/control.hgl) is the accepted module part for
  variadic `merge`, `race`, `all_`, and `any_` contracts; their implementations
  remain native.
- [`hgraph/operators.hgl`](hgraph/operators.hgl) supplies 16 accepted
  arithmetic/comparison/Boolean contracts and 75 native-delegating primitive
  materializations under parallel `hgraph.operators.*` identities.
- [`hgraph/std`](hgraph/std) contains recovered operator-family designs for
  review. Their `.hgl.proposed` suffix makes them ineligible compiler inputs;
  they are deliberately excluded from the build.

The family prototypes preserve the breadth of the earlier standard-library
extraction without implying compiler or runtime support. Every file begins
with `DESIGN PROTOTYPE`, and every unresolved form names an entry in the
[`HGL-MIG` requirements ledger](../requirements.md). `HGL-MIG-*` identifiers
belong only to this migration-design corpus; they are intentionally distinct
from the `HGL-LIB-*` implementation blockers beside the compiled
`standard.hgl` slice.

The [status table](../requirements.md#progress-at-a-glance) records merged
progress and the exact remaining boundaries. In particular, module parts,
parameter-pack runtime lowering/cardinality/reflection, fixed system symbol
names, domain-bound algebraic property declarations, and runtime-node
collection mutation no longer need syntax design. The completed pack stack is
still awaiting integration into `main`; multiple aggregate runtime inputs,
operator identity binding, generic publication, and production parity remain.

## Reading the prototypes

The prototypes mix three kinds of input:

1. ordinary syntax and semantics already accepted by the language;
2. candidate HGL implementations that still need behavioral and parity
   review; and
3. visibly provisional spellings used to expose an unresolved design need.

They are not passing examples. `.hgl.proposed` is the repository marker for
an unaccepted design as a whole, even when individual declarations use accepted
syntax. HGL line comments use `#`; `//` now means floor division. Unsupported
forms must continue to be rejected by the compiler until their associated
requirement is agreed and implemented. Once a candidate is accepted, it moves
into compiled library source as `.hgl` and gains a reviewable generated-C++
validation snapshot.
Where primitive candidates have already graduated into `operators.hgl`, that
compiled source supersedes the old proposed implementation bodies. Remaining
family declarations show inventory breadth, not additional shipped coverage.
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

- [`arithmetic.hgl.proposed`](hgraph/std/arithmetic.hgl.proposed)
- [`comparison.hgl.proposed`](hgraph/std/comparison.hgl.proposed)
- [`collection.hgl.proposed`](hgraph/std/collection.hgl.proposed)
- [`control.hgl.proposed`](hgraph/std/control.hgl.proposed)
- [`conversion.hgl.proposed`](hgraph/std/conversion.hgl.proposed)
- [`stream.hgl.proposed`](hgraph/std/stream.hgl.proposed)
- [`temporal.hgl.proposed`](hgraph/std/temporal.hgl.proposed)
- [`text-io.hgl.proposed`](hgraph/std/text-io.hgl.proposed)
- [`frames.hgl.proposed`](hgraph/std/frames.hgl.proposed)

The `module hgraph.std part ...` headers use the accepted, implemented
multi-file module syntax from
[`ADR 0006`](../../docs/design/decisions/0006-multi-file-module-parts.md). The
complete source set is supplied explicitly by CLI and build tooling; part names
are ownership labels, not namespaces or export routes. These files retain the
`.hgl.proposed` suffix because other forms and blockers in them remain
provisional, not because `part` is provisional.

## Migration boundary

Promoting a candidate into a compiled **parallel prototype** requires accepted
source semantics for that candidate, generated-C++ snapshots and behavior tests,
and explicit documentation of any remaining identity/parity gaps. It need not
wait for unrelated extensions of a partially implemented requirement.

Completing **production migration** additionally requires:

- agreement and implementation of every `HGL-MIG` dependency the candidate uses;
- a declaration-by-declaration audit against the authoritative C++ operator;
- formatted, readable generated C++ using public hgraph contracts;
- native and Python behavioral parity, including lifecycle and delta behavior;
- operator registration under the existing identity; and
- removal or explicit native-kernel classification of the former C++
  implementation.

The recovered [`inventory`](../inventory.md) is a historical checkpoint, not a
claim that its counts match the current headers. The
[remaining-work checklist](../recovery.md#remaining-work) calls for a current
candidate-level audit before choosing a production migration slice.
