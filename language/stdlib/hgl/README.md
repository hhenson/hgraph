# HGL standard-library source prototype

Status: conceptual migration corpus; not part of the compiler test suite

This folder is the proposed source root for the hgraph library implemented in
HGL. Files under `hgraph/std` are parts of one logical `hgraph.std` module so
operator identities remain compatible with the current native registry.

The `hgraph.std` source is intentionally not compiled yet. It combines accepted
HGL syntax with explicitly marked provisional forms needed to expose migration
blockers. Its `hgraph.native` dependency is different: that module is compiled
from HGL into a real C++ library, installed as `hgl::core_native`, and exercised
by the package-backed [`core-native-library.hgl`](examples/core-native-library.hgl)
consumer. Every remaining provisional form references an entry in
[`requirements.md`](../requirements.md). The compiler must reject unsupported
forms until that requirement is designed and implemented.

Runtime handlers use the most compact agreed selector form. `when { ... }`
means any temporal input activates the handler after every temporal input is
valid. A handler writes only its residual predicate when those two defaults
still apply. Explicit `modified(...)` and `valid(...)` calls remain only where
the implementation deliberately selects different input subsets, such as
`sample` and the ordered per-input handlers in `merge`.

## Explicit materialization policy

Closed implementation domains use generic source once and enumerate their
generated candidates with `instantiate`. For example, the arithmetic part
shares one implementation body between `i64` and `f64` while still advertising
and registering two concrete overload candidates. Those declarations exercise
implemented HGL syntax rather than a provisional library spelling.

An `_` argument retains that generic in the published resolver signature. The
collection part consequently uses
`instantiate sum_<i64, _>, sum_<f64, _>`: its accumulator type must be concrete,
but list size is only a type marker and one candidate accepts every resolved
fixed size. Retention and body availability are independent.

`len_<T, size>` does not reify that marker. Its runtime body calls the real
native `len(value)` overload, which receives the live typed collection input
view and reads its current size. The marker still participates in overload
selection; it is neither stored in the node nor recovered from the schema each
tick. The same substrate implements `is_empty`. Because these bodies do not
need element, key, value, or window-bound generics, the prototype retains those
positions with `instantiate len_<_, _>, len_<_>, len_<_, _, _>` and the
equivalent `is_empty` declaration. Non-generic string candidates are published
directly.

That model is still insufficient for every open library candidate. `sample<T>`
must work for downstream nominal schemas, and other implementations may
genuinely need a selected generic as body-visible metadata. Such templates are
deliberately left without a fake finite materialization list and marked
`HGL-LIB-015` until the publication owner and portable representation of
reified open generics are settled.

## Why a separate source root

- [`examples`](../../examples/README.md) remains the executable compiler
  corpus.
- [`stdlib/examples`](../../examples) remains the language-design fixture
  corpus.
- `stdlib/hgl` is a prospective shippable library whose generated C++ will
  eventually replace hand-written algorithmic nodes and graphs.

Moving a file into the executable or production build requires its provisional
markers to be removed, generated C++ to remain readable, and native/Python
behavioral parity to pass.

## Module layout

The first extraction mirrors the public C++ operator families:

- [`arithmetic.hgl`](hgraph/std/arithmetic.hgl)
- [`comparison.hgl`](hgraph/std/comparison.hgl)
- [`collection.hgl`](hgraph/std/collection.hgl)
- [`control.hgl`](hgraph/std/control.hgl)
- [`conversion.hgl`](hgraph/std/conversion.hgl)
- [`stream.hgl`](hgraph/std/stream.hgl)
- [`temporal.hgl`](hgraph/std/temporal.hgl)
- [`text-io.hgl`](hgraph/std/text-io.hgl)
- [`frames.hgl`](hgraph/std/frames.hgl)

The declarations are an extraction aid, not a claim that every current C++
signature has been fully reproduced. Defaults, output resolvers, keyword packs,
policy constraints, and internal marker operators still require a declaration-
by-declaration audit before a module can replace the native provider.
