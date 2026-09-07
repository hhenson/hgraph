# HGL standard-library source prototype

Status: conceptual migration corpus; not part of the compiler test suite

This folder is the proposed source root for the hgraph library implemented in
HGL. Files under `hgraph/std` are parts of one logical `hgraph.std` module so
operator identities remain compatible with the current native registry.

The source is intentionally not compiled yet. It combines accepted HGL syntax
with explicitly marked provisional forms needed to expose migration blockers.
Every provisional form references an entry in
[`requirements.md`](../requirements.md). The compiler must reject unsupported
forms until that requirement is designed and implemented.

## Explicit materialization policy

Closed implementation domains use generic source once and enumerate their
generated candidates with `instantiate`. For example, the arithmetic part
shares one implementation body between `i64` and `f64` while still advertising
and registering two concrete overload candidates. Those declarations exercise
implemented HGL syntax rather than a provisional library spelling.

That model is insufficient for truly open library candidates. `sample<T>` must
also work for downstream nominal schemas, and `len_<T, size>` cannot enumerate
every element type and fixed-list size when this provider is built. Such
templates are deliberately left without a fake finite materialization list and
marked `HGL-LIB-015` until the publication owner and portable representation of
open generic candidates are settled.

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
