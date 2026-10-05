HGL scalar collection keys
==========================

The publication profile admits every built-in scalar and declared enum as a
set member or sparse map key. Keys retain their exact ordinary schema: numeric
coercions, enum/integer interchange, and zone alias normalization are rejected.
Fixed list and tuple indices remain constant, in-range ``i64`` values.

The frontend compares materialized constants using scalar equality, including
one identity for positive and negative floating zero. Provider-dependent keys
remain cold recipes. Both backends materialize each key once, before its child
payload, and check one owning hash set across all arguments in the constructor.
This rejects duplicate keys and addition/removal overlap before target start.
Immutable ordinary local aliases reuse their retained key values, including
alias chains. Reusing an alias for a key, payload, or replay does not repeat its
provider-dependent initializer. Mutable and temporal bindings remain outside
the sparse-key constant grammar.
The prepared runtime storage uses the existing scalar hash and equality ops;
no ordering requirement or string conversion is introduced.

The contract is ``scalar-collection-keys.md`` in the pinned language specification.
Non-NaN floating keys include both infinities; NaN membership remains outside the
profile. Composite keys and runtime-dependent constructor keys remain excluded.
Shared and native fixtures exercise all scalar families, nominal enums, signed
zero, cold validation, key ordering independence, and repeated child ticks.
