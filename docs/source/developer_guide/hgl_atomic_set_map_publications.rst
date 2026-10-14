HGL atomic set and map publications
===================================

Complete ordinary sets and maps use explicit ``set<K>(items: [...])`` and
``map<K, V>(items: [key: value, ...])`` constructors. Empty constructors retain
their exact types. Members, keys, and values may be ordinary runtime expressions;
keys follow the admitted scalar-key profile.

Both backends independently retain each expression in written order and check a
key before evaluating its value. Duplicate keys fail without exposing a partial
container. The frontend diagnoses duplicates when their values are known.
Provider-dependent values retain the existing cold-context requirements.

An atomic set or map publication replaces the whole held value. Empty containers
are present values, equal replacements publish, and nested containers retain
independent ownership through eval, replay, recording, and struct defaults.
Sparse outer shapes may publish empty complete children. The implementation uses
the existing core set/map storage and equality; it adds no new mutation API.
Replacing a set or map field writes its owning parent slot. It does not require
or grant permission to mutate the retained child container in place.

The contract is ``atomic-set-map-publications.md`` in the pinned specification.
Optional fields, recursive nominal values, composite keys, NaN keys, and
invalidation retain their separate profile boundaries.

Ordinary dictionary lookup
--------------------------

The compiler implements the pinned ``nullable-map-lookup.md`` contract through
``PreparedValuePlan`` map operations. Strict lookup checks membership before
reading the child and preserves ``value.missing_key`` through the backend error
boundary. Safe lookup returns a typed absent observation or the existing child
view. It does not allocate a reference or publish a graph output.

The frontend reuses evaluation-local presence facts for inferred immutable
bindings. Generated bodies retain scalar observations until a payload read;
presence comparisons read only observation validity. An explicit fallback is an
ordinary argument evaluated once in written order and must have the exact map
value type. Atomic map inputs use their ordinary payload plan; temporal map
connection selection retains its existing wiring path.
