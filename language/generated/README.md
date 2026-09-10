# Generated C++ validation snapshots

This directory holds reviewable snapshots of the C++ that HGL generates for
the compiled standard-library modules. It is an acceptance fixture, not a
second implementation: the authoritative sources remain
[`stdlib/hgl/hgraph/native.hgl`](../stdlib/hgl/hgraph/native.hgl),
[`stdlib/hgl/hgraph/standard.hgl`](../stdlib/hgl/hgraph/standard.hgl), and
[`stdlib/hgl/hgraph/operators.hgl`](../stdlib/hgl/hgraph/operators.hgl).
Only accepted `.hgl` source is represented here. Review-only
`.hgl.proposed` designs are not compiler inputs and must not produce snapshots.

[`cpp/hgraph`](cpp/hgraph) contains the generated headers and translation units
that CMake actually compiles into `hgl::core_native` and
`hgl::standard_library`. The snapshot check normalizes only the release version
in the first-line generator banner. Operator identities, types, control flow,
registration, native calls, source locations, includes, and formatting must
otherwise match byte for byte.

Module descriptors are not copied here. Their canonical structure,
fingerprints, and compatibility rules have dedicated descriptor tests; the
fingerprints also intentionally include the generating hgraph release version.

Configure a language-enabled build, then check the snapshots with:

```sh
cmake --preset cpp --fresh -DHGRAPH_BUILD_LANGUAGE=ON
cmake --build --preset cpp --target hgl_check_generated_cpp_snapshots --parallel
```

After an intentional HGL or C++-emitter change, refresh and review them with:

```sh
cmake --build --preset cpp --target hgl_refresh_generated_cpp_snapshots --parallel
git diff -- language/generated/cpp/hgraph
```

CTest also registers `hgraph_language_generated_cpp_snapshots`, so a normal
language-enabled acceptance run detects an unrefreshed snapshot.
