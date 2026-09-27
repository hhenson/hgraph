# Shared sources

[Specification and examples](https://github.com/hhenson/hgraph_spec),
[validation code and results](https://github.com/hhenson/hgraph_spec_audit), and
[portable HGL standard library](https://github.com/hhenson/hgraph_std) have
separate owners. Compiler/runtime internals and target providers stay here.

Initialize the pinned packages after cloning or changing dependency pins:

```sh
python3 tools/shared_artifacts.py
python3 tools/shared_artifacts.py --check
```

Builds and tests read the submodules in `external/` directly. No compatibility
copies are created. Setup removes unchanged copies left by earlier versions;
it refuses to remove edited copies. Move those edits into their owning package
before retrying. Use `--offline` when dependencies are already initialized.
Commit updated submodule pins with the corresponding implementation change.

The standard library separates operator declarations and properties from
`impl/` bodies and `tests/` parts. Production builds assemble declarations with
implementations; validation additionally supplies the test parts.

From a clean commit, export the checkout and its pinned dependencies:

```sh
python3 tools/source_archive.py
```

The fixed output `dist/hgraph-source.tar.gz` builds without Git or submodule
initialization. GitHub-generated tag archives omit dependencies. Core runtime
builds without language tools or conformance tests do not need shared sources.
