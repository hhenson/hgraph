# Context7 MCP Baseline Indexing Guide for HGraph

This repository includes a baseline configuration for Context7 MCP to improve indexing quality and retrieval for the HGraph project.

What you get:
- Prioritized indexing of core source code (src/hgraph) and top-level docs.
- Reduced noise from virtualenvs, caches, and build artifacts.
- Helpful chunking and summarization hints to produce higher-quality overviews.
- A short glossary of HGraph-specific terminology for better understanding and linking.

## Files added
- context7.yaml — root-level configuration used by Context7 MCP.
- docs/context7_indexing.md — this usage guide and high-level overview.

## Recommended indexing targets
- High priority: src/hgraph/** (core library)
- Medium priority: README.md/README.rst, docs/**
- Lower priority: examples/**, hgraph_unit_tests/**, test_hgraph/**

## Repo high-level overview
- src/hgraph: Core framework and runtime.
  - _types/: Type system and time-series type aliases (e.g., TS, TSD, TSS, REF, SCALAR, TIME_SERIES_TYPE).
  - nodes/: Built-in compute nodes and utilities (e.g., mesh utilities, operator implementations).
  - _impl/: Runtime engine, schedulers, wiring, evaluation clock, mesh node internals.
- docs/: Project documentation (how-tos, tasks, etc.).
- examples/: Small usage examples and demos.
- hgraph_unit_tests/ and test_hgraph/: Test suites. Useful for learning behaviors but usually lower priority for RAG.

## Glossary (for summarizers and retrieval)
- TS: Time Series scalar value at a given engine time.
- TSD: Time Series Dictionary mapping scalar keys → time-series values.
- TSS: Time Series Set or Sequence of time-series values.
- REF: Reference wrapper allowing indirection to a time-series container.
- SCALAR: Primitive or compound scalar type wrapped in a time-series.
- TIME_SERIES_TYPE: Generic placeholder for time-series types.
- compute_node: Decorator to declare a compute node.
- graph: Decorator to declare a computation graph.
- scheduler: Component driving evaluations over time deltas.

## Operator and node modules
When summarizing src/hgraph/nodes and src/hgraph/_impl, prefer documenting:
- Public decorators and their intent (compute_node, graph, etc.).
- Core runtime interactions (scheduler, engine evaluation clock, mesh node dependencies).
- Key internal utilities such as mesh subscription and dependency management logic.

Example: src/hgraph/nodes/_mesh_util.py contains mesh_subscribe_node, which subscribes a graph to a mesh by adding/removing graph dependencies on a PythonMeshNodeImpl and scheduling evaluation when necessary.

## Chunking and summarization
- Code: chunk by ~200 lines with ~20 lines overlap to preserve context across definitions.
- Markdown: ~2000 characters with ~200 characters overlap for coherent topic-level summaries.
- Summaries should emphasize signatures, responsibilities, side effects, and cross-module relationships.

## Running Context7 MCP
1. Ensure your Context7 tool can detect context7.yaml at the repo root.
2. Point the indexer at the repository root. The include/exclude and priorities in context7.yaml will guide ingestion.
3. After indexing, try queries like:
   - "How do I subscribe a graph to a mesh in HGraph?"
   - "What does TS/TSD/TSS mean in hgraph types?"
   - "Where are the built-in operators for time-series?"

## Customizing
- Adjust priorities in context7.yaml if you want tests or examples to be surfaced more often.
- Add per-directory overrides by placing .c7local.yaml files with narrower include/exclude and custom hints.
- Extend the glossary with domain-specific language relevant to your usage.

## Maintenance tips
- Keep README.md updated with a minimal usage example and links to API docs.
- Add short module-level docstrings to important files; these improve auto-summaries.
- Prefer descriptive function/class docstrings for public APIs; Context7 uses them in summaries.
