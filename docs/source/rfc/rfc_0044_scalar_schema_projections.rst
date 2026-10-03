RFC 0044: Scalar schema projections at wiring time
==================================================

:Status: Proposed, implementation under review
:Created: 2026-10-03
:Target: Shared C++ wiring type patterns

Problem
-------

An ordinary generic value may carry a complete originating temporal shape.
For example, a typed empty list of timed publication deltas must infer a replay
output from its element schema. Inferring from sparse payload storage loses
fixed extents and nominal origins; reading elements cannot handle empty lists.
The normative examples are DELTA-INFER and DELTA-TYPE in the shared ordinary
delta specification.

Contract
--------

``ScalarPattern::SchemaProjection`` holds an immutable ``TypePattern`` and a
pair of provider-owned metadata conversion functions: scalar schema to source
temporal schema, and source temporal schema to scalar schema. A projection
matches only when both conversions round-trip to the exact scalar schema.
The ordinary shared matcher then resolves the nested temporal pattern in the
output/type-carrier direction. Scalar substitutions and size substitutions copy
the nested pattern; ranking, coverage and variable discovery visit it.

A provider must reject schemas outside its formation domain, preserve exact
identity, and keep all conversion work at wiring time. There is no persistent
source-shape lookup table, runtime value inspection or per-tick callback. The
core owns only the relation and matching protocol, not provider formation rules.
HGL supplies the initial Held and Delta projections for its finite shape domain;
its former unresolved concrete-constraint workaround is removed rather than
retained as a parallel matching path. The same public C++ pattern supports
runtime-authored bridge candidates; no Python-specific value API is added.

``ScalarPattern::List`` separately represents ordinary lists, with an optional
exact fixed extent. Dynamic, fixed-zero, other fixed lists, variadic tuples and
arrays remain distinct. Element schemas are matched even when no list value
exists. Generic nominal scalar resolution interns resolved arguments and fields
together, preserving their origin and specialization.

Validation
----------

Native tests exercise empty replay inference, recording schema resolution,
nominal and fixed-extent delta identity, incompatible repeated bindings,
tuple/list separation, constrained variables, substitution and variable-use
reporting. Generated HGL integration and installed SDK compilation exercise
these public headers. The callbacks are reachable only through wiring patterns;
prepared value plans are responsible for runtime access, so no runtime hot-path
branch or additional lookup is introduced.
