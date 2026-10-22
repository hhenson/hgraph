RFC 0049: Wiring dependency inspection
=====================================

Status: Proposed

Motivation
----------

Downstream adapters need to discover which declared input sources contribute to
an output without inspecting private node layouts or parsing diagnostic traces.
They also need to distinguish paths containing passive inputs. The native wiring
already retains source edges, but Python exposes neither traversal nor packed
input activity. This proposal adds one read-only query over that existing graph.

Contract
--------

``inspect_wiring_dependencies(output, sources)`` returns source indices in the
order supplied, each with a ``passive`` flag. Traversal stops at a matching source
boundary, including projections of that source. Sources not reached are omitted.
A source is passive only if every discovered path contains a passive input:
an entirely active path takes precedence over any number of passive paths.

The C++ API is declared in ``hgraph/types/wiring_inspection.h`` and returns owned
``WiringDependency`` records. Python exposes the same function and records from
``hgraph``. All input ports must belong to the live wiring being inspected.
Neither API reads runtime values, changes graph structure, or retains graph
pointers in its result. This is declared activity: later calls to runtime
``make_active`` or ``make_passive`` cannot be inferred during wiring.

Ordinary nodes use their schema's active input selectors. Packed-input
implementations provide ``WiringInputActivityOps`` to describe individual
consumer paths. Python nodes install this contract using their existing call
layout; core does not interpret Python configuration. The NodeOps ABI version
advances because its inspection contract gains this field.

``wiring_extension_state(key, factory)`` exposes the existing C++
``Wiring::acquire_extension_state`` lifetime to Python. Package-qualified keys
isolate callers; inline graph calls share the current wiring's state. It is
released with that wiring, including when composition fails. It must not be
used to retain the owning wiring object itself.

Supported boundary
------------------

Inline graphs, structural bundles/lists, projected source boundaries and bound
delayed inputs are supported. Cycles terminate and each producer is explored at
most once for each activity class. Structural inputs are inspected at their
consumer leaf paths, not at the packed root's activity.

Compiled child graphs are rejected unless their output is supplied as a source
boundary. Their current inspection view does not expose complete parent/child
input routing; guessing would misclassify passive paths. Unbound delayed inputs
and unresolved compiled inputs also fail explicitly. A caller can supply known
boundaries or defer inspection until bindings are resolved. Extending this to
compiled children requires a separate precise input-routing contract.

Validation and cost
-------------------

C++ and Python tests cover boundaries, source order, passive paths, mixed paths,
structural projections, delayed bindings, cycles and wiring-owned state.
The installed SDK consumer invokes the exported API. Runtime evaluation never
calls this query or its activity contract: there is no per-tick overhead.
Traversal memory is proportional to visited producers and pending edges; source
matching is linear in the number of supplied boundaries per visited port.
