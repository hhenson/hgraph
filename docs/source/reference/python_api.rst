The supported Python surface
============================

Install ``hgraph`` and import the top-level package:

.. code-block:: python

   import hgraph as hg

The supported end-user surface consists of:

* names exported by ``hgraph`` for types, authoring decorators, wiring helpers,
  execution, services and diagnostics;
* standard operators exposed lazily from the native operator registry; and
* documented public submodules such as ``hgraph.temporal``, ``hgraph.test`` and
  ``hgraph.adaptors.tornado``.

The top-level ``__all__`` is the wildcard-import contract, not an exhaustive
operator catalogue. Operators such as ``add_`` and ``filter_`` are created on
first access through ``hgraph.__getattr__`` and then cached:

.. testcode::

   import hgraph as hg

   assert "TS" in hg.__all__
   assert "datetime" not in hg.__all__
   assert "add_" not in hg.__all__
   assert callable(hg.add_)
   assert hg.add_ is hg.add_

This distinction keeps ``from hgraph import *`` manageable without making the
dynamic operator names private. The :doc:`python_api_inventory` combines both
sources and is regenerated from a built wheel. Standard-library values are not
re-exported merely because the implementation uses them; import values such as
``datetime`` and ``timedelta`` from their defining modules.

Private boundary
----------------

``_hgraph`` and Python modules or attributes whose names begin with an
underscore are implementation details unless a public page explicitly says
otherwise. Runtime endpoint mutation, native wiring objects and registry
registration helpers are not an alternative Python authoring API.

The compatibility commitment applies to the public Python surface in the 0.8
release line. The native C++ authoring API is available for library authors and
performance-sensitive integrations, but remains source- and binary-provisional.

Typing and generated declarations
---------------------------------

The wheel includes a ``py.typed`` marker, a nanobind-generated ``_hgraph.pyi``
for native values, and generated declarations for lazy operators. Nanobind
reads structured binding signatures (including captured binding docstrings)
rather than parsing rendered ``__doc__`` text. HGraph augments that result with
the native operator registry because lazy operators are Python proxy objects,
not nanobind functions.

Operator overload selection still happens while the graph is wired, but the
typing declarations are not reduced to a common ``(*args, **kwargs)`` shape.
They contain every distinct native overload, including parameter names,
required and defaulted arguments, variadic and keyword-only boundaries, scalar
types, and whether the overload returns a wiring port or is a sink. Semantic
summaries come from the Doxygen comments on the public C++ operator
declarations, and every lazy operator has a runtime docstring combining that
summary with the native time-series patterns used for dispatch. Python's
``WiringPort`` is not yet schema-generic to static type checkers, so
relationships such as ``TS[T] + TS[T] -> TS[T]`` remain documented in those
native patterns while the runtime registry enforces them.

Operators implemented as explicit Python helpers, including ``map_``,
``reduce`` and ``switch_``, keep their handwritten public signatures; generated
lazy declarations do not replace them during type checking.

Regenerate and check the inventory from an installed development wheel with:

.. code-block:: bash

   python tools/api_inventory.py
   python tools/api_inventory.py --check
