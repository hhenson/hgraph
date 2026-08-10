Types and schemas
=================

Time-series types
-----------------

The type expression says both what flows over an edge and how the runtime
stores and updates it.

.. list-table::
   :header-rows: 1
   :widths: 22 38 40

   * - Type
     - Meaning
     - Typical Python value
   * - ``TS[T]``
     - One scalar value changing over time
     - ``T``
   * - ``TSS[T]``
     - Set state plus additions and removals
     - ``frozenset[T]``
   * - ``TSD[K, V]``
     - Dynamic keyed collection of time-series values
     - delta ``dict`` at the node boundary
   * - ``TSL[V, Size[N]]``
     - Fixed-length collection of time-series values
     - ``tuple``
   * - ``TSB[Schema]``
     - Named bundle of time-series fields
     - schema-shaped mapping or record
   * - ``TSW[T, WindowSize[...]]``
     - Tick- or duration-based rolling window
     - window value for ``T``
   * - ``REF[V]``
     - Reference value naming another time-series output
     - ``TimeSeriesReference``

``Size`` and ``WindowSize`` carry values that ordinary Python generic syntax
cannot express directly. ``Frame`` and ``Array`` are native scalar schemas for
Arrow tables and shaped numeric arrays. See :doc:`../user_guide/data_and_analytics`
for their storage and conversion rules.

Named scalar and bundle schemas
-------------------------------

Use ``CompoundScalar`` for a native-layout scalar record and
``TimeSeriesSchema`` for named time-series fields. A standard dataclass can
also be used directly as a nominal scalar schema:

.. testcode::

   from dataclasses import dataclass
   from hgraph import TS, TSB, TimeSeriesSchema

   @dataclass(frozen=True)
   class Quote:
       symbol: str
       price: float

   class QuoteBundle(TimeSeriesSchema):
       symbol: TS[str]
       price: TS[float]

   assert repr(TS[Quote]) == "TS[Quote]"
   assert repr(TSB[QuoteBundle]) == "TSB[QuoteBundle]"

``compound_scalar`` and ``ts_schema`` create dynamic schemas when a class
declaration is inconvenient. ``register_python_object_type`` opts an annotated
non-dataclass class into the nominal scalar contract.

Generic markers
---------------

``SCALAR``, ``NUMBER``, ``TIME_SERIES_TYPE`` and their numbered variants link
positions in a generic wiring signature. ``OUT`` selects an output type,
``SIZE`` selects a fixed collection size, and ``AUTO_RESOLVE`` asks wiring to
resolve a scalar type argument. All generic variables must be concrete before
execution begins.

Node-facing runtime views
-------------------------

Inside a node, an input exposes ``value``, ``valid``, ``modified``,
``delta_value`` and ``last_modified_time``. Collection views add structural
access such as fields, keys and modified items. ``STATE``, ``SCHEDULER``,
``CLOCK``, ``LOGGER`` and ``EvaluationEngineApi`` are injectable parameters,
not time-series edges.
