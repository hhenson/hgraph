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
   * - ``SIGNAL``
     - A tick notification whose source value is intentionally ignored
     - ``True`` when the signal ticks

``Size`` and ``WindowSize`` carry values that ordinary Python generic syntax
cannot express directly. ``Frame`` and ``Array`` are native scalar schemas for
Arrow tables and shaped numeric arrays. See :doc:`../user_guide/data_and_analytics`
for their storage and conversion rules.

For ``TSW``, one size supplies both the maximum and minimum window period:
``TSW[int, WindowSize[3]]`` is the same type as
``TSW[int, WindowSize[3], WindowSize[3]]``. Supply the third parameter only
when the window should become valid before it reaches its maximum size.

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

Python node inputs are lazy, callback-scoped ``TimeSeries`` views over native
runtime storage. Reading ``value`` converts the current value to Python;
``delta_value`` converts only the current change. Use ``valid`` before relying
on an optional input, ``modified`` to detect a tick in the current cycle, and
``last_modified_time`` when the tick time matters. ``make_passive`` and
``make_active`` change whether future input ticks schedule the node. Every
input also exposes ``all_valid``, ``owning_node``, ``owning_graph`` and
``is_reference()``. These are live views, not copies of the runtime endpoint.

Collection types add shape-specific access:

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Input kind
     - Additional runtime access
   * - ``TSS``
     - ``values()``, ``added()``, ``removed()``, ``was_added(value)``,
       ``was_removed(value)``, iteration, membership and length
   * - ``TSD``
     - ``keys()``/``values()``/``items()``; corresponding ``modified_*``,
       ``valid_*``, ``added_*`` and ``removed_*`` views; ``get(key)``, child
       lookup, ``key_from_value(child)``, ``key_set``, iteration, membership
       and length
   * - ``TSL`` / ``TSB``
     - ``keys()``/``values()``/``items()`` and the ``modified_*`` and
       ``valid_*`` forms; child lookup, ``key_from_value(child)``, iteration
       and length. Bundles also provide named-field access and ``as_schema``
   * - ``TSW``
     - configured ``size`` and ``min_size``; current occupancy through
       ``len(window)``; NumPy-compatible ``value`` and ``value_times``
       buffers; ``first_modified_time``, ``has_removed_value`` and
       ``removed_value``
   * - ``REF``
     - the common input API; ``value`` is the current reference token, with
       safe ``is_empty``, ``has_output`` and ``is_valid`` metadata; compound
       references also expose read-only ``items``
   * - ``SIGNAL``
     - the common input API; ``value`` and ``delta_value`` are ``True`` on a
       signal tick

A reference token deliberately does not expose ``output``. Direct traversal
from ``REF.value`` to a live output endpoint is not and will not become part of
the supported Python API.

Every input view exposes lazy, read-only topology diagnostics:
``parent_input``, ``has_parent_input``, ``bound``, ``has_peer`` and ``output``.
The last property is the input's directly bound peer and returns a guarded
``TimeSeriesOutput`` view, or ``None``. That diagnostic output supports value,
delta, collection and window inspection but no value mutation, endpoint
binding or subscription control. It is distinct from the deliberately
excluded ``REF.value.output`` path.

For a ``TSW``, ``size`` and ``min_size`` describe configuration rather than
current occupancy. They are integers for tick-count windows and
``datetime.timedelta`` values for duration windows. ``value`` is a one-
dimensional NumPy array of retained values, and ``value_times`` is a matching
``datetime64[us]`` array in the same oldest-to-newest order:

.. testcode::

   import numpy as np
   from hgraph import MIN_ST, TS, TSW, WindowSize, compute_node, graph, to_window
   from hgraph.test import eval_node

   observations = []

   @compute_node
   def inspect_window(window: TSW[int, WindowSize[3], WindowSize[1]]) -> TS[int]:
       assert isinstance(window.value, np.ndarray)
       assert isinstance(window.value_times, np.ndarray)
       assert window.value_times.dtype == np.dtype("datetime64[us]")
       assert len(window.value) == len(window.value_times) == len(window)
       observations.append((window.value.copy(), window.value_times.copy()))
       return int(window.value[-1])

   @graph
   def window_example(value: TS[int]) -> TS[int]:
       return inspect_window(to_window(value, 3, 1))

   assert eval_node(window_example, [1, 2, 3, 4]) == [1, 2, 3, 4]
   assert observations[-1][0].tolist() == [2, 3, 4]
   assert observations[-1][1].tolist()[0] > MIN_ST

Input, output, node, graph, clock and engine views expire when the callback
returns. Do not retain them in ``STATE`` or pass them to another thread.

Output and state views
~~~~~~~~~~~~~~~~~~~~~~

Returning a non-``None`` value is the ordinary way to publish a node output.
Annotate the parameter named ``_output`` with ``TS_OUT[T]`` when the callback
also needs its existing output or must mutate a collection output directly:

.. testcode::

   from hgraph import TS, TS_OUT, compute_node
   from hgraph.test import eval_node

   @compute_node
   def change_only(value: TS[int],
                   _output: TS_OUT[int] = None) -> TS[int]:
       if _output.valid and _output.value == value.value:
           return None
       return value.value

   assert eval_node(change_only, [1, 1, 2]) == [1, None, 2]

Output views expose the same observational collection and window methods as
inputs. They additionally provide native mutation: assign ``value``, call
``invalidate()`` or ``clear()``, use ``add()``/``remove()`` for ``TSS``, and
use ``get_or_create(key)``, item assignment/deletion or ``pop(key)`` for
``TSD``. Mutating a child view publishes through the same C++ output; it does
not build a second Python-side time-series implementation.

``STATE`` preserves ordinary Python state for one node instance.
``RECORDABLE_STATE[Schema]`` instead supplies a native output-backed view whose
writes participate in record/replay. Both are distinct from graph-scoped
``GlobalState``. Naked ``STATE`` provides attribute and item reads,
``as_schema``, ``keys()``, ``values()``, ``items()``, ``is_updated()`` and
``reset_updated()``. A typed ``STATE[T]`` is the single lazily constructed
``T`` instance. A recordable-state view provides named and indexed child
access, ``as_schema``, and each child's ``value``, ``valid`` and ``modified``
state.

Injectable parameters
~~~~~~~~~~~~~~~~~~~~~

Injectables are runtime services rather than time-series edges. Declare them
as parameters with a ``None`` default; graph callers do not supply them.

Python ``@graph`` functions are the wiring-time exception: they support only
``GlobalState`` and ``LOGGER``. The other injectables below are available only
to node callbacks because graphs do not exist at runtime.

.. list-table::
   :header-rows: 1
   :widths: 28 72

   * - Annotation
     - Callback value
   * - ``STATE`` / ``STATE[T]``
     - Per-node Python namespace or one lazily constructed ``T`` instance
   * - ``SCHEDULER``
     - Current node scheduler for absolute, relative and tagged events
   * - ``CLOCK`` / ``EvaluationClock``
     - Logical evaluation time and cycle timing
   * - ``EvaluationEngineApi``
     - Run interval, execution mode, cycle notifications and graceful stop
   * - ``GlobalState``
     - Guarded mapping over the graph's copied-in native state
   * - ``Traits``
     - Read-only access to graph wiring traits, including parent lookup
   * - ``LOGGER``
     - Narrow facade over the executor-owned native run logger
   * - ``NODE``
     - Current node identity, graph view and next-cycle notification

``SCHEDULER`` exposes the complete node-scheduling contract: inspect
``next_scheduled_time``, ``is_scheduled`` and ``is_scheduled_now``; call
``schedule(when, tag=None, on_wall_clock=False)`` with an absolute
``datetime`` or relative ``timedelta``; inspect or remove tagged events with
``has_tag()`` and ``pop_tag()``; cancel one event with ``un_schedule()``; and
cancel all events with ``reset()``. A tagged schedule replaces the previous
event with that tag. Calling ``un_schedule()`` without a tag removes the
earliest pending event.

``CLOCK`` provides ``evaluation_time``, ``now``, ``cycle_time`` and
``next_cycle_evaluation_time``. ``EvaluationEngineApi`` adds ``start_time``,
``end_time``, ``evaluation_mode``, ``evaluation_clock``, before/after-cycle
notifications, ``is_stop_requested`` and ``request_engine_stop()``.

The injected ``GlobalState`` is a mutable mapping view. It supports item read,
write and deletion, membership, iteration, length and truth testing, plus
``get()``, ``keys()``, ``values()``, ``items()``, ``setdefault()`` and
``pop()``. Mutations are copied back to the selected wiring-time
``GlobalState`` after execution.

``Traits.get_trait(name)`` searches the graph and its parent chain and raises
``ValueError`` when the trait is absent. ``get_trait_or(name, default)`` reads
the current graph's own value and otherwise returns ``default``. The injected
view is deliberately read-only; traits are immutable wiring metadata once the
graph is running.

``NODE`` exposes stable identity and diagnostic properties (``node_id``,
``node_ndx``, ``owning_graph_id``, ``label`` and ``node_type``), native
``is_started``/``is_starting``/``is_stopping`` state, and its read-only
``graph``. For breakpoint inspection it also provides ``scalars``, ``input`` /
``inputs``, ``output``, ``recordable_state``, ``error_output`` and a read-only
``scheduler`` state. These are resolved only when queried. ``signature`` and
``start_inputs`` are not reproduced from the old Python runtime. Output
properties return ``TimeSeriesOutput`` and scheduler inspection cannot
schedule or cancel events.

``NODE.notify()`` is deliberately not part of the Python interface;
``notify_next_cycle()`` remains the narrow convenience and the injected
``SCHEDULER`` is the explicit scheduling API. The graph view exposes identity,
label, nodes, ``parent_node``, read-only traits, lifecycle/evaluating state and
``evaluation_clock``. Graph and node execution or topology mutation is not
available through these views.

``LOGGER`` exposes only ``debug()``, ``info()``, ``warning()``, ``error()``,
``exception()``, ``critical()`` and ``log()`` with normal logging-style
``%`` arguments. It is callback-scoped and backed by the graph's native
``LoggerView`` and executor-owned spdlog logger. A Python
``GraphConfiguration.graph_logger`` is an optional sink for that native
logger; it is not the object injected into the node.
