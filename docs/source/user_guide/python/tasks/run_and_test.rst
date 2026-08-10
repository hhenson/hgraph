Run and test a graph
====================

Test a node or graph
--------------------

``hgraph.test.eval_node`` turns lists into per-cycle inputs. ``None`` means no
tick during that cycle:

.. testcode::

   from hgraph import TS, compute_node
   from hgraph.test import eval_node

   @compute_node
   def square(value: TS[int]) -> TS[int]:
       return value.value ** 2

   assert eval_node(square, [2, None, 4]) == [4, None, 16]

Use keyword arguments for scalar wiring configuration and additional lists for
time-series inputs. ``__start_time__``, ``__end_time__`` and
``__evaluation_mode__`` control the test run; diagnostic options use the same
double-underscore convention documented in :doc:`debug_and_profile`.

Run an application
------------------

Create a top-level ``@graph`` and pass it to ``evaluate_graph`` with a
``GraphConfiguration``:

.. testcode::

   from hgraph import GraphConfiguration, const, debug_print, evaluate_graph, graph

   @graph
   def application():
       debug_print("answer", const(42))

   evaluate_graph(application, GraphConfiguration())

.. testoutput::

   answer: 42

Simulation mode advances to scheduled event times without waiting. Real-time
mode aligns evaluation with the wall clock. Set the mode and run interval once
in ``GraphConfiguration`` rather than introducing timing branches inside
nodes.

Configure graph-scoped state
----------------------------

Use ``GlobalContext`` while wiring when an application must install services,
record/replay settings or other graph-scoped configuration. ``GlobalState`` is
copied into the executor before execution and copied back afterwards for the
Python caller. It is preferable to process globals because multiple graph
engines can coexist.
