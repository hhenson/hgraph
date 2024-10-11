Graph Run Loop
==============

The graph run loop consists of a configuration and a run loop function.

.. autoclass:: hgraph.EvaluationMode
    :members:
    :undoc-members:

.. autoclass:: hgraph.GraphConfiguration
    :members:
    :undoc-members:

.. autofunction:: hgraph.evaluate_graph

For testing there is this dedicated run function that will allow for easy evaluation of a ``graph`` or ``node``:

.. autofunction:: hgraph.test.eval_node

The graph supports observing the state transitions, this allows for some useful utilities to be added, the extensions
must implement the ``EvaluationLifeCycleObserver`` interface shown below:

.. autoclass:: hgraph.EvaluationLifeCycleObserver
    :members:
    :undoc-members:

.. autoclass:: hgraph.test.EvaluationTrace
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: hgraph.test.EvaluationProfiler
    :members:
    :undoc-members:
    :show-inheritance:

Another useful observer is the wiring observer, this allows to hook into the wiring process and see what it is doing.
This is achieved by implementing:

.. autoclass:: hgraph.WiringObserver
    :members:
    :undoc-members:


.. autoclass:: hgraph.test.WiringTracer
    :members:
    :undoc-members:
    :show-inheritance: