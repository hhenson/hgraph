Compute / Sink Node
===================

Compute nodes are the main working nodes in the graph, they consume ticks and
produce results that are used by other nodes.

Sink nodes consume inputs but do not produce outputs, they can be viewed as taking
ticks off of the graph.

Compute nodes, as with sink nodes, take in one or more time-series inputs. They also,
unlike sink nodes, define an output time-series type. The output type holds the result
of the nodes computation.

Unlike ``graph`` functions, the ``compute_node`` and ``sink_node`` function is called each time
an active input is modified. It is possible to constrain, using the decorator,
which inputs are to be marked as active, it is also possible to mark which of the
inputs must be deemed valid for the node to be called.

.. note:: Where possible avoid creating your own nodes, it is best to operate
          at the ``graph`` level as much as is possible.

That said, there are a few times where using a compute node is a good idea, these include:

* When the atomic behaviour is not already available in the standard library.
* For performance reasons, for example when there are multiple steps in a computation
  that have no other dependencies, collapsing into a single node reduces the overhead.
* To have control over node activation. Doing so using graph methods can be more complicated.

The design philosophy of HGraph is to make nodes small and single purpose, with the
objective of creating simple, reusable building blocks. Each block should be
extensively tested and should always be designed from the perspective of the concept
the node represents rather than the expected use case.

Then when using the nodes in a graph, the user can rest assured that the bulk of the
testing complexity is handled and all that is to be focused on is the business logic.

