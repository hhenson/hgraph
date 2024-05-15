Quick Start
===========

The minimum python version currently supported is Python 3.11.

Use pip to install the hgraph package:

```bash
pip install hgraph
```

Now try the hello world example:

[hello world](hello_world.md)

You have now successfully run your first HGraph program.

Note: All examples are also available as python scripts in the docs/quick_start folder of the
project. This is great to try out the examples and play around with them.

Next, let's take a look as simple [graph and nodes](graphs_and_nodes.md).

A design principle of HGraph is that code should be built from small reusable nodes, with 
business logic being largely implemented using graph wiring. The nodes should be
well described in terms of pre- and post-conditions, and should be well tested.
To facilitate this, HGraph provides a set of helpful tools to make node testing easy.

[Testing nodes](node_testing.md).

The type system also supports the concept of generics. Generics are a useful way to 
describe a generic type. The generic types describe the constraints that the function
can support. For more information on generics, see the [generics](generics.md) page.

Nodes support life-cycle methods, namely start and stop. These methods are called when
the node is started prior to evaluation and when the node is stopped after evaluation.

Here are details on the [life-cycle](life_cycle.md) methods.

Next steps, we take a look at some of the more advanced features of node construction.

[Using injectable attributes](injectable_attributes.md).

Now we take a look at some of the more advanced graph wiring features.

[map_, reduce, switch](map_reduce_switch.md).

Occasionally, there will be times when it is useful to create a cycle in the graph. Where
data from a computation is required as an input back into a computation. An example of this
is when we compute a position and require it as an input into the positions calcuation.

There are enough of these kinds of examples that providing a mechanism to solve this is 
provided by the framework. We call this the [``feedback``](feedback.md).

Up till now we have focused on SIMULATION-based examples. Obviously the framework is
designed to work in both SIMULATION and REAL_TIME modes. So almost everything
that works in one will work in the other. However, there is one thing that only
works in REAL_TIME mode, namely the PUSH source node.

Here is an example of the [push source node](push_source_node.md) in python.

Of course there is also the problem of dealing with exceptions.
Here is how hgraph can [capture and integrate exception handling](exception_handling.md).
