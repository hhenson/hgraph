Graph Construction
==================

There are three phases in the graph construction, namely:
* Wiring
* Building
* Runtime

The wiring phase is used to initially describe the graph using a standard
Python coding pattern, the graph will appear as functions being called
using variables (created by calling functions typically) and should be
readable (from a logic perspective) as imperative code.

In fact the wiring consists of wiring nodes being constructed and bound
as inputs to other wiring nodes. These nodes are responsible for 
capturing inputs and ultimately validating the shape of the graph as 
well as the typing of the inputs and outputs.

Once the user logic is resolved into a valid wiring graph, the next step
is to produce a builder graph. The builder graph takes the shape of the
optimised wiring graph and it's key distinction is that the graph is only
defined in terms of actual types (no generics or auto-resolved types).
The graph is described in topological sorted order and node-ids
for the graph are set. The builder nodes know exactly how to construct
a runtime instance node to evaluate the graph.

In the Python implementation the builder nodes are largely to provide the 
prototype structure, when re-implemented in C++ the builder graph
provides for supporting efficient memory allocation for constructing
nodes as well as improved performance when building dynamically 
instantiated nodes.
