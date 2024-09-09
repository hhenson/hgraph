HGraph
======

HGraph is a framework for writing functional reactive programs.
The core concepts are described below:

[Core Concepts](concepts/forward_propagation_graph.md)

For the impatient, the following is a quick start guide to get you up and running:

[Quick Start](quick_start/quick_start.md)


Python API
----------

The front-end to the framework is exposed via a Python API. The developer makes use
of a Python DSL to describe the logic of their programs. The framework separates 
implementation of the runtime from the description of the program. This allows for
the runtime to have different implementations. The current default implementation
is a Python evaluation engine, but the end-goal is to have a C++ (or other languages, 
for example Rust) implementation of the runtime and library nodes.


[Program Anatomy](python/program_anatomy.md)

The HGraph defines a set of scalar and time-series types, these types are described in the
link below.

[HGraph Types](python/hg_types.md)

Framework Details
-----------------

For developers wishing to contribute to the HGraph framework, the following
documents provide details on the internals of the framework and further explanations
as to the inner workings of the framework. They may be useful for users to better 
understand the framework.

[Graph Runtime](concepts/graph_runtime.md)
