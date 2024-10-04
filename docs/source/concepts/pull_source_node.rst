Pull Source Node
================

This source node is used to introduce ticks (or events) into the graph. The pull version
is used to introduce ticks from a source that operates on the graphs thread and can
determine the next event to schedule when requested.

Pull source nodes wrap sources such as databases, dataframes, constant values and
internal feedback loops.

Pull source nodes are active during simulation and real-time modes, unlike push
source nodes which are only active in real-time modes.

The pull source node is exposed in the Python API using the ``generator`` decorator.

A function decorated with this decorator will be evaluated during the start life-cycle
and is expected to return a Python generator which emits a tuple of scheduled time and
value to apply to the output at that time. For example:

::

    @generator
    def const(value: SCALAR) -> TS[SCALAR]:
        yield MIN_ST, value

This is a very naive example, but shows the key idea of what needs to be done.

