Component Signature
===================

The component signature describes the interface shape of the component
being defined. All components (graph or node) follow the same basic rules.

The signature declares the following attributes:

* The name of the node
* The incoming edges (or time-series inputs)
* The node configuration (or scalar values)
* The outgoing edges (or time-series outputs)
* Documentation of the component

The component decorator provides additional meta-data, which will be
discussed on a type-by-type basis.

The basic component signature looks like this:

```python

@decorator
def component_name(ts1: TIME_SERIES_TYPE, ..., s1: str, ...) -> TIME_SERIES_TYPE_2:
    """
    Documentation
    """
    ... # component behavior
```

The ``@decorator`` is the appropriate component decorator that indicates
the type of component, for example: ``@graph`` or ``@compute_node``.

ALL arguments MUST use the type annotations to indicate the expected type,
this allows the component signature parser to build the appropriate
meta-data for components shape. There is no requirement as to ordering
of inputs (i.e. to separate time-series inputs from scalar inputs.), but
by convention we will often accumulate the time-series inputs first and the
scalar inputs last.

The scalar inputs are considered as configuration information to the 
component, the time-series inputs are the value inputs to the component.
Time-series inputs vary over time, scalar inputs are fixed at time of
wiring the node and may not change after that.

It is possible for components to take no arguments (typically a source node).

The output can take three forms, none in the case of components that
terminate the flow of the graph (typically sink nodes), a single output,
or an un-named bundle output. The latter is expressed as a dictionary or
values, for example:

```python
@graph
def my_component() -> {"out1": TS[str], "out2": TS[float]}:
    ...
```
