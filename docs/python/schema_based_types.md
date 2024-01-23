Schema Based Types
==================

HGraph supports the concept of a schema based data structures. These are very similar to the Python ``dataclass``. 
The schema object takes the form:

```python

@dataclass
class MySchema(ParentSchemaType):
    p1: type_1
    p2: type_2 = default_value

```

The schema types contain useful meta-data to describe the schema as well as validation of the data-types when
defined as well as when setting values. There are two key types of schema types implemented in HGraph, namely:
``CompoundScalar`` and ``TimeSeriesSchema``. These represent scalar and time-series compound types respectively.

## CompoundScalar

This represents a complex scalar object. It can be composed of scalar values, including instances of ``CompoundScalar``.

an example of this is:

```python
from hgraph import CompoundScalar
from dataclasses import dataclass

@dataclass(frozen=True)
class MyCompoundScalar(CompoundScalar):
    p1: int
    p2: str = "temp"
```

In this case we are constructing a compound type with two properties, where ``p2`` has a default value.

## TimeSeriesSchema

The time-series schema is used to define a complex time-series collection. All the properties must be time-series 
values. They can be any time-series types including ``TimeSeriesSchema`` objects. For example:

```python
from hgraph import TimeSeriesSchema, TS
from dataclasses import dataclass

@dataclass
class MyTimeSeriesSchema(TimeSeriesSchema):
    p1: TS[int]
    p2: TS[str]
```

However, unlike the ``CompoundScalar`` the ``TimeSeriesSchema`` is used as a property to the ``TSB`` time-series
data type.

For example:

```python
from hgraph import TSB, sink_node

@sink_node
def write(tsb: TSB[MyTimeSeriesSchema]):
    print("p1", tsb.p1.value)
```

Each element of the schema can be accessed directly from the ``TSB``, it is also possible to use the ``as_schema`` method
to return the ``TSB`` instance as though it were the schema type to make it easier to perform code completion, for 
example:

```python

tsb: TSB[MyTimeSeriesSchema]
tsb.as_schema.p1
```

There is a special ``TimeSeriesSchema``, namely the ``UnNamedTimeSeriesSchema``, which is capable of creating a 
``TimeSeriesSchema`` instance on the fly using either the ``create`` or ``create_resolved_schema`` methods.
This is useful when creating dynamic schema. Though this should be generally used via the ``ts_schema`` function.


