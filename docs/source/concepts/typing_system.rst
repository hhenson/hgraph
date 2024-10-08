Typing System
=============

HGraph is a type-checked solution. That is, unlike standard Python, HGraph
requires the user to provide fully defined types for all of it's functions.
These types are checked to ensure that that edges (connections from an output
to an input) are type checked. Additionally, at runtime the output will
check to ensure the value been set is of the correct type. This check
is a light weight instance of check, so contents of collection classes can
still be in-correct (for example).

It is possible to turn off type-checking in production to improve performance.
But it is valuable to run in development, testing and UAT/PRE environments.

Typing is split into two key types, namely scalar and time-series types.
Scalar types refer to types that have time dimension. Time-series types refer
to types that have a time dimension.

Time-series types ultimately decompose into scalar types for their values.

Examples of a scalar type include:

::

    - bool
    - date
    - datetime
    - str
    - int
    - tuple[str, ...]
    - tuple[int, float]
    - frozenset[int]
    - frozendict[str, int]

For time-series types, the following are currently supported:

::

    - TS[int]  # A time-series of integer values
    - TSS[str]  # A time-series set of string values
    - TSL[TS[int], Size[2]]  # A time-series list of length 2 containing TS[int] entries
    - TSB[MySchema]  # A time-series bundle, or named collection of time-series values
    - TSD[str, TS[int]]  # A time-series dictionary of time-series elements.
    - REF[TS[int]]  # A reference to a time-series output.

All time-series types are generics and require specification as to the contained
types.

A detailed description of the time-series types can be found here: :doc:`time_series_types`.

The typing system is represented by the class :class:`hgraph.HgTypeMetaData`.

HGraph has strong type support for a limited set of types, but will support almost any type by mapping to
Python object as the type. This is a catch-all type and is available to support arbitrary types.
The fundamental types are intended to be supported via native types when the runtime is implemented in a native
language such as C++, whereas the python object support is intended to be limited to Python code only.

Schema Based Types
------------------

Besides the standard types listed above there are two special types that are derived from the ``AbstractSchema``
type, namely: ``CompoundScalar`` and ``TimeSeriesSchema``. These types support the ability to define
named collection types, the ``CompoundScalar`` class supports defining scalar values made up of simple types
including ``CompoundScalar`` types.

This is an example of it's use:

::

    from dataclasses import dataclass
    from hgraph import CompoundScalar

    @dataclass(frozen=True)
    class MyScalar(CompoundScalar):
        p1: str
        p2: int

This type defines it's constituents in much the same way as for a dataclass. In fact it is generally a good practice
to wrap the class with the ``dataclass`` wrapper.

The reason for using this and not a standard dataclass is that this type will perform validation of the types and
maintains the type schema that can be used to perform type-checking and for implementing useful library functions
such as ``getattr_`` and other useful functions.

The ``TimeSeriesSchema`` is the parallel for time-series collections. In this paradigm, it's use is to define a schema
describing a collection of named time-series values. For example:

::

    from dataclasses import dataclass
    from hgraph import TimeSeriesSchema, TS

    @dataclass(frozen=True)
    class MySchema(TimeSeriesSchema):
        p1: TS[str]
        p2: TS[int]

This defines a schema that is supplied to the ``TSB`` to define a collection of time-series values. This is done
as below:

::

    @graph
    def my_function(tsb: TSB[MySchema]) -> TS[int]:
        return tsb.p2

In this scenario the ``TSB`` has a schema of ``MySchema``, this means it contains a collection of two time-series values
one called ``p1`` and the other ``p2``. The ``TSB`` still a time-series object with all the attributes that that comes
with, but in addition it is possible to dereference the time-series values from the instance object using the normal
attribute syntax.

The use of this strategy also ensures that the type-system is able to track the type of each usage (for example, in the
above example it can validate that the type of ``p2`` is ``TS[int]`` matching the expected return type of the graph).

See the details of the schema properties here: :class:`hgraph.AbstractSchema`.

Generics
--------

One of the features of the type system is the ability to define generic types. Generic types are similar to template
types found in many languages. Generics provide for limited constraint management, allowing a generic to specify
if it is a time-series type generic or scalar generic and in some cases providing a bound to validate the matches
against.

Generics allow the user and framework writer to specify logic that can operate on a number of potential input types
or return a value that is determined at wiring time.

All generics MUST be resolved prior to the running of a graph and are resolved during the wiring logic.

A generic is specified using a ``TypeVar`` with a bound. For example:

::

    from typing import TypeVar

    NUMBER_TYPE = TypeVar("NUMBER_TYPE", int, float)

This defines a generic type that can be either an integer or a floating point number. To write a function using the
type it can be used to substitute the part of the function that would normally use the ``int`` or ``float`` value. For example:

::

    @compute_node
    def add_(lhs: TS[NUMBER_TYPE], rhs: TS[NUMBER_TYPE]) -> TS[NUMBER_TYPE]:
        ...


This method can now be instantiated with ``TS[int]`` or ``TS[float]`` inputs.

The typing system will also ensure constraints are met, not only the on the types supplied, but also to ensure
consistency, thus in the example above, all input and output types are of type ``NUMBER_TYPE``, this adds a constraint
that lhs and rhs must both receive the same type as inputs. Thus if one side with provided a ``TS[int]`` and the other
a ``TS[float]``, then the typing system will raise an exception since the types do not match. If the code required
the types to be able to be defined independently Then it would need to define independent type vars, for example:

::

    from hgraph import clone_type_var

    NUMBER_TYPE_1 = TypeVar("NUMBER_TYPE_1", int, float)
    NUMBER_TYPE_2 = clone_type_var(NUMBER_TYPE_1, "NUMBER_TYPE_2")

    @compute_node
    def div_(lhs: TS[NUMBER_TYPE_1], rhs: TS[NUMBER_TYPE_2]) -> TS[float]:
        ...

In this case we define two instances of ``TypeVar`` with the same properties, but since they are distinct they are
treated as being unique and separate definitions. This will allow a ``TS[int]`` supplied to lhs and ``TS[float]``
to be provided to the rhs without any issue.

Generic types are resolved during wiring time. The system relies on the fact that outputs are always resolved. Thus
resolution occurs based on the resolved types of the outputs supplied to the functions inputs. The inputs are validated
against the supplied output ports.

When using a generic on the output, the generic must either be resolvable by having the generic defined in the inputs,
or the user must provide the resolution using the `[]` syntax as below:

::

    @compute_node
    def do_something(ts: TS[int]) -> TS[SCALAR]:
        ...

    @graph
    def my_graph():
        out = do_something[SCALAR: TS[int]](const(1))

In this case we define the type var ``SCALAR`` to be ``TS[int]``. This forces the resolution which would, in this case,
otherwise not be possible.

To make this easier to define it is possible to indicate which of the potential type-vars to use if no type-var is
provided, for example:

::

    @compute_node
    def do_something(ts: TS[int]) -> TS[Default[SCALAR]]:
        ...

    @graph
    def my_graph():
        out = do_something[TS[int]](const(1))


In this case we mark the type-var with Default, this will allow the user to drop the ``SCALAR:`` part when providing
the expected resolution, making the code a little cleaner.

Extending the Type Support
--------------------------

The type system was designed to be user extensible, at least for scalar types. This allows the framework user to add
to the types the system can resolve and correctly process. To contribute a new type, the user must implement an
appropriate ``HgScalarTypeMetaData`` instance representing the new type.

Then prior to the use of the type the type should be registered with the type system as below:

::

    from hgraph import HgScalarTypeMetaData

    HgScalarTypeMetaData.register_parser(MyNewTypeMetaData)

The advantage of registering the type is that it can become a fully functioning type including being able to participate
in type resolution. This is as apposed to the python object type which has very limited ability to integrate into the
type system.

