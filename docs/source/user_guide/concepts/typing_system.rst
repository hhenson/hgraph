Typing System
=============

HGraph checks graph types while wiring. Unlike ordinary Python execution,
nodes and graphs declare the scalar and time-series types that cross their
boundaries. Wiring rejects incompatible edges before the graph runs, and the
runtime checks values as they enter typed storage. Collection checks are
shallow at the boundary; their declared element types still determine wiring
and conversion.

It is possible to turn off type-checking in production to improve performance.
But it is valuable to run in development, testing and UAT/PRE environments.

Typing is split into two key types, namely scalar and time-series types.
Scalar types refer to types that do **not** have a time dimension. Time-series
types refer to types that **do** have a time dimension.

This distinction is the single most important idea in the type system, so it is
worth being precise about it. A scalar is a plain value: the integer ``3``, the
string ``"USD"``, the tuple ``(1, 2, 3)``. It is simply a value, and asking
"when did it change?" is not a meaningful question of the value itself.
A time-series is a value *plus* the time dimension: it knows what its value is
now, when that value last changed, whether it changed in this engine cycle, and
whether it has been set at all yet.

.. note:: "Scalar" here means *scalar with respect to time*, not "simple" or
          "single valued". A ``tuple[int, ...]`` or a ``frozendict[str, int]``
          is still a scalar type, even though it is a collection, because the
          type carries no time dimension of its own.

The two are not competing alternatives, they compose. Every time-series type
ultimately decomposes into scalar types for its values: ``TS[int]`` is a
time-series *of* the scalar type ``int``. The time-series supplies the time
dimension, the scalar supplies the value.

Where they appear in a signature also differs. Time-series types are what flow
along the edges of the graph, so they can be inputs and are the only thing that
can be an output. Scalar types are configuration, they are fixed when the node
is wired and cannot change during evaluation, so they can only be inputs:

.. testcode::

    from hgraph import compute_node, TS
    from hgraph.test import eval_node

    @compute_node
    def scale(ts: TS[float], factor: float) -> TS[float]:
        #          ^ time-series    ^ scalar     ^ time-series
        return ts.value * factor

    assert eval_node(scale, [1.0, 2.0, 3.0], factor=10.0) == [10.0, 20.0, 30.0]

Here ``factor`` is bound once, at wiring time, and is the same for the life of
the node, which is why it is supplied as a single value rather than a list of
ticks. ``ts`` changes over the life of the graph, and each change may cause this
function to be evaluated again, which is why it is supplied as a list. Note also
that inside a node ``ts`` is the time-series object, so we ask it for its
``value`` to get at the scalar, whereas ``factor`` is already a plain Python
value.

If you needed the scale factor to change during the run, it would have to become
a time-series too::

    @compute_node
    def scale(ts: TS[float], factor: TS[float]) -> TS[float]:
        return ts.value * factor.value

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

Python authors work with the public type expressions on this page. The native
runtime owns their parsed metadata; the old ``HgTypeMetaData`` Python class is
not part of the 0.8 public API.

HGraph has strong type support for a limited set of types, but will support almost any type by mapping to
Python object as the type. This is a catch-all type and is available to support arbitrary types.
The fundamental types are intended to be supported via native types when the runtime is implemented in a native
language such as C++, whereas the python object support is intended to be limited to Python code only.

Schema Based Types
------------------

Two public base classes describe named records: ``CompoundScalar`` for scalar
records and ``TimeSeriesSchema`` for bundles of time-series values. The 0.5
``AbstractSchema`` implementation base is no longer a supported authoring
type. Define one of the concrete public classes, or use a standard dataclass
as a nominal scalar schema.

This is an example of it's use:

::

    from dataclasses import dataclass
    from hgraph import CompoundScalar

    @dataclass(frozen=True)
    class MyScalar(CompoundScalar):
        p1: str
        p2: int

This type defines its constituents in much the same way as a dataclass. It is generally good practice to wrap the
class with the ``dataclass`` decorator.

Standard-library dataclasses can also be used directly as nominal scalar schemas without inheriting from
``CompoundScalar``:

::

    from dataclasses import dataclass
    from hgraph import TS, combine, graph

    @dataclass(frozen=True)
    class Quote:
        instrument: str
        bid: float
        ask: float = 0.0

    @graph
    def make_quote(bid: TS[float]) -> TS[Quote]:
        return combine[TS[Quote]](instrument="ABC", bid=bid)

The original dataclass instance is retained as the time-series value. Its ordered fields form the schema used for
wiring, reflection, field access, generic resolution, conversion, and dispatch. Dataclass defaults and default
factories are honoured when constructing a value. ``TSB[Quote]`` provides the corresponding field-expanded
time-series form and is the canonical public spelling; downstream code does not need to create a peer
``TimeSeriesSchema`` with ``TimeSeriesSchema.from_scalar_schema``.

The lift from a dataclass to a bundle is deliberately conservative. Each stored dataclass field ``field: T`` becomes
``field: TS[T]``. Collection values remain scalar values (for example, ``items: tuple[int, ...]`` becomes
``items: TS[tuple[int, ...]]``), and a nested dataclass remains ``TS[Nested]`` rather than becoming a nested ``TSB``.
HGraph does not infer ``TSL``, ``TSD``, ``TSS``, or another time-series topology from scalar annotations. ``ClassVar``,
``InitVar``, and computed properties are not stored fields. An unresolved field annotation or a time-series annotation
inside the scalar dataclass is rejected when the schema is constructed.

Dataclass annotations describe the wiring schema; assigning a whole dataclass does not recursively validate or
replace the object's fields. Frozen dataclasses are recommended because mutating a retained object in place does not
produce a new time-series tick.

Schema discovery lazily attaches reserved ``__hgraph_bundle_*__`` and ``__meta_data_schema__`` metadata to the
dataclass itself for compatibility with the Python and C++ runtimes. Defining incompatible values for these reserved
attributes raises a type parsing error instead of being silently overwritten.

Use ``CompoundScalar`` when its hgraph-specific helpers, serialisation hierarchy, or C++ field-expanded storage are
required. Use a plain dataclass when preserving the application's Python class, constructor, properties, equality,
and object identity is the priority. The legacy ``CS[Model]`` adaptor remains useful when a distinct
``CompoundScalar`` must be generated from a dataclass, Pydantic model, or annotated plain class.

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

See :doc:`time_series_types` for the collection type behaviours and
:doc:`../python/tutorial/typing` for worked Python examples.

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

.. testcode::

    from typing import TypeVar
    from hgraph import compute_node, TS

    NUMBER_TYPE_1 = TypeVar("NUMBER_TYPE_1", int, float)
    NUMBER_TYPE_2 = TypeVar("NUMBER_TYPE_2", int, float)

    @compute_node
    def div_(lhs: TS[NUMBER_TYPE_1], rhs: TS[NUMBER_TYPE_2]) -> TS[float]:
        return lhs.value / rhs.value

In this case we define two instances of ``TypeVar`` with the same properties, but since they are distinct they are
treated as being unique and separate definitions. This will allow a ``TS[int]`` supplied to lhs and ``TS[float]``
to be provided to the rhs without any issue.

HGraph ships pre-declared pairs for the cases that come up most often, so you
usually do not need to declare your own: ``NUMBER``/``NUMBER_2``,
``SCALAR_1``/``SCALAR_2``, ``TIME_SERIES_TYPE_1``/``TIME_SERIES_TYPE_2`` and
``COMPOUND_SCALAR``/``COMPOUND_SCALAR_1``. The declaration above could equally
be written:

.. testcode::

    from hgraph import compute_node, TS, NUMBER, NUMBER_2

    @compute_node
    def div_(lhs: TS[NUMBER], rhs: TS[NUMBER_2]) -> TS[float]:
        return lhs.value / rhs.value

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
to the types the system can resolve and correctly process. There are two registration paths, depending on where the
value's storage lives.

**Python-owned structured scalars.** A plain Python class can opt into the nominal Bundle contract, so its fields
participate in type resolution, wiring checks and value conversion. Standard-library dataclasses are recognised
automatically; other classes register explicitly, which also works as a decorator:

::

    from hgraph import register_python_object_type

    @register_python_object_type
    class MyValue:
        name: str
        weight: float

An ordered ``fields`` mapping can be supplied when the annotations do not fully describe the public schema.

**Native scalars.** An extension that defines its storage in C++ registers a native schema and then associates a
Python class with it, so the same value is a first-class scalar on both sides of the boundary:

::

    from hgraph import register_native_scalar_type

    register_native_scalar_type(MyValue, "my_extension.my_value")

The advantage of registering the type either way is that it becomes a fully functioning type, including being able to
participate in type resolution — as opposed to the opaque Python object type, which has very limited ability to
integrate into the type system.

The registration contract, and what an extension is required to guarantee, is defined by
:doc:`../../developer_guide/extension_policy`, :doc:`../../rfc/rfc_0003_extension_scalar_registration` and
:doc:`../../rfc/rfc_0004_python_owned_structured_scalars`.
