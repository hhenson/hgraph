Generics
========

The type-system supports the use of generics (using the python ``TypeVar`` and generics).
The objective is to support the construction of generic logic where only the requires attributes
of input values and output values are specified.

Mostly, generics are used by library code, but it is still useful in user code.

The typing system leverages the Python ``TypeVar`` with a few restrictions, please familiarise yourself with TypeVar's
before continuing.

The type-var can (and should) set a bound or list of valid types. This information is used to validate supplied types
to ensure they meet with the target conditions.

HGraph defines a number of standard generics to support the constraints for scalar and various time-series inputs.

SCALAR
------

Here is an example of the use of generics:

.. testcode::

    from hgraph import compute_node, TS, SCALAR
    from hgraph.test import eval_node

    @compute_node
    def my_add(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[SCALAR]:
        return lhs.value + rhs.value

    assert eval_node(my_add[int], [1, 2, 3], [4, 5, 6]) == [5, 7, 9]

In this example, ``SCALAR`` is a type-var representing non-time-series values. We substitute the type with the type-var
to indicate the loosened type constraint. Once a type-var is resolved (by explicitly setting the type as we have done
in this example) or through supplying an input (which is shown in a subsequence example). All instances of the type-var
must meet the same resolution. The use of the ``[int]`` at the end of the function is a means of explicitly resolving
the type of the generic. If there is only one generic in the signature, it is possible to just specify the type inside
the square brackets. If there is more then one resolvable type the use of the type-var is required, for example, the
long form of the above would be::

    my_add[SCALAR: int](...)

Because we were passing the ``my_add`` as a function to the ``eval_node``, resolving the type-var's was required,
the next example shows how this can be used in a more friendly manor.

.. testcode::

    from hgraph import compute_node, TS, SCALAR, graph
    from hgraph.test import eval_node

    @compute_node
    def my_add(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[SCALAR]:
        return lhs.value + rhs.value

    @graph
    def g(lhs: TS[float], rhs: TS[float]) -> TS[float]:
        return my_add(lhs, rhs)

    assert eval_node(g, [1.0, 2.0, 3.0], [4.0, 5.0, 6.0]) == [5.0, 7.0, 9.0]

In this example, the use of the ``my_add`` function did not require an explicit type resolution. It resolves it's
type from the inputs supplied. The wiring time logic will ensure that the bounds / constraints of the type-var are
honoured.

Exercise
........

Try this example where ``lhs`` and ``rhs`` types are different.

TIME_SERIES_TYPE
----------------

Another frequently used type-var is ``TIME_SERIES_TYPE``, there are a number of addition named typed-vars with the
same constraint such as: ``OUT``, ``TIME_SERIES_TYPE_``, ``TIME_SERIES_TYPE_2``, and ``V``.

These represent an arbitrary time-series value. The various instances are to allow the specification of multiple
different generic types. For example:

.. testcode::

    from hgraph import compute_node, graph, TS, TIME_SERIES_TYPE, OUT
    from hgraph.test import eval_node

    @compute_node
    def my_add(lhs: OUT, rhs: TIME_SERIES_TYPE) -> OUT:
        return lhs.value + rhs.value

    @graph
    def g(lhs: TS[float], rhs: TS[int]) -> TS[float]:
        return my_add(lhs, rhs)

    assert eval_node(g, [1.0, 2.0, 3.0], [4, 5, 6]) == [5.0, 7.0, 9.0]

In the above example we use two potentially different types. In this case we constrain the output to the same
as the ``lhs`` type.

Auto-resolution can generally only support input types, when only the output type is requiring resolution, this
type must be user specified or use the user-defined function approach to type resolution.

.. testcode::

    from hgraph import compute_node, graph, TS, TIME_SERIES_TYPE, OUT
    from hgraph.test import eval_node

    @compute_node
    def my_add(lhs: TS[float], rhs: TS[int]) -> OUT:
        return lhs.value + rhs.value

    @graph
    def g(lhs: TS[float], rhs: TS[int]) -> TS[float]:
        return my_add[TS[float]](lhs, rhs)

    assert eval_node(g, [1.0, 2.0, 3.0], [4, 5, 6]) == [5.0, 7.0, 9.0]

In this example we are required to explicitly resolve the type-var ``OUT`` as there is no way for the framework to
resolve this.

Exercise
........

Remove the ``[TS[float]]`` from ``my_add`` and see the error that results.

Resolvers
---------

There are times, where the type resolution could be determine computationally using the provided inputs, but are
not possible to resolve without explicit logic. A simple example is:

.. testcode::

    from hgraph import compute_node, TS, TSB, TS_SCHEMA, TimeSeriesSchema, OUT
    from hgraph.test import eval_node
    from dataclasses import dataclass
    from frozendict import frozendict as fd

    def _resolve_out(mappings, scalars):
        tsb_tp = mappings[TS_SCHEMA]
        key = scalars["key"]
        out_tp = tsb_tp.py_type.__meta_data_schema__[key].py_type
        return out_tp

    @compute_node(resolvers={OUT: _resolve_out})
    def my_get_item(tsb: TSB[TS_SCHEMA], key: str) -> OUT:
        return tsb[key].value

    @dataclass
    class MySchema(TimeSeriesSchema):
        p1: TS[int]
        p2: TS[str]

    assert eval_node(my_get_item[TS_SCHEMA: MySchema], [fd(p1=1, p2="a")], "p1") == [1]

In this example we define the ``resolvers`` attribute. The resolvers defines the type-var's that have logic associated
to resolve the type. When the node is being wired, the function will be called and the return value of the type is
used to resolve the type. The resolver function is provided with two inputs, namely the ``mappings`` and the ``scalars``,
the ``mappings`` is a dictionary of types that have been resolved to date. The dictionary is keyed by ``TypeVar`` and
contain ``HgTypeMetaData`` instances describing the types resolution. The ``scalars`` is a dictionary keyed by the
name of the scalar inputs and contains the values supplied.

If, using this information, it is possible to resolve a type, then the resolver function is a great tool to make
generic types more usable making the user experience a bit better.

Requires
--------

Very closely related to ``resolvers`` is the requires attribute, this allows a graph or node to specify requirements
that must be met in order to pass wiring successfully. The signature for a requires function is the same as for
the resolver for inputs, but is expected to return True if the requirements are met, otherwise it can return False or
a message indicating why it did not resolve the inputs.

There are many potential uses for this feature, however, a simple example is provided below:

.. testcode::

    import pytest
    from hgraph import compute_node, TS, RequirementsNotMetWiringError
    from hgraph.test import eval_node

    def _requires_true(mappings, scalars):
        return scalars["__strict__"] or "This requires strict to be True"

    @compute_node(requires=_requires_true)
    def add_strict(lhs: TS[int], rhs: TS[int], __strict__: bool) -> TS[int]:
        return lhs.value + rhs.value

    assert eval_node(add_strict, [1], [2], True) == [3]

    with pytest.raises(RequirementsNotMetWiringError):
        assert eval_node(add_strict, [1], [2], False) == [3]

The above example may seem a bit strange, however, this will make more sense when reviewing the ``operators`` section.

This can also be helpful when constraining generics, or the interoperability of different generic inputs, for example
if the function has an ``TS[int]`` for type one, then the second one can only be of type ``TS[int]`` or ``TS[float]``.

(For example when performing a division).

The use of requires and resolvers do add additional cost to type resolution and as such should only be used when
absolutely necessary.


