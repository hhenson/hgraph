Generics
========

Generics make use of the Python ``TypeVar`` type. This allows us to specify bounds as 
describing other relationships such as consistency of inputs.

Here are some examples of generics:

```python
from typing import TypeVar
from hgraph import compute_node, TS

NUMERIC = TypeVar("NUMERIC", int, float)


@compute_node
def add(a: TS[NUMERIC], b: TS[NUMERIC]) -> TS[NUMERIC]:
    return a.value + b.value
```

In this scenario we define a ``TypeVar`` called ``NUMERIC`` that is either an ``int`` or a ``float``.
Then we use the ``NUMERIC`` type in the ``TS`` type annotation to indicate that the TS can be either
a ``TS[int]`` or a ``TS[float]``, depending on the input provided. Once a resolution for
one instance of the ``TypeVar`` is provided, all other resolutions should be consistent.

The output type is inferred from the input types resolution.

So used as below:

```python
@graph
def main():
    debug_print("1+2", add(a=1, b=2))


run_graph(main)
```

In this case the attributes ``a`` and ``b`` are both time-series types, so the scalar's 1 and 2 will
be upcast to ``const(1)`` and ``const(2)`` respectively. The ``add`` function will then be called, the
``TypeVar`` will be resolved to ``int`` and the output will be ``TS[int]``. This requires
``b`` to be resolved to ``TS[int]`` as well, which it is, also making the output ``TS[int]``.

There are a few standard ``TypeVar``'s that are provided by the ``hgraph`` package:
* ``SCALAR`` - Represents a valid scalar supported by the ``hgraph`` package.
* ``TIME_SERIES_TYPE`` - Represents a valid time-series supported by the ``hgraph`` package.
* ``K``, ``V`` - a scalar and time-series value that we typically use in the context of TSD[K, V]

There are a few variants of the above.

There are times when resolving the ``TypeVar`` is not possible, in this case the ``TypeVar`` can
be pre-resolved by performing explicit resolution using the [] syntax as depicted below:

```python
from typing import Mapping
from frozendict import frozendict
from hgraph import compute_node, TS, TSD, TSL, SCALAR, TIME_SERIES_TYPE, graph, run_graph, Size
from hgraph.nodes import debug_print

@compute_node
def cast(value: TS[SCALAR]) -> TIME_SERIES_TYPE:
    return value.value


@graph
def main():
    debug_print("TS[Mapping[int, str]]", cast[TIME_SERIES_TYPE: TS[Mapping[int, str]]](value=frozendict({1: 'a'})))
    debug_print("TSL[TS[str], Size[2]]", cast[TIME_SERIES_TYPE: TSL[TS[str], Size[2]]](value=frozendict({1: 'a'})))
    debug_print("TSD[int, TS[str]]", cast[TIME_SERIES_TYPE: TSD[int, TS[str]]](value=frozendict({1: 'a'})))


run_graph(main)
```

In this case, you can see the same function being used in three different ways. In any scenario, there 
is no way to resolve the output type (``TIME_SERIES_TYPE``) from the input type (``TS[SCALAR]``).
So by providing the type resolution explicitly, we can resolve the output type.
