Library Writer's Guide
======================

This guide focuses on the features of the language that makes for better user experiences
with the library functions.

When writing a library function, it is important to be able to make that the use of the
node / graph as transparent as possible.

Key concepts
-----------

### Operator

The ``@operator`` decorator allows the library function writer to describe an interface
that can have multiple implementations (using the ``override`` option in the ``graph`` and 
``compute_node`` decorators)

The basic approach is to describe the generic function, for example:

```python
from hgraph import operator, TIME_SERIES_TYPE

@operator
def add_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    """ My documentation """
```

In this example we have described the ``add`` operator. This is similar in concept
to ``interface`` in Java, ``protocol`` in Swift or ``traits`` in Rust.

This does not define actual behavior, it should be well documented to descibe
the expected behavior that should be expected when using this operator.

**NOTE**: *An operator must use generic elements to indicate which components can
be overridden.*

To implement the operator, do the following:

```python
from hgraph import compute_node, TS

@compute_node(overloads=add_)
def add_ts_int(lhs: TS[int], rhs: TS[int]) -> TS[int]:
    return lhs.value + rhs.value
```

In this example, we implement a variant of the ``add_`` operator that takes
a time-series of integer values.

When the user makes use of the operator (by calling ``add_``) the operator will
attempt to select the closest matching implementation to make use of.

**NOTE**: *All implementations must be imported prior to the use of the
operator. This just requires the script to be imported, in hgraph this is
done by adding the implementation files to the ``__init__.py`` files of
hgraph. Thus, when you import anything from hgraph you automatically load
the implementations (causing them to be registered with the operator).*

#### Matching overloads

To match, the operator itself must be resolvable, once it is resolved, 
the operator will attempt to find the best resolution, discarding implementations
that fail to resolve and then ranking those as being the most specific.
An exact type is deemed as the closest match, a partial specializing 
(such as ``TSD[K, TS[int]]``) will have a higher ranking than say ``TIME_SERIES_TYPE``.

When more than one parameter needs resolving the rankings is the accumulation
of individual rankings. If more than one resolution has the same ranking level
then the resolution will be non-deterministic.

### OUT

Prefer using ``OUT`` to represent the result of a graph or node, espcially
when the output is not determined by the inputs. ``OUT`` is the same as ``TIME_SERIES_TYPE``
but shorter to use when providing a pre-resolution.

For example:

```python
from hgraph import compute_node, OUT, TIME_SERIES_TYPE, TS

@compute_node
def div_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> OUT:
    return lhs.value / rhs.value

...

c = div_[OUT: TS[float]](1, 2)
```

Without the type hint, (i.e. ``div_(1, 2)``) would not resolve and will raise a wiring exception.

By using ``OUT`` the type hint is much smaller.

### DEFAULT

In the above example, another option would be to tag the ``OUT`` TypeVar with the tag
``DEFAULT`` as follows:

```python
from hgraph import graph, TIME_SERIES_TYPE, OUT, DEFAULT, TS

@graph
def div_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE) -> DEFAULT[OUT]:
    ...

...

c = div_[TS[float]](1, 2)
```

In this example, we can drop the ``OUT`` in the resolution. This makes
the signature slightly more simplistic.

This can be applied to any of the template inputs, but only to one of them
in a signature.

The reason why we need to support at least one of these mechanisms is that the outer
signature still needs to be resolved, and when there is no obvious resolution, we must
have a user provided resolution.

### resolvers

This option, available in the general signature of graph and node decorators, allows for 
programmatic resolution of generics. This is helpful when there is sufficient type information
provided, but there is no simplistic mechanism for the default type resolution to determine
the non-provided type information.

For example:

class TypeVar:
pass

```python
from hgraph import compute_node, COMPOUND_SCALAR, OUT, TS, HgTypeMetaData, TIME_SERIES_TYPE
from typing import TypeVar, Any

def _resolve_type(mappings: dict[TypeVar: HgTypeMetaData], scalars: dict[str, Any]) -> TIME_SERIES_TYPE:
    """
    mappings - the type vars that have been resolved so far
    scalars - the scalar values provided to the function.
    
    This must return the resolved type
    """
    cs = mappings[COMPOUND_SCALAR].py_type
    key = scalars['key']
    return TS[cs.__meta_data_schema__[key]]
    

@compute_node(resolvers={OUT: _resolve_type})
def select_field(ts: TS[COMPOUND_SCALAR], key: str) -> OUT:
    return getattr(ts.value, key)
```

In this example, we the node ``select_field`` will select a value from the incoming ``COMPOUND_SCALAR``.
The compound scalar has a schema associated with it. The resolver is able to leverage this; since
this is resolved during the wiring phase, we can use the resolved type to extract the schema
meta-data.

Also notice the resolves is a dictionary of types that are requiring resolution, so for each type
that requires resolution it just needs to appear in the resolvers dict and the function to resolve
it provided.

If the resolution is simple, then just use a ``lamda``, you will notice that a lot of the code
in the core libraries follow the convention: ``lambda m, s: ...``.

### requires

It is often useful to provide constraints to define when a graph or node is considered valid.
Normally, it is used to constrain a valid input beyond a simple type constraint.
But it has also been used to activate a node only when a context variable has been set (as in the
case of the record / replay nodes). This latter approach allows for the correct selection of similarly ranked
nodes.

Here is an example:

```python
from hgraph import TS, generator, SCALAR, MIN_ST

def _check_int_convertable(m, s) -> bool:
    """
    As with resolvers m and s represent the resolved types and the scalar values.
    True implies the requires is successful. False fails the resolution.
    """
    try:
        int(s['v'])
        return True
    finally:
        return False
        

@generator(requires=_check_int_convertable)
def int_const(v: SCALAR) -> TS[int]:
    yield MIN_ST, int(v)
```

Here is another example using the ``GlobalState`` instead:

```python
from hgraph import GlobalState, graph, TS, record

@graph(overloads=record, requires=lambda m, s: GlobalState.instance().get("record_to_memory", False))
def record_to_memory(ts: TS[float], key: str, record_delta_values: bool = True, suffix: str = None):
    ...
```

In this case we look to see if the ``record_to_memory`` is set (to ``True``). This will ensure
that the graph is not used if the record to memory flag is not set.

### deprecated

There are times when better or cleaner approaches to doing things are discovered. In this scenario,
it is good practice to provide a migration path from the old api to the new. Using the 
``depricated`` option in the decorator to mark a graph or node as being deprecated, this
will generate a warning. Use a string to provide information to the user as to what / how to
correct the situation.

For example:

```python
from hgraph import graph, TS

@graph(deprecated="accumulate is deprecated, use sums(ts) instead.")
def accumulate(ts: TS[float]) -> TS[float]:
    ...

```

