Data Fabric
===========

The ability to compute 'things' (referred to as compute atoms or just atoms), these atoms are either 
shared pre-cursors of other computations, values that are used both as final outputs as well as shared pre-cursors, 
or just final outputs.

The atoms represent a meaningful chunk of computation logic, and are generally graph instances.
The atoms are treated in a similar way to a subscription service.

```
+-------+      +------+
| atom  |      | atom |
+-------+      +------+
    |              |
    +------+-------+
           |
      +---------+
      |  atom   |
      +---------+     
```

We would like to instantiate atoms on demand, and dynamically, but have the graph tick in one
engine cycle with no delay loops in the process.

To do this, we have a specialised node to control the dynamic construction and destruction of 
nodes in the fabric. This will have a specialised scheduler that will allow for changes
in shape.

The new scheduler will use a rank, array with an associated node list at each rank.

```
    +-------+------+
    | sched | Rank |
    +-------+------+    +-------+---------+
    | dt1   |  0   | -> | sched |  atoms  |
    |       |  1   |    +-------+---------+
    |       |  2   |    |  dt1  | <a1_id> | -> Atom Graph
    
```

The atom id is a string and is used to identify the atom, and it's result.

The ranking is determined by the dependencies with other atoms.

Atoms can access reference data and subscription based resources, but should avoid request
reply service. The idea is to support computations and keep them in sync with the changes in
data and not require cycles in computation. This does not restrict the use of request reply if
required.

The shape of output computations need to be consistent, i.e. all atoms have the same output
signature.

To setup a computation fabric, do:

```python
from hgraph import compute_fabric, compute_fabric_atom, TS, TSD, graph


@compute_fabric
def compute_prices(key: TS[str]) -> TS[float]:
    """Describes the shape of a compute node"""

    
@compute_fabric_atom(interface=compute_prices)
def compute_price_fair(key: TS[str]) -> TS[float]:
    ...

    
@compute_fabric_atom(interface=compute_prices)
def compute_prices_tier( key: TS[str] ) -> TS[float]:
    fair_price = compute_price_fair(key)
    some_price = ...
    return some_price


@graph
def my_pricer():
    price = compute_prices_tier('MCU_3M')
    ...

```

In the above approach, the price-types required are know and called explicitly,
but the pricing graph is established dynamically, i.e. nodes are instantiated on demand, or
if they have already been requested are bound to the existing nodes.

ALT

```python

@compute_fabric_atom(
    interface=compute_prices,
    matcher=lambda **kwargs: 0.1,
)
def compute_prices_tier( key: TS[str] ) -> TS[float]:
    tier, instrument = split_(':', key)
    fair_price = compute_price(format_('fair:{}', instrument))
    some_price = ...
    return some_price

```

In this case nodes are determined using matching logic, to detemine if the key matches the
node, if a match is determined, the best match is used to respond to the request.

This is more akin to how we do matching of operators. Then we would determine ranking
dynamically, with each reference to 'compute_price' being tracked (the compute_fabric_atom would
act as a graph). The graph would need to proxy the requests to compute_price and track their
resolutions to determine ranking. If the rank of the node changes based on compute_price
changes (i.e. subscription changed and the resultant graph would produce a new ranking order
that is larger than the current one) the node would need to be removed, re-created, re-ranked
and all dependencies re-ranked as appropriate.

Given the expectation that this would not be a regular occurrence it may be acceptable to keep
the signature simple.
