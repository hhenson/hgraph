Node Life-cycles
================

Nodes have a start and stop life-cycle that can be hooked into.

This works by annotating the start and stop method in a similar fashion to the property setter.

For example:

```python
from hgraph import compute_node, TS


@compute_node
def life_cycle_example(a: TS[str]) -> TS[str]:
    return a.value


@life_cycle_example.start
def life_cycle_example_start():
    print("Start")


@life_cycle_example.stop
def life_cycle_example_stop():
    print("Stop")
```

In this example, the life_cycle_example method is extended with start and stop methods.
The start and stop method do not need to have the same name as the node.

I like the pattern of extending the node name with _start and _stop, some like using ``_``.

To try the example, run the life_cycle.py script in the docs/quick_start folder.
