Node Testing
============

HGraph attempts to leverage as much as possible off the existing tooling in this space,
[pytest](https://docs.pytest.org) is the tool of choice.

To test a node, we have provided a method ``eval_node`` in ``hgraph.test``.

This wraps a graph or node and allows it to be evaluated in a test environment.
The function takes the node/graph function and a set of scalar parameters to be used.
Time-series parameters are provided as a list of values or None, where None represents
no tick at that time-point. 

Time will start at ``MIN_ST`` and be incremented by ``MIN_TD`` for each entry in the list.
The results are returned as a list of values representing the output of the node/graph over
time with the first entry representing the output at ``MIN_ST``.

Here is an example of the usage of the ``eval_node`` function:

```python
import pytest

from hgraph import compute_node, TS
from hgraph.test import eval_node


@compute_node
def add(a: TS[int], b: TS[int]) -> TS[int]:
    return a.value + b.value


@pytest.mark.parametrize(
    "a,b,expected", [
       [[1, 2, 3], [2, 3, 4], [3, 5, 7]],
       [[None, 2, None], [2, 3, 4], [None, 5, 6]],
    ])
def test_add(a, b, expected):
    assert eval_node(add, a=a, b=b) == expected
```

Here we are using the standard pytest ``parametrize`` decorator to provide a set of test cases.
The ``eval_node`` is provided the node function, and the parameters to be used in the test 
and asserted against the comparison to the expected result.

When using generic types, the ``eval_node`` function will attempt to infer the type from the inputs
but if it is not able to do so, the types can be provided directly using the arg ``resolution_dict``
and supplying a dictionary with the type resolution provided.

There is also a very useful ``__trace__`` option, when set to True, the ``eval_node`` function will
ensure each step in evaluating the graph is printed to the console. This is useful for debugging.

Use the node_testing file to experiment with this feature.
