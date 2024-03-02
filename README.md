# hgraph
A functional reactive programming engine with a Python front-end.

This provides a DSL and runtime to support the computation of results over time, featuring
a graph based directed acyclic dependency graph and the concept of time-series properties.
The language is function based, and promotes composition to extend behaviour.

Here is a simple example:

```python
from hgraph import graph, run_graph
from hgraph.nodes import const, debug_print

@graph
def main():
    a = const(1)
    c = a + 2
    debug_print("a + 2", c)

run_graph(main)
```
Results in:
```
[1970-01-01 00:00:00.000385][1970-01-01 00:00:00.000001] a + 2: 3
```

See [this](docs/index.md) for more information.

## Development

The project is currently configured to make use of [Hatchling](https://hatch.pypa.io/latest/) for dependency management. 
Take a look at the website to see how best to install the tool.
Once you have checked out the project, you can install the project for development using the following command:

```bash
hatch env create all.py3.11
```

Then you can find the location of the installation using:

```bash
hatch env find all.py3.11
```

For users of CLion / PyCharm, you can then add the environment by selecting an existing virtual environment using
the location above.

### Run MyPy Type Checking

```bash
hatch run types:check
```

### Run Tests

```bash
# No Coverage
hatch run test
```

```bash
# Generate Coverage Report
hatch run cov
```
