# hgraph
A functional reactive programming engine with a Python front-end.

This provides a DSL and runtime to support the computation of results over time, featuring
a graph based directed acyclic dependency graph and the concept of time-series properties.
The language is function based, and promotes composition to extend behaviour.

Here is a simple example:

```python
from hgraph import graph, run_graph, const
from hgraph.nodes import debug_print

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

See [this](https://hgraph.readthedocs.io/en/latest/) for more information.

## Development

The project is currently configured to make use of [Poetry](https://python-poetry.org) for dependency management. 
Take a look at the website to see how best to install the tool.
Once you have checked out the project, you can install the project for development using the following command:

This is optional, but you can ensure python uses the version of python you require.

```bash
poetry env use 3.11
```

Then use the following command to install the project and it's depenencies:

```bash
poetry install
```

Then you can find the location of the installation using:

```bash
poetry env info
```

PyCharm can make use of poetry to ``setup`` the project.

### Run Tests

```bash
# No Coverage
poetry run pytest
```

```bash
# Generate Coverage Report
poetry run pytest --cov=your_package_name --cov-report=xml
```
