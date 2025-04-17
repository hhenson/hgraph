# hgraph
A functional reactive programming engine with a Python front-end.

This provides a DSL and runtime to support the computation of results over time, featuring
a graph based directed acyclic dependency graph and the concept of time-series properties.
The language is function-based, and promotes composition to extend behaviour.

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

The project is currently configured to make use of [uv](https://github.com/astral-sh/uv) for dependency management. 
Take a look at the website to see how best to install the tool.

Here are some useful commands:

First, create a virtual environment in the project directory:

```bash
uv venv
```

Then use the following command to install the project and its dependencies:

```bash
# Install the project with all dependencies
uv pip install -e .

# Install with optional dependencies
uv pip install -e ".[docs,web,notebook]"

# Install with all optional dependencies
uv pip install -e ".[docs,web,notebook,test]"
```

PyCharm can make use of the virtual environment created by uv to ``setup`` the project.

### Run Tests

```bash
# No Coverage
python -m pytest
```

```bash
# Generate Coverage Report
python -m pytest --cov=hgraph --cov-report=xml
```
