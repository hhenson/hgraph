Jupyter Notebook Support
========================

This provides a collection of utilities and extensions to support writing graphs in a 
Jupyter notebook.

To use the graph in a notebook, start with:
 
```python   
from hgraph.notebook import *
start_wiring_graph()
```

Then you can build the graph cell by cell. For example:

```python
from hgraph import const
c = const("Hello World")
```

If you want to see the result of evaluating the node (this only works with compute nodes or source
nodes) you can call the `eval()` method on the result, for example:

```python
c.eval()
```
This will display the result from evaluating the graph. Currently, evaluation only supports
`SIMULATION` mode.

To evaluate the graph in general, use the function:

```python
notebook_evaluate_graph()
```
This would evaluate the current state of the graph.
