Hgraph Inspector
----------------

Inspector is a way to see the state of the graph at any point in time. It is a tool for debugging and introspection. 
To add an inspector to the graph, you need to add the `inspector` node to the graph and have perspective adaptor running:

```python
import os
from _socket import gethostname

from hgraph import graph
from hgraph.adaptors.perspective import perspective_web, PerspectiveTablesManager, register_perspective_adaptors
from hgraph.debug import inspector

@graph
def main():
    inspector()

    register_perspective_adaptors()
    perspective_web(gethostname(), 8080)
    
    # optional if you want to use the client side tables (improves performance for smaller frequently updated tables)
    PerspectiveTablesManager.set_current(PerspectiveTablesManager(host_server_tables=False))  

```

When started the graph will print out the URL to the inspector (http://<hostname>:8080/inspector/view). You can use this
URL to connect a browser to the graph and inspect the state of the graph.

The first time you connect to the inspector, you will see an empty page. Right click on the page and select `Add Table` 
to add the 'inspector' table to the page. The inspector table will show the state of the graph at the time of the last
tick. Make sure it is ordered by the 'ord' column to see the nodes in the order they were executed.

Inspector columns
=================

- X - expand/collapse the row
- name - the name of the node/graph/input/output/value
- type - the type of the node/graph/input/output/value
- value - the value of the node/graph/input/output/value - this will update with the latest value observed on the graph every few seconds
- timestamp - the time the value was last updated, last tick time for a graph, scheduled time for a node
- evals - number of times the node has been evaluated
- time - total time spent evaluating the node
- of_graph - fraction of the time this node took of the total time spent evaluating the subgraph that contains it
- of_total - fraction of the time this node took of the total time spent evaluating the top level graph
- ord - the sort column for the table so that items are shown in the correct order. always have the table sorted by this column ascending
- id - the unique id of the node/graph/input/output/value

Do not remote the ord and id columns as they are required for the inspector to work correctly. You can move them to the 
right end and make them smaller if you want to hide them.

Operating inspector
===================

- Click + to expand an element of the graph, it could be a node, graph, input, output, or a value like dict.
- Click - to collapse an element of the graph
- Double-click on a name to expand/collapse
- Ctrl-click an input value to navigate to the output it is getting its value from
- Ctrl-click an REF output value to navigate to the output that the reference points to
- Double-click on a Frame XxY value to open the frame in a new window (the new window is static i.e. does not update)
- Click on an item to select it (it will highlight)
- Hitting enter expands/collapses the selected item
- Typing '?' will open a search box at the top of the grid, type the name of the item you are looking for 
in the items already shown in the grid. THe search string will be highlighted in the items that match. Use the Up and 
Down arrows to jump between the matching items and Enter/Escape to close the search  
- Typing '/' when there is a highlighted item in the grid will open a search box over the selected item, allowing you to 
search for items that are not shown on the grid that are children of the selected item up to 3 level deep. Type the name 
of the item you are looking for and hit Enter to keep them shown, Esc to close the search box. THis type of search only 
shows the first 10 items to avoid blocking the graph too long, narrow down the search string if the item you are looking 
for it not shown  


Graph performance table
=======================

There is an additional table called 'graph_performance' that shows the performance of the graph. Add it through right 
click -> add table to the page. The columns are:

- time - wall clock time of statistics collection
- evaluation_time - graph's evaluation_time
- cycles - graph cycles rate per second in the period since last statistics collection
- graph_time - time spent evaluating the graph to the wall clock time
- graph_load - ratio of graph_time to the wall clock time
- avg_lag - average lag of the graph in the period since last statistics collection
- max_lag - maximum lag of the graph in the period since last statistics collection
- inspection_time - fraction of the time spent inspecting the graph to the graph evaluation time, this show how 
much the inspector is slowing down the graph evaluation, this can be very high!

You can create graphs in perspective from this table to visualise the performance of the graph over time.
