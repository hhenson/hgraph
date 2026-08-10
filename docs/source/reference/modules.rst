Public modules and adaptors
===========================

Core modules
------------

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Module
     - Purpose
   * - ``hgraph.temporal``
     - Calendar periods, zones, civil/zoned datetimes, ranges and rounding.
   * - ``hgraph.test``
     - ``eval_node`` and testing diagnostics.
   * - ``hgraph.debug``
     - Inspector and owned graph-diagnostics snapshots.
   * - ``hgraph.stream``
     - Stream conversion, multiplexing and stream utility nodes.
   * - ``hgraph.reflection``
     - Public type and schema inspection helpers.
   * - ``hgraph.numpy_``
     - Native shaped-array operators with NumPy boundary values.
   * - ``hgraph.arrow``
     - Python arrow-combinator authoring DSL (unrelated to Apache Arrow).
   * - ``hgraph.notebook``
     - Stateful notebook evaluation helpers.
   * - ``hgraph.nodes``
     - Compatibility node helpers built on the public runtime path.

Adaptor packages
----------------

.. list-table::
   :header-rows: 1
   :widths: 42 34 24

   * - Module
     - Purpose
     - Installation
   * - ``hgraph.adaptors.data_frame``
     - Arrow/Polars data sources and frame record/replay
     - ``hgraph[dataframe]``
   * - ``hgraph.adaptors.sql``
     - SQL sources, sinks and batch access
     - ``hgraph[sql]``
   * - ``hgraph.adaptors.delta``
     - Delta Lake publication and queries
     - ``hgraph[delta]``
   * - ``hgraph.adaptors.tornado``
     - HTTP, REST and WebSocket clients/servers
     - ``hgraph[web]``
   * - ``hgraph.adaptors.perspective``
     - Perspective publication and web workspace
     - ``hgraph[perspective]`` plus ``web``
   * - ``hgraph.adaptors.data_catalogue``
     - Catalogue-backed subscription/publication contracts
     - core package
   * - ``hgraph.adaptors.json``
     - JSON source and adaptor helpers
     - core package
   * - ``hgraph.adaptors.dataclass``
     - Dataclass-to-``CompoundScalar`` compatibility adapter
     - core package
   * - ``hgraph.adaptors.executor``
     - Executor adaptor helper
     - core package
   * - ``hgraph.adaptors.run_graph_on_thread``
     - Run child graph engines on worker threads
     - core package

Kafka is a separate first-party extension distribution, ``hgraph-kafka``, not
an extra of the core wheel. Each module's exact public names are listed in
:doc:`python_api_inventory`.
