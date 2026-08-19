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
     - Arrow/Polars data sources; durable record/replay resolves lazily from
       ``hgraph-persistence``
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

``hgraph-kafka``, ``hgraph-analytics``, ``hgraph-web`` and
``hgraph-persistence`` are separate first-party extension distributions, not
extras of the core wheel — though an extra may depend on one, as
``hgraph[dataframe]`` depends on ``hgraph-persistence``. The durable
record/replay surface reached through ``hgraph.adaptors.data_frame`` keeps its
released import paths and resolves lazily from that distribution, raising a
pointed install error only when a durable name is used. Each module's exact
public names are listed in :doc:`python_api_inventory`.
