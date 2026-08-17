Adaptors
========

Adaptors extend HGraph with functionality built on third-party libraries,
exposed through HGraph-style wrappers. Integrations with heavy third-party
runtimes ship behind packaging extras, so installing ``hgraph`` does not pull
in a web server or database driver. Core-only helpers such as ``json``,
``dataclass``, ``executor`` and ``run_graph_on_thread`` need no extra.

The adaptor families live under ``hgraph.adaptors``: ``tornado`` (HTTP, REST
and WebSocket), ``sql`` (and Snowflake), ``delta``, ``perspective``,
``data_frame``, ``json``, ``dataclass``, ``executor``,
``run_graph_on_thread`` and ``data_catalogue``. The installation matrix and
public module list are in :doc:`../../reference/modules`.

Kafka and the web transports are the exceptions. Each is a first-party
*extension* rather than an extra: a separate distribution
(``hgraph-kafka``, ``hgraph-web``) with its own native library, built
against the hgraph SDK. Install them with ``pip install hgraph-kafka`` /
``pip install hgraph-web``.

.. toctree::
    :maxdepth: 1

    tornado
    web
    kafka
