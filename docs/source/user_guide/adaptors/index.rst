Adaptors
========

Adaptors extend HGraph with functionality built on third-party libraries,
exposed through HGraph-style wrappers. Each family ships behind its own
packaging extra and lazily imports its heavy dependencies, so installing
``hgraph`` does not pull in a web server or a database driver.

The adaptor families live under ``hgraph.adaptors``: ``tornado`` (HTTP, REST
and WebSocket), ``sql`` (and Snowflake), ``delta``, ``perspective``,
``data_frame``, ``json``, and ``data_catalogue``.

Kafka is the exception. It is a first-party *extension* rather than an extra:
a separate ``hgraph-kafka`` distribution with its own native library, built
against the hgraph SDK. Install it with ``pip install hgraph-kafka``.

.. toctree::
    :maxdepth: 1

    tornado
