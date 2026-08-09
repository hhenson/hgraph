Adaptors
========

Adaptors extend HGraph with functionality built on third-party libraries,
exposed through HGraph-style wrappers. Each family ships behind its own
packaging extra and lazily imports its heavy dependencies, so installing
``hgraph`` does not pull in a web server or a database driver.

The adaptor families live under ``hgraph.adaptors``: ``tornado`` (HTTP, REST
and WebSocket), ``sql`` (and Snowflake), ``delta``, ``kafka``,
``perspective``, ``data_frame``, ``json``, and ``data_catalogue``.

.. toctree::
    :maxdepth: 1

    tornado
