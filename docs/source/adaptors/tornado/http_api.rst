HTTP Adaptor
============

The http adaptor wraps the Tornado package exposing the ability to interact as a client or implement the server.

HTTP Server
-----------

The HTTP server provides a handler based approach to implementing server side logic. These are found in:
``hgraph.adaptors.tornado.http_server_adaptor``.


.. autofunction:: hgraph.adaptors.tornado.http_server_adaptor.http_server_handler

.. autoclass:: hgraph.adaptors.tornado.http_server_adaptor.HttpRequest

.. autoclass:: hgraph.adaptors.tornado.http_server_adaptor.HttpGetRequest

.. autoclass:: hgraph.adaptors.tornado.http_server_adaptor.HttpDeleteRequest

.. autoclass:: hgraph.adaptors.tornado.http_server_adaptor.HttpPutRequest

.. autoclass:: hgraph.adaptors.tornado.http_server_adaptor.HttpPostRequest

.. autoclass:: hgraph.adaptors.tornado.http_server_adaptor.HttpResponse


Use the following to assist setting up the services to make this work:

.. autofunction:: hgraph.adaptors.tornado.register_http_server_adaptor


HTTP Client
-----------

The client is accessed via a service adaptor, namely: ``http_client_adaptor``. This is found in the package:
``hgraph.adaptors.tornado.http_client_adaptor``.

.. autofunction:: hgraph.adaptors.tornado.http_client_adaptor.http_client_adaptor


The adaptor implementation is here:

.. autofunction:: hgraph.adaptors.tornado.http_client_adaptor.http_client_adaptor_impl

