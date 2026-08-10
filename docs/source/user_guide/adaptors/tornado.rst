Tornado HTTP, REST and WebSocket adaptors
==========================================

Install the ``web`` extra to use the Tornado transports.  Application code
normally imports from the curated package and registers one transport entry
point in its top-level graph:

.. code-block:: python

   from hgraph import graph
   from hgraph.adaptors.tornado import (
       register_http_server_adaptor,
       register_rest_client,
       register_websocket_server_adaptor,
   )

   @graph
   def application():
       register_http_server_adaptor(port=8080)
       register_websocket_server_adaptor(port=8081)
       register_rest_client()

The HTTP surface consists of ``Credentials``, the ``HttpRequest`` base and
``HttpGetRequest``, ``HttpDeleteRequest``, ``HttpPutRequest`` and
``HttpPostRequest`` leaves, ``HttpResponse``, ``http_server_handler``,
``register_http_server_adaptor``, ``http_client_adaptor`` and
``http_client_adaptor_impl``.  REST adds the corresponding
``Rest*Request``/``Rest*Response`` values, ``RestResultEnum``,
``rest_handler``, ``register_rest_client`` and the five ``rest_list``,
``rest_read``, ``rest_create``, ``rest_update`` and ``rest_delete`` helpers.
WebSocket applications use ``WebSocketConnectRequest``,
``WebSocketServerRequest``, ``WebSocketClientRequest``, ``WebSocketResponse``,
the server handler/registration functions, and the client adaptor and its
implementation token.  These names are the explicit
``hgraph.adaptors.tornado.__all__`` contract.

Handlers accept either one request or a keyed ``TSD`` batch.  A handler with
no additional required arguments is wired by its server registration
function.  A handler with application inputs is called explicitly from the
graph.  Bare annotated functions, ``@graph`` functions and Python-authored
nodes are supported.  Runtime injectables such as ``STATE`` and
``EvaluationEngineApi`` are supplied by the runtime and are not handler call
arguments.

Advanced raw wiring
-------------------

Most code should use the registration functions.  Five lower-level names are
also supported for applications and independently built extensions that wire
or register an adaptor directly:

- ``http_server_adaptor`` and ``http_server_adaptor_impl``;
- ``websocket_server_adaptor``, ``websocket_server_adaptor_helper`` and
  ``websocket_server_adaptor_impl``.

The WebSocket helper is a registration marker for manually wired handler
graphs. The C++ registry discovers their specialised graph-side clients and
the shared implementation owns the route. Pass the helper to
``register_adaptor`` with the server port; do not depend on the marker's Python
object representation.

Transport managers and Tornado handlers are implementation details.  In
particular, ``HttpAdaptorManager``, ``HttpHandler``,
``WebSocketAdaptorManager``, ``WebSocketHandler``, ``TornadoWeb`` and
``BaseHandler`` are not package exports.  ``REST_RESPONSE`` and
``STR_OR_BYTES`` are internal type variables; bind concrete payload and
``str``/``bytes`` types instead.

Runtime ownership
-----------------

Python owns Tornado sockets, futures and event-loop tasks.  Adaptor/service
registration, keyed request/reply multiplexing, feedback, deltas, scheduling
and graph execution use the native runtime.  Per-run queue senders live in
``GlobalState`` and are removed during graph stop.  Route declaration order is
preserved when Tornado installs overlapping patterns, and server state is
partitioned by port and WebSocket message type.
