Web (HTTP/S and WebSockets)
===========================

``hgraph-web`` is a first-party *extension* (like Kafka): a separate
distribution with its own native transports, built against the hgraph SDK.
Install it with ``pip install hgraph-web``.  The design record is
:doc:`RFC 0024 <../../rfc/rfc_0024_web_extension_api>`.

It provides all four transports — HTTP(S) server, HTTP(S) client, WebSocket
server, and WebSocket client — as native services: an Asio/Beast server and
a libcurl client (HTTP/2 capable on the client; the server activates HTTP/2
in a later release behind its ALPN seam).  Requests, frames, and connection
lifecycles are ordinary graph data with bounded queues throughout; there is
no user-held server or client object outside the graph.

Serving HTTP
------------

A server is registered at a service path; routes are data.  A route's
requests arrive as a keyed stream, and responses are sent back through the
respond service correlated by the transport-assigned request id:

.. code-block:: python

    from hgraph import TS, TSB, graph
    from hgraph_web import (
        HttpMethod, HttpResponse, HttpServerRequest, WebRoute, WebRouteOutput,
        WebServerConfig, register_web_server, web_respond, web_serve,
    )

    @graph
    def serve_orders():
        register_web_server(WebServerConfig(port=8080), path="site")
        routed = web_serve(WebRoute(HttpMethod.GET, "/orders/{id}"), path="site")
        response = handle_order(routed.request)      # your graph logic
        web_respond(request_id(routed.request), response, path="site")

Key properties of the value contract (all defects of the tornado adaptor's
shapes are unrepresentable here):

* headers and query parameters are ordered sequences of name/value pairs —
  duplicates and order survive;
* bodies are ``bytes``; text and JSON are codec decisions above the
  transport;
* path captures (``{id}``) arrive decoded and typed in ``path_params``;
* an unanswered request receives a transport ``503`` at the configured
  request timeout — requests are never silently dropped;
* TLS (including mTLS, with the verified client-certificate subject on
  ``WebPeer``) is part of ``WebServerConfig``.

WebSockets
----------

Upgrade routes (``WebRoute(..., upgrade=True)``) stream connection events
and inbound frames; sends are keyed by connection id.  Connection open and
close are data (``WsEvent``), so session lifecycle is ordinary graph logic.

HTTP client
-----------

``web_http_request`` maps one request time series to one result with
distinct arms: an HTTP ``response`` (whatever its status) or a transport
``failure`` (``WebTransportError`` with the curl error code) — transport
failure is never disguised as a status code.  The WebSocket client connects
per immutable ``WsClientKey``; removing the key closes the connection.

Events, statistics, and flow control
------------------------------------

Every server/client path also publishes a typed event stream
(``web_server_events`` / ``web_client_events``) and periodic statistics
(including the bound ``listening_port`` when ``port=0`` binds ephemerally —
the deterministic-test hook).  All boundary queues are bounded in records
and bytes with explicit overflow policies; watermarks pause socket reads
while the graph catches up.

Relationship to ``hgraph.adaptors.tornado``
-------------------------------------------

The released tornado adaptor keeps working unchanged and does not require
this extension.  A compatibility surface re-implementing the tornado API on
the native transports ships as ``hgraph_web.compat``; migration of the
tornado package itself is a separate, evidence-driven decision
(RFC 0024, compatibility and migration).
