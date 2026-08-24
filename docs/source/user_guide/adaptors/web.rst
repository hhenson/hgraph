Web (HTTP/S and WebSockets)
===========================

``hgraph-web`` is a first-party *extension* (like Kafka): a separate
distribution with its own native transports, built against the hgraph SDK.
Install it with ``pip install hgraph-web``.  The design record is
:doc:`RFC 0024 <../../rfc/rfc_0024_web_extension_api>`; this page is the
user-facing guide.

It provides all four transports — HTTP(S) server, HTTP(S) client, WebSocket
server, and WebSocket client — as native services: an Asio/Beast + nghttp2
server and a libcurl client.  Both sides speak HTTP/1.1 and HTTP/2 (the
server negotiates HTTP/2 over TLS when ``"h2"`` is advertised in the ALPN
list; the client via its version policy).  Requests, frames, and connection
lifecycles are ordinary graph data with bounded queues throughout; there is
no user-held server or client object outside the graph.

The model
---------

The extension adds no web-specific runtime concepts.  Each operation maps
onto an existing hgraph service contract, registered once per *service
path* and used from anywhere in the graph:

.. list-table::
   :header-rows: 1
   :widths: 24 26 50

   * - Verb
     - Contract
     - Meaning
   * - ``web_serve(route, path=)``
     - subscription
     - The immutable :class:`WebRoute` is the key; requests matching it
       stream out, already routed — no in-graph URL matching.
   * - ``web_respond(request_id, response, path=)``
     - request/reply
     - Answers are correlated by the transport-assigned ``request_id``;
       each responder receives delivery reports for its own answers.
   * - ``web_ws_serve(route, path=)`` / ``web_ws_send(...)``
     - subscription / request/reply
     - Upgrade routes stream connection events and inbound frames; sends
       are keyed by ``connection_id`` and acknowledged by reports.
   * - ``web_http_request(request, options=, path=)``
     - request/reply
     - One request time series maps to one result with distinct
       ``response`` / ``failure`` arms.
   * - ``web_ws_connect(key, path=)`` / ``web_ws_client_send(...)``
     - subscription / request/reply
     - The immutable :class:`WsClientKey` *is* the connection: adding it
       connects, removing it closes.
   * - ``web_server_events`` / ``web_client_events``
     - reference
     - One shared, typed :class:`WebEvent` stream per path.
   * - ``web_server_stats`` / ``web_client_stats``
     - reference
     - One shared, periodically ticking statistics record per path.

Under the hood, transport I/O threads deliver into the graph only through
root push sources, and the graph reaches the sockets only through sink
nodes — evaluation itself stays single-threaded, with the bounded boundary
queues (and their short handoff locks) as the only cross-thread
touchpoints.  Each independently ordered service-output channel has its own
burst push source: HTTP ingress cannot sit behind a WebSocket, delivery-report,
diagnostic, or statistics backlog.  Distinct route, connection, or request-id
keys pending in one burst tick together; repeated values for one key retain
FIFO order over consecutive ``MIN_TD`` cycles.  The keyed paths use standard
``collect`` plus mapped ``emit`` graph composition; scalar diagnostic streams
use standard ``emit`` directly, while statistics use the latest sample.  No total
order is invented across channels, and active channels may tick in the same
engine cycle.  A request
that a same-cycle handler can answer dispatches its response in that same
engine cycle; there is no per-request feedback cycle.

Nothing exists before the owning graph starts: sockets, TLS contexts, and
worker threads are created at node start (a bind failure fails graph
start), and stop drains bounded work before tearing down.  Several server
registrations may share one listening port when their configurations are
identical; requests dispatch per route, and a stopping registration
retires only its own work while the shared listener keeps serving the
rest.

Execution mode
--------------

The live server and client are asynchronous adaptors backed by root push
sources, so a graph that materializes either implementation must be wired and
run in **real-time mode**.  The socketless fake transports use the same
callback-driven boundary and have the same requirement.  Python applications
select the mode when running the graph:

.. code-block:: python

    hg.run_graph(app, run_mode=hg.EvaluationMode.REAL_TIME)

Native C++ applications select real-time topology before composition and use a
matching executor mode:

.. code-block:: cpp

    auto graph = build_graph<App>(WiringOptions{.is_realtime = true});
    GraphExecutorBuilder executor;
    executor.graph_builder(std::move(graph))
        .mode(GraphExecutorMode::RealTime);

Using the live or fake web registration from a simulation graph is a wiring
error; no socket is bound and no transport thread is started.  Deterministic
simulation needs a separate graph-owned scheduled or pull-source
implementation over recorded web data rather than an emulated push source.

Serving HTTP
------------

A server is registered at a path; routes are data.  A route's requests
arrive as a keyed stream and responses go back through ``web_respond``,
correlated by the transport-assigned request id:

.. code-block:: python

    import hgraph as hg
    import hgraph_web as web

    @hg.compute_node
    def request_id(request: hg.TS[web.HttpServerRequest]) -> hg.TS[int]:
        return request.value.request_id

    @hg.compute_node
    def echo(request: hg.TS[web.HttpServerRequest]) -> hg.TS[web.HttpResponse]:
        inbound = request.value.request
        name = next(
            (p.value for p in inbound.path_params if p.name == "name"), ""
        )
        return web.HttpResponse(
            200,
            headers=(web.WebHeader("content-type", "text/plain"),),
            body=f"hello {name}".encode(),
        )

    @hg.graph
    def app():
        web.register_web_server(web.WebServerConfig(port=8080), path="site")
        served = web.web_serve(
            web.WebRoute(web.HttpMethod.GET, "/echo/{name}"), path="site"
        )
        web.web_respond(
            request_id(served["request"]), echo(served["request"]), path="site"
        )

Route patterns are literal segments, ``{name}`` captures, and an optional
trailing ``/*rest``; precedence is literal over capture over rest, and
path segments are percent-decoded before matching.  ``HEAD`` requests are
served by a matching ``GET`` route with the body suppressed on the way
out.  An unanswered request receives a transport ``503`` at the configured
``request_timeout_ms`` and again at shutdown — requests are never silently
dropped.

Key properties of the value contract:

* headers, query parameters, and trailers are **ordered sequences** of
  name/value pairs — duplicates and arrival order survive;
* bodies are ``bytes``; text and JSON are codec decisions above the
  transport (see `Endpoints and codecs`_);
* path captures arrive decoded and named in ``path_params``;
* TLS (including mTLS, with the verified client-certificate subject on
  :class:`WebPeer`) is part of :class:`WebServerConfig`.

Data structures
---------------

Server side
~~~~~~~~~~~

:class:`WebRoute` — ``(method, pattern, upgrade=False)``.  The immutable
subscription key.  ``upgrade=True`` marks a WebSocket route.

:class:`HttpServerRequest` — what ``web_serve`` streams (the ``request``
field of its output bundle):

* ``request_id`` — transport-assigned, monotonic; the correlation token
  ``web_respond`` needs;
* ``connection_id`` / ``stream_id`` — the physical connection and (on
  HTTP/2) the real stream, for logging and diagnostics;
* ``request`` — the :class:`HttpRequest` payload;
* ``peer`` — the :class:`WebPeer` transport facts.

:class:`HttpRequest` — one shape for both directions: ``method``
(:class:`HttpMethod`), ``target`` (the raw request target), ``path``
(percent-decoded), ``query`` and ``path_params`` (tuples of
:class:`WebParam`), ``headers`` / ``trailers`` (tuples of
:class:`WebHeader`), ``body`` (``bytes``).  On HTTP/2, request trailers
(gRPC-style) arrive on ``trailers``.

:class:`HttpResponse` — ``status``, ``headers``, ``body``, ``trailers``.
Trailers on an HTTP/1.1 response are carried via chunked transfer; on
HTTP/2 they are native trailing headers.

:class:`WebPeer` — ``remote_address``, ``remote_port``, ``local_port``,
``tls``, ``negotiated_protocol`` (``"h2"`` when HTTP/2 was negotiated),
``sni``, and ``client_cert_subject`` (populated under mTLS).

WebSockets
~~~~~~~~~~

:class:`WsEvent` — connection lifecycle as data: ``connection_id``,
``state`` (:class:`WsConnectionState`: ``OPEN`` / ``CLOSING`` /
``CLOSED`` / ``FAILED``), the originating upgrade ``request`` on open, and
``close_code`` / ``close_reason`` on close.

:class:`WsInboundFrame` — ``connection_id`` plus a :class:`WsFrame`.

:class:`WsFrame` — ``kind`` (:class:`WsFrameKind`: ``TEXT`` / ``BINARY``
/ ``PING`` / ``PONG`` / ``CLOSE``) with ``text`` or ``data`` payloads and
close fields; build them with ``text_frame`` / ``binary_frame`` or the
codecs below.  Inbound messages are delivered complete (reassembled up to
``ws_max_message_bytes``); on the server, outbound messages are fragmented
at ``ws_max_frame_bytes``, while the client rejects an oversized outbound
frame with a delivery report instead of fragmenting it.

:class:`WsClientKey` — ``url``, extra ``headers``, ``subprotocols``; the
client-side connection identity and lifecycle.

Client side
~~~~~~~~~~~

:class:`HttpClientRequest` — ``method``, absolute ``url``, ``headers``,
``body``.

:class:`HttpClientOptions` — per-call ``connect_timeout_ms``,
``request_timeout_ms``, redirect behaviour, and the ``http_version``
policy (:class:`WebHttpVersionPolicy`: ``AUTO`` negotiates, ``H1_ONLY``
pins 1.1, ``H2_ONLY`` *requires* HTTP/2 — an h1 answer is reported as a
transport failure, never silently downgraded).

``web_http_request`` output — a bundle with two arms: ``response``
(:class:`HttpResponse`, whatever its status) and ``failure``
(:class:`WebTransportError` with the curl error code).  A ``404`` is a
response; a refused connection is a failure — fault domains never mix.

Reports, events, statistics
~~~~~~~~~~~~~~~~~~~~~~~~~~~

:class:`WebDeliveryReport` — the asynchronous acknowledgement for
responses and WebSocket sends: ``request_id`` (or connection id for
sends), ``status`` (:class:`WebDeliveryStatus`: ``DELIVERED`` only after
the bytes left the connection; ``DROPPED`` when the peer cancelled or a
slow consumer was cut; ``ENQUEUE_REJECTED``, ``RETRIABLE_FAILURE``, or
``PERMANENT_FAILURE`` otherwise), plus ``retriable`` / ``fatal`` /
``message``.

:class:`WebEvent` — the shared diagnostics stream: ``severity``
(:class:`WebSeverity`), ``component`` / ``category`` / ``message``,
``service_path``, and ``connection_id`` where relevant.  Fatal transport
conditions arrive here (and stop the graph under
``WebFailurePolicy.STOP_GRAPH``).

:class:`WebServerStats` / :class:`WebClientStats` — periodic (enable with
``stats_interval_ms > 0``): the bound ``listening_port`` (the
deterministic-test hook when ``port=0`` binds ephemerally), connection and
pending counts, ingress queue depths in records and bytes, and the
``dropped_count``.

WebSockets, served and dialled
------------------------------

.. code-block:: python

    @hg.graph
    def live():
        web.register_web_server(web.WebServerConfig(port=8080), path="site")
        ws = web.web_ws_serve(
            web.WebRoute(web.HttpMethod.GET, "/live", upgrade=True), path="site"
        )
        # ws["event"]  : TS[WsEvent]        — open/close as data
        # ws["frame"]  : TS[WsInboundFrame] — complete inbound messages
        replies = handle(web.ws_text(ws["frame"]))       # your logic
        web.web_ws_send(connection_of(ws), web.text_frame(replies), path="site")

Connection open and close are just events, so session lifecycle is
ordinary graph logic; the served request that opened the socket rides on
the ``OPEN`` event.  The client mirror is ``web_ws_connect(key)`` —
producing the same event/frame shape — with ``web_ws_client_send(key,
frame)`` for output; removing the key sends a Close frame and tears the
connection down promptly.  Both transports send a Close frame; they then
tear down once that write completes or peer closure is observed rather than
waiting indefinitely for a close echo.

Calling out
-----------

.. code-block:: python

    result = web.web_http_request(
        hg.const(
            web.HttpClientRequest(web.HttpMethod.GET, "https://example.com/api")
        ),
        options=hg.const(web.HttpClientOptions(request_timeout_ms=5_000)),
        path="api",
    )
    handle_response(result["response"])
    handle_outage(result["failure"])

The client pool is bounded (``max_connections_per_host`` /
``max_total_connections``) and every call **reserves** its maximum
response size (``max_response_bytes``) before it is handed to the
transport, so a completed response can never be dropped for lack of queue
space — the in-flight ceiling is ``ingress_byte_limit /
max_response_bytes``.  Calls beyond the ceiling are staged or failed per
``overflow`` / ``stage_overflow``.

HTTP/2
------

*Server*: advertise it in the TLS ALPN list —

.. code-block:: python

    web.WebServerConfig(
        port=8443,
        tls=web.TlsServerConfig(
            cert_path="server.pem", key_path="server.key",
            alpn=("h2", "http/1.1"),
        ),
        h2_max_concurrent_streams=100,
        h2_initial_window_bytes=1024 * 1024,
    )

Clients that negotiate ``h2`` get genuine multiplexing: concurrent
streams on one connection (each with the real ``stream_id`` on its
requests), per-stream flow control mapped onto the same ingress admission
as HTTP/1.1, per-stream cancellation (a client ``RST_STREAM`` retires
exactly that request; a late graph answer reports ``DROPPED``), and
GOAWAY-based draining at shutdown.  Everything else on this page applies
unchanged — same routes, same values, same reports.  What HTTP/2 does
*not* include (WebSocket-over-h2, server push, streaming bodies, HTTP/3)
is recorded in the RFC's protocol matrix.

*Client*: ``WebHttpVersionPolicy.AUTO`` (the default) negotiates per
connection; ``H2_ONLY`` uses prior knowledge on ``http://`` and ALPN on
``https://``, verifying the negotiated version after completion.

TLS
---

:class:`TlsServerConfig` takes the certificate chain and key as file
paths *or* inline PEM (exactly one of each), an optional key password,
and — for mTLS — a client CA plus ``client_verify``
(:class:`WebClientVerify`: ``NONE`` / ``OPTIONAL`` / ``REQUIRED``); the
verified subject appears on ``WebPeer.client_cert_subject``.  The minimum
TLS version defaults to 1.2.  :class:`TlsClientConfig` mirrors it for the
client (custom CA, client certificate, ``sni``, ``verify_peer`` /
``verify_host``, ALPN).

Endpoints and codecs
--------------------

The decorators wrap the serve/respond pairing for the common shapes, and
the codec helpers compose hgraph's native JSON codec:

.. code-block:: python

    @web.http_endpoint(web.WebRoute(web.HttpMethod.POST, "/greet"), path="site")
    @hg.graph
    def greet(request: hg.TS[web.HttpServerRequest]) -> hg.TS[web.HttpResponse]:
        return web.json_response(bump(web.request_json(request, Greeting)))

``request_json(request, T)`` decodes the body into ``TS[T]``;
``json_response(value, status=200)`` encodes with the right content type.
``ws_endpoint`` does the same for WebSocket routes, and ``ws_text`` /
``ws_binary`` / ``ws_json`` convert between frames and payload streams.

Authentication is a wiring argument, not a process global: pass
``auth=`` a graph ``TS[HttpServerRequest] -> TS[AuthResult]``; a denial
short-circuits with the given status before your handler runs:

.. code-block:: python

    @hg.compute_node
    def authenticate(request: hg.TS[web.HttpServerRequest]) -> hg.TS[web.AuthResult]:
        ok = any(h.name == "authorization" for h in request.value.request.headers)
        return web.AuthResult(allowed=ok, status=401, reason="credentials required")

    @web.http_endpoint("/private", path="site", auth=authenticate)
    @hg.compute_node
    def private(request: hg.TS[web.HttpServerRequest]) -> hg.TS[web.HttpResponse]:
        return web.HttpResponse(200, body=b"secret")

Flow control and memory bounds
------------------------------

Every boundary queue is bounded in **records and bytes**, and the byte
accounting is real: a request's admission is *reserved* against
``ingress_byte_limit`` before its body is even read off the socket, so a
flood cannot make the transport boundary retain more than you configured —
the excess stays in the kernel (HTTP/1.1 and WebSocket via paused reads,
HTTP/2 via withheld per-stream flow-control window).  Capacity is released
when a burst enters graph processing; any later mapped ``emit`` backlog is
ordinary graph state and is not charged to the transport budget.  At the
configured watermarks
(``watermark_high_pct`` / ``watermark_low_pct``) socket reads pause and
resume; at the hard limit the ``inbound_overflow`` policy applies:

* ``BACKPRESSURE`` (default) — leave the request unread until capacity
  returns;
* ``REJECT`` — answer ``503`` (or close a WebSocket with ``1013``) from
  the transport without graph involvement.

A payload that could *never* fit is rejected outright (``413``, or
WebSocket close ``1009``) rather than waiting forever, and configuration
that would make legal payloads unadmittable — limits below the documented
floors, or a route whose pattern is too large for the configured limits —
fails at wiring or route-application time with a pointed error.

Outbound, each connection owns a bounded queue; a slow WebSocket consumer
triggers ``slow_consumer_policy`` (``CLOSE`` with code ``1013``, or
``DROP_NEWEST``), and on HTTP/2 a slow stream is reset without harming
its connection.  Delivery reports tell you which fate each send met.

Testing
-------

The loopback pattern makes live-transport tests deterministic without
fixed ports: bind with ``port=0``, enable ``stats_interval_ms``, and
trigger the client off the discovered port —

.. code-block:: python

    web.register_web_server(
        web.WebServerConfig(port=0, stats_interval_ms=50), path="site"
    )
    web.register_web_client(web.WebClientConfig(), path="api")

    @hg.compute_node
    def when_listening(stats: hg.TS[web.WebServerStats]) -> hg.TS[web.HttpClientRequest]:
        port = stats.value.listening_port
        if port:
            return web.HttpClientRequest(
                web.HttpMethod.GET, f"http://127.0.0.1:{port}/echo/it"
            )

    result = web.web_http_request(
        when_listening(web.web_server_stats(path="site")), path="api"
    )

The extension's own suites run exactly this shape — the graph is both
server and client of the same process.

Configuration reference
-----------------------

:class:`WebServerConfig` (defaults in parentheses):

* ``bind_address`` (``0.0.0.0``), ``port`` (``0`` = ephemeral),
  ``bind_deferred`` (bind at first route instead of start), ``io_threads``
  (1), ``max_connections`` (10 000);
* request shape: ``max_header_bytes`` (64 KiB), ``max_body_bytes``
  (16 MiB);
* time: ``request_timeout_ms`` (30 s — the transport-503 deadline),
  ``idle_timeout_ms`` (60 s), ``keep_alive_timeout_ms`` (15 s),
  ``shutdown_drain_timeout_ms`` (5 s);
* ingress bounds: ``ingress_record_limit`` / ``ingress_byte_limit``
  (10 000 / 64 MiB) and the WS pair, ``watermark_high_pct`` /
  ``watermark_low_pct`` (80/50), ``inbound_overflow`` (``BACKPRESSURE``);
* outbound: ``outbound_message_limit`` / ``outbound_byte_limit``
  (1 000 / 16 MiB), ``slow_consumer_policy`` (``CLOSE``);
* WebSocket: ``ws_max_frame_bytes`` (1 MiB), ``ws_max_message_bytes``
  (16 MiB), ``ping_interval_ms`` / ``pong_timeout_ms`` (30 s / 10 s);
* HTTP/2: ``h2_max_concurrent_streams`` (100),
  ``h2_initial_window_bytes`` (1 MiB);
* ``failure_policy`` (``REPORT``; ``STOP_GRAPH`` turns fatal transport
  events into an engine stop), ``stats_interval_ms`` (0 = off),
  ``tls`` (``None`` = plaintext).

:class:`WebClientConfig`: ``http_version_policy`` (``AUTO``), pool sizes
``max_connections_per_host`` / ``max_total_connections`` (6/64), the
timeout family, ``proxy``, ``max_response_bytes`` (16 MiB — the per-call
reservation), the same ingress/watermark family, call staging
``overflow`` / ``stage_overflow`` (``STAGE`` / ``FAIL``), the WebSocket
family, ``failure_policy``, ``stats_interval_ms``, ``tls``.

Relationship to ``hgraph.adaptors.tornado``
-------------------------------------------

The released tornado *server* surface (``http_server_handler``,
``websocket_server_handler``, their adaptors, and ``@rest_handler`` above
them) is now **implemented by this extension**: ``hgraph_web.compat``
reproduces the released semantics on the native transports, and the
``hgraph[web]`` extra installs ``hgraph-web`` alongside Tornado so those
import paths keep working unchanged.  The tornado-backed HTTP/WebSocket
*clients* remain in core.  New code should use ``hgraph_web`` directly —
everything above this section.
