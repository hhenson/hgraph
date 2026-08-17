RFC 0024: C++-First HTTP/S and WebSocket Extension API
======================================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-08-16
:Target: Web extension, public C++ SDK, and Python compatibility surface

Summary
-------

Replace the current Python-only tornado adaptor family with a separately
installed, C++-first ``hgraph-web`` extension covering all four transports:
HTTP(S) server, HTTP(S) client, WebSocket server, and WebSocket client.
Each side is exposed as a small set of service interfaces implemented by one
multi-interface ``service_impl`` at a path.  Registration supplies one
immutable compound-scalar configuration; the implementation materializes once
for that binding point and creates its sockets, TLS contexts, and worker
threads only when the owning graph starts.  There is no user-held web server
or client object outside the graph.

The extension uses hgraph's existing boundary vocabulary rather than adding a
web-specific runtime to the core:

.. list-table::
   :header-rows: 1
   :widths: 26 27 47

   * - Operation
     - hgraph contract
     - Reason
   * - Serve a route
     - subscription service
     - The immutable route (method, pattern, upgrade flag) is the key and the
       request stream is keyed by that same route.
   * - Respond
     - request/reply service
     - Each responder correlates by transport-assigned request id and receives
       asynchronous delivery reports for its own responses.
   * - WebSocket serve
     - subscription service
     - Upgrade routes are keys; connection events and inbound frames stream
       per route with the connection id in band.
   * - WebSocket send
     - request/reply service
     - Outbound frames are keyed sends acknowledged by delivery reports.
   * - HTTP client call
     - request/reply service
     - One request time-series maps to one result with a distinct
       transport-failure arm.
   * - WebSocket connect
     - subscription service
     - The immutable connection key (url, headers, subprotocols) is the key;
       its lifecycle is the subscription lifecycle.
   * - Service events
     - reference service
     - Listener, connection, TLS, retry, and fatal events are one shared
       source per service path.
   * - Statistics
     - reference service
     - Queue depths, connection counts, and the bound listening port are one
       shared, periodically ticking source.

The ``service_impl`` obtains route sets, responses, and outbound frames
through ``service::impl_input`` and feeds them to graph sink nodes.  Its
I/O threads return requests, frames, delivery reports, events, and stats only
through root push-source nodes bound with ``service::impl_output``.  These
are decoupled external sinks and sources, so RFC 0014's automatic transport
planner gives them direct request and response paths.  No feedback delay,
adaptor-specific cycle, or user transport flag is required.  A request that
can be answered by a same-cycle handler dispatches its response in that same
engine cycle; the tornado adaptor's mandatory extra feedback cycle per
request is retired.

Routing lives in the transport, not the graph: the route key set arrives as
``TSS`` deltas, is compiled into a native route table, and requests are
delivered already keyed by route.  The tornado pattern of partitioning a
request dictionary by string matching inside the graph is retired with it.

The released tornado decorator surface remains importable from core
unchanged, but its server tier is now implemented by the extension's
compatibility module: ``hgraph.adaptors.tornado``'s HTTP and WebSocket
server modules re-export ``hgraph_web.compat``, which re-implements the
released decorator surface on the native transport (decision taken during
review of the initial implementation, eliminating the parallel tornado
server implementation).  The client adaptors and the REST lowering remain
Tornado-backed.

Motivation
----------

The tornado adaptor family reached useful compatibility quickly, but its
public shape and runtime ownership are not a suitable basis for a native
extension:

* there is no TLS support at all — no HTTPS listener, no ``wss://``, no
  certificate configuration surface;
* multi-value query parameters are collapsed by joining values into one
  string, and repeated headers collapse to the last value because headers are
  projected through a dictionary
  (``python/hgraph/adaptors/tornado/http_server_adaptor.py``);
* request and response bodies are ``str``, making binary payloads
  unrepresentable without encoding tricks;
* the HTTP method set is a closed union and the URL is matched by in-graph
  string partitioning over a request dictionary rather than by a route
  table;
* route registries and the authentication hook are process-global mutable
  state, so two graphs in one process interfere;
* ingress queues are unbounded, allowing socket rate to determine graph
  memory use;
* a handler exception and a client error both surface as status 400,
  conflating fault domains;
* timeouts are hardcoded in the adaptor rather than configuration; and
* every request pays a feedback engine cycle and two GIL crossings because
  the adaptor round-trips through Python queues.

The kafka extension (RFC 0015) established the architecture this RFC reuses:
bounded bridge channels with conflating wake push-sources, graph-owned drain
nodes, one multi-interface implementation per path, strict start/stop
orderings, a fake transport seam for socketless testing, and a separately
versioned wheel over the installed SDK.  Where this RFC is silent on a
mechanism, RFC 0015's contract applies unchanged.

Prior art and evidence
----------------------

Reactive and dataflow stacks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* **Akka/Pekko HTTP** models a server as a ``Flow[HttpRequest,
  HttpResponse]`` per connection: request-in, response-out as one streaming
  pair.  hgraph's keyed subscription plus request/reply services express the
  same shape without a per-connection materialized flow.
* **Reactor Netty / Spring WebFlux** models a WebSocket session as a duplex
  pair of independent streams (``receive()`` and ``send()``).  The extension
  mirrors this: inbound frames and outbound sends are separate services that
  share a connection id.
* **Flink Async I/O** is the canonical client-in-dataflow operator: bounded
  in-flight capacity, per-request timeout, and an explicit error path.  The
  HTTP client service adopts exactly that contract.
* **RxJS ``webSocket()``** treats a WS client as one subject keyed by its
  connection arguments; reconnect is layered policy.  The WS client
  subscription key plays the same role.
* **Phoenix channels** treat connection lifecycle as data (join/leave).  The
  extension delivers connection open/close as typed events so key lifecycle
  is observable graph data.
* **kdb+ tick** feeds a single-threaded main loop from I/O threads — the
  same shape as hgraph's push-source boundary.
* The cross-cutting lesson from uWebSockets and the Reactive Streams
  ecosystem: backpressure must be an explicit, observable queue policy at
  the process boundary.  Every channel in this extension is bounded with a
  declared overflow action; slow WebSocket consumers get an explicit
  per-connection outbound policy.

C++ substrate
~~~~~~~~~~~~~

Boost.Beast provides mature HTTP/1.1 parsing and WebSocket support over
Asio, but has no HTTP/2 support and none scheduled (boostorg/beast#2950).
libcurl provides HTTP/1.1, HTTP/2, and (from 8.11, post-experimental) a
WebSocket client API, but no server.  libnghttp2 is the reference HTTP/2
session library — and is already libcurl's own h2 engine, so it is in the
dependency tree regardless; the deprecated ``nghttp2-asio`` wrapper is not
used.  The composite therefore is:

* **client**: libcurl (h1 + h2 day one; WS client via ``curl_ws_send`` /
  ``curl_ws_recv``);
* **server**: Asio + Beast for h1.1 and WebSocket, with an ALPN-selected
  protocol seam behind which an nghttp2 server session activates HTTP/2 in
  a later release; and
* **TLS**: OpenSSL on every platform (``CURL_USE_OPENSSL`` everywhere,
  including Windows), giving one TLS configuration surface and reusing the
  kafka static-linking wheels precedent.

Ownership boundary
------------------

``hgraph-web`` owns:

* all web request, response, frame, route, peer, configuration, delivery-
  report, event, and statistics types;
* Boost/Asio/Beast, libcurl, libnghttp2, and OpenSSL discovery, linking,
  configuration, and RAII wrappers;
* listener sockets, TLS contexts, connection strands, the curl multi loop,
  route tables, bounded queues, watermarks, and shutdown;
* web-specific serve, respond, send, call, connect, event, and stats
  service descriptors and implementations;
* codec helpers layered above byte payloads and its C++ and Python user
  APIs; and
* integration tests and performance evidence involving live sockets.

hg_cpp owns only the already-public facilities the extension consumes:
native graph and node authoring; subscription, request/reply,
reference-service, and transport planning; root push sources and
graph-thread scheduling; native extension scalar and Python-class
registration; and installed-SDK extension boundaries.

The core must not link to curl, Boost, nghttp2, or OpenSSL, include their
headers, or import ``hgraph_web`` during a normal ``hgraph`` import; the
only core package dependency on the extension is the ``hgraph[web]`` extra,
which installs ``hgraph-web`` alongside Tornado because the released
``hgraph.adaptors.tornado`` server tier is served by ``hgraph_web.compat``.
The core wheel owns a guarded lazy shim at ``hgraph.adaptors.web`` which
imports ``hgraph_web`` only when that path is explicitly used, and
``hgraph.adaptors.tornado`` exports every name lazily (PEP 562) so the
Perspective adaptor and the debug inspector — which use only its Tornado
plumbing — keep working without hgraph-web installed.  The extension wheel
installs only the ``hgraph_web`` package and never contributes files to
``hgraph``.

The first-party extension lives under ``extensions/web`` in the hg_cpp
monorepo, with its own CMake package, ``pyproject.toml``, version, and
release artifacts, exactly as ``extensions/kafka`` does.

No generic connector layer is introduced by this RFC.  The bridge pattern is
deliberately re-instantiated per extension; promotion to core waits for the
promotion evidence rule of RFC 0000.

Public value contract
---------------------

The semantic shapes below are normative; exact field spelling remains
reviewable while the RFC is Proposed.  Structured values are named
``Bundle`` schemas in the ``hgraph.web`` namespace; collections of
time-series fields are named ``TSB`` schemas.  The Python classes are
registrations of these same schemas.

.. code-block:: cpp

   using WebHeader = Bundle<"hgraph.web::WebHeader",
       Field<"name", Str>,
       Field<"value", Str>>;

   using WebParam = Bundle<"hgraph.web::WebParam",
       Field<"name", Str>,
       Field<"value", Str>>;

   using WebRoute = Bundle<"hgraph.web::WebRoute",
       Field<"method", HttpMethod>,
       Field<"pattern", Str>,
       Field<"upgrade", Bool>>;

   using HttpRequest = Bundle<"hgraph.web::HttpRequest",
       Field<"method", HttpMethod>,
       Field<"target", Str>,
       Field<"path", Str>,
       Field<"query", HomogeneousTuple<WebParam>>,
       Field<"path_params", HomogeneousTuple<WebParam>>,
       Field<"headers", HomogeneousTuple<WebHeader>>,
       Field<"body", Bytes>,
       Field<"trailers", HomogeneousTuple<WebHeader>>>;

   using HttpServerRequest = Bundle<"hgraph.web::HttpServerRequest",
       Field<"request_id", Int>,
       Field<"connection_id", Int>,
       Field<"stream_id", Int>,
       Field<"request", HttpRequest>,
       Field<"peer", WebPeer>>;

   using HttpResponse = Bundle<"hgraph.web::HttpResponse",
       Field<"status", Int>,
       Field<"headers", HomogeneousTuple<WebHeader>>,
       Field<"body", Bytes>,
       Field<"trailers", HomogeneousTuple<WebHeader>>>;

Key semantic rules, each closing a released defect:

``WebHeader`` / ``WebParam``
   Headers, query parameters, and decoded path parameters are ALWAYS ordered
   sequences of name/value pairs, never maps.  Duplicate names and arrival
   order are preserved by construction; the dictionary collapse of the
   tornado adaptor is unrepresentable.

``HttpMethod``
   A typed enum: ``Get``, ``Head``, ``Post``, ``Put``, ``Delete``,
   ``Patch``, ``Options``, ``Trace``.  This covers the closed set the
   transport itself must special-case (HEAD suppresses bodies, CONNECT is
   rejected).  An open-set extension-method form is deferred to a later
   revision; it is a schema addition, not a change.

``HttpRequest``
   ``target`` is the raw request target; ``path`` is the decoded path;
   ``query`` preserves raw multi-value parameters in order; ``path_params``
   carries the decoded ``{param}`` captures the route match produced;
   ``body`` is ``Bytes``.  Text is a codec decision above the transport,
   never a transport assumption.

``HttpServerRequest``
   Adds the transport-assigned identifiers and peer.  ``request_id`` is a
   monotonic integer unique within one server runtime for one graph run and
   is the respond/report correlation key.  ``stream_id`` is 0 on HTTP/1.1
   and the real stream id on HTTP/2 — the model is stream-aware from day
   one.  ``WebPeer`` carries remote address/port, local port, whether TLS
   is in use, the negotiated ALPN protocol, SNI, and the verified client
   certificate subject when mTLS is configured, so authentication can be
   built above the transport from data.

``HttpResponse``
   Complete-message bodies in v1: no streaming or server-sent events.  The
   stream-aware request shape does not preclude a later streaming RFC.

``WebTransportError`` and ``HttpCallResult``
   A client call result is a ``TSB`` with a ``response`` arm and a distinct
   ``failure`` arm (``WebTransportError`` with the curl error code, message,
   and a retriable flag).  Transport failure is never disguised as an HTTP
   status.

``WsFrame`` / ``WsInboundFrame`` / ``WsEvent``
   ``WsFrame`` carries kind (``Text``, ``Binary``, ``Ping``, ``Pong``,
   ``Close``), text, data, close code, and close reason, and serves both
   directions.  ``WsInboundFrame`` pairs a frame with its ``connection_id``.
   ``WsEvent`` carries connection id, state (``Open``, ``Closing``,
   ``Closed``, ``Failed``), the upgrade ``HttpServerRequest`` on open, and
   close code/reason.  Connection lifecycle is data.

``WsClientKey``
   The WebSocket client subscription key: url, ordered headers, and
   subprotocol list.  It is immutable and hashable; its subscription
   lifecycle is the connection lifecycle.

``WebDeliveryReport`` / ``WebEvent`` / ``WebServerStats`` / ``WebClientStats``
   Field-for-field mirrors of their kafka counterparts where applicable.
   ``WebDeliveryStatus`` is ``EnqueueRejected``, ``Dropped``, ``Delivered``,
   ``RetriableFailure``, or ``PermanentFailure``; enqueue acceptance is not
   delivery.  Events carry severity, component, category, error code,
   retriable/fatal flags, service path, connection id, and message; never
   credentials.  Stats carry connection counts, queue depths and retained
   bytes, and the bound ``listening_port`` — the deterministic-test hook for
   ephemeral ports.

The transport is byte-oriented.  Typed serialization is graph composition:
JSON, form, and text codecs are helpers over ``Bytes`` using the existing
native JSON codec, not hidden connection state.

Configuration contract
----------------------

Configuration is immutable compound-scalar data registered at one service
path.  A materially different configuration requires a different path.

``TlsServerConfig``
   Certificate chain and private key (filesystem path XOR inline PEM, per
   item), optional key password, optional client CA and ``client_verify``
   (``None`` / ``Optional`` / ``Required`` — ``Required`` is mTLS and
   surfaces the subject in ``WebPeer``), ALPN protocol list, and minimum
   TLS version (default 1.2).  An empty config means a plaintext listener.
   Advertising ``h2`` in ALPN is a validation error until the server HTTP/2
   session is activated: an implemented protocol list is a contract, not an
   aspiration.

``TlsClientConfig``
   CA override (path XOR PEM), client certificate/key for mTLS, ``sni``,
   ``verify_peer``/``verify_host``, ALPN, minimum version.

``WebServerConfig``
   ``bind_address`` (default ``0.0.0.0``), ``port`` (0 = ephemeral; the
   bound port is surfaced via a Listening event and stats), ``tls``,
   ``io_threads`` (default 1), connection/header/body limits, request and
   idle and keep-alive timeouts, ``bind_deferred`` (delay ``listen()``
   until the first route set applies, for zero-404 startup), bounded
   ingress limits in records and bytes for the request and WS channels,
   watermark percentages (default 80/50), ``inbound_overflow``
   (``Backpressure`` default — the request waits unread in the socket; or
   ``Reject`` — the transport answers 503 itself), per-connection outbound
   message/byte limits, ``slow_consumer_policy`` (``Close`` default,
   ``DropNewest`` option), ``failure_policy`` (``Report`` / ``StopGraph``),
   shutdown drain timeout, WS frame/message caps and ping/pong intervals,
   stats interval, and HTTP/2 placeholders (``h2_max_concurrent_streams``,
   ``h2_initial_window_bytes``) present in the schema from day one so
   activation is not a schema break.

``WebClientConfig``
   ``http_version_policy`` (``Auto`` — curl negotiates h2 day one;
   ``H1Only``; ``H2Only``), per-host and total connection limits, connect
   and request timeouts, keep-alive, redirect policy, proxy, ``tls``,
   ``max_response_bytes`` (the ingress reservation unit; transfers
   exceeding it abort with a typed failure), ingress limits and watermarks,
   and outbound limits/overflow/drain/failure mirroring
   ``KafkaProducerOptions``, plus WS mirrors and stats interval.

All validation happens at wiring time; a graph that wires successfully has
a structurally valid configuration.

Public C++ wiring API
---------------------

One explicit service implementation registration per side and ordinary
service calls; there is deliberately no ``WebServer`` or ``WebClient``
instance:

.. code-block:: cpp

   static void compose(Wiring &w)
   {
       const auto site = service::path("site");

       web::register_server(w, site,
           web::server_config().port(8443)
               .tls(web::tls_server().cert_path("...").key_path("..."))
               .build());

       auto route = wire<stdlib::const_>(
           w, web::route(HttpMethod::Get, "/api/orders/{id}"))
           .as<TS<WebRoute>>();

       auto orders = wire<web::HttpServeService>(w, site, route)
           .as<WebRouteOutput>();

       auto response = wire<HandleOrder>(w, orders.field<"request">());
       auto reports  = wire<web::HttpRespondService>(w, site, response);

       wire<ObserveWebEvent>(w, wire<web::WebServerEventService>(w, site));
   }

The primary operations are:

``wire<HttpServeService>(w, path, TS[WebRoute])``
   Returns ``WebRouteOutput`` — a ``TSB`` of ``request:
   TS[HttpServerRequest]`` and ``state: TS[WebRouteState]``.  The service
   mechanism converts live route keys to the implementation's
   ``TSS[WebRoute]``; the transport compiles the table and delivers each
   matched request on its route's stream.  Distinct routes tick in the same
   cycle; multiple pending requests on ONE route are paced one per engine
   cycle at ``MIN_TD`` — the same drain the kafka subscription service
   uses.  A batched nested-TSD delivery shape is the identified follow-up
   if pacing proves limiting on very hot routes.

``wire<HttpRespondService>(w, path, HttpRespondRequest)``
   ``HttpRespondRequest`` is a ``TSB`` of ``request_id: TS[Int]`` and
   ``response: TS[HttpResponse]``.  Returns ``TS[WebDeliveryReport]``.
   Responding to an unknown or already-answered id is a reported error,
   not silence.  A request the graph never answers is answered by the
   transport with 503 at its timeout or at shutdown; requests are never
   silently dropped.

``wire<WsServeService>(w, path, TS[WebRoute])`` (``upgrade`` routes)
   Returns a ``TSB`` of ``event: TS[WsEvent]`` and
   ``frame: TS[WsInboundFrame]``.  Per-connection demultiplexing is graph
   logic over data: routes are wiring-time structure, connections are
   runtime data.

``wire<WsSendService>(w, path, WsSendRequest)``
   ``TSB`` of ``connection_id`` and ``frame``; a ``Close`` frame is the
   graph-initiated close.  Returns ``TS[WebDeliveryReport]``.

``wire<HttpClientService>(w, path, HttpClientCall)``
   ``TSB`` of ``request: TS[HttpClientRequest]`` and ``options:
   TS[HttpClientOptions]`` (constant options tick once).  Returns
   ``HttpCallResult`` with distinct response and failure arms.  In-flight
   capacity is bounded by configuration, Flink-async-I/O style.

``wire<WsClientService>(w, path, TS[WsClientKey])``
   Returns ``TSB`` of ``event`` and ``frame`` for that key; removing the
   key closes the connection.  ``wire<WsClientSendService>`` sends frames
   keyed by the same key.

``wire<WebServerEventService>(w, path)`` / ``wire<WebClientEventService>``
   The shared typed event stream.  ``WebServerStatsService`` /
   ``WebClientStatsService`` are the periodic statistics sources.

``web::register_server`` / ``web::register_client`` are graph-time only and
delegate to one ``service::register_services<Impl, Interfaces...>`` call.
They record one lazy multi-interface implementation candidate; they do not
bind a socket, create a TLS context, start a thread, or perform I/O.
Duplicate registration at a path is a wiring error.

Routing
-------

``WebRoute`` patterns consist of literal segments, ``{param}`` captures,
and an optional trailing ``/*rest``.  Match precedence is literal over
``{param}`` over ``/*rest``; path segments are percent-decoded before
matching and the captures are delivered decoded in ``path_params``.  The
route table is a per-method segment trie compiled copy-on-write from the
current ``TSS[WebRoute]`` set, swapped atomically, and read lock-free on
I/O threads.

Port sharing replaces the tornado ``TornadoWeb`` refcount singleton: a
process-wide listener registry keyed by (bind address, port).  The first
server runtime to start binds and listens; later starts attach only after
verifying an identical TLS configuration (mismatch is a start error); an
overlapping (method, pattern) registration across service paths is a start
error naming both paths; the last detach closes the listener so the port
can be rebound by a later run.  Keep-alive connections may span attached
services; dispatch is per-request by route match.

Runtime architecture
--------------------

Each materialized implementation owns one graph-local runtime resource for
its path and configuration; nothing is process-global except the listener
registry, and that holds no graph state.

* **Server**: one ``asio::io_context`` served by ``io_threads`` (default 1)
  with a strand per connection, so raising the thread count is safe without
  code change.  Accept, TLS handshake, h1 parse, and WS frames run on
  strands.  There is no egress staging thread: the graph-side respond sink
  resolves ``request_id`` to its connection and ``asio::post``\ s the
  prebuilt response onto the connection's strand — non-blocking, and the
  evaluation thread never touches a socket.
* **Client**: one curl-multi owner thread per client runtime, sleeping in
  ``curl_multi_poll`` and woken by ``curl_multi_wakeup`` when the graph
  stages a submission.  WebSocket handles join the same loop; all
  ``curl_ws_send``/``curl_ws_recv`` calls happen on this thread.
* **Bridge**: the kafka service-bridge contract verbatim — one mutex over
  all channels, per-channel bounded deques with record and byte limits and
  payload/control lanes, one payload-free conflating ``TS<Int>`` wake
  push-source per channel, drain nodes on the evaluation thread, and a
  reserve/push-reserved protocol so a response or report always has
  guaranteed queue capacity before the corresponding work is accepted.
  Server channels: requests, WS ingress, respond deliveries, WS send
  deliveries, events, stats.  Client channels: responses, WS ingress, send
  deliveries, events, stats.  Stats is the only channel permitted
  drop-oldest overflow (it is self-superseding).
* No worker thread calls ``EvaluationEngineApi``, mutates a time series, or
  retains a borrowed graph value.  ``PushSourceSender`` is the only
  cross-thread runtime boundary; off-thread value construction runs under
  the sender's type-realization scope.  Fatal conditions (acceptor death,
  io_context failure, curl fatal) cross the event channel, and a
  graph-thread drain node applies the configured ``Report`` or
  ``StopGraph`` policy.

Flow control and bounded memory
-------------------------------

Every queue is bounded in records and bytes.  At the configured high
watermark (default 80%) the transport stops issuing socket reads — HTTP/1.1
backpressure propagates naturally through TCP; the client pauses transfers
with ``curl_easy_pause``; the future h2 session withholds window updates —
and resumes below the low watermark (default 50%).  At the hard limit the
explicit ``inbound_overflow`` policy applies: ``Backpressure`` leaves the
request unread in the kernel; ``Reject`` answers 503 from the transport
without graph involvement.

Outbound, each connection owns a bounded queue on its strand.  A slow
WebSocket consumer triggers the explicit ``slow_consumer_policy``: ``Close``
(1013, with queued-unsent frames reported ``Dropped``) or ``DropNewest``.
There is no silent unbounded mode and no frame conflation in v1 —
conflation requires message identity, which is a codec-tier concept.

Client-side, the response reservation is taken before a request is handed
to curl: ``max_response_bytes`` is reserved on the response channel, so the
in-flight concurrency ceiling is ``ingress_byte_limit /
max_response_bytes`` and a completion can never be dropped for lack of
queue space.  Reservation failure rejects the call with a typed
``EnqueueRejected`` failure through the guaranteed control lane.

Lifecycle and teardown
----------------------

Wiring is parse/validate only.  Start order: push sources start first and
attach senders; the runtime then constructs, binds or attaches its listener
(bind failure fails graph start), starts I/O threads, verifies every sender
is attached, and begins accepting.  A start exception unwinds in reverse.

Server stop order is strict: (1) stop intake — detach routes, and if last
attachee cancel accept and close the listener; late keep-alive requests get
503 with ``Connection: close``; (2) drain — every request still pending
gets a transport-generated 503 and queued outbound bytes flush within the
shutdown drain timeout; (3) WebSockets get Close(1001) with a bounded
handshake wait; (4) cancel outstanding operations, stop the io_context,
join I/O threads; (5) only then stop the bridge — no worker can push into a
cleared bridge; (6) release state.  The client mirrors this around the curl
multi loop.  Destructors repeat a noexcept emergency stop if normal graph
stop was skipped.

Simulation mode hard-rejects live transport, exactly as kafka does: the
server does not bind, the client does not connect, and sends are rejected
with typed reports plus a warning event.  Deterministic simulation tests
use the fake transport seam.

Ordering and time
-----------------

* Requests on one connection preserve arrival order; no total order is
  invented across connections.
* One hgraph tick per request/frame; no conflation unless the user wires
  it.
* A request's evaluation time is the cycle in which the drain emits it;
  ``Date`` headers and payload timestamps are metadata.
* Multiple requests pending on one route are delivered on consecutive
  ``MIN_TD`` cycles in arrival order.

Python API
----------

Python mirrors the C++ service model.  Registration and service calls, not
objects:

.. code-block:: python

   register_web_server(WebServerConfig(port=8443, tls=...), path="site")

   @http_endpoint(route="/api/orders/{id}", server="site")
   def get_order(request: TS[HttpServerRequest]) -> TS[HttpResponse]:
       ...

   @ws_endpoint(route="/live", server="site")
   def live(event: TS[WsEvent], frame: TS[WsInboundFrame]) -> TS[WsSendRequest]:
       ...

   register_web_client(WebClientConfig(), path="api")
   result = http_request(request_ts, path="api")   # HttpCallResult

``@http_endpoint`` and ``@ws_endpoint`` support the four released handler
shapes — single, keyed, aux-output single, aux-output keyed — and the WS
decorator gains the aux-output forms the tornado surface lacked.
Authentication is a wiring-supplied authenticator graph
``TS[HttpServerRequest] → TS[AuthResult]`` attached to a server or route;
``Denied`` short-circuits before the handler.  No process-global registries
or hooks exist.  Codec helpers (``request_json``, ``json_response``,
``ws_text``/``ws_binary``/``ws_json``) compose the native JSON codec over
``Bytes``.

Compatibility and migration
---------------------------

``hgraph_web.compat`` re-implements the released tornado surface on the
native transport: all four ``@http_server_handler`` shapes,
``@websocket_server_handler``, ``@rest_handler`` (reusing the released
``convert``/``dispatch_`` lowering), the four adaptor forms, and
``register_*_adaptor(port)``.  Legacy defective behaviors — header and
query collapsing, exception-to-400 conflation, REST f-string bodies — are
reproduced only inside ``compat`` for parity, never in the primary API.
The core-owned ``hgraph.adaptors.tornado`` package re-points its server
modules at ``hgraph_web.compat`` (lazy re-export shims raising a pointed
``ModuleNotFoundError`` without hgraph-web); the parallel tornado server
implementation is removed.  The ``hgraph[web]`` extra installs hgraph-web
so the released install path keeps working.  The Tornado-backed client
tier and REST lowering stay in core.

The released tornado adaptor test suites are ported to run against the
fake transport (socketless, deterministic) with a live loopback subset as
the transport oracle; observed deviations are recorded in a
``web_parity`` document following the pattern of the kafka parity notes.

Packaging and ABI
-----------------

The extension builds and installs separately, exporting ``hgraph::web``,
consuming only installed hg_cpp headers and ``hgraph::core``.  All
third-party libraries are PRIVATE.  Dependency acquisition follows the
kafka pattern — ``find_package(... CONFIG QUIET)`` first, pinned
``FetchContent`` fallback — in the order nghttp2 (lib only, static), curl
(≥ 8.11, static, ``CURL_USE_OPENSSL`` on all platforms, ``USE_NGHTTP2``
pointed at the same nghttp2 target), then Boost (CMake-enabled release
archive, ``BOOST_INCLUDE_LIBRARIES=beast;asio;system``, ~1.87).  Static
OpenSSL on macOS and Windows wheels; MSVC gets the
``/NODEFAULTLIB:libcrypto.lib;libssl.lib`` guards, ``NOMINMAX``, and
``WIN32_LEAN_AND_MEAN``.  Each third-party library is confined to one
translation unit (``asio_server.cpp`` for Beast/Asio, ``curl_client.cpp``
for curl, later ``nghttp2_session.cpp`` for nghttp2) behind
extension-owned seams; public headers contain only schemas, descriptors,
and wiring helpers.  One wheel, ``hgraph-web`` (cp312-abi3), covers server
and client; the optional Python module uses the stable-ABI bridge via
``hgraph_add_python_module``.

HTTP/2 activation plan
----------------------

Day one the server speaks HTTP/1.1 and WebSocket; the client speaks h1 and
h2.  The server ships the complete h2 seam: the ALPN callback, the
protocol-dispatch interface expressed in the stream-aware internal model,
and the h2 configuration placeholders.  Config validation rejects ``h2`` in
server ALPN until ``nghttp2_session.cpp`` lands in a v1.x release — a
correct nghttp2 server session (callback framing, flow-control mapping to
the per-connection queues and watermarks, stream lifecycle, RST/GOAWAY,
h2spec conformance) is a multi-week item that must not gate v1.0, and its
activation is purely additive: one translation unit and a registry entry,
zero schema or API change.

Alternatives considered
-----------------------

Port the tornado adaptors to C++ directly
   Rejected.  It would preserve the collapsed headers, string bodies,
   global registries, unbounded queues, and per-request feedback cycle
   while merely moving their implementation language.

Boost.Beast alone
   Rejected on the HTTP/2 requirement: Beast has no h2 and none scheduled.

libcurl alone
   Rejected: curl has no server.

nghttp2 for the day-one server
   Deferred, not rejected — see the activation plan.  ``nghttp2-asio`` is
   deprecated upstream and is not used.

A per-request adaptor-duplex endpoint per route
   Rejected in favor of routes as subscription keys.  Route-set delivery
   through ``TSS`` deltas reuses the kafka subscription mechanism
   unchanged, keeps the bridge channel count compile-time fixed, and makes
   routes dynamic data instead of requiring a new wiring-time route
   enumeration mechanism.

One shared wake push-source across channels
   Rejected: shared wakes couple unrelated channels' scheduling, and
   conflating wakes are nearly free.

Blocking sends or unbounded queues
   Rejected, as in RFC 0015.  Every boundary queue is bounded with an
   explicit, observable overflow policy.

Acceptance criteria
-------------------

Public C++ and extension boundary
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* A separately built, Python-free installed-SDK consumer wires serve,
  respond, WS serve/send, client call, WS connect, events, and stats.
* Core builds and tests without curl, Boost, nghttp2, OpenSSL, Python, or
  the extension.
* No third-party type appears in a public hgraph value or service
  interface.
* One implementation materializes per demanded path; duplicate
  registration is rejected.
* Graph-to-transport edges are sinks over ``impl_input``; transport-to-
  graph edges are root push sources published with ``impl_output``.
* A same-cycle handler's response dispatches with zero added engine cycles
  between request arrival and response handoff (the RFC 0014 criterion),
  asserted by test.

Behavior
~~~~~~~~

* Duplicate and ordered headers, multi-value query parameters, and binary
  bodies round-trip in C++ and Python.
* Route precedence (literal > param > rest), percent-decoding, and typed
  path captures behave as specified; route add/remove at runtime takes
  effect without restart.
* TLS termination, mTLS subject surfacing, SNI, and ALPN negotiation work
  on server and client; the client negotiates HTTP/2 end-to-end.
* Transport failure and HTTP error status arrive on distinct arms of the
  client result.
* WebSocket connect/disconnect arrives as typed events; close codes and
  reasons round-trip; server and client both fragment and reassemble
  messages within the configured caps.
* An unanswered request receives the transport 503 at timeout and at
  shutdown; delivery reports distinguish enqueue rejection, drop,
  delivery, and failure.
* Two engines in one process run independent server paths on distinct
  ports, and shared-port attachment across paths dispatches by route.

Lifetime, failure, and flow control
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Watermarks pause and resume socket reads; a deliberately stalled graph
  holds memory bounded on server and client.
* Slow-consumer policies act as configured and are observable in reports
  and stats.
* Stop prevents new intake, drains within its budget, closes WebSockets
  with 1001, joins all threads, and only then stops the bridge; repeated
  start/stop and partial-start failure pass under AddressSanitizer, and
  race-sensitive paths under ThreadSanitizer where platforms permit.
* Only a graph-thread node applies graph-stop policy.

Compatibility
~~~~~~~~~~~~~

* The ported tornado suites pass against ``hgraph_web.compat`` on the fake
  transport, with the live loopback subset as oracle; deviations are
  recorded in the parity document.
* Core imports work without the extension installed;
  ``hgraph.adaptors.tornado``'s Tornado plumbing (Perspective, inspector)
  keeps working without hgraph-web, while its server names raise a pointed
  install error only when used.

Performance and release
~~~~~~~~~~~~~~~~~~~~~~~

* Native byte transport has no GIL acquisition.
* Benchmarks cover request throughput, request-to-response latency
  (p50/p99), WS frame throughput, memory at both watermarks, and shutdown
  drain time, with small and large payloads.
* The extension's native suite, Python suite, packaging audit,
  installed-SDK consumer, and wheel builds pass on Linux, macOS arm64, and
  Windows.

Implementation plan
-------------------

1. Land this RFC with the extension skeleton: CMake package, pyproject,
   public headers (types, service, value builders), CI wiring, and the
   packaging audit.
2. Implement the service bridge, route table, and drain nodes against a
   fake transport; prove bridge semantics, routing, pacing, reservation,
   and lifecycle without sockets.
3. Implement the Asio/Beast server transport: listener registry, strands,
   h1 + WS, TLS/ALPN seam, watermarks, stop orderings.
4. Implement the curl client transport: multi loop, h1/h2, WS pump,
   reservation protocol.
5. Add the Python surface: schema registrations, decorators, codec
   helpers, authenticator wiring.
6. Add ``hgraph_web.compat``, port the tornado parity suites, and record
   deviations.
7. Land HTTP/2 server activation (``nghttp2_session.cpp``) in a v1.x
   release behind the already-shipped seam.

Implementation status
---------------------

No implementation is included with this proposed RFC.  The tornado
adaptors remain the shipped behavior until the extension satisfies the
acceptance criteria.

References
----------

* :doc:`rfc_0000` — RFC and downstream-promotion process.
* :doc:`rfc_0014_request_reply_transport_planning` — direct transport for
  decoupled external sinks and sources.
* :doc:`rfc_0015_kafka_extension_api` — the extension architecture this
  RFC instantiates for the web domain.
* :doc:`../developer_guide/services` — authoritative service and adaptor
  boundary behavior.
* `Boost.Beast HTTP/2 status <https://github.com/boostorg/beast/issues/2950>`_.
* `libcurl WebSocket API <https://curl.se/libcurl/c/libcurl-ws.html>`_.
* `nghttp2 <https://nghttp2.org/>`_ and the nghttp2-asio deprecation
  notice.
* `Akka HTTP server as a Flow
  <https://doc.akka.io/docs/akka-http/current/server-side/low-level-api.html>`_.
* `Reactor Netty WebSocket
  <https://projectreactor.io/docs/netty/release/reference/index.html>`_.
* `Apache Flink Async I/O
  <https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/asyncio/>`_.
* `RxJS webSocket subject
  <https://rxjs.dev/api/webSocket/webSocket>`_.
* `Phoenix Channels
  <https://hexdocs.pm/phoenix/channels.html>`_.
