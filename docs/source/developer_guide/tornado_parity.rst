Tornado parity disposition
==========================

This is the test disposition for the Tornado API audit against released Python
``hgraph`` 0.5.34.  The upstream tests are a design reference as well as an
output oracle.  All of their user-observable workflows are retained; obsolete
Python runtime ownership and implementation-object layouts are not.

Upstream test mapping
---------------------

``hgraph_unit_tests/adaptors/http_adaptor/test_http_adaptor.py``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1

   * - Upstream test
     - hg_cpp coverage
   * - ``test_single_request_graph``
     - ``test_http_server_handler_registers_and_maps_single_requests``
   * - ``test_multiple_request_graph``
     - ``test_http_server_handler_registers_batch_requests``
   * - ``test_http_server_adaptor_graph``
     - ``test_http_server_supports_late_manual_handler_and_idempotent_call``
       and the keyed auxiliary-output tests
   * - ``test_single_request_graph_client``
     - Near-verbatim
       ``test_http_client_concurrent_requests_match_upstream_recorded_flow``

The local module additionally covers all four methods and request fields,
direct client registration, authentication challenge loading, pending and
cancelled requests, teardown, overlapping-route precedence and invalid
handler signatures.

``hgraph_unit_tests/adaptors/rest_adaptor/test_rest_adaptor.py``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1

   * - Upstream test
     - hg_cpp coverage
   * - ``test_to_request`` (five cases)
     - ``test_http_request_converts_to_rest_request``
   * - ``test_from_response`` (six cases)
     - ``test_rest_response_converts_to_http_response`` plus
       ``test_http_response_converts_to_typed_rest_response``
   * - ``test_single_rest_request_graph``
     - ``test_rest_handler_maps_live_delete_request``
   * - ``test_multiple_request_graph``
     - ``test_rest_handler_maps_batch_requests``
   * - ``test_rest_list_client``, ``test_rest_read_client``,
       ``test_rest_create_client``, ``test_rest_update_client`` and
       ``test_rest_delete_client``
     - ``test_rest_client_helpers_round_trip_against_rest_handler`` executes
       all five helpers together against a live registered server

Single and batch auxiliary outputs and ``GlobalState`` cleanup have separate
regressions in the same local module.

``hgraph_unit_tests/adaptors/websocket_adaptor/test_websocket_adaptor.py``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1

   * - Upstream test
     - hg_cpp coverage
   * - ``test_single_websocket_request_graph``
     - ``test_websocket_server_handler_round_trips_binary_messages`` uses a
       bare graph function and ordered two-message exchange
   * - ``test_multiple_websocket_request_graph``
     - ``test_websocket_server_handler_multiplexes_batch_requests`` includes a
       runtime ``STATE`` injectable
   * - ``test_websocket_server_adaptor_graph``
     - The single-handler test explicitly registers the helper and wires a
       required application input; the upstream xfail is a transport fixture
       limitation and is not retained
   * - ``test_single_request_graph_client``
     - ``test_websocket_client_service_adaptor_infers_message_specialization``
       runs both ``str`` and ``bytes`` through default implementation
       registration

Manager buffering/removal, message-type and port isolation, invalid handler
signatures, explicit connection rejection and lifecycle cleanup have
additional focused coverage.  The
upstream module's broad ``ImportError`` guard and xdist-related xfail markers
are test infrastructure, not supported runtime behaviour.

C++-first correspondence
------------------------

The transport layer adapts Tornado events to native adaptor clients:

- ordinary server streams use the public C++ adaptor contract exercised by
  ``tests/cpp/test_adaptor_wiring.cpp`` (paths, duplex routing,
  multi-interface wiring, generic identity and duplicate rejection);
- HTTP and WebSocket clients use the native service-adaptor request/reply
  exchange exercised by ``tests/cpp/test_service_wiring.cpp`` (keyed clients,
  successive-cycle feedback, recursive bundle deltas, map/mesh ownership and
  generic specialization);
- Python handlers compile to native graph/node callables.  Borrowed wrappers
  alias the owning ``Wiring`` and must not select or mutate graph/run
  configuration;
- Python managers own only Tornado sockets, futures and queues.  Queue senders
  are copied through the graph's ``GlobalState`` seed/result lifecycle and
  removed by native node stop hooks.

No upstream Python graph executor, service registry or time-series runtime is
used.  Differences in manager methods and implementation-token classes are
therefore intentional representation differences outside the curated API.
