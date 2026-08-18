"""Shared live-loopback scaffolding for the hgraph-web python suites.

Each suite runs a real server and client inside one graph on 127.0.0.1: the
server binds an ephemeral port, the bound port arrives through the stats
service, and the first stats tick with a real port triggers the client call.
"""

from __future__ import annotations

import hgraph as hg
import hgraph_web as web

SERVER_PATH = "site"
CLIENT_PATH = "api"


def register_loopback(server_config: web.WebServerConfig | None = None) -> None:
    """Register the loopback server/client pair (call inside a graph)."""

    web.register_web_server(
        server_config or web.WebServerConfig(port=0, stats_interval_ms=50),
        path=SERVER_PATH,
    )
    web.register_web_client(web.WebClientConfig(), path=CLIENT_PATH)


def result_captures(observed: list, failures: list):
    """Sink-node pair recording a call result and stopping the engine."""

    @hg.sink_node
    def capture(
        response: hg.TS[web.HttpResponse],
        _api: hg.EvaluationEngineApi = None,
    ):
        observed.append(response.value)
        _api.request_engine_stop()

    @hg.sink_node
    def capture_failure(
        failure: hg.TS[web.WebTransportError],
        _api: hg.EvaluationEngineApi = None,
    ):
        failures.append(failure.value)
        _api.request_engine_stop()

    return capture, capture_failure


def port_triggered_request(requested: list, build):
    """A client request emitted once, when the bound port becomes known.

    ``build(port)`` returns the ``HttpClientRequest`` for the discovered
    port; ``requested`` records the port and suppresses re-triggering.
    """

    @hg.compute_node
    def client_request(
        stats: hg.TS[web.WebServerStats],
    ) -> hg.TS[web.HttpClientRequest]:
        port = stats.value.listening_port
        if port == 0 or requested:
            return None
        requested.append(port)
        return build(port)

    return client_request
