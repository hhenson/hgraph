import runpy
from pathlib import Path

import _hgraph
import hgraph_web as web
from hgraph.test import use_wiring

EXAMPLES = Path(__file__).parents[1] / "examples"


def _example(name):
    return runpy.run_path(EXAMPLES / name)


def _build(graph, *args):
    wiring = _hgraph.Wiring(is_realtime=True)
    with use_wiring(wiring):
        graph(*args)
    wiring.build_services()


def test_hello_server_example_wires_without_binding_a_socket():
    _build(_example("hello_server.py")["app"], 0)


def test_http_client_example_wires_without_making_a_request():
    _build(
        _example("http_client.py")["app"],
        web.WebClientConfig(),
        "http://127.0.0.1:8080/hello",
    )


def test_websocket_loopback_example_runs_to_completion():
    [frame] = _example("websocket_loopback.py")["run_example"]()

    assert frame.kind == web.WsFrameKind.TEXT
    assert frame.text == "ping"
