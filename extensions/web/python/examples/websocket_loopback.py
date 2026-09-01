"""Run a WebSocket echo server and client together on an ephemeral port."""

from datetime import timedelta

import hgraph_web as web

import hgraph as hg

_received = []


@hg.compute_node
def text_connection_id(frame: hg.TS[web.WsInboundFrame]) -> hg.TS[int]:
    inbound = frame.value.frame
    if inbound is None or inbound.kind != web.WsFrameKind.TEXT:
        return None
    return frame.value.connection_id


@web.ws_endpoint("/echo", path="site")
@hg.graph
def echo(
    event: hg.TS[web.WsEvent], frame: hg.TS[web.WsInboundFrame]
) -> hg.TSB[web.WsSendRequest]:
    del event
    return hg.TSB[web.WsSendRequest].from_ts(
        connection_id=text_connection_id(frame),
        frame=web.text_frame(web.ws_text(frame)),
    )


@hg.compute_node
def client_key(
    stats: hg.TS[web.WebServerStats], _state: hg.STATE = None
) -> hg.TS[web.WsClientKey]:
    port = stats.value.listening_port
    if port == 0 or getattr(_state, "emitted", False):
        return None
    _state.emitted = True
    return web.WsClientKey(f"ws://127.0.0.1:{port}/echo")


@hg.compute_node
def send_once(event: hg.TS[web.WsEvent], _state: hg.STATE = None) -> hg.TS[web.WsFrame]:
    if getattr(_state, "emitted", False) or (
        event.value.state != web.WsConnectionState.OPEN
    ):
        return None
    _state.emitted = True
    return web.WsFrame.text_frame("ping")


@hg.sink_node
def capture(
    frame: hg.TS[web.WsFrame],
    _api: hg.EvaluationEngineApi = None,
) -> None:
    _received.append(frame.value)
    _api.request_engine_stop()


@hg.graph
def app() -> None:
    web.register_web_server(
        web.WebServerConfig(bind_address="127.0.0.1", port=0, stats_interval_ms=50),
        path="site",
    )
    web.register_web_client(web.WebClientConfig(), path="client")

    echo()

    key = client_key(web.web_server_stats(path="site"))
    connected = web.web_ws_connect(key, path="client")
    web.web_ws_client_send(key, send_once(connected["event"]), path="client")
    capture(connected["frame"])


def run_example(seconds: int = 10):
    _received.clear()
    hg.run_graph(
        app,
        run_mode=hg.EvaluationMode.REAL_TIME,
        end_time=hg.utc_now() + timedelta(seconds=seconds),
    )
    return list(_received)


if __name__ == "__main__":
    for frame in run_example():
        print(frame)
