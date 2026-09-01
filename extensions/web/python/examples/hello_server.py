"""Serve a simple HTTP endpoint from a graph-owned Web service."""

import argparse
from datetime import timedelta

import hgraph_web as web

import hgraph as hg


@web.http_endpoint("/hello", path="site")
@hg.compute_node
def hello(request: hg.TS[web.HttpServerRequest]) -> hg.TS[web.HttpResponse]:
    del request
    return web.HttpResponse(
        200,
        headers=(web.WebHeader("content-type", "text/plain; charset=utf-8"),),
        body=b"hello from hgraph\n",
    )


@hg.graph
def app(port: int = 8080) -> None:
    web.register_web_server(
        web.WebServerConfig(bind_address="127.0.0.1", port=port), path="site"
    )
    hello()
    hg.debug_print("Web event", web.web_server_events(path="site"))


def run_example(port: int, seconds: int):
    hg.run_graph(
        app,
        port,
        run_mode=hg.EvaluationMode.REAL_TIME,
        end_time=hg.utc_now() + timedelta(seconds=seconds),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--seconds", type=int, default=300)
    args = parser.parse_args()
    run_example(args.port, args.seconds)
