"""Make one HTTP request and keep HTTP and transport failures distinct."""

import argparse
from datetime import timedelta

import hgraph_web as web

import hgraph as hg


@hg.sink_node
def show_response(
    response: hg.TS[web.HttpResponse],
    _api: hg.EvaluationEngineApi = None,
) -> None:
    print(response.value.status, response.value.body.decode(errors="replace"))
    _api.request_engine_stop()


@hg.sink_node
def show_failure(
    failure: hg.TS[web.WebTransportError],
    _api: hg.EvaluationEngineApi = None,
) -> None:
    print("transport failure:", failure.value.message)
    _api.request_engine_stop()


@hg.graph
def fetch(url: str, path: str = "client") -> None:
    result = web.web_http_request(
        web.HttpClientRequest(
            web.HttpMethod.GET,
            url,
            headers=(web.WebHeader("accept", "text/plain"),),
        ),
        path=path,
    )
    show_response(result["response"])
    show_failure(result["failure"])


@hg.graph
def app(config: web.WebClientConfig, url: str) -> None:
    web.register_web_client(config, path="client")
    fetch(url, path="client")


def run_example(url: str, seconds: int):
    hg.run_graph(
        app,
        web.WebClientConfig(),
        url,
        run_mode=hg.EvaluationMode.REAL_TIME,
        end_time=hg.utc_now() + timedelta(seconds=seconds),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url", nargs="?", default="http://127.0.0.1:8080/hello")
    parser.add_argument("--seconds", type=int, default=30)
    args = parser.parse_args()
    run_example(args.url, args.seconds)
