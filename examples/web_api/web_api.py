"""
This shows a very naive example of how one could interact with a graph and a web app in Flask (though this could
be extended to different paradigms.

We could greate an arbitrarily complex queue type and add additional context to direct the responses, but the basic
idea should hold.
"""

import threading
from queue import Queue
from typing import Callable

from flask import Flask

from hgraph import graph, push_queue, TS, sink_node, evaluate_graph, GraphConfiguration, EvaluationMode

app = Flask(__name__)

SENDER: Callable[[str], None] = None
RECEIVER: Queue = None


@app.route("/<cmd>")
def do(cmd):
    SENDER(cmd)
    return RECEIVER.get()


@push_queue(TS[str])
def web_request(sender: Callable[[str], None] = None):
    global SENDER
    SENDER = sender


@sink_node
def web_response(data: TS[str]):
    global RECEIVER
    RECEIVER.put(data.value)


@web_response.start
def web_response_start():
    global RECEIVER
    RECEIVER = Queue(maxsize=1)


@graph
def web_api():
    request = web_request()
    web_response(request)


class HGraphApp:

    def __init__(self, graph):
        self._graph = graph

    def init_app(self, app):
        threading.Thread(target=self.run).start()

    def run(self):
        evaluate_graph(self._graph, GraphConfiguration(run_mode=EvaluationMode.REAL_TIME))


def main():
    graph_app = HGraphApp(graph=web_api)
    graph_app.init_app(app)
    app.run()


if __name__ == "__main__":
    main()
