import runpy
from pathlib import Path

import _hgraph
import hgraph_kafka as kafka
from hgraph.test import use_wiring

EXAMPLES = Path(__file__).parents[1] / "examples"


def _example(name):
    return runpy.run_path(EXAMPLES / name)


def _config(client_id):
    return kafka.KafkaServiceConfig.from_bootstrap_servers(
        ["localhost:9092"], client_id=client_id
    )


def _build(graph, *args, realtime):
    wiring = _hgraph.Wiring(is_realtime=realtime)
    with use_wiring(wiring):
        graph(*args)
    wiring.build_services()


def test_consume_and_commit_example_wires_without_contacting_a_broker():
    app = _example("consume_and_commit.py")["app"]
    _build(app, _config("consumer-example"), "orders", "example", realtime=True)


def test_publish_with_delivery_example_wires_without_contacting_a_broker():
    app = _example("publish_with_delivery.py")["app"]
    _build(app, _config("publisher-example"), "events", "payload", realtime=True)


def test_bounded_replay_example_wires_as_simulation():
    app = _example("bounded_replay.py")["app"]
    _build(app, _config("replay-example"), "orders", "replay", realtime=False)
