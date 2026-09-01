"""Publish one Kafka record and wait for its delivery report."""

import argparse
from datetime import timedelta

import hgraph_kafka as kafka

import hgraph as hg


@hg.compute_node
def encode(value: hg.TS[str]) -> hg.TS[kafka.KafkaProduceRecord]:
    return kafka.KafkaProduceRecord(
        value=value.value.encode("utf-8"),
        headers=(
            kafka.KafkaHeader("content-type", b"text/plain"),
            kafka.KafkaHeader("source", b"hgraph-example"),
        ),
        user_token="example-message",
    )


@hg.graph
def publish_value(
    value: hg.TS[str], topic: str, path: str = "broker"
) -> hg.TS[kafka.KafkaDeliveryReport]:
    return kafka.kafka_publish(topic, encode(value), path=path)


@hg.sink_node
def report_and_stop(
    report: hg.TS[kafka.KafkaDeliveryReport],
    _api: hg.EvaluationEngineApi = None,
) -> None:
    print(report.value)
    _api.request_engine_stop()


@hg.graph
def app(
    config: kafka.KafkaServiceConfig,
    topic: str = "events",
    value: str = "hello from hgraph",
) -> None:
    kafka.register_kafka_service(config, path="broker")
    report_and_stop(publish_value(hg.const(value, tp=hg.TS[str]), topic, path="broker"))
    hg.debug_print("Kafka event", kafka.kafka_events(path="broker"))


def run_example(bootstrap_servers: str, topic: str, value: str, seconds: int):
    config = kafka.KafkaServiceConfig.from_bootstrap_servers(
        [bootstrap_servers], client_id="hgraph-example-publisher"
    )
    hg.run_graph(
        app,
        config,
        topic,
        value,
        run_mode=hg.EvaluationMode.REAL_TIME,
        end_time=hg.utc_now() + timedelta(seconds=seconds),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="events")
    parser.add_argument("--value", default="hello from hgraph")
    parser.add_argument("--seconds", type=int, default=30)
    args = parser.parse_args()
    run_example(args.bootstrap_servers, args.topic, args.value, args.seconds)
