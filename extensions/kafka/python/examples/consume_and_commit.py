"""Consume Kafka records and acknowledge only successfully decoded records."""

import argparse
from datetime import timedelta

import hgraph_kafka as kafka

import hgraph as hg


class ProcessedRecord(hg.TimeSeriesSchema):
    payload: hg.TS[str]
    cursor: hg.TS[kafka.KafkaCursor]


@hg.compute_node
def decode_then_ack(
    record: hg.TS[kafka.KafkaRecord],
    cursor: hg.TS[kafka.KafkaCursor],
) -> hg.TSB[ProcessedRecord]:
    """Decode first; a decoding failure therefore produces no commit cursor."""

    value = record.value.value
    if value is None:
        raise ValueError("the application must handle Kafka tombstones explicitly")
    payload = value.decode("utf-8")
    return {"payload": payload, "cursor": cursor.value}


@hg.graph
def consume_topic(topic: str, group_id: str, path: str = "broker") -> None:
    key = kafka.KafkaSubscriptionKey(
        topics=(topic,),
        group_id=group_id,
        start_position=kafka.KafkaStartPosition.committed(),
        commit_mode=kafka.KafkaCommitMode.EXPLICIT,
    )
    subscription = kafka.kafka_subscribe(
        hg.const(key, tp=hg.TS[kafka.KafkaSubscriptionKey]), path=path
    )

    processed = decode_then_ack(subscription["record"], subscription["cursor"])
    hg.debug_print("processed payload", processed["payload"])
    kafka.kafka_commit(processed["cursor"], path=path)
    hg.debug_print("Kafka event", kafka.kafka_events(path=path))


@hg.graph
def app(
    config: kafka.KafkaServiceConfig,
    topic: str = "orders",
    group_id: str = "hgraph-example-worker",
) -> None:
    kafka.register_kafka_service(config, path="broker")
    consume_topic(topic, group_id, path="broker")


def run_example(bootstrap_servers: str, topic: str, group_id: str, seconds: int):
    config = kafka.KafkaServiceConfig.from_bootstrap_servers(
        [bootstrap_servers], client_id=group_id
    )
    hg.run_graph(
        app,
        config,
        topic,
        group_id,
        run_mode=hg.EvaluationMode.REAL_TIME,
        end_time=hg.utc_now() + timedelta(seconds=seconds),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="orders")
    parser.add_argument("--group-id", default="hgraph-example-worker")
    parser.add_argument("--seconds", type=int, default=60)
    args = parser.parse_args()
    run_example(args.bootstrap_servers, args.topic, args.group_id, args.seconds)
