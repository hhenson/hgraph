"""Replay finite Kafka history on deterministic graph timestamps."""

import argparse
from datetime import datetime

import hgraph_kafka as kafka

import hgraph as hg


@hg.sink_node
def show_record(record: hg.TS[kafka.KafkaRecord], _clock: hg.CLOCK = None) -> None:
    print(_clock.evaluation_time, record.value)


@hg.sink_node
def stop_when_complete(
    state: hg.TS[kafka.KafkaSubscriptionState],
    _api: hg.EvaluationEngineApi = None,
) -> None:
    if state.value == kafka.KafkaSubscriptionState.BOUNDED_COMPLETE:
        _api.request_engine_stop()


@hg.graph
def replay_topic(topic: str, group_id: str, path: str = "broker") -> None:
    key = kafka.KafkaSubscriptionKey(
        topics=(topic,),
        group_id=group_id,
        assignment_mode=kafka.KafkaAssignmentMode.INDEPENDENT,
        start_position=kafka.KafkaStartPosition.graph_start_time(),
        stop_position=kafka.KafkaStopPosition.graph_lifetime(),
        commit_mode=kafka.KafkaCommitMode.NONE,
        recovery_clock=kafka.KafkaRecoveryClock.RECORD_TIMESTAMP,
        merge_policy=kafka.KafkaMergePolicy.TIMESTAMP_TOPIC_PARTITION_OFFSET,
        sharing_identity=f"{group_id}-replay",
    )
    subscription = kafka.kafka_subscribe(
        hg.const(key, tp=hg.TS[kafka.KafkaSubscriptionKey]), path=path
    )
    show_record(subscription["record"])
    stop_when_complete(subscription["state"])


@hg.graph
def app(
    config: kafka.KafkaServiceConfig,
    topic: str = "orders",
    group_id: str = "hgraph-example-replay",
) -> None:
    kafka.register_kafka_service(config, path="broker")
    replay_topic(topic, group_id, path="broker")


def run_example(
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    start_time: datetime,
    end_time: datetime,
):
    config = kafka.KafkaServiceConfig.from_bootstrap_servers(
        [bootstrap_servers], client_id=group_id
    )
    hg.run_graph(
        app,
        config,
        topic,
        group_id,
        run_mode=hg.EvaluationMode.SIMULATION,
        start_time=start_time,
        end_time=end_time,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="orders")
    parser.add_argument("--group-id", default="hgraph-example-replay")
    parser.add_argument("--start", type=datetime.fromisoformat, required=True)
    parser.add_argument("--end", type=datetime.fromisoformat, required=True)
    args = parser.parse_args()
    run_example(
        args.bootstrap_servers,
        args.topic,
        args.group_id,
        args.start,
        args.end,
    )
