from dataclasses import dataclass, field
from datetime import timedelta, datetime
from logging import error
from threading import Thread, Event
from typing import Callable, Mapping

import pytz
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

from hgraph import (
    GlobalState,
    register_service,
    TS,
    const,
    service_impl,
    MIN_TD,
    TSB,
    sink_node,
    STATE,
    EvaluationEngineApi,
    SCHEDULER,
    generator,
    EvaluationMode,
    push_queue,
    SCALAR,
    set_service_output,
    adaptor,
    adaptor_impl,
    register_adaptor,
    debug_print,
)
from hgraph.adaptors.kafka._api import (
    message_publisher_operator,
    message_subscriber_service,
    message_history_subscriber_service,
    MessageState,
)

__all__ = ("register_kafka_adaptor",)


def register_kafka_adaptor(config: dict):
    # At some point, we can use a path to put the service message state on, then this can be used to support
    # multiple messaging services concurrently.
    (ms := KafkaMessageState.instance()).config = config


@dataclass
class KafkaMessageState(MessageState):
    """Tracks the registered topics and their replay state."""

    subscribers: set[str] = field(default_factory=set)
    history_subscribers: set[str] = field(default_factory=set)
    publishers: set[str] = field(default_factory=set)
    _kafka_producer: KafkaProducer = None
    _kafka_producer_count: int = 0
    _kafka_sender: dict[str, Callable[[bytes], None]] = field(default_factory=dict)
    _kafka_consumer: dict[str, "KafkaConsumerThread"] = field(default_factory=dict)

    config: dict = None

    @classmethod
    def instance(cls) -> "KafkaMessageState":
        if "service.messaging.state" not in (gs := GlobalState.instance()):
            gs["service.messaging.state"] = cls()
        return gs["service.messaging.state"]

    def add_subscriber(self, topic: str):
        self._register(topic)
        if topic not in self.subscribers:
            register_adaptor(topic, _real_time_message_subscriber_impl, topic=topic)
        self.subscribers.add(topic)

    def add_historical_subscriber(self, topic: str):
        self._register(topic)
        self.history_subscribers.add(topic)

    def _register(self, topic: str):
        if topic not in self.subscribers and topic not in self.history_subscribers:
            register_service(topic, _message_subscriber_impl, topic=topic)

    def add_publisher(self, topic: str):
        if topic in self.publishers:
            # There can only be one publisher per topic.
            raise ValueError(f"Topic {topic} already has a publisher")
        self.publishers.add(topic)

    @property
    def producer(self) -> KafkaProducer:
        if self._kafka_producer is None:
            self._kafka_producer = KafkaProducer(**self.config)
        self._kafka_producer_count += 1
        return self._kafka_producer

    def close_producer(self):
        if self._kafka_producer is None:
            raise ValueError("No producer to close")
        self._kafka_producer_count -= 1
        if self._kafka_producer_count == 0:
            self._kafka_producer.close()
            self._kafka_producer = None

    def set_subscriber_sender(self, topic: str, sender: Callable[[SCALAR], None]):
        self._kafka_sender[topic] = sender

    def start_subscriber(self, topic: str, consumer: KafkaConsumer):
        self._kafka_consumer[topic] = (thread := KafkaConsumerThread(topic, consumer, self._kafka_sender[topic]))
        thread.start()

    def stop_subscriber(self, topic: str):
        if topic in self._kafka_consumer:
            self._kafka_consumer.pop(topic).stop()


def _registered_topics(m, s):
    """
    Makes sure we have registered this in this implementation's topic registry, this compensates for lack of service
    impl infra for the sink node.
    """
    ms = KafkaMessageState.instance()
    topic = s["topic"]
    return topic in ms.publishers


@sink_node(overloads=message_publisher_operator, requires=_registered_topics)
def _kafka_message_publisher(msg: TS[bytes], topic: str, _state: STATE = None, _scheduler: SCHEDULER = None) -> None:
    if msg.modified:
        _state.producer.send(topic, msg.value)
        _scheduler.schedule(
            timedelta(milliseconds=100), tag="flush_timer"
        )  # This will re-schedule the flush timer if already set.

    if _scheduler.is_scheduled_now:
        # Make sure we flush reasonably regularly.
        _state.producer.flush()


@_kafka_message_publisher.start
def _kafka_message_publisher_start(topic: str, _state: STATE):
    _state.producer = KafkaMessageState.instance().producer


@_kafka_message_publisher.stop
def _kafka_message_publisher_stop(_state: STATE):
    _state.producer.flush()
    _state.producer = None
    KafkaMessageState.instance().close_producer()


@service_impl(interfaces=message_subscriber_service)
def _message_subscriber_aggregator(path: str, topic: str) -> TS[bytes]:
    print(f"subscribe topic: {topic}")
    topic_b = b"sub: " + topic.encode("utf-8")
    return const(topic_b, delay=MIN_TD * 2)


@service_impl(interfaces=(message_history_subscriber_service, message_subscriber_service))
def _message_subscriber_impl(path: str, topic: str):
    consumer = KafkaConsumer(**(ks := KafkaMessageState.instance()).config)
    # First, get partition information by calling 'partitions_for_topic'.
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        raise ValueError(f"No partitions found for topic '{topic}'")

    # Create TopicPartition objects for each partition.
    topic_partitions = tuple(TopicPartition(topic, p) for p in partitions)
    # Assign these partitions to the consumer.
    consumer.assign(topic_partitions)

    if topic in ks.history_subscribers:
        historical_out = _message_subscriber_history_aggregator(path, consumer, topic_partitions)
        set_service_output(path, message_history_subscriber_service, historical_out)
        start_real_time_service = historical_out.recovered
    else:
        start_real_time_service = const(True)

    if topic in ks.subscribers:
        set_service_output(path, message_subscriber_service, _real_time_message_subscriber(path=topic))
        _start_realtime_message_subscriber(topic, start_real_time_service, consumer)


@generator
def _message_subscriber_history_aggregator(
    path: str, consumer: KafkaConsumer, topic_partitions: tuple[tuple[str, int], ...], _api: EvaluationEngineApi = None
) -> TSB["msg" : TS[bytes], "recovered" : TS[bool]]:
    """Recovered must tick after the last message has been delivered."""
    start_time = _api.start_time
    if _api.evaluation_mode == EvaluationMode.SIMULATION:
        end_time = _api.end_time
    else:
        # Use now as the base-line to catch up to in real-time mode. By the time we actually catch up if this is still
        # ticking, then we can move to real-time processing.
        end_time = _api.evaluation_clock.now

    # Convert the start_time to milliseconds (Kafka uses epoch time in ms).
    timestamp_ms = int(start_time.replace(tzinfo=pytz.UTC).timestamp() * 1000)
    # Prepare a timestamp lookup dict for each TopicPartition.
    timestamps = {tp: timestamp_ms for tp in topic_partitions}
    # Retrieve offset information for each partition at the given timestamp.
    offsets = {k: v for k, v in consumer.offsets_for_times(timestamps).items() if v is not None}
    for tp, offset in offsets.items():
        consumer.seek(tp, offset.offset)
    first = True
    last_time = _timestamp_to_datetime(timestamp_ms)
    yield start_time, dict(recovered=False)
    while last_time < end_time:
        records = consumer.poll(timeout_ms=500, max_records=1000)
        if records is None or len(records) == 0:
            break
        all_messages = [m for tp, messages in records.items() for m in messages]
        if len(records) > 1:
            all_messages = sorted(all_messages, key=lambda m: (m.timestamp, m.topic, m.offset))
        for msg in all_messages:
            # We won't exit historical replay unless the engine exits to ensure smooth playback of messages.
            tm = _timestamp_to_datetime(msg.timestamp)
            if tm <= last_time:
                # Offset if it is the same
                tm = last_time + MIN_TD
            last_time = tm
            yield tm, dict(msg=msg.value)
    tm = last_time
    tm = max(tm, start_time - MIN_TD)
    yield tm + MIN_TD, dict(recovered=True)


def _timestamp_to_datetime(t: int) -> datetime:
    return datetime.utcfromtimestamp(t / 1000) + timedelta(milliseconds=t % 1000)


@adaptor
def _real_time_message_subscriber(path: str) -> TS[bytes]:
    """Expose the real-time message subscriber as an adaptor"""


@adaptor_impl(interfaces=_real_time_message_subscriber)
def _real_time_message_subscriber_impl(path: str, topic: str) -> TS[bytes]:
    return _message_subscriber_queue(topic=topic)


@push_queue(TS[bytes])
def _message_subscriber_queue(sender: Callable[[SCALAR], None] = None, *, topic: str):
    KafkaMessageState.instance().set_subscriber_sender(topic, sender)


@sink_node
def _start_realtime_message_subscriber(topic: str, start_real_time_service: TS[bool], consumer: KafkaConsumer):
    if start_real_time_service.value:
        start_real_time_service.make_passive()
        KafkaMessageState.instance().start_subscriber(topic, consumer)


@_start_realtime_message_subscriber.stop
def _start_realtime_message_subscriber_stop(topic: str):
    KafkaMessageState.instance().stop_subscriber(topic)


class KafkaConsumerThread(Thread):

    def __init__(self, topic, consumer: KafkaConsumer, sender: Callable[[bytes], None]):
        super().__init__()
        self.topic = topic
        self.consumer = consumer
        self.sender = sender
        self._stop_event = Event()

    def run(self):
        # TODO: How to communicate failures to the graph if this blows up?
        try:
            while not self._stop_event.is_set():
                records = self.consumer.poll(timeout_ms=1000, max_records=1000)
                all_messages = [m for tp, messages in records.items() for m in messages]
                if len(records) > 1:
                    all_messages = sorted(all_messages, key=lambda m: (m.timestamp, m.topic, m.offset))
                for msg in all_messages:
                    self.sender(msg.value)
        except:
            error(f"Failure occurred whilst reading from Kafka on topic: {self.topic}", exc_info=True)
        finally:
            self.consumer.close()

    def stop(self):
        self._stop_event.set()
