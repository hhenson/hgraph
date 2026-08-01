import logging
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime

from frozendict import frozendict

from hgraph import (
    EvaluationEngineApi,
    GlobalState,
    MIN_TD,
    TS,
    TSB,
    combine,
    const,
    generator,
    nothing,
    push_queue,
    register_service,
    service_impl,
    set_service_output,
    sink_node,
)

from ._api import (
    KafkaMessage,
    MessageState,
    message_history_subscriber_service,
    message_subscriber_service,
)

__all__ = ("register_kafka_adaptor",)

CONTENT_TYPE_HEADER = "content-type"
_STATE_KEY = ":adaptors:kafka:state"
_CONFIG_HELPERS = {"consumer_factory", "producer_factory"}
logger = logging.getLogger(__name__)


def _record_to_kafka_message(record):
    """Convert a Kafka ConsumerRecord while lifting content-type."""
    content_type = None
    headers = {}
    for key, value in record.headers or ():
        if key == CONTENT_TYPE_HEADER:
            content_type = value.decode("utf-8") if isinstance(value, bytes) else value
        else:
            headers[key] = value
    return KafkaMessage(
        payload=record.value,
        key=record.key,
        content_type=content_type,
        headers=frozendict(headers),
    )


@dataclass(frozen=True)
class _FallbackTopicPartition:
    """Test-double partition key used when kafka-python is not installed."""

    topic: str
    partition: int


def _topic_partition(topic, partition):
    try:
        from kafka import TopicPartition
    except ModuleNotFoundError:
        return _FallbackTopicPartition(topic, partition)
    return TopicPartition(topic, partition)


def _timestamp_to_datetime(timestamp_ms):
    """Kafka timestamps are UTC epoch milliseconds; hgraph times are naive UTC."""
    return datetime.fromtimestamp(timestamp_ms / 1000, UTC).replace(tzinfo=None)


def _record_order(record):
    return (
        getattr(record, "timestamp", 0),
        getattr(record, "topic", ""),
        getattr(record, "partition", 0),
        getattr(record, "offset", 0),
    )


class _ConsumerSession:
    """One consumer shared by historical replay and its live continuation."""

    def __init__(self, state, topic, *, live_requested):
        self.state = state
        self.topic = topic
        self.live_requested = live_requested
        self.consumer = None
        self.topic_partitions = ()
        self.sender = None
        self.thread = None
        self.stop_event = threading.Event()
        self.start_requested = False
        self.closed = False
        self.lock = threading.RLock()

    def prepare(self):
        with self.lock:
            if self.closed:
                raise RuntimeError(f"Kafka consumer for topic {self.topic!r} is closed")
            if self.consumer is not None:
                return self.consumer, self.topic_partitions

            consumer = self.state.create_consumer()
            try:
                partitions = consumer.partitions_for_topic(self.topic)
                if not partitions:
                    raise ValueError(f"No partitions found for topic {self.topic!r}")
                topic_partitions = tuple(
                    _topic_partition(self.topic, partition)
                    for partition in sorted(partitions)
                )
                consumer.assign(topic_partitions)
            except BaseException:
                consumer.close()
                raise

            self.consumer = consumer
            self.topic_partitions = topic_partitions
            return consumer, topic_partitions

    def attach_sender(self, sender):
        with self.lock:
            self.sender = sender
            launch = self.start_requested and self.thread is None and not self.closed
        if launch:
            self._launch()

    def request_start(self):
        with self.lock:
            self.start_requested = True
            launch = self.sender is not None and self.thread is None and not self.closed
        if launch:
            self._launch()

    def _launch(self):
        consumer, _ = self.prepare()
        with self.lock:
            if self.thread is not None or self.closed:
                return
            thread = threading.Thread(
                target=self._consume,
                args=(consumer,),
                name=f"hgraph-kafka-{self.topic}",
                daemon=False,
            )
            self.thread = thread
            thread.start()

    def _consume(self, consumer):
        try:
            while not self.stop_event.is_set():
                records = consumer.poll(timeout_ms=100, max_records=1000) or {}
                messages = [record for batch in records.values() for record in batch]
                if len(records) > 1:
                    messages.sort(key=_record_order)
                for record in messages:
                    if self.stop_event.is_set():
                        break
                    self.sender(_record_to_kafka_message(record))
        except BaseException:
            logger.exception(
                "Failure occurred while reading Kafka topic %s", self.topic)
        finally:
            self._close_consumer()

    def history_finished(self):
        if not self.live_requested:
            self.stop()

    def _close_consumer(self):
        with self.lock:
            if self.closed:
                return
            self.closed = True
            consumer = self.consumer
        if consumer is not None:
            consumer.close()

    def stop(self):
        self.stop_event.set()
        with self.lock:
            thread = self.thread
        if thread is not None and thread is not threading.current_thread():
            thread.join()
        self._close_consumer()


@dataclass
class KafkaMessageState(MessageState):
    config: dict = field(default_factory=dict)
    publishers: set[str] = field(default_factory=set)
    subscribers: set[str] = field(default_factory=set)
    history_subscribers: set[str] = field(default_factory=set)
    _kafka_producer: object = None
    _kafka_producer_count: int = 0
    _producer_injected: bool = False
    _sessions: dict[str, _ConsumerSession] = field(default_factory=dict)

    @classmethod
    def instance(cls):
        state = GlobalState.instance()
        value = state.get(_STATE_KEY)
        if value is None:
            value = cls()
            state[_STATE_KEY] = value
        return value

    def configure(self, config):
        self.config = dict(config)
        producer = self.config.pop("producer", None)
        if producer is not None:
            self._kafka_producer = producer
            self._producer_injected = True

    def _register(self, topic):
        if topic not in self.subscribers and topic not in self.history_subscribers:
            register_service(topic, _message_subscriber_impl, topic=topic)

    def add_publisher(self, topic):
        if topic in self.publishers:
            raise ValueError(f"topic {topic!r} already has a publisher")
        self.publishers.add(topic)

    def add_subscriber(self, topic, replay=False):
        del replay  # The history service records replay demand separately.
        self._register(topic)
        self.subscribers.add(topic)

    def add_historical_subscriber(self, topic):
        self._register(topic)
        self.history_subscribers.add(topic)

    def _client_options(self):
        return {
            key: value
            for key, value in self.config.items()
            if key not in _CONFIG_HELPERS
        }

    def create_consumer(self):
        factory = self.config.get("consumer_factory")
        if factory is None:
            try:
                from kafka import KafkaConsumer
            except ModuleNotFoundError as error:
                raise RuntimeError(
                    "Kafka subscribing requires the 'kafka' extra") from error
            factory = KafkaConsumer
        return factory(**self._client_options())

    def consumer_session(self, topic):
        session = self._sessions.get(topic)
        if session is None or session.closed:
            session = _ConsumerSession(
                self, topic, live_requested=topic in self.subscribers)
            self._sessions[topic] = session
        return session

    def acquire_producer(self):
        if self._kafka_producer is None:
            factory = self.config.get("producer_factory")
            if factory is None:
                try:
                    from kafka import KafkaProducer
                except ModuleNotFoundError as error:
                    raise RuntimeError(
                        "Kafka publishing requires the 'kafka' extra") from error
                factory = KafkaProducer
            self._kafka_producer = factory(**self._client_options())
            self._producer_injected = False
        self._kafka_producer_count += 1
        return self._kafka_producer

    def close_producer(self):
        if self._kafka_producer is None:
            return
        self._kafka_producer_count -= 1
        if self._kafka_producer_count <= 0:
            if not self._producer_injected:
                self._kafka_producer.close()
            self._kafka_producer = None
            self._kafka_producer_count = 0

    def producer(self):
        if self._kafka_producer is None:
            return self.acquire_producer()
        return self._kafka_producer

    def publish(self, topic, message):
        producer = self.producer()
        if isinstance(message, KafkaMessage):
            headers = list(message.headers.items())
            if message.content_type is not None:
                headers.append(
                    (CONTENT_TYPE_HEADER, message.content_type.encode("utf-8")))
            producer.send(
                topic,
                message.payload,
                key=message.key,
                headers=headers or None,
            )
        else:
            producer.send(topic, message)

    def flush(self):
        if self._kafka_producer is not None:
            self._kafka_producer.flush()

    def close(self):
        for session in tuple(self._sessions.values()):
            session.stop()
        self._sessions.clear()
        if self._kafka_producer is not None:
            self._kafka_producer.flush()
            if not self._producer_injected:
                self._kafka_producer.close()
            self._kafka_producer = None
            self._kafka_producer_count = 0


def register_kafka_adaptor(config: dict):
    KafkaMessageState.instance().configure(config)


@generator
def _message_subscriber_history_aggregator(
        session: object,
        _api: EvaluationEngineApi = None,
) -> TSB["msg" : TS[KafkaMessage], "recovered" : TS[bool]]:
    consumer, topic_partitions = session.prepare()
    start_time = _api.start_time
    recovery_offsets = consumer.end_offsets(topic_partitions)
    recovery_offsets_by_partition = {
        (partition.topic, partition.partition): offset
        for partition, offset in recovery_offsets.items()
    }
    timestamp_ms = int(start_time.replace(tzinfo=UTC).timestamp() * 1000)
    offsets = consumer.offsets_for_times(
        {partition: timestamp_ms for partition in topic_partitions})
    for partition in topic_partitions:
        offset = offsets.get(partition)
        consumer.seek(
            partition,
            offset.offset if offset is not None else recovery_offsets[partition],
        )

    def recovery_complete():
        return all(
            consumer.position(partition) >= recovery_offsets[partition]
            for partition in topic_partitions
        )

    last_time = start_time
    try:
        yield start_time, {"recovered": False}
        while not recovery_complete():
            records = consumer.poll(timeout_ms=500, max_records=1000) or {}
            messages = [record for batch in records.values() for record in batch]
            if len(records) > 1:
                messages.sort(key=_record_order)
            for record in messages:
                boundary = recovery_offsets_by_partition[
                    (record.topic, record.partition)]
                if record.offset >= boundary:
                    continue
                message_time = _timestamp_to_datetime(record.timestamp)
                if message_time <= last_time:
                    message_time = last_time + MIN_TD
                last_time = message_time
                yield message_time, {"msg": _record_to_kafka_message(record)}

        # A poll may fetch records published after the recovery snapshot. Rewind
        # each partition to that snapshot so the same consumer's live phase
        # observes every post-recovery record exactly once.
        for partition in topic_partitions:
            consumer.seek(partition, recovery_offsets[partition])
        yield last_time + MIN_TD, {"recovered": True}
    finally:
        session.history_finished()


@push_queue(TS[KafkaMessage])
def _message_subscriber_queue(sender, *, session: object):
    session.attach_sender(sender)


@sink_node
def _start_realtime_message_subscriber(
        start_real_time_service: TS[bool], session: object):
    if start_real_time_service.value:
        start_real_time_service.make_passive()
        session.request_start()


@sink_node
def _consumer_lifetime(trigger: TS[bool], session: object):
    pass


@_consumer_lifetime.stop
def _stop_consumer_lifetime(session: object):
    session.stop()


@service_impl(
    interfaces=(message_history_subscriber_service, message_subscriber_service))
def _message_subscriber_impl(path: str, topic: str):
    state = KafkaMessageState.instance()
    session = state.consumer_session(topic)
    _consumer_lifetime(const(True, tp=TS[bool]), session=session)

    if topic in state.history_subscribers:
        historical = _message_subscriber_history_aggregator(session=session)
        start_real_time = historical["recovered"]
    else:
        historical = combine(
            msg=nothing(TS[KafkaMessage]),
            recovered=const(True, tp=TS[bool]),
        )
        start_real_time = const(True, tp=TS[bool])
    set_service_output(path, message_history_subscriber_service, historical)

    if topic in state.subscribers:
        real_time = _message_subscriber_queue(session=session)
        _start_realtime_message_subscriber(
            start_real_time, session=session)
    else:
        real_time = nothing(TS[KafkaMessage])
    set_service_output(path, message_subscriber_service, real_time)
