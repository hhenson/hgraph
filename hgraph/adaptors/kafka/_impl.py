from dataclasses import dataclass, field
from datetime import timedelta, datetime
from logging import error
from threading import Thread, Event
from typing import Callable, Mapping

import pytz
from frozendict import frozendict
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

from hgraph import (
    GlobalState,
    TS,
    combine,
    const,
    default,
    if_then_else,
    MIN_TD,
    sink_node,
    STATE,
    EvaluationEngineApi,
    SCHEDULER,
    generator,
    EvaluationMode,
    push_queue,
    SCALAR,
    adaptor_impl,
    register_adaptor,
)
from hgraph.adaptors.kafka._api import (
    message_publisher_operator,
    message_subscriber_service,
    MessageState,
    MessageSubscription,
    KafkaMessage,
)

__all__ = ("register_kafka_adaptor",)

CONTENT_TYPE_HEADER = "content-type"


def _record_to_kafka_message(record) -> KafkaMessage:
    """Convert a kafka ConsumerRecord into a KafkaMessage, lifting the content-type header out."""
    content_type = None
    headers = {}
    for k, v in record.headers or ():
        if k == CONTENT_TYPE_HEADER:
            content_type = v.decode("utf-8") if isinstance(v, bytes) else v
        else:
            headers[k] = v
    return KafkaMessage(
        payload=record.value, key=record.key, content_type=content_type, headers=frozendict(headers)
    )


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
    # Topics whose implementation has been registered. Kept apart from the two sets above because
    # a topic can be added to either of them, in either order, and must register exactly once.
    _registered: set[str] = field(default_factory=set)
    _kafka_producer: KafkaProducer = None
    _kafka_producer_count: int = 0
    _kafka_sender: dict[str, Callable[[KafkaMessage], None]] = field(default_factory=dict)
    _kafka_consumer: dict[str, "KafkaConsumerThread"] = field(default_factory=dict)

    config: dict = None

    @classmethod
    def instance(cls, global_state: GlobalState = None) -> "KafkaMessageState":
        if "service.messaging.state" not in (gs := global_state or GlobalState.instance()):
            gs["service.messaging.state"] = cls()
        return gs["service.messaging.state"]

    def add_subscriber(self, topic: str, replay: bool = False):
        self._register(topic)
        self.subscribers.add(topic)

    def add_historical_subscriber(self, topic: str):
        self._register(topic)
        self.history_subscribers.add(topic)

    def _register(self, topic: str):
        # One implementation serves the topic however it is used. It reads `subscribers` and
        # `history_subscribers` when it is expanded, which is after all callers have registered,
        # so no registration argument has to predict what a later caller will ask for.
        if topic not in self._registered:
            self._registered.add(topic)
            register_adaptor(topic, _kafka_subscriber_impl, topic=topic)

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

    def start_subscriber(self, topic: str, consumer: KafkaConsumer, on_error: Callable = None):
        self._kafka_consumer[topic] = (
            thread := KafkaConsumerThread(topic, consumer, self._kafka_sender[topic], on_error)
        )
        thread.start()

    def stop_subscriber(self, topic: str):
        if topic in self._kafka_consumer:
            self._kafka_consumer.pop(topic).stop()


def _registered_topics(m, topic):
    """
    Makes sure we have registered this in this implementation's topic registry, this compensates for lack of service
    impl infra for the sink node.
    """
    ms = KafkaMessageState.instance()
    return topic in ms.publishers


@sink_node(overloads=message_publisher_operator, requires=_registered_topics)
def _kafka_message_publisher(
    msg: TS[bytes],
    topic: str,
    _state: STATE = None,
    _scheduler: SCHEDULER = None,
    _global_state: GlobalState = None,
) -> None:
    if msg.modified:
        _state.producer.send(topic, msg.value)
        _scheduler.schedule(
            timedelta(milliseconds=100), tag="flush_timer", on_wall_clock=True,
        )  # This will re-schedule the flush timer if already set.

    if _scheduler.is_scheduled_now:
        # Make sure we flush reasonably regularly.
        _state.producer.flush()


@_kafka_message_publisher.start
def _kafka_message_publisher_start(topic: str, _state: STATE, _global_state: GlobalState = None):
    _state.producer = KafkaMessageState.instance(_global_state).producer


@_kafka_message_publisher.stop
def _kafka_message_publisher_stop(_state: STATE, _global_state: GlobalState = None):
    _state.producer.flush()
    _state.producer = None
    KafkaMessageState.instance(_global_state).close_producer()


@sink_node(overloads=message_publisher_operator, requires=_registered_topics)
def _kafka_full_message_publisher(
    msg: TS[KafkaMessage],
    topic: str,
    _state: STATE = None,
    _scheduler: SCHEDULER = None,
    _global_state: GlobalState = None,
) -> None:
    if msg.modified:
        m: KafkaMessage = msg.value
        headers = [(k, v) for k, v in m.headers.items()]
        if m.content_type is not None:
            headers.append((CONTENT_TYPE_HEADER, m.content_type.encode("utf-8")))
        _state.producer.send(topic, m.payload, key=m.key, headers=headers or None)
        _scheduler.schedule(
            timedelta(milliseconds=100), tag="flush_timer", on_wall_clock=True,
        )  # This will re-schedule the flush timer if already set.

    if _scheduler.is_scheduled_now:
        # Make sure we flush reasonably regularly.
        _state.producer.flush()


@_kafka_full_message_publisher.start
def _kafka_full_message_publisher_start(topic: str, _state: STATE, _global_state: GlobalState = None):
    _state.producer = KafkaMessageState.instance(_global_state).producer


@_kafka_full_message_publisher.stop
def _kafka_full_message_publisher_stop(_state: STATE, _global_state: GlobalState = None):
    _state.producer.flush()
    _state.producer = None
    KafkaMessageState.instance(_global_state).close_producer()


@adaptor_impl(interfaces=message_subscriber_service)
def _kafka_subscriber_impl(path: str, topic: str, _global_state: GlobalState = None) -> MessageSubscription:
    """
    Serves a topic to every graph that uses it, whatever mix of replay and live delivery they asked
    for. Splicing history to live here rather than in the caller is what keeps this acyclic: the
    handover is one node's business, so no client has to depend on a second service to perform it.

    Being an ``adaptor_impl`` rather than a ``service_impl`` is what makes that possible at all —
    the push source below cannot be wired inside a service implementation's nested graph.
    """
    ks = KafkaMessageState.instance(_global_state)
    wants_history = topic in ks.history_subscribers
    wants_live = topic in ks.subscribers

    consumer = KafkaConsumer(**ks.config)
    # Partitions have to be resolved and assigned before either the replay seek or the live poll.
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        raise ValueError(f"No partitions found for topic '{topic}'")
    topic_partitions = tuple(TopicPartition(topic, p) for p in partitions)
    consumer.assign(topic_partitions)

    if wants_history:
        # The consumer is handed on to the live thread when replay finishes, so it is only closed
        # by the aggregator when nothing is going to take it over.
        history = _message_subscriber_history_aggregator(
            path, consumer, topic_partitions, close_when_done=not wants_live
        )
        recovered = history.recovered
    else:
        recovered = const(True)

    if wants_live:
        live = _message_subscriber_queue(topic=topic)
        # `recovered` is const(True) without history, so the thread starts on the first cycle.
        _start_realtime_message_subscriber(topic, recovered, consumer)
        msg = if_then_else(default(recovered, False), live, history.msg) if wants_history else live
    else:
        msg = history.msg

    return combine[MessageSubscription](msg=msg, recovered=recovered)


@generator
def _message_subscriber_history_aggregator(
    path: str,
    consumer: KafkaConsumer,
    topic_partitions: tuple[tuple[str, int], ...],
    close_when_done: bool = False,
    _api: EvaluationEngineApi = None,
) -> MessageSubscription:
    """
    Recovered must tick after the last message has been delivered.

    ``close_when_done`` closes the consumer once replay ends. Set it when no live subscriber will
    inherit this consumer, otherwise the connection stays open for the life of the graph with
    nothing reading it.
    """
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
    # Seeded from start_time, which is what the first yield below uses. Seeding it from
    # timestamp_ms instead is wrong: that value is start_time truncated to whole milliseconds, so
    # it can be up to a millisecond earlier, and the next message is then offset to a time before
    # the one already emitted. The generator rejects that as a duplicate or out-of-order time.
    last_time = start_time
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
            yield tm, dict(msg=_record_to_kafka_message(msg))
    if close_when_done:
        consumer.close()
    tm = last_time
    tm = max(tm, start_time - MIN_TD)
    yield tm + MIN_TD, dict(recovered=True)


def _timestamp_to_datetime(t: int) -> datetime:
    """
    Kafka timestamps are epoch milliseconds. The engine works in naive UTC, so that is what is
    returned.

    This used to be utcfromtimestamp(t / 1000) plus timedelta(milliseconds=t % 1000), which counted
    the sub-second part twice: a message at .999 was replayed a whole second late, shifting replay
    timings and, where two messages straddled a second boundary, their order.
    """
    return _EPOCH + timedelta(milliseconds=t)


_EPOCH = datetime(1970, 1, 1)


@push_queue(TS[KafkaMessage])
def _message_subscriber_queue(
    sender: Callable[[SCALAR], None] = None, *, topic: str, _global_state: GlobalState = None
):
    KafkaMessageState.instance(_global_state).set_subscriber_sender(topic, sender)


@sink_node
def _start_realtime_message_subscriber(
    topic: str,
    start_real_time_service: TS[bool],
    consumer: KafkaConsumer,
    _global_state: GlobalState = None,
    _api: EvaluationEngineApi = None,
):
    if start_real_time_service.value:
        start_real_time_service.make_passive()

        def _on_error(_e: BaseException, _api=_api):
            # Called from the consumer thread. Stopping the engine is the honest outcome: the
            # topic will not deliver again, so continuing would silently produce wrong results.
            _api.request_engine_stop()

        KafkaMessageState.instance(_global_state).start_subscriber(topic, consumer, _on_error)


@_start_realtime_message_subscriber.stop
def _start_realtime_message_subscriber_stop(topic: str, _global_state: GlobalState = None):
    KafkaMessageState.instance(_global_state).stop_subscriber(topic)


class KafkaConsumerThread(Thread):

    def __init__(
        self,
        topic,
        consumer: KafkaConsumer,
        sender: Callable[[KafkaMessage], None],
        on_error: Callable[[BaseException], None] = None,
    ):
        super().__init__()
        self.topic = topic
        self.consumer = consumer
        self.sender = sender
        self.on_error = on_error
        self._stop_event = Event()

    def run(self):
        try:
            while not self._stop_event.is_set():
                records = self.consumer.poll(timeout_ms=1000, max_records=1000)
                all_messages = [m for tp, messages in records.items() for m in messages]
                if len(records) > 1:
                    all_messages = sorted(all_messages, key=lambda m: (m.timestamp, m.topic, m.offset))
                for msg in all_messages:
                    self.sender(_record_to_kafka_message(msg))
        except BaseException as e:
            # Reaching here means delivery for this topic has stopped. Logging alone leaves the
            # graph running against a feed that will never tick again, which looks like a quiet
            # market rather than a broken one, so the failure is reported to the graph as well.
            error(f"Failure occurred whilst reading from Kafka on topic: {self.topic}", exc_info=True)
            if self.on_error is not None:
                try:
                    self.on_error(e)
                except BaseException:
                    error("Failed to report the Kafka failure to the graph", exc_info=True)
        finally:
            self.consumer.close()

    def stop(self):
        self._stop_event.set()
