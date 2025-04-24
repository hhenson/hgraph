from dataclasses import dataclass, field
from datetime import timedelta

from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient

from hgraph import (
    adaptor_impl,
    adaptor,
    WiringGraphContext,
    register_adaptor,
    GlobalState,
    register_service,
    TS,
    debug_print,
    const,
    service_impl,
    MIN_TD,
    TSB,
    combine,
    sink_node,
    STATE,
    EvaluationEngineApi,
    SCHEDULER,
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

    config: dict = None

    @classmethod
    def instance(cls) -> "KafkaMessageState":
        if "service.messaging.state" not in (gs := GlobalState.instance()):
            gs["service.messaging.state"] = cls()
        return gs["service.messaging.state"]

    def add_subscriber(self, topic: str, replay_history: bool):
        if topic not in self.subscribers:
            self.subscribers.add(topic)
            register_service(topic, _message_subscriber_aggregator, topic=topic)
        if replay_history and topic not in self.history_subscribers:
            self.history_subscribers.add(topic)
            register_service(topic, _message_subscriber_history_aggregator, topic=topic)

    def add_publisher(self, topic: str, replay_history: bool):
        if topic in self.publishers:
            # There can only be one publisher per topic.
            raise ValueError(f"Topic {topic} already has a publisher")
        self.publishers.add(topic)
        if replay_history and topic not in self.history_subscribers:
            self.history_subscribers.add(topic)
            register_service(topic, _message_subscriber_history_aggregator, topic=topic)

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


@service_impl(interfaces=(message_history_subscriber_service,))
def _message_subscriber_history_aggregator(path: str, topic: str) -> TSB["msg" : TS[bytes], "recovered" : TS[bool]]:
    """Recovered must tick after the last message has been delivered."""
    print(f"subscribe topic: {topic}")
    topic_b = b"sub_h: " + topic.encode("utf-8")
    return combine[TSB](msg=const(topic_b), recovered=const(True, delay=MIN_TD))
