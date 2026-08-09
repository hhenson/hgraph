"""Compatibility surface for the former in-core Kafka adaptor."""

from ._api import KafkaMessage, MessageState, message_publisher, message_subscriber
from ._impl import register_kafka_adaptor

__all__ = (
    "message_publisher",
    "message_subscriber",
    "MessageState",
    "KafkaMessage",
    "register_kafka_adaptor",
)
