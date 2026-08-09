"""Released Kafka authoring API, implemented by :mod:`hgraph_kafka`."""

from frozendict import frozendict  # noqa: F401 - needed to resolve legacy annotations

from hgraph_kafka.compat import (
    KafkaMessage,
    MessageState,
    get_message_state,
    message_publisher,
    message_subscriber,
)

__all__ = (
    "message_publisher",
    "message_subscriber",
    "MessageState",
    "KafkaMessage",
)
