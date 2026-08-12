"""Compatibility surface for the former in-core Kafka adaptor.

The implementation is supplied by the optional :mod:`hgraph_kafka`
extension. Keeping this shim in the core distribution avoids two wheels
installing files into the same ``hgraph`` package.
"""

try:
    from ._api import (
        KafkaMessage,
        MessageState,
        message_publisher,
        message_subscriber,
    )
    from ._impl import register_kafka_adaptor
except ModuleNotFoundError as error:
    if error.name != "hgraph_kafka":
        raise
    raise ModuleNotFoundError(
        "hgraph.adaptors.kafka requires the optional 'hgraph-kafka' "
        "distribution; install it with `pip install hgraph-kafka` and use "
        "`import hgraph_kafka` for new code",
        name="hgraph_kafka",
    ) from error

__all__ = (
    "message_publisher",
    "message_subscriber",
    "MessageState",
    "KafkaMessage",
    "register_kafka_adaptor",
)
