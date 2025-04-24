from hgraph import adaptor_impl, adaptor, WiringGraphContext, register_adaptor
from hgraph.adaptors.kafka._api import message_publisher_adaptor


__all__ = ("register_kafka_adaptor",)


def register_kafka_adaptor():
    register_adaptor(MESSAGE_ADAPTOR_PATH, kafka_message_adaptor_impl)


MESSAGE_ADAPTOR_PATH = "service.messaging"


@adaptor_impl(interfaces=(message_publisher_adaptor, message_publisher_adaptor))
def kafka_message_adaptor_impl(path: str):
    """
    Exposes the message publisher and subscriber interfaces using Kafka for the messaging bus.
    """

    # Get a handle on the adaptors that have been implemented
    adaptors_dedup_publisher = set()
    adaptors_publisher = set()
    for path, type_map, node, receive in WiringGraphContext.__stack__[0].registered_service_clients(
            message_publisher_adaptor
    ):
        assert type_map == {}, "message_publisher_adaptor does not support type generics"
        if (path, receive) in adaptors_dedup_publisher:
            raise ValueError(f"Duplicate message_publisher_adaptor adaptor client for path {path}: only one client is allowed")
        adaptors_dedup_publisher.add((path, receive))
        adaptors_publisher.add(path.replace("/from_graph", "").replace("/to_graph", ""))

    # adaptors_dedup_subscriber = set()
    # adaptors_subscriber = set()
    # for path, type_map, node, receive in WiringGraphContext.__stack__[0].registered_service_clients(
    #         message_subscriber_adaptor
    # ):
    #     assert type_map == {}, "message_subscriber_adaptor does not support type generics"
    #     adaptors_dedup_subscriber.add((path, receive))
    #     adaptors_subscriber.add(path.replace("/from_graph", "").replace("/to_graph", ""))
