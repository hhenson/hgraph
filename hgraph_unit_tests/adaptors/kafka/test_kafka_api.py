from hgraph import graph, register_adaptor, default_path, TS, debug_print, const
from hgraph.adaptors.kafka import register_kafka_adaptor, message_subscriber, message_publisher
from hgraph.test import eval_node


def test_subscriber():

    @message_subscriber(topic="my_topic")
    def my_subscriber(msg: TS[bytes]):
        debug_print("test_subs1", msg)

    @message_subscriber(topic="my_topic")
    def my_other_subscriber(msg: TS[bytes], recovered: TS[bool]):
        debug_print("test_subs2:msg", msg)
        debug_print("test_subs2:recovered", recovered)

    @graph
    def g():
        register_kafka_adaptor({})
        my_subscriber()
        my_other_subscriber()

    assert eval_node(g) == None


def test_publisher():

    @message_publisher(topic="my_topic")
    def my_publisher() -> TS[bytes]:
        return const(b'my publisher')

    @graph
    def g():
        register_kafka_adaptor({})
        my_publisher()

    assert eval_node(g) == None
