from hgraph import graph, register_adaptor, default_path, TS, debug_print
from hgraph.adaptors.kafka import register_kafka_adaptor, message_subscriber
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
        register_kafka_adaptor()
        my_subscriber()
        my_other_subscriber()

    assert eval_node(g) == None
