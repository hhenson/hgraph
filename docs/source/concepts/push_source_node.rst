Push Source Node
================

The push source node provides the means to introduce ticks into the graph from
external sources that are asynchronous in their nature.

The push source nodes are only available to be used in real-time mode.

Push source nodes are required to support the introduction of ticks via
thread safe means since, by definition, a push source node takes events from
other threads.

The exposure of the push source node in Python is via the decorator ``push_queue``.

As with the ``generator`` in the pull source node, a ``push_queue`` wrapped function
is called during the start life-cycle. It takes at least one input, the sender, as per below:

::

    @push_queue(TS[bool])
    def my_message_sender(sender: Callable[[SCALAR], None]):
        ...

The ``push_queue`` takes the output type, as a parameter, then the function is
injected with a callable instance that can be used to inject events / ticks into the graph.

Typically the method will construct a thread and pass the sender to the code that runs
on the thread.
