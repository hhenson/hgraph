Common Pitfalls
===============

Here we address some common pitfalls that a new (or even an experienced) HGraph developer may run into.

.. _infinite_loop:

The infinite loop
-----------------

Symptom
.......

The code seems to be stuck, nothing is happening but the process is pegged to 100% CPU.

Likely Cause
............

The code is stuck in a virtual loop, this is most often caused when using feedback's in a computation, but can also
be caused due to scheduled tasks that are set to a very high frequency causing the engine to become stuck processing
the schedulers callbacks and not catching up to real-time (this most often seen in REALTIME evaluations).

How to validate
...............

The most simplistic way to test this is to turn on trace logging. This can be very heavy, especially in a large process
but you will see volumes of logs with the engine time moving in very small (typically 1 microsecond) increments.
Other approach is to force random break points into the code and spot check the engine-clock. You will see it
incrementing very slowly.

When using the trace, it is often possible to see the cycle by searching for the start of evaluation cycles, whatever
is ticking is the thing introducing the cycle this generally becomes a steady state evaluation path.

Another option is static analysis, take a look at feedbacks, if they are being consumed without being made passive
this is likely cause of the cycle.

Corrective Actions
..................

In the case of feedback loops, make sure they will terminate, either through marking the uses as passive or if they
become eventually stable (for example if they are wrapped with dedup logic and the values converges).

Make sure that schedulers do not overwhelm the engine, this is generally applicable in real-time clocks. For example
a timer function causes the engine to evaluate more frequently than it takes to compute the graph (for one engine cycle).
This creates an ever increasing lag ultimately resulting in a computation that appears to be a endless loop.
In this case either reduce the time of computation on the graph or increase the delay on the scheduler.
Also consider the use of last value queues, where information is dropped when the engine cannot keep up.


