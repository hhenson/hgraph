Monitoring Example
==================

This example is designed to show a more complex project. The project has a couple of
phases, to show the steps involved in the design process and how to apply the different
technologies in a phased approach.

The core problem is to track and report the status of various processed and checks / measures
in a running system. The actual implementation of the checks are of limited interest to this
problem.

Phase I
-------

This is the minimalistic approach and will focus on the use of the core API features only.

Let's split the problem into a couple of different elements, namely:
1. Process Monitoring
2. Gate Check
3. Liveness

### Process Monitoring

This focuses on identifying the following conditions:
* **Timeliness** - Did the task start on time \[Green] or is it pending execution \[Orange] or has it 
   missed the window of normal operations \[Red]. Did the process end on time, with success \[Green],
   late, but in tolerance \[Orange], late and out of tolerance \[Red].
* **Success** - How did the task complete, successfully \[Green], failed \[Red]

In order to make this a bit more interesting, we will also assume that the process may itself,
depend on other tasks, in which case we introduce the concept of:

* **Pending** - The process depends on a prior task that has not yet completed successfully \[Grey]
  until the start time is past the tolerance, then \[Yellow], finally if the task exceeds it's
  end time the process will show as \[Purple].

#### API

The interface to the monitoring application is exposed via a web-api.
The client will configure the service by sending a configuration message.
The client will then send a message to indicate it has started and subsequently
when it completes. The completion message should include the outcome of the process,
that is success or failure.

There is then an API to query the system to determine the current state.

#### Approach

Start with describing the API with ``@graph`` functions.

