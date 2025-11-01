C++ Redesign of TimeSeries Types
--------------------------------

Time Series types represent the state of a value that changes over time, values change through the application of 
events. We typically do not record the history of the events but rather keep the current state (the concesquence
of all events to date) and the knowledge of the most recent event received.

Thus, a time-series has two key values associated to it, namely:
1. The current state of the time-series
2. The most recent event received

Events are time-based, thus we need to track the time of the most recent event.

At the most simple, the event represents a single atomic value, for example, an integer or float.
This would look like:

<time><value>

There is also the reset event, which removes the value associated to the time-series. This takes the form of:

<time><reset/invalidate sentinel>

Then there is the nothing happened event.

<time><no change senstinel>

Thus, there are 3 states that a delta value could represent. The no-change event is useful when requesting the event
for a given time, and no event occurred, so more of a response to a query than a state one may expect to be transmitted.

In the current HGraph implementation there is a property on the time-series type called delta_value. This is really
a query to the time-series type requesting the event for a given time, in this case the current engine time. In
this case the use of the no-change event is necessary to indicate that no event has occurred at the current engine time.

The value is the accumulation of all prior events, in the case of TS, the most simplistic of the time-series types, 
this will be either: invalid or the value of the last event.

To build a value state, the event should be able to be applied to the current value.

The time-series type is the component that knows how to manage the transitions, or rather the application of events.
It has a method called apply_event that takes the event and mutates the value.

Basic Time-Series Structure
---------------------------

TimeSeriesEvent: [This is like a c++ optional with an extra state and time]
    * time
    * type [no-change, reset, value]
    * value [optional, only when type is value]

TimeSeriesValue: [This is equivalent to C++ optional.]
    * has_value
    * value [optional, only when has_value is true]

TimeSeries:
    * value - The current state of the time-series as a consequence of apply events up to now (the engine time), this can be invalid or a value.
    * delta_value - A query requesting the event at the current engine time. If there is no event, the delta_value is the no-change sentinel.
    * last_modified_time - The time of the last event applied to the time-series.
    
Then there are some useful queries that can be performed on the time-series to query the state of the time-series:
   * valid - Is the value valid.
   * modified - Was the value modified (had an event applied to it) in this engine cycle (at the current engine time)

The time-series set already has the concept of the SetDelta, a delta value for a set. This can be modified to better
match collection time-series representations. For all collection time-series types (list, set, map), the event is:

CollectionTimeSeriesEvent:
    * time
    * type [no-change, reset, value]
    * added/modified items
        * key
        * type [reset, value] [optional when there is only a key, i.e. in the case of set]
        * value [optional, only when type is value]
    * removed items
        * key

Within the class of collection time-series, there are two key classes, those that collect other time-series elements
together (such as TSL, TSB and TSD) and those that just represent a collection of scalar values (TSS).

TSS is more akin to a simple time-series, except that it has an event structure similar to that of the collection time-series.

The encoding of the key can be optimised based on the nature of the collection type, for example, both TSL and TSB
can have their keys encoded as an integer. The size of the integer could also be determined at wiring time, thus
the builder could be primed with an event builder/decoder function that could have the ability to encode/decode 
different sized keys (i.e. 8/16/32/64 bit keys depending on need) this would allow for improved size representations
of delta. This is good for serialisation of events.

The TSD and TSS have key types that can be any hashable value. Thus their encoding is likely to be type dependent, with
little special attention to optimisation.

State Machine
-------------

Current we distinguish between input and output ts types, where output types are event generators and input types
are event consumers, but when inputs and outputs are in the same process, it makes sense to use the accumulated state
of the output time-series by the input. However, with the introduction of reference types and non-peered collection
types, this becomes a lot more complicated.

So the suggestion is to follow a type-erased / PIMPL style design, where the user-exposed interface is flat, but 
depending on the connection type, the implementation can be swapped in and out.

For example:

* A simple peered relationship with no-references. In this case the input is wrapped over the output time-series.
  this is the most simplistic binding and should produce close to a reference to the output.
* A reference-bound relationship will swap in a reference-tracked input which will have additional state as to
  current bound reference, previous reference (to compute deltas) etc. Perhaps we can simplify further.

This would allow us to swap in complexity as required, but if not required, we can keep the binding as light weight
as possible. We can also try and determine which implementation is required at wiring time and ensure the builder
builds the appropriate input element.

Observability / Notifiable
--------------------------

Ultimately, an event-based computational engine is observer-based, that is an event occurs, and we respond to it.
The inputs subscribe to the events they are interested to respond to. The outputs (or event generators) are 
the targets of the event subscription.

To reduce overhead, if we keep a list of all edges (input to output, reverse of data flow) on the output. The we can
keep a pivot point which indicates the balance between subscribed and passive; then all we need to see if we are active
is to check if we are in the left of the pivot point. If we keep the list in some form of sorted order, we could 
ensure active checks are not too expensive. This may be a way to reduce the overhead we need to carry and since it is
not often the case where we need to query the active state, it may not be too expensive an operation to track. [Option]

We can register anything that implements the notification protocol to the subscriber list. But some information that
will probably be needed is:

* Graph to schedule
* Node id to schedule within the graph.

Engine Time
-----------

This is used super often, we need to provide a fast lookup mechanism to identify the current engine time. One
option is to provide this information to methods such as eval, which would make the engine time immediately available.

This needs to be looked into ASAP, but it is an intrusive change to the current design, so the benefit would only be
to newly written code, and I don't really want to make this change to existing Python logic.
Perhaps it could be made available to the internal types, for example, we could pass it to eval, then the eval
could pass it to the apply_result, etc. This could reduce a few more engine time lookups.

Notification
------------

Need to look into making this as fast as possible, the bubbling up of scheduled time could possibly be improved?
This may be improved if we standardise the notify method to be graph/node_id/time


Complexity and TSS/TSD
----------------------

The TSS and TSD interactions, especially with "Transplanted inputs" and Reference bindings of the non-peered variety, 
are super complex and have many outstanding bugs from what I saw during the initial port. We need to clean this up
to make sure at least basic invariants such as the keys in the _ts_values match the keys in the key_set. This is
not always true especially in the transplanted inputs case. This is where the selectable state machines approach 
will probably help at least keep a better handle on the issues and should help with debugging if we can determine
which state we are in when debugging an issue (i.e. simple peer / reference / non-peered / non-peered reference / transplanted)

Value Support
-------------

Values represent keys and simple-time-series values. The collections are all just variations on this theme.
If we can create a solid delta-value and collected-value representation for each that uses a builder to compose
and decompose, then the scalar values are the only ones that need special attention. At the moment, we have
partitioned the set of scalars into primitive types that can be represented in size_t size and python objects.

This will do for the short term, but over time we will need to look to provide a more robust data representation.
We should at least extend the python object representation to track the type of the object stored from improved 
debugging and type safety.

Ultimately, we want to be able to have binary representations of most or all of the data types, this to support
network or shared memory transports. We also want to work toward cleanly supporting tabular data sets and having
a clean mapping to and from these data types. At the moment we use Polars for data frames. It may be better to use
a format such as Arrow for the underlying representation and supporting zero-copy wrapping to Polars, etc.
The zero-copy wrapping would allow for supporting more data frame manipulation tools such as Pandas as well, which
may not be my favourite wrapper, but is still enjoyed by many. This should also be transparently able to support numpy,
and by extension, sci-py, tensor-flow, etc.

C++ Library Nodes
-----------------

Up to now, the nodes have been written to support complex custom behaviour. But it would be nice to have a lightweight
mechanism to construct nodes from more simplistic functions (i.e. lift and compute_node wrapper). We had this previously,
needs to be simple and dynamic. That is for now, would like to avoid complex template metaprogramming if possible.
This needs to be a wrapper that can support eval, stop, start and direct binding to the inputs types, etc.





