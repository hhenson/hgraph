# Time-series events

The time-series events form the base of the event flow within HGraph.
These events represent the state changes that a time-series value has undergone over-time.

There are two key types of time-series events:

* **TsEventAny** - This represents a stream of scalar values.
* **TsCollectionEventAny** - This represents a collection of time-series events.

## TsEventAny

The event structure is comprised of:
* **time** - The time for which this event is associated to.
* **kind** - The nature of the event (an enum).
* **value** - The value of the event (only present when required by the event kind)

### TsEventKind

* **None** - There was no event at this time. This is a response to a query for the event
             for a given time.

* **Recover** - Used to represent the result of a recovery request where the time in this case
                is the last event time.
* **Modify** - The event changes the current value of the time-series. 
* **Invalidate** - A scalar value.

There are two query types: ``None`` and ``Recover``. These do not represent actual
state changes to the time-series, but rather represent the event state at a given time.
With ``None`` this represent the absence of an event at a given time.
With ``Recover`` this represents the last event at a given time.

On the time-series value, there is a property ``delta_value``, which will return the
event received at the current engine time, or if no value was receieved this will return a ``None`` event.

The ``Recover`` event is used when a graph is in recovery mode. This will re-initialise the graph using the last
event state for the time-series values.

## TsCollectionEventAny

This represents an event of a collection time-series value, there are four collection time-series values:
* **TSS** - The time-series set.
* **TSL** - The time-series list, an indexed homogeneous list of time-series values.
* **TSB** - The time-series bundle, a named heterogeneous list of time-series values.
* **TSD** - The time-series dictionary, a name homogeneous list of time-series values of variable size.

The collection event is an event, that implies it has a time and a kind.
But instead of having a single value it supports a collection of keyed events.