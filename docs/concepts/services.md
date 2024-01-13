Services
========

The service pattern describes a component in the graph that is self-contained and provides
functionality that can be evaluated asynchronously from the rest of the graph. That is there
are no guarantees of time-correlation between events or ticks processed in the main graph
and that of a service graph.

There are different patterns of service, these include:
* Reference Data
* Subscriptions
* Request-Reply

Each pattern follows a set of rules controlling their structure and behaviour.

Services can be local or remove and can have multiple independent implementations of
the same service interface. This has multiple benefits, including the ability to
change deployment of parts of the code to be remote or local, and have back-test stubs of
services when appropriate.

The service interface is used anywhere in the graph, there are no dependency injections
required to use the service. The only requirement is that the service is registered,
either locally or on the network.

All service components (graphs / nodes) will appear as a source node in the graph,
as the service is run as a nested graph.

Reference Data
--------------

Reference data refers to a pattern of service where a common collection of data is produced
by a service and consumed by multiple different clients. The rules for this include:
* The service takes the form of a source node in the graph, in that it has no inputs and 
  only produces outputs. There is no interactivity and the data is present independent 
  of use. For example a set of available accounts or holiday data. It is normal for the
  data to be exposed as a TSD (or other collection API).

An example of a reference service is presented below:

```python
    from hgraph import reference_service, TSD, TSS, service_impl, register_service, \
         graph, TS, compute_node
    from datetime import date

    @reference_service
    def holiday_calendar(path: str | None = None) -> TSD[str, TSS[date]]:
        """
        The holidays for locals, e.g. GB for UK or GBP for currency holiday or
        LME for exchange holidays.
        """

    @service_impl(interface=holiday_calendar)
    @compute_node
    def holiday_calendar_impl() -> TSD[str, TSS[date]]:
        """Do the work to compute / update the holiday calendar"""
        return ...
    
    
    @graph
    def price_quote(...) -> TS[float]:
        ...
        trade_currency: TS[str]
        holiday = holiday_calendar()[trade_currency]
        ...
        return ...

    @graph
    def main():
        register_service(None, holiday_calendar, holiday_calendar_impl)
        ...
        quote = price_quote(...)
```

This example is really stripped down to try and highlight the usage pattern.

By default, a service implementation is assumed to be a graph definition, however,
if required, the user can wrap the function with ``@compute_node`` and then this will
be a compute node.

The actual return type of the final node will be a ``REF`` of the output type, but
to make the signature more readable, the user just specifies the un-referenced output
type, the wrapping logic will re-phrase the code to return a reference.

