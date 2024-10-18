Services
========

A mechanism to expose shared graphs to user code. This de-couples an interface describing the
service from a consumers perspective and an implementation, it is possible to bind the interface
to different implementation and (through the path) also have multiple potential implementations
bound to the same interface.

There are a number of service patterns that represent common client / server patterns. These
include:

reference_service
    This is a publish style service with an initial image. The use-case is to provide access
    to a common set of data that is used by many clients in the graph.
    For example: a set of account ids, or a calendar, or instruments, etc.

subscription_service
    Represents a dynamic pub-sub style service where data is published on demand and will
    continue to be published until the last known subscriber goes away. There is only
    one stream of results, so if a new subscriber joins part way through, they receive
    an initial image and updates from that point on.
    For example: positions for an account id, prices for a symbol, etc.

request_reply_service
    This is loosely equivalent to an RPC service. This will deal with requests on an individual
    basis and provide bespoke responses. Unlike subscription services, there are no attempts
    to re-use request graphs based on inputs. This treats each request as being unique and
    will create a new implementation graph instance for each request received.
    For example: create a new order, add a new account, compute a response for a given set of
    bespoke inputs.

All services are streaming, in that the results will continue to be produced and be updated
until the consumer stops requesting the response. For subscriptions and reqeust reply the
request (or subscription) can be modified over time, that is requests / subscriptions are time
series inputs.

All services take at least one parameter, namely: ``path``; this is a string which is used to disambiguate
different instances of implementations associated to the path. You can always supply ``default_path``
as a parameter value. This will use a default name for the path (which is obtained from the
name of the service interface). Most graphs will likely use the default path unless there are more
than one implementation involved.

.. note:: Currently the feature set for services is still under development, the key component
          that is missing is the network distribution capability. This ensures that an implementation
          does not need to be located within a graph, but can be run anywhere on the network.

Another feature of the services, is that when implemented within a graph, the services are ranked to ensure that
the nodes are ranked above any user code, additionally reference and subscription nodes are ranked amongst each other
to ensure that service dependencies between services also ensure they are ranked based on dependency. The
request reply services are ranked above user nodes, but not based on usage. The consequences of this are:

* outputs ticks, from reference and subscription services, are delivered in the engine cycle they are produced.
* output ticks, from request reply services, are delivered one engine cycle after they are created in the implementation
  graph.
* subscriptions and requests are processed by the implementation service one engine cycle after requested by the user
  code.

This is important to remember when reasoning about behaviour when using local services.

For network implementations the timing from creating a request (or referencing a reference service) to first tick
is non-deterministic and depends on the latency of the network and time taken to process the request on the server.
The network versions are asynchronous and can only be used in ``REAL_TIME`` mode. Embedded services can be used
in either mode.

Reference Service
-----------------

::

    @reference_service
    def account_ids(path: str = default_path) -> TSS[str]:
        """ The set of available clients """

The above is an example of a reference service interface definition. Note that there are no implementation in
this interface. This should have appropriate code-doc to describe the expected behaviour and use of the service.
As a reference service, there are no time-series inputs supported. Thus it takes the form of a source node.

In this case implementation has the same shape as the service definition, for example:

::

    @service_impl(interfaces=[account_ids])
    def static_account_ids() -> TSS[str]:
        return const(frozenset("a", "b"), TSS[str])

This is a simplistic example, but shows the shape of the implementation. The signature is the same as for the client,
with the exception that the ``path`` element which is not required to be present. All service implementations take
``interfaces`` as a parameter to the ``services_impl`` decorator. This informs the decorator which interfaces
are being implemented.

When only one interface is being implemented the graph (or compute node) being provided takes the expected implementation
form of interface.

To use this service there are two elements that need to be done:

1. Register the implementation
2. Use the interface where appropriate

For example:

::

    @graph
    def main():
        register_service(default_path, static_account_ids)
        ...

    @graph
    def my_logic():
        ...
        accounts = account_ids()
        ...

The first step here registers a service implementation using the ``register_service`` and binds the implementation
to the ``default_path``.

Later the ``account_ids`` interface is used (without params, so uses ``default_path``).

If this is used numerous times, there is still only one instance of the implementation graph created, the interface
produces stubs that will find the implementation and bind the outputs into the graph at point of use. This is an
efficient means to share a common result without having to pass it as a parameter to every graph in the call stack.

Subscription Service
--------------------

::

    @subscription_service
    def market_data(instrument_id: TS[str], path: str=default_path) -> TS[float]:
        """ A simplified concept of subscription to market data returning a mid price """

This example shows a definition for a market data subscription. A subscription can only have one time-series input,
the type must be a ``TS`` and the value type must be keyable (suitable to be type of ``TSS``).

.. note:: The ``path`` is normally the first parameter of a service definition, but if ``path`` is likely to be
          ``default_path`` it is possible to place it at the end (or in the kwargs) section of the interface definition,
          allowing us to default the path and reduce the importance of the path element.

The implementation of this is more complicated then the reference service as the implementation is responsible for
handling many requests, but each requester operates on a single request basis. The example impl is below:

::

    @service_impl(interfaces=[market_data])
    def static_market_data(instrument_id: TSS[str]) -> TSD[str, TS[float]]:
        return map_(lambda key: const(1.0), __key_set__=instrument_id)

The implementation will take in a set of requests (or subscriptions). The name of the subscription has to be the same
as that of the interface's definition. The response is a ``TSD`` with the key type being the same type as the request
or subscription type (in this case ``str``). The time-series type of the ``TSD`` is the output type of the interface.

Typically, most implementations will use ``map_`` to decompose the requests back to the individual input / output
shapes.

The usages of subscriptions is much the same as with reference services.

Request Reply Service
---------------------

::

    @request_reply_service
    def create_order(path: str, order: TS[Order]) -> TS[str]:
        """ Creates a new order and returns the order id of the newly created order"

Here we show the interface description, in this case we follow the first param pattern for ``path``. This makes
path a required parameter to be supplied when calling this service. As with subscriptions, only one request parameter
is supported, however, unlike with subscriptions, the time-series type is arbitrary. Thus it is possible to use
``TSB``, ``TSD``, etc.

The implementation structure is below:

::

    @service_impl(interfaces=[create_order])
    def static_create_order(order: TSD[int, TS[Order]]) -> TSD[int, TS[str]]:
        return map_(lambda order: const("order_id"), order)

With a request reply, each request is identified by a unique integer value. The id belongs to the instance of the
requester client and not a tick of the request, for example:

::

    ...
    order_path = "main_order_handler"
    order: TS[Order] = ...
    order_id = create_order(order_path, order)
    ...

In this case the instance of create_order above will get a unique id associated to it. This will remain until this
instance is no longer used.

Registering the service is the same as for reference services.


