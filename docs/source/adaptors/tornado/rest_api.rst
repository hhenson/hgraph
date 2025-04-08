REST API
========

The Rest API currently resides in the module: ``hgraph.adaptors.tornado``

Rest describes a standard pattern for exposing a web based API. This considers managing a data object that has a
unique id. The data object is represented as a JSON value. The id is a string value.

The following operations are possible:

list
    Returns a list of valid id's

read
    Returns the value for a given id

create
    Creates a new instance of the data object for a given id, the id MUST NOT already exist in the collection.

update
    Modifies the data object associated to the given id, the id MUST already exist in the collection.

delete
    Removes the id associated to the collection.

Both a client API and a service implementation API are provided. The client API requires the service to follow
the standard REST patterns for implementing these services. The client API is not required to be used when implementing
the server API. The server API exposes the behaviours using a REST compliant web API.

Client API
----------

The client API is below, to make use of the client API, the web client must be registered. This can be done by
including a call to ``register_rest_client``. The graph must be run in ``REAL_TIME`` mode for this to work.

.. autofunction:: hgraph.adaptors.tornado.rest_list

.. autofunction:: hgraph.adaptors.tornado.rest_read

.. autofunction:: hgraph.adaptors.tornado.rest_create

.. autofunction:: hgraph.adaptors.tornado.rest_update

.. autofunction:: hgraph.adaptors.tornado.rest_delete


Service API
-----------

The server API makes use of a handler pattern, where the user is responsible for writing a handler function and wrapping
it with the ``@rest_handler`` decorator. This decorator takes the relative URL for the end-point and the type of the
data-object to be managed. The ``register_http_server_adaptor`` needs to be called to register the server process.
Finally, the graph must be run in ``REAL_TIME`` mode for this to work.

.. autofunction:: hgraph.adaptors.tornado.rest_handler


There are a number of request objects that can be received, this allows logic to make use of the ``dispatch`` mechanism
to process incoming requests. The requests are:

.. autoclass:: hgraph.adaptors.tornado.RestRequest

.. autoclass:: hgraph.adaptors.tornado.RestCreateRequest

.. autoclass:: hgraph.adaptors.tornado.RestUpdateRequest

.. autoclass:: hgraph.adaptors.tornado.RestReadRequest

.. autoclass:: hgraph.adaptors.tornado.RestDeleteRequest

.. autoclass:: hgraph.adaptors.tornado.RestListRequest


The response makes use of an enum for indicating the result.

.. autoclass:: hgraph.adaptors.tornado.RestResultEnum

With the response class' being:

.. autoclass:: hgraph.adaptors.tornado.RestResponse

.. autoclass:: hgraph.adaptors.tornado.RestCreateResponse

.. autoclass:: hgraph.adaptors.tornado.RestUpdateResponse

.. autoclass:: hgraph.adaptors.tornado.RestReadResponse

.. autoclass:: hgraph.adaptors.tornado.RestDeleteResponse

.. autoclass:: hgraph.adaptors.tornado.RestListResponse

