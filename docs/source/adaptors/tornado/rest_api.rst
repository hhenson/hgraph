REST API
========

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
including a call to ``register_rest_client``.

.. autofunction:: hgraph.adaptors.tornado.rest_list

.. autofunction:: hgraph.adaptors.tornado.rest_read

.. autofunction:: hgraph.adaptors.tornado.rest_create

.. autofunction:: hgraph.adaptors.tornado.rest_update

.. autofunction:: hgraph.adaptors.tornado.rest_delete

