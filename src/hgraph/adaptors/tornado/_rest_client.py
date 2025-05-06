from frozendict import frozendict

from hgraph import (
    TS,
    graph,
    register_adaptor,
    convert,
    combine,
    COMPOUND_SCALAR,
    format_,
    log_,
    AUTO_RESOLVE,
    to_json,
)
from hgraph.adaptors.tornado import (
    RestListResponse,
    RestReadResponse,
    RestCreateResponse,
    RestUpdateResponse,
    RestDeleteResponse,
)
from hgraph.adaptors.tornado.http_client_adaptor import http_client_adaptor_impl, http_client_adaptor
from hgraph.adaptors.tornado.http_server_adaptor import (
    HttpGetRequest,
    HttpPostRequest,
    HttpPutRequest,
    HttpDeleteRequest,
)


@graph
def rest_list(base_url: TS[str]) -> TS[RestListResponse]:
    """
    Lists the ids available in the rest server, this is a once off operation. To force a refresh, re-tick the URL.

    This is performed by sending a GET request to the rest server with the base_url provided.

    :param base_url: The base url of the rest server, e.g. http://localhost:8080/my_cs/v1/cs/
    :return: TS[RestListResponse] contains ids property, which is a tuple of the ids available in the rest server.
    """
    req = combine[TS[HttpGetRequest]](url=base_url)
    log_("Sending {}", req)
    resp = convert[TS[RestListResponse]](http_client_adaptor(req))
    log_("Received {}", resp)
    return resp


@graph
def rest_read(
    base_url: TS[str], id_: TS[str], value_type: type[COMPOUND_SCALAR] = AUTO_RESOLVE
) -> TS[RestReadResponse[COMPOUND_SCALAR]]:
    """
    Requests the value associated with the id from the rest server. This is a once off operation. To force a refresh,
    re-tick the id_.

    This performs a GET request to the rest server with the id_ provided. The request is sent to ``url/id_``.

    :param base_url: The base url of the rest server, e.g. http://localhost:8080/my_cs/v1/cs/
    :param id_: The id of the data object to retrieve
    :param value_type: The type of the value associated with the id. This is used to de-serialise the value.
    :return: TS[RestReadResponse[value_type]] The response will contain the id and value properties.
    """
    req = combine[TS[HttpGetRequest]](url=format_("{}/{}", base_url, id_))
    log_("Sending {}", req)
    resp = convert[TS[RestReadResponse[value_type]]](http_client_adaptor(req))
    log_("Received {}", resp)
    return resp


@graph
def rest_create(
    base_url: TS[str], id_: TS[str], value: TS[COMPOUND_SCALAR], _cs_tp: type[COMPOUND_SCALAR] = AUTO_RESOLVE
) -> TS[RestCreateResponse[COMPOUND_SCALAR]]:
    """
    Requests the ``value`` to be created using the ``id_`` provided.

    This converts the value to json and then performs a POST request to the rest server with content of
    ``{'id': id_, 'value': value}``.

    :param base_url: The base url of the rest server, e.g. http://localhost:8080/my_cs/v1/cs/
    :param id_: The id to use for the value.
    :param value: The value to be created.
    :return: The response indicating if the status is OK or not.
    """
    req = combine[TS[HttpPostRequest]](
        url=base_url,
        headers=frozendict({"Content-Type": "application/json"}),
        body=format_('{{ "id": "{}", "value": {} }}', id_, to_json(value)),
    )
    log_("Sending {}", req)
    resp = convert[TS[RestCreateResponse[_cs_tp]]](http_client_adaptor(req))
    log_("Received {}", resp)
    return resp


@graph
def rest_update(
    base_url: TS[str], id_: TS[str], value: TS[COMPOUND_SCALAR], _cs_tp: type[COMPOUND_SCALAR] = AUTO_RESOLVE
) -> TS[RestUpdateResponse[COMPOUND_SCALAR]]:
    """
    Requests the ``value`` to be updated using the ``id_`` provided.

    This converts the value to json and then performs a PUT request to the rest server using the URL of  ``url/id_``
    and the body content being the json representation of the value.

    :param base_url: The base url of the rest server, e.g. http://localhost:8080/my_cs/v1/cs/
    :param id_: The id to use for the value.
    :param value: The value to be updated.
    :return: The response indicating if the status is OK or not.
    """
    req = combine[TS[HttpPutRequest]](
        url=format_("{}/{}", base_url, id_),
        url_parsed_args=convert[TS[tuple[str, ...]]](id_),
        headers=frozendict({"Content-Type": "application/json"}),
        body=to_json(value),
    )
    log_("Sending {}", req)
    resp = convert[TS[RestUpdateResponse[_cs_tp]]](http_client_adaptor(req))
    log_("Received {}", resp)
    return resp


@graph
def rest_delete(base_url: TS[str], id_: TS[str]) -> TS[RestDeleteResponse]:
    """
    Requests the ``id_`` to be deleted.

    This sends a DELETE request to the rest server using the URL of  ``url/id_``.

    :param base_url: The base url of the rest server, e.g. http://localhost:8080/my_cs/v1/cs/
    :param id_: The id to delete.
    :return: The response indicating if the status is OK or not.
    """
    req = combine[TS[HttpDeleteRequest]](
        url=format_("{}/{}", base_url, id_), url_parsed_args=convert[TS[tuple[str, ...]]](id_)
    )
    log_("Sending {}", req)
    resp = convert[TS[RestDeleteResponse]](http_client_adaptor(req))
    log_("Received {}", resp)
    return resp


def register_rest_client():
    """Registers the http client adaptor required for the rest client to operate"""
    register_adaptor("http_client", http_client_adaptor_impl)
