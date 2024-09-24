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
    REST_RESPONSE,
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
    req = combine[TS[HttpGetRequest]](url=base_url)
    log_("Sending {}", req)
    resp = convert[TS[RestListResponse]](http_client_adaptor(req))
    log_("Received {}", resp)
    return resp


@graph(resolvers={REST_RESPONSE: lambda m, s: RestReadResponse[m[COMPOUND_SCALAR].py_type]})
def rest_read(base_url: TS[str], id_: TS[str], value_type: type[COMPOUND_SCALAR]) -> TS[REST_RESPONSE]:
    req = combine[TS[HttpGetRequest]](url=format_("{}/{}", base_url, id_))
    log_("Sending {}", req)
    resp = convert[TS[RestReadResponse[value_type]]](http_client_adaptor(req))
    log_("Received {}", resp)
    return resp


@graph(resolvers={REST_RESPONSE: lambda m, s: RestCreateResponse[m[COMPOUND_SCALAR].py_type]})
def rest_create(
    base_url: TS[str], id_: TS[str], value: TS[COMPOUND_SCALAR], _cs_tp: type[COMPOUND_SCALAR] = AUTO_RESOLVE
) -> TS[REST_RESPONSE]:
    req = combine[TS[HttpPostRequest]](
        url=base_url,
        headers=frozendict({"Content-Type": "application/json"}),
        body=format_('{{ "id": "{}", "value": {} }}', id_, to_json(value)),
    )
    log_("Sending {}", req)
    resp = convert[TS[RestCreateResponse[_cs_tp]]](http_client_adaptor(req))
    log_("Received {}", resp)
    return resp


@graph(resolvers={REST_RESPONSE: lambda m, s: RestUpdateResponse[m[COMPOUND_SCALAR].py_type]})
def rest_update(
    base_url: TS[str], id_: TS[str], value: TS[COMPOUND_SCALAR], _cs_tp: type[COMPOUND_SCALAR] = AUTO_RESOLVE
) -> TS[REST_RESPONSE]:
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
