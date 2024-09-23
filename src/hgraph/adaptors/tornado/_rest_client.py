from hgraph import (
    TS,
    graph,
    register_adaptor,
    convert,
    combine,
    CompoundScalar,
    COMPOUND_SCALAR,
    format_,
    log_,
)

from hgraph.adaptors.tornado import RestListResponse, RestResponse, RestReadResponse, REST_RESPONSE
from hgraph.adaptors.tornado.http_client_adaptor import http_client_adaptor_impl, http_client_adaptor
from hgraph.adaptors.tornado.http_server_adaptor import HttpGetRequest


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


def register_rest_client():
    """Registers the http client adaptor required for the rest client to operate"""
    register_adaptor("http_client", http_client_adaptor_impl)
