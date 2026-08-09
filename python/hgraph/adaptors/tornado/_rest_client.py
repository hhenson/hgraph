"""Convenience graphs for the REST protocol over the HTTP client adaptor."""

from frozendict import frozendict

from hgraph import (
    AUTO_RESOLVE,
    COMPOUND_SCALAR,
    TS,
    combine,
    convert,
    format_,
    graph,
    log_,
    register_adaptor,
    to_json,
)

from ._rest_handler import (
    RestCreateResponse,
    RestDeleteResponse,
    RestListResponse,
    RestReadResponse,
    RestUpdateResponse,
)
from .http_client_adaptor import http_client_adaptor, http_client_adaptor_impl
from .http_server_adaptor import (
    HttpDeleteRequest,
    HttpGetRequest,
    HttpPostRequest,
    HttpPutRequest,
)


@graph
def rest_list(base_url: TS[str]) -> TS[RestListResponse]:
    """List the IDs exposed by a REST handler."""
    request = combine[TS[HttpGetRequest]](url=base_url)
    log_("Sending {}", request)
    response = convert[TS[RestListResponse]](http_client_adaptor(request))
    log_("Received {}", response)
    return response


@graph
def rest_read(
    base_url: TS[str],
    id_: TS[str],
    value_type: type[COMPOUND_SCALAR] = AUTO_RESOLVE,
) -> TS[RestReadResponse[COMPOUND_SCALAR]]:
    """Read one typed value by ID."""
    request = combine[TS[HttpGetRequest]](
        url=format_("{}/{}", base_url, id_)
    )
    log_("Sending {}", request)
    response = convert[TS[RestReadResponse[value_type]]](
        http_client_adaptor(request)
    )
    log_("Received {}", response)
    return response


@graph
def rest_create(
    base_url: TS[str],
    id_: TS[str],
    value: TS[COMPOUND_SCALAR],
    _cs_tp: type[COMPOUND_SCALAR] = AUTO_RESOLVE,
) -> TS[RestCreateResponse[COMPOUND_SCALAR]]:
    """Create one typed value by ID."""
    request = combine[TS[HttpPostRequest]](
        url=base_url,
        headers=frozendict({"Content-Type": "application/json"}),
        body=format_(
            '{{ "id": "{}", "value": {} }}',
            id_,
            to_json(value),
        ),
    )
    log_("Sending {}", request)
    response = convert[TS[RestCreateResponse[_cs_tp]]](
        http_client_adaptor(request)
    )
    log_("Received {}", response)
    return response


@graph
def rest_update(
    base_url: TS[str],
    id_: TS[str],
    value: TS[COMPOUND_SCALAR],
    _cs_tp: type[COMPOUND_SCALAR] = AUTO_RESOLVE,
) -> TS[RestUpdateResponse[COMPOUND_SCALAR]]:
    """Update one typed value by ID."""
    request = combine[TS[HttpPutRequest]](
        url=format_("{}/{}", base_url, id_),
        url_parsed_args=convert[TS[tuple[str, ...]]](id_),
        headers=frozendict({"Content-Type": "application/json"}),
        body=to_json(value),
    )
    log_("Sending {}", request)
    response = convert[TS[RestUpdateResponse[_cs_tp]]](
        http_client_adaptor(request)
    )
    log_("Received {}", response)
    return response


@graph
def rest_delete(
    base_url: TS[str],
    id_: TS[str],
) -> TS[RestDeleteResponse]:
    """Delete one value by ID."""
    request = combine[TS[HttpDeleteRequest]](
        url=format_("{}/{}", base_url, id_),
        url_parsed_args=convert[TS[tuple[str, ...]]](id_),
    )
    log_("Sending {}", request)
    response = convert[TS[RestDeleteResponse]](http_client_adaptor(request))
    log_("Received {}", response)
    return response


def register_rest_client() -> None:
    """Register the HTTP service implementation used by REST client graphs."""
    register_adaptor("http_client", http_client_adaptor_impl)


__all__ = (
    "register_rest_client",
    "rest_create",
    "rest_delete",
    "rest_list",
    "rest_read",
    "rest_update",
)
