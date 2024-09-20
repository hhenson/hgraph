from dataclasses import dataclass
from enum import Enum
from typing import Callable, Generic, TypeVar

from frozendict import frozendict

from hgraph import (
    CompoundScalar,
    COMPOUND_SCALAR,
    graph,
    TS,
    TSD,
    HgTSBTypeMetaData,
    combine,
    map_,
    convert,
    OUT,
    compute_node,
    dispatch_,
    AUTO_RESOLVE,
    from_json,
    nothing,
    operator,
    DEFAULT,
    Base,
    to_json_builder,
    with_signature,
)

__all__ = (
    "RestResultEnum",
    "RestResponse",
    "RestCreateResponse",
    "RestUpdateResponse",
    "RestDeleteResponse",
    "RestRequest",
    "RestCreateRequest",
    "RestUpdateRequest",
    "RestListRequest",
    "RestReadRequest",
    "RestDeleteRequest",
    "rest_handler",
    "RestResultEnum",
)

from hgraph.adaptors.tornado.http_server_adaptor import (
    HttpRequest,
    HttpResponse,
    HttpGetRequest,
    HttpPostRequest,
    HttpPutRequest,
    HttpDeleteRequest,
    http_server_handler,
)


class RestResultEnum(Enum):
    OK = 200
    CREATED = 201  # From a POST
    NO_CONTENT = 204  # Success but there is no body-content
    BAD_REQUEST = 400  # Response with validation error
    UNAUTHORIZED = 401
    FORBIDDEN = 403  # Even if authorised, the user cannot perform the action
    NOT_FOUND = 404
    CONFLICT = 409  # For example, trying to create an object that already exists
    INTERNAL_SERVER_ERROR = 500  # A problem (for example, an exception has occurred)


@dataclass(frozen=True)
class RestRequest(CompoundScalar, Generic[COMPOUND_SCALAR]):
    """Marker class for all rest operations"""

    url: str


REST_REQUEST = TypeVar("REST_REQUEST", bound=RestRequest)


@dataclass(frozen=True)
class RestCreateRequest(Base[REST_REQUEST], Generic[REST_REQUEST, COMPOUND_SCALAR]):
    """The value associated to the id should be created"""

    id: str
    value: COMPOUND_SCALAR


@dataclass(frozen=True)
class RestUpdateRequest(Base[REST_REQUEST], Generic[REST_REQUEST, COMPOUND_SCALAR]):
    """The value associated to the id should be updated"""

    id: str
    value: COMPOUND_SCALAR


@dataclass(frozen=True)
class RestReadRequest(Base[REST_REQUEST], Generic[REST_REQUEST]):
    """The id is requested to have it's value returned"""

    id: str


@dataclass(frozen=True)
class RestDeleteRequest(Base[REST_REQUEST], Generic[REST_REQUEST]):
    """The id is requested to be removed"""

    id: str


@dataclass(frozen=True)
class RestListRequest(Base[REST_REQUEST], Generic[REST_REQUEST]):
    """No attributes provided"""


@dataclass(frozen=True)
class RestResponse(CompoundScalar, Generic[COMPOUND_SCALAR]):
    status: RestResultEnum
    reason: str = ""  # Populated when OK (or success equivalent) is NOT set as the status


REST_RESPONSE = TypeVar("REST_RESPONSE", bound=RestResponse)
REST_RESPONSE_ = TypeVar("REST_RESPONSE_", bound=RestResponse)


@dataclass(frozen=True)
class RestCreateResponse(Base[REST_RESPONSE], Generic[REST_RESPONSE, COMPOUND_SCALAR]):
    """
    The status property should be set as follows:

    * If successfully created, this should set the status to CREATED.
    * If the value already exists, this should be set to CONFLICT.
    * If the attempt fails due to a validation error, this should be set to BAD_REQUEST.
    * The user is not authorised, this should be set to UNAUTHORIZED
    * If the task cannot be completed, this should be set to FORBIDDEN.

    The value is returned when the value is modified during the construction process.
    The id will always be provided if the value is created.
    """

    id: str = None
    value: COMPOUND_SCALAR = None


@dataclass(frozen=True)
class RestReadResponse(Base[REST_RESPONSE], Generic[REST_RESPONSE, COMPOUND_SCALAR]):
    """
    The status property should be set as follows:

    * If found, this should be set to OK.
    * If the content is not found, this should be set to NOT_FOUND.
    * The user is not authorised, this should be set to UNAUTHORIZED
    * If the task cannot be completed, this should be set to FORBIDDEN.

    The id and value are returned if found.
    """

    id: str = None
    value: COMPOUND_SCALAR = None


@dataclass(frozen=True)
class RestUpdateResponse(Base[REST_RESPONSE], Generic[REST_RESPONSE, COMPOUND_SCALAR]):
    """
    The status property should be set as follows:

    * If successfully updated, this should set the status to OK if the value is going to be returned or NO_CONTENT if
      no value is to be returned.
    * If the content is not found, this should be set to NOT_FOUND.
    * If the attempt fails due to a validation error, this should be set to BAD_REQUEST.
    * The user is not authorised, this should be set to UNAUTHORIZED
    * If the task cannot be completed, this should be set to FORBIDDEN.

    The id and value should be set when the value is modified during the update process.
    """

    id: str = None
    value: COMPOUND_SCALAR = None


@dataclass(frozen=True)
class RestDeleteResponse(Base[REST_RESPONSE], Generic[REST_RESPONSE]):
    """
    The status property should be set as follows:

    * If successfully deleted, this should set the status to NO_CONTENT.
    * If the content is not found, this should be set to NOT_FOUND.
    * The user is not authorised, this should be set to UNAUTHORIZED
    * If the task cannot be completed, this should be set to FORBIDDEN.
    """


@dataclass(frozen=True)
class RestListResponse(Base[REST_RESPONSE], Generic[REST_RESPONSE]):
    """
    Returns the list of id's available.

    The status property should be set as follows:

    * If the operation is successfully performed, this should set the status to OK.
    * The user is not authorised, this should be set to UNAUTHORIZED
    * If the task cannot be completed, this should be set to FORBIDDEN.
    """

    ids: tuple[str, ...] = tuple()


def rest_handler(fn: Callable = None, *, url: str, data_type: type[COMPOUND_SCALAR]):
    """
    A rest handler wraps a function of the form:

    @rest_handler(url="http://example.com/my_cs")
    def my_fn(request: TS[RestRequest[MyCS], ...) -> TS[RestResponse[MyCS]]:
        ...
    """
    if fn is None:
        return lambda fn: rest_handler(fn, url=url, data_type=data_type)

    from hgraph import WiringNodeClass

    if not isinstance(fn, WiringNodeClass):
        fn = graph(fn)

    assert "request" in fn.signature.time_series_inputs.keys(), "Rest handler graph must have an input named 'request'"
    assert fn.signature.time_series_inputs["request"].matches_type(
        TS[RestRequest[data_type]]
    ) or fn.signature.time_series_inputs["request"].matches_type(TSD[int, TS[RestRequest[data_type]]]), (
        f"Graph must have a single input named 'request' of type TS[RestRequest[{data_type}]] or TSD[int,"
        f" TS[RestRequest[{data_type}]]]"
    )
    assert not url.endswith("/"), "URL cannot end with a '/'"

    output_type = fn.signature.output_type
    if isinstance(output_type, HgTSBTypeMetaData):
        output_type = output_type["response"]

    assert (single_value := output_type.matches_type(TS[RestResponse[data_type]])) or output_type.matches_type(
        TSD[int, TS[RestResponse[data_type]]]
    ), "Graph must have a single output of type TS[RestResponse] or TSD[int, TS[RestResponse]]"

    # If inputs or outputs are not standard, then we use the graph, otherwise we can wire up?
    url = f"{url}/?(.*)"  # Create a URL that can respond to list, post and appropriate /<id> requests
    if single_value:

        @http_server_handler(url=url)
        @with_signature(
            kwargs={k: v for k, v in fn.signature.non_injectable_or_auto_resolvable_inputs.items() if k != "request"},
            return_annotation=TS[HttpResponse],
        )
        def rest_handler_graph(request: TS[HttpRequest], **kwargs) -> TS[HttpResponse]:
            rest_request = convert[TS[RestRequest[data_type]]](request)
            response = fn(request=rest_request, **kwargs)
            rest_response = convert[TS[HttpResponse]](response)
            return rest_response

    else:

        @http_server_handler(url=url)
        @with_signature(
            kwargs={k: v for k, v in fn.signature.non_injectable_or_auto_resolvable_inputs.items() if k != "request"},
            return_annotation=TSD[int, TS[HttpResponse]],
        )
        def rest_handler_graph(request: TSD[int, TS[HttpRequest]], **kwargs) -> TSD[int, TS[HttpResponse]]:
            rest_requests = map_(convert[TS[RestRequest[data_type]]], request)
            responses = fn(request=rest_requests, **kwargs)
            rest_responses = map_(convert[TS[HttpResponse]], responses)
            return rest_responses

    return rest_handler_graph


@operator
def _convert_to_rest_request(
    ts: TS[HttpRequest],
    cs_tp: type[COMPOUND_SCALAR],
) -> DEFAULT[OUT]:
    return nothing[TS[RestRequest[cs_tp]]]()


def _resolve_rest_request_out(m, s):
    return TS[RestRequest[m[COMPOUND_SCALAR].py_type]]


@compute_node(overloads=_convert_to_rest_request, resolvers={OUT: _resolve_rest_request_out})
def _(ts: TS[HttpGetRequest], cs_tp: type[COMPOUND_SCALAR]) -> OUT:
    value = ts.value
    if value.url_parsed_args:
        return RestReadRequest[RestRequest[cs_tp]](url=value.url, id=value.url_parsed_args[0])
    else:
        return RestListRequest[RestRequest[cs_tp]](url=value.url)


@dataclass(frozen=True)
class RestIdValueReqResp(CompoundScalar, Generic[COMPOUND_SCALAR]):
    id: str
    value: COMPOUND_SCALAR


@graph(overloads=_convert_to_rest_request, resolvers={OUT: _resolve_rest_request_out})
def _(ts: TS[HttpPostRequest], cs_tp: type[COMPOUND_SCALAR]) -> OUT:
    # A POST should imply create new
    request = from_json[TS[RestIdValueReqResp[cs_tp]]](ts.body)
    url = ts.url
    id_ = request.id
    value_ = request.value
    return convert[TS[RestRequest[cs_tp]]](
        combine[TS[RestCreateRequest[RestRequest[cs_tp], cs_tp]]](url=url, id=id_, value=value_)
    )


@graph(overloads=_convert_to_rest_request, resolvers={OUT: _resolve_rest_request_out})
def _(ts: TS[HttpPutRequest], cs_tp: type[COMPOUND_SCALAR]) -> OUT:
    return convert[TS[RestRequest[cs_tp]]](
        combine[TS[RestUpdateRequest[RestRequest[cs_tp], cs_tp]]](
            url=ts.url, id=ts.url_parsed_args[0], value=from_json[TS[cs_tp]](ts.body)
        ),
    )


@graph(overloads=_convert_to_rest_request, resolvers={OUT: _resolve_rest_request_out})
def _(ts: TS[HttpDeleteRequest], cs_tp: type[COMPOUND_SCALAR], _tp: type[OUT] = AUTO_RESOLVE) -> OUT:
    return convert[TS[RestRequest[cs_tp]]](
        combine[TS[RestDeleteRequest[RestRequest[cs_tp]]]](url=ts.url, id=ts.url_parsed_args[0])
    )


def _extract_cs(m, s):
    return m[OUT].value_scalar_tp.py_type.__args__[0]


@graph(overloads=convert, resolvers={COMPOUND_SCALAR: _extract_cs})
def convert_to_rest_request(
    ts: TS[HttpRequest],
    to: type[OUT] = OUT,
    cs_tp: type[COMPOUND_SCALAR] = AUTO_RESOLVE,
) -> DEFAULT[OUT]:
    return dispatch_(_convert_to_rest_request[TS[RestRequest[cs_tp]]], ts=ts, cs_tp=cs_tp)


def _resolve_cs_from_response(m, s):
    return m[REST_RESPONSE].py_type.__args__[0]


@compute_node(overloads=convert, resolvers={COMPOUND_SCALAR: _resolve_cs_from_response})
def convert_from_rest_response(
    ts: TS[REST_RESPONSE],
    to: type[OUT] = OUT,
    _cs_tp: type[COMPOUND_SCALAR] = AUTO_RESOLVE,
) -> TS[HttpResponse]:
    value: RestResponse = ts.value

    if value.status not in (RestResultEnum.OK, RestResultEnum.CREATED):
        body = f'{{ "reason": "{value.reason}" }}'
    elif isinstance(value, RestListResponse):
        values = (f'"{v}"' for v in value.ids)
        body = f'[ {", ".join(values)} ]'
    elif isinstance(value, RestReadResponse):
        body = f'{{ "id": "{value.id}", "value": {to_json_builder(_cs_tp)(value.value)} }}'
    elif isinstance(value, (RestCreateResponse, RestUpdateResponse, RestReadResponse)):
        body = f'{{ "id": "{value.id}", "value": {to_json_builder(_cs_tp)(value.value)} }}'
    else:
        body = ""

    return HttpResponse(
        status_code=value.status.value,
        headers=frozendict({"Content-Type": "application/json"}),
        body=body,
    )
