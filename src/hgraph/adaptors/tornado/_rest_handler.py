import json
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
    ts_schema,
    TSB,
    from_json_builder,
)

__all__ = (
    "REST_RESPONSE",
    "RestCreateRequest",
    "RestCreateResponse",
    "RestDeleteRequest",
    "RestDeleteResponse",
    "RestListRequest",
    "RestListResponse",
    "RestReadRequest",
    "RestReadResponse",
    "RestRequest",
    "RestResponse",
    "RestResultEnum",
    "RestUpdateRequest",
    "RestUpdateResponse",
    "rest_handler",
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
class RestRequest(CompoundScalar):
    """Marker class for all rest operations"""

    url: str


REST_REQUEST = TypeVar("REST_REQUEST", bound=RestRequest)


@dataclass(frozen=True)
class RestCreateRequest(RestRequest, Generic[COMPOUND_SCALAR]):
    """The value associated to the id should be created"""

    id: str
    value: COMPOUND_SCALAR


@dataclass(frozen=True)
class RestUpdateRequest(RestRequest, Generic[COMPOUND_SCALAR]):
    """The value associated to the id should be updated"""

    id: str
    value: COMPOUND_SCALAR


@dataclass(frozen=True)
class RestReadRequest(RestRequest):
    """The id is requested to have it's value returned"""

    id: str


@dataclass(frozen=True)
class RestDeleteRequest(RestRequest):
    """The id is requested to be removed"""

    id: str


@dataclass(frozen=True)
class RestListRequest(RestRequest):
    """No attributes provided"""


@dataclass(frozen=True)
class RestResponse(CompoundScalar):
    status: RestResultEnum
    reason: str = ""  # Populated when OK (or success equivalent) is NOT set as the status


REST_RESPONSE = TypeVar("REST_RESPONSE", bound=RestResponse)
REST_RESPONSE_ = TypeVar("REST_RESPONSE_", bound=RestResponse)


@dataclass(frozen=True)
class RestCreateResponse(RestResponse, Generic[COMPOUND_SCALAR]):
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
class RestReadResponse(RestResponse, Generic[COMPOUND_SCALAR]):
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
class RestUpdateResponse(RestResponse, Generic[COMPOUND_SCALAR]):
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
class RestDeleteResponse(RestResponse):
    """
    The status property should be set as follows:

    * If successfully deleted, this should set the status to NO_CONTENT.
    * If the content is not found, this should be set to NOT_FOUND.
    * The user is not authorised, this should be set to UNAUTHORIZED
    * If the task cannot be completed, this should be set to FORBIDDEN.
    """


@dataclass(frozen=True)
class RestListResponse(RestResponse):
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
    assert fn.signature.time_series_inputs["request"].matches_type(TS[RestRequest]) or fn.signature.time_series_inputs[
        "request"
    ].matches_type(
        TSD[int, TS[RestRequest]]
    ), f"Graph must have a single input named 'request' of type TS[RestRequest] or TSD[int, TS[RestRequest]]"
    assert not url.endswith("/"), "URL cannot end with a '/'"

    output_type = fn.signature.output_type
    is_tsb = False
    if isinstance(output_type, HgTSBTypeMetaData):
        is_tsb = True
        output_type = output_type["response"]

    assert (single_value := output_type.matches_type(TS[RestResponse])) or output_type.matches_type(
        TSD[int, TS[RestResponse]]
    ), "Graph must have a single output of type TS[RestResponse] or TSD[int, TS[RestResponse]]"

    if is_tsb:
        kwargs = {k: v.py_type for k, v in fn.signature.output_type.bundle_schema_tp.meta_data_schema.items()} | {
            "response": TS[HttpResponse] if single_value else TSD[int, TS[HttpResponse]]
        }
        final_output_type = TSB[ts_schema(**kwargs)]
    else:
        final_output_type = TS[HttpResponse] if single_value else TSD[int, TS[HttpResponse]]

    # If inputs or outputs are not standard, then we use the graph, otherwise we can wire up?
    url = f"{url}/?(.*)"  # Create a URL that can respond to list, post and appropriate /<id> requests
    if single_value:

        @http_server_handler(url=url)
        @with_signature(
            kwargs={k: v for k, v in fn.signature.non_injectable_or_auto_resolvable_inputs.items() if k != "request"},
            return_annotation=TS[HttpResponse],
        )
        def rest_handler_graph(request: TS[HttpRequest], **kwargs) -> TS[HttpResponse]:
            rest_request = convert[TS[RestRequest]](request, value_type=data_type)
            response = fn(request=rest_request, **kwargs)
            rest_response = convert[TS[HttpResponse]](response)
            return rest_response

    else:

        @http_server_handler(url=url)
        @with_signature(
            kwargs={k: v for k, v in fn.signature.non_injectable_or_auto_resolvable_inputs.items() if k != "request"},
            return_annotation=final_output_type,
        )
        def rest_handler_graph(request: TSD[int, TS[HttpRequest]], **kwargs) -> final_output_type:
            rest_requests = map_(lambda request: convert[TS[RestRequest]](request, value_type=data_type), request)
            responses = fn(request=rest_requests, **kwargs)
            if is_tsb:
                rest_reponses = map_(convert[TS[HttpResponse]], responses.response)
                return final_output_type.from_ts(
                    response=rest_reponses, **{k: v for k, v in responses.as_dict().items() if k != "response"}
                )
            else:
                return map_(convert[TS[HttpResponse]], responses)

    return rest_handler_graph


@operator
def _convert_to_rest_request(ts: TS[HttpRequest], cs_tp: type[COMPOUND_SCALAR] = None) -> TS[RestRequest]:
    return nothing[TS[RestRequest]]()


@compute_node(overloads=_convert_to_rest_request)
def _(ts: TS[HttpGetRequest], cs_tp: type[COMPOUND_SCALAR] = None) -> TS[RestRequest]:
    value = ts.value
    if value.url_parsed_args and value.url_parsed_args[0]:
        return RestReadRequest(url=value.url, id=value.url_parsed_args[0])
    else:
        return RestListRequest(url=value.url)


@dataclass(frozen=True)
class RestIdValueReqResp(CompoundScalar, Generic[COMPOUND_SCALAR]):
    id: str
    value: COMPOUND_SCALAR


@graph(overloads=_convert_to_rest_request)
def _(ts: TS[HttpPostRequest], cs_tp: type[COMPOUND_SCALAR] = None) -> TS[RestRequest]:
    # A POST should imply create new
    request = from_json[TS[RestIdValueReqResp[cs_tp]]](ts.body)
    url = ts.url
    id_ = request.id
    value_ = request.value
    return convert[TS[RestRequest]](combine[TS[RestCreateRequest[cs_tp]]](url=url, id=id_, value=value_))


@graph(overloads=_convert_to_rest_request)
def _(ts: TS[HttpPutRequest], cs_tp: type[COMPOUND_SCALAR] = None) -> TS[RestRequest]:
    return convert[TS[RestRequest]](
        combine[TS[RestUpdateRequest[cs_tp]]](
            url=ts.url, id=ts.url_parsed_args[0], value=from_json[TS[cs_tp]](ts.body)
        ),
    )


@graph(overloads=_convert_to_rest_request)
def _(ts: TS[HttpDeleteRequest], cs_tp: type[COMPOUND_SCALAR] = None) -> TS[RestRequest]:
    return convert[TS[RestRequest]](combine[TS[RestDeleteRequest]](url=ts.url, id=ts.url_parsed_args[0]))


@graph(overloads=convert)
def convert_to_rest_request(
    ts: TS[HttpRequest],
    to: type[OUT] = OUT,
    value_type: type[COMPOUND_SCALAR] = None,
) -> TS[RestRequest]:
    return dispatch_(_convert_to_rest_request, ts=ts, cs_tp=value_type)


def _process_response_error(value: HttpResponse) -> tuple[RestResultEnum, str | None]:
    status = RestResultEnum(value.status_code)
    if status not in (RestResultEnum.OK, RestResultEnum.CREATED):
        reason = json.loads(value.body).get("reason", "No Reason Provided")
        return status, reason
    else:
        return status, None


@compute_node(overloads=convert)
def convert_to_rest_list_response(
    ts: TS[HttpResponse],
    to: type[TS[RestListResponse]] = OUT,
) -> TS[RestListResponse]:
    value: HttpResponse = ts.value
    status, reason = _process_response_error(value)
    if reason:
        return RestListResponse(status=status, reason=reason)
    else:
        ids = tuple(ids_ := json.loads(value.body))
        if not isinstance(ids_, (tuple, list)):
            return RestListResponse(status=RestResultEnum.BAD_REQUEST, reason="Invalid response body")
        return RestListResponse(status=status, ids=ids)


def _extract_id_value_rest_response(
    tp: type[REST_RESPONSE], cs_tp: type[COMPOUND_SCALAR], value: HttpResponse
) -> RestResponse:
    status, reason = _process_response_error(value)
    if reason:
        return tp(status=status, reason=reason)
    else:
        value_ = json.loads(value.body)
        v = from_json_builder(RestIdValueReqResp[cs_tp])(value_)
        return tp(status=status, id=v.id, value=v.value)


@compute_node(overloads=convert)
def convert_to_rest_read_response(
    ts: TS[HttpResponse],
    to: type[TS[RestReadResponse[COMPOUND_SCALAR]]] = OUT,
    _cs_tp: type[COMPOUND_SCALAR] = AUTO_RESOLVE,
) -> TS[RestReadResponse[COMPOUND_SCALAR]]:
    value: HttpResponse = ts.value
    return _extract_id_value_rest_response(RestReadResponse[_cs_tp], _cs_tp, value)


@compute_node(overloads=convert)
def convert_to_rest_create_response(
    ts: TS[HttpResponse],
    to: type[TS[RestCreateResponse[COMPOUND_SCALAR]]] = OUT,
    _cs_tp: type[COMPOUND_SCALAR] = AUTO_RESOLVE,
) -> TS[RestCreateResponse[COMPOUND_SCALAR]]:
    value: HttpResponse = ts.value
    return _extract_id_value_rest_response(RestCreateResponse[_cs_tp], _cs_tp, value)


@compute_node(overloads=convert)
def convert_to_rest_update_response(
    ts: TS[HttpResponse],
    to: type[TS[RestUpdateResponse[COMPOUND_SCALAR]]] = OUT,
    _cs_tp: type[COMPOUND_SCALAR] = AUTO_RESOLVE,
) -> TS[RestUpdateResponse[COMPOUND_SCALAR]]:
    value: HttpResponse = ts.value
    return _extract_id_value_rest_response(RestUpdateResponse[_cs_tp], _cs_tp, value)


@compute_node(overloads=convert)
def convert_to_rest_delete_response(
    ts: TS[HttpResponse],
    to: type[TS[RestDeleteResponse]] = OUT,
) -> TS[RestDeleteResponse]:
    value: HttpResponse = ts.value
    status, reason = _process_response_error(value)
    if reason:
        return RestDeleteResponse(status=status, reason=reason)
    else:
        return RestDeleteResponse(status=status)


@compute_node(overloads=convert)
def convert_from_rest_response(
    ts: TS[REST_RESPONSE],
    to: type[OUT] = OUT,
) -> TS[HttpResponse]:
    value: RestResponse = ts.value

    if value.status not in (RestResultEnum.OK, RestResultEnum.CREATED):
        body = f'{{ "reason": "{value.reason}" }}'
    elif isinstance(value, RestListResponse):
        values = (f'"{v}"' for v in value.ids)
        body = f'[ {", ".join(values)} ]'
    elif isinstance(value, (RestCreateResponse, RestUpdateResponse, RestReadResponse)):
        v = value.value
        body = f'{{ "id": "{value.id}", "value": {to_json_builder(type(v))(v)} }}'
    else:
        body = ""

    return HttpResponse(
        status_code=value.status.value,
        headers=frozendict({"Content-Type": "application/json"}),
        body=body,
    )
