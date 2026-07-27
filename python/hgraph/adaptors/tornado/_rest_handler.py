"""REST request/response values and HTTP handler conversion."""

import inspect
import json
from dataclasses import dataclass
from enum import Enum
from typing import Generic, TypeVar

from frozendict import frozendict

from hgraph import (
    AUTO_RESOLVE,
    OUT,
    CompoundScalar,
    TS,
    TSB,
    TSD,
    combine,
    compute_node,
    convert,
    dispatch_,
    from_json,
    from_json_builder,
    graph,
    map_,
    nothing,
    operator,
    ts_schema,
    to_json_builder,
    with_signature,
)

from .http_server_adaptor import (
    HttpDeleteRequest,
    HttpGetRequest,
    HttpPostRequest,
    HttpPutRequest,
    HttpRequest,
    HttpResponse,
    _bundle_field_types,
    _handler_parameters,
    http_server_handler,
)


class RestResultEnum(Enum):
    OK = 200
    CREATED = 201
    NO_CONTENT = 204
    BAD_REQUEST = 400
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    NOT_FOUND = 404
    REQUEST_TIMED_OUT = 408
    CONFLICT = 409
    TOO_MANY_REQUESTS = 429
    INTERNAL_SERVER_ERROR = 500
    NOT_IMPLEMENTED = 501
    BAD_GATEWAY = 502
    SERVICE_UNAVAILABLE = 503
    GATEWAY_TIMEOUT = 504
    HTTP_VERSION_NOT_SUPPORTED = 505
    TORNADO_TIMED_OUT = 599


REST_DATA = TypeVar("REST_DATA", bound=CompoundScalar)


@dataclass(frozen=True)
class RestRequest(CompoundScalar):
    url: str


@dataclass(frozen=True)
class RestCreateRequest(RestRequest, Generic[REST_DATA]):
    id: str
    value: REST_DATA


@dataclass(frozen=True)
class RestUpdateRequest(RestRequest, Generic[REST_DATA]):
    id: str
    value: REST_DATA


@dataclass(frozen=True)
class RestReadRequest(RestRequest):
    id: str


@dataclass(frozen=True)
class RestDeleteRequest(RestRequest):
    id: str


@dataclass(frozen=True)
class RestListRequest(RestRequest):
    pass


@dataclass(frozen=True)
class RestResponse(CompoundScalar):
    status: RestResultEnum
    reason: str = ""


@dataclass(frozen=True)
class RestCreateResponse(RestResponse, Generic[REST_DATA]):
    id: str = None
    value: REST_DATA = None


@dataclass(frozen=True)
class RestReadResponse(RestResponse, Generic[REST_DATA]):
    id: str = None
    value: REST_DATA = None


@dataclass(frozen=True)
class RestUpdateResponse(RestResponse, Generic[REST_DATA]):
    id: str = None
    value: REST_DATA = None


@dataclass(frozen=True)
class RestDeleteResponse(RestResponse):
    pass


@dataclass(frozen=True)
class RestListResponse(RestResponse):
    ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class RestIdValueReqResp(CompoundScalar, Generic[REST_DATA]):
    id: str
    value: REST_DATA


@graph
def _convert_rest_response_to_http(
    response: TS[RestResponse],
) -> TS[HttpResponse]:
    return convert[TS[HttpResponse]](response)


def rest_handler(fn=None, *, url: str, data_type: type[CompoundScalar]):
    """Expose a graph REST handler through the HTTP server adaptor."""
    if fn is None:
        return lambda decorated: rest_handler(
            decorated,
            url=url,
            data_type=data_type,
        )
    if url.endswith("/"):
        raise ValueError("REST handler URL cannot end with '/'")

    target = getattr(fn, "fn", fn)
    if target is fn:
        fn = graph(fn)
        target = fn.fn
    signature = inspect.signature(target, eval_str=True)
    request = signature.parameters.get("request")
    if request is None:
        raise TypeError("REST handler requires a 'request' time-series input")

    single_request = TS[RestRequest]
    batch_request = TSD[int, TS[RestRequest]]
    if request.annotation == single_request:
        single = True
        expected_output = TS[RestResponse]
    elif request.annotation == batch_request:
        single = False
        expected_output = TSD[int, TS[RestResponse]]
    else:
        raise TypeError(
            "REST handler request must be TS[RestRequest] or "
            "TSD[int, TS[RestRequest]]"
        )
    output = signature.return_annotation
    output_fields = _bundle_field_types(output)
    auxiliary_output = (
        output_fields is not None
        and output_fields.get("response") == expected_output
    )
    if output != expected_output and not auxiliary_output:
        raise TypeError(
            f"REST handler output must be {expected_output!r} or a TSB with a "
            f"'response' field of that type"
        )

    if auxiliary_output:
        http_output_fields = dict(output_fields)
        http_output_fields["response"] = (
            TS[HttpResponse] if single else TSD[int, TS[HttpResponse]]
        )
        final_output_type = TSB[ts_schema(**http_output_fields)]
    else:
        final_output_type = TS[HttpResponse] if single else TSD[int, TS[HttpResponse]]

    def with_http_response(responses, http_response):
        if not auxiliary_output:
            return http_response
        return final_output_type.from_ts(**{
            name: http_response if name == "response" else getattr(responses, name)
            for name in output_fields
        })

    # Make every concrete generic leaf visible before the graph's closed
    # RestRequest/RestResponse unions are frozen at wiring completion.
    for leaf in (
        RestCreateRequest[data_type],
        RestUpdateRequest[data_type],
        RestCreateResponse[data_type],
        RestReadResponse[data_type],
        RestUpdateResponse[data_type],
    ):
        TS[leaf]

    visible_parameters = _handler_parameters(signature)
    parameters = {
        parameter.name: parameter.annotation
        for parameter in visible_parameters
    }
    defaults = {
        parameter.name: parameter.default
        for parameter in visible_parameters
        if parameter.default is not inspect.Parameter.empty
    }
    route = f"{url}/?(.*)"

    if single:
        def rest_handler_graph(request: TS[HttpRequest], **kwargs):
            rest_request = convert[TS[RestRequest]](
                request,
                value_type=data_type,
            )
            responses = fn(request=rest_request, **kwargs)
            response = responses.response if auxiliary_output else responses
            return with_http_response(
                responses,
                convert[TS[HttpResponse]](response),
            )

        with_signature(
            rest_handler_graph,
            kwargs=parameters,
            defaults=defaults,
            return_annotation=final_output_type,
        )
    else:
        def rest_handler_graph(
            request: TSD[int, TS[HttpRequest]],
            **kwargs,
        ):
            rest_requests = map_(
                lambda value: convert[TS[RestRequest]](
                    value,
                    value_type=data_type,
                ),
                request,
            )
            responses = fn(request=rest_requests, **kwargs)
            response = responses.response if auxiliary_output else responses
            return with_http_response(
                responses,
                map_(_convert_rest_response_to_http, response),
            )

        with_signature(
            rest_handler_graph,
            kwargs=parameters,
            defaults=defaults,
            return_annotation=final_output_type,
        )

    return http_server_handler(graph(rest_handler_graph), url=route)


@operator
def _convert_to_rest_request(
    ts: TS[HttpRequest],
    cs_tp: type[REST_DATA] = None,
) -> TS[RestRequest]:
    return nothing[TS[RestRequest]]()


@compute_node(overloads=_convert_to_rest_request)
def convert_get_to_rest_request(
    ts: TS[HttpGetRequest],
    cs_tp: type[REST_DATA] = None,
) -> TS[RestRequest]:
    value = ts.value
    if value.url_parsed_args and value.url_parsed_args[0]:
        return RestReadRequest(url=value.url, id=value.url_parsed_args[0])
    return RestListRequest(url=value.url)


@graph(overloads=_convert_to_rest_request)
def convert_post_to_rest_request(
    ts: TS[HttpPostRequest],
    cs_tp: type[REST_DATA] = None,
) -> TS[RestRequest]:
    request = from_json[TS[RestIdValueReqResp[cs_tp]]](ts.body)
    return convert[TS[RestRequest]](
        combine[TS[RestCreateRequest[cs_tp]]](
            url=ts.url,
            id=request.id,
            value=request.value,
        )
    )


@graph(overloads=_convert_to_rest_request)
def convert_put_to_rest_request(
    ts: TS[HttpPutRequest],
    cs_tp: type[REST_DATA] = None,
) -> TS[RestRequest]:
    return convert[TS[RestRequest]](
        combine[TS[RestUpdateRequest[cs_tp]]](
            url=ts.url,
            id=ts.url_parsed_args[0],
            value=from_json[TS[cs_tp]](ts.body),
        )
    )


@graph(overloads=_convert_to_rest_request)
def convert_delete_to_rest_request(
    ts: TS[HttpDeleteRequest],
    cs_tp: type[REST_DATA] = None,
) -> TS[RestRequest]:
    return convert[TS[RestRequest]](
        combine[TS[RestDeleteRequest]](
            url=ts.url,
            id=ts.url_parsed_args[0],
        )
    )


@graph(overloads=convert)
def convert_to_rest_request(
    ts: TS[HttpRequest],
    to: type[OUT] = OUT,
    value_type: type[REST_DATA] = None,
) -> TS[RestRequest]:
    return dispatch_(_convert_to_rest_request, ts=ts, cs_tp=value_type)


def _process_response_error(value: HttpResponse):
    status = RestResultEnum(value.status_code)
    if status not in (RestResultEnum.OK, RestResultEnum.CREATED):
        reason = json.loads(value.body).get("reason", "No Reason Provided")
        return status, reason
    return status, None


@compute_node(overloads=convert)
def convert_to_rest_list_response(
    ts: TS[HttpResponse],
    to: type[TS[RestListResponse]] = OUT,
) -> TS[RestListResponse]:
    status, reason = _process_response_error(ts.value)
    if reason:
        return RestListResponse(status=status, reason=reason)
    ids = json.loads(ts.value.body)
    if not isinstance(ids, (tuple, list)):
        return RestListResponse(
            status=RestResultEnum.BAD_REQUEST,
            reason="Invalid response body",
        )
    return RestListResponse(status=status, ids=tuple(ids))


def _extract_id_value_rest_response(tp, data_type, value):
    status, reason = _process_response_error(value)
    if reason:
        return tp(status=status, reason=reason)
    payload = from_json_builder(RestIdValueReqResp[data_type])(
        json.loads(value.body)
    )
    return tp(status=status, id=payload.id, value=payload.value)


@compute_node(overloads=convert)
def convert_to_rest_read_response(
    ts: TS[HttpResponse],
    to: type[TS[RestReadResponse[REST_DATA]]] = OUT,
    _cs_tp: type[REST_DATA] = AUTO_RESOLVE,
) -> TS[RestReadResponse[REST_DATA]]:
    return _extract_id_value_rest_response(
        RestReadResponse[_cs_tp],
        _cs_tp,
        ts.value,
    )


@compute_node(overloads=convert)
def convert_to_rest_create_response(
    ts: TS[HttpResponse],
    to: type[TS[RestCreateResponse[REST_DATA]]] = OUT,
    _cs_tp: type[REST_DATA] = AUTO_RESOLVE,
) -> TS[RestCreateResponse[REST_DATA]]:
    return _extract_id_value_rest_response(
        RestCreateResponse[_cs_tp],
        _cs_tp,
        ts.value,
    )


@compute_node(overloads=convert)
def convert_to_rest_update_response(
    ts: TS[HttpResponse],
    to: type[TS[RestUpdateResponse[REST_DATA]]] = OUT,
    _cs_tp: type[REST_DATA] = AUTO_RESOLVE,
) -> TS[RestUpdateResponse[REST_DATA]]:
    return _extract_id_value_rest_response(
        RestUpdateResponse[_cs_tp],
        _cs_tp,
        ts.value,
    )


@compute_node(overloads=convert)
def convert_to_rest_delete_response(
    ts: TS[HttpResponse],
    to: type[TS[RestDeleteResponse]] = OUT,
) -> TS[RestDeleteResponse]:
    status, reason = _process_response_error(ts.value)
    return RestDeleteResponse(status=status, reason=reason or "")


REST_RESPONSE = TypeVar("REST_RESPONSE", bound=RestResponse)


@compute_node(overloads=convert)
def convert_from_rest_response(
    ts: TS[REST_RESPONSE],
    to: type[TS[HttpResponse]] = OUT,
) -> TS[HttpResponse]:
    value = ts.value
    if value.status not in (RestResultEnum.OK, RestResultEnum.CREATED):
        body = f'{{ "reason": "{value.reason}" }}'.encode()
    elif isinstance(value, RestListResponse):
        values = (f'"{item}"' for item in value.ids)
        body = f'[ {", ".join(values)} ]'.encode()
    elif isinstance(value, (RestCreateResponse, RestUpdateResponse, RestReadResponse)):
        body = (
            f'{{ "id": "{value.id}", "value": '
            f'{to_json_builder(type(value.value))(value.value)} }}'
        ).encode()
    else:
        body = b""
    return HttpResponse(
        status_code=value.status.value,
        headers=frozendict({"Content-Type": "application/json"}),
        body=body,
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
