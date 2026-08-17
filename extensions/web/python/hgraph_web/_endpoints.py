"""Authoring decorators, authenticator wiring, and codec helpers (RFC 0024).

The decorators here are the primary Python authoring surface: they own the
serve/respond wiring so a handler graph only ever sees a request and returns a
response.  Nothing is registered in a process-global table — an endpoint wires
itself when it is called inside the graph that wants it, so two graphs in one
process never share route state.

Codec helpers compose the native ``to_json``/``from_json`` operators over the
``bytes`` bodies of the transport values; no JSON is encoded or decoded here.
"""

from __future__ import annotations

import inspect
import weakref
from dataclasses import dataclass

import _hgraph
from hgraph import (
    TS,
    CompoundScalar,
    compute_node,
    filter_,
    from_json,
    graph,
    not_,
    to_json,
)
from hgraph._types import _TsExpr
from hgraph._wiring import _GraphFn, _PyNode
from hgraph._wiring._core import _current_wiring
from hgraph._wiring._operator import _is_hidden_node_parameter

from . import (
    HttpMethod,
    HttpResponse,
    HttpServerRequest,
    WebHeader,
    WebRoute,
    WsEvent,
    WsFrame,
    WsFrameKind,
    WsInboundFrame,
    web_respond,
    web_serve,
    web_ws_send,
    web_ws_serve,
)

# The namespace the rest of the package registers its values under; restated
# here rather than imported so this module reaches into no private of
# ``__init__``, which imports it.
_NAMESPACE = "hgraph.web"

_JSON_CONTENT_TYPE = "application/json"
_TEXT_CONTENT_TYPE = "text/plain; charset=utf-8"


@dataclass(frozen=True)
class AuthResult(CompoundScalar, namespace=_NAMESPACE):
    """The verdict of an authenticator graph for one request.

    ``status`` and ``reason`` are only consulted when ``allowed`` is false;
    they become the status and body of the refusal the caller receives.
    """

    allowed: bool
    status: int = 401
    reason: str = ""

    def __post_init__(self) -> None:
        if self.status < 100 or self.status > 599:
            raise ValueError("Web auth status must be 100..599")

    @classmethod
    def allow(cls) -> "AuthResult":
        return cls(True)

    @classmethod
    def deny(cls, status: int = 401, reason: str = "") -> "AuthResult":
        return cls(False, status=status, reason=reason)


def _ts_expr(handle) -> _TsExpr:
    return _TsExpr(handle, repr(handle))


def _bundle_field_types(annotation) -> dict[str, _TsExpr] | None:
    if not isinstance(annotation, _TsExpr) or not annotation.handle.is_tsb:
        return None
    return {
        name: _ts_expr(field_type)
        for name, field_type in _hgraph.ts_field_types(annotation.handle)
    }


def _as_time_series(tp):
    """Accept either a scalar type or an explicit time-series expression."""

    return tp if isinstance(tp, _TsExpr) else TS[tp]


def _as_wired(fn):
    # Bare functions are treated as graphs, matching the released handler
    # decorators; already-decorated graphs and nodes pass through unchanged.
    return fn if isinstance(fn, (_GraphFn, _PyNode)) else graph(fn)


def _handler_signature(fn):
    return inspect.signature(getattr(fn, "fn", fn), eval_str=True)


def _extra_parameters(signature, wired_names):
    return tuple(
        parameter
        for name, parameter in signature.parameters.items()
        if name not in wired_names and not _is_hidden_node_parameter(parameter)
    )


def _resolve_route(route, *, upgrade: bool):
    """Normalise the decorator's route argument.

    A bare pattern becomes a ``GET`` route; an explicit :class:`WebRoute` must
    already agree with the endpoint kind, because the upgrade flag is part of
    the route's identity and a mismatch would serve a route nobody dispatches
    to.  Wiring ports pass through so a dynamically computed route still works.
    """

    if isinstance(route, str):
        return WebRoute(HttpMethod.GET, route, upgrade=upgrade)
    if isinstance(route, WebRoute):
        if route.upgrade != upgrade:
            kind = "ws_endpoint" if upgrade else "http_endpoint"
            raise ValueError(
                f"{kind} requires a route with upgrade={upgrade}, not {route.upgrade}"
            )
        return route
    return route


# ---------------------------------------------------------------------------
# Small field accessors.  Each one exists because a compound scalar field is
# only reachable from inside a node.  They tick with their source except where
# the field is optional, in which case an absent field simply does not tick.
# ---------------------------------------------------------------------------


@compute_node
def _request_id(request: TS[HttpServerRequest]) -> TS[int]:
    return request.value.request_id


@compute_node
def _auth_allowed(result: TS[AuthResult]) -> TS[bool]:
    return result.value.allowed


@compute_node
def _refusal(result: TS[AuthResult]) -> TS[HttpResponse]:
    verdict = result.value
    return HttpResponse(
        verdict.status,
        headers=(WebHeader("content-type", _TEXT_CONTENT_TYPE),),
        body=verdict.reason.encode("utf-8"),
    )


@compute_node
def _request_body_text(request: TS[HttpServerRequest]) -> TS[str]:
    inbound = request.value.request
    if inbound is None:
        return None
    return inbound.body.decode("utf-8")


@compute_node
def _response_from_text(
    payload: TS[str],
    status: int = 200,
    content_type: str = _JSON_CONTENT_TYPE,
) -> TS[HttpResponse]:
    return HttpResponse(
        status,
        headers=(WebHeader("content-type", content_type),),
        body=payload.value.encode("utf-8"),
    )


# ---------------------------------------------------------------------------
# Codec helpers
# ---------------------------------------------------------------------------


def request_json(request, tp):
    """Decode a served request's body as JSON into ``tp``."""

    return from_json[_as_time_series(tp)](_request_body_text(request))


def json_response(value, status: int = 200):
    """Encode a time series as a JSON response body."""

    return _response_from_text(to_json(value), status=status)


@compute_node
def ws_text(frame: TS[WsInboundFrame]) -> TS[str]:
    """The payload of inbound text frames; other frame kinds do not tick."""

    inbound = frame.value.frame
    if inbound is None or inbound.kind != WsFrameKind.TEXT:
        return None
    return inbound.text or ""


@compute_node
def ws_binary(frame: TS[WsInboundFrame]) -> TS[bytes]:
    """The payload of inbound binary frames; other frame kinds do not tick."""

    inbound = frame.value.frame
    if inbound is None or inbound.kind != WsFrameKind.BINARY:
        return None
    return inbound.data or b""


def ws_json(frame, tp):
    """Decode inbound text frames as JSON into ``tp``."""

    return from_json[_as_time_series(tp)](ws_text(frame))


@compute_node
def text_frame(ts: TS[str]) -> TS[WsFrame]:
    return WsFrame.text_frame(ts.value)


@compute_node
def binary_frame(ts: TS[bytes]) -> TS[WsFrame]:
    return WsFrame.binary_frame(ts.value)


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------


class _Authenticator:
    """A wiring-supplied ``TS[HttpServerRequest] -> TS[AuthResult]`` graph."""

    def __init__(self, fn):
        self._fn = _as_wired(fn)
        signature = _handler_signature(fn)
        request = signature.parameters.get("request")
        if request is None or request.annotation != TS[HttpServerRequest]:
            raise TypeError(
                "An authenticator requires a 'request: TS[HttpServerRequest]' input"
            )
        if signature.return_annotation != TS[AuthResult]:
            raise TypeError("An authenticator must return TS[AuthResult]")

    def gate(self, request, path: str):
        """Answer refused requests directly; return the permitted ones.

        Both arms are gated on the same verdict, so exactly one of them ticks
        per request and the refusal is answered in the arrival cycle.
        """

        verdict = self._fn(request)
        allowed = _auth_allowed(verdict)
        refused = not_(allowed)
        web_respond(
            _request_id(filter_(refused, request)),
            _refusal(filter_(refused, verdict)),
            path=path,
        )
        return filter_(allowed, request)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


class _HttpEndpoint:
    """One HTTP route, its handler, and the serve/respond wiring around it."""

    def __init__(self, fn, route, path: str, auth):
        self._fn = _as_wired(fn)
        self._route = _resolve_route(route, upgrade=False)
        self._path = path
        self._auth = None if auth is None else _Authenticator(auth)
        self.__name__ = getattr(fn, "__name__", "http_endpoint")

        signature = _handler_signature(fn)
        request = signature.parameters.get("request")
        if request is None:
            raise TypeError("An HTTP endpoint requires a 'request' time-series input")
        if request.annotation != TS[HttpServerRequest]:
            raise TypeError(
                "An HTTP endpoint's request must be TS[HttpServerRequest]"
            )

        output = signature.return_annotation
        self._auxiliary_output = output != TS[HttpResponse]
        if self._auxiliary_output:
            fields = _bundle_field_types(output)
            if fields is None or fields.get("response") != TS[HttpResponse]:
                raise TypeError(
                    "An HTTP endpoint must return TS[HttpResponse], or a TSB with a "
                    "'response' field of that type alongside its auxiliary outputs"
                )

        self.__signature__ = signature.replace(
            parameters=_extra_parameters(signature, {"request"})
        )
        self._wired = weakref.WeakKeyDictionary()

    def __call__(self, *args, **kwargs):
        wiring = _current_wiring()
        if not args and not kwargs and wiring in self._wired:
            return self._wired[wiring]
        bound = self.__signature__.bind(*args, **kwargs)
        bound.apply_defaults()

        request = web_serve(self._route, path=self._path)["request"]
        if self._auth is not None:
            request = self._auth.gate(request, self._path)

        handled = self._fn(request=request, **bound.arguments)
        response = handled["response"] if self._auxiliary_output else handled
        web_respond(_request_id(request), response, path=self._path)

        result = handled if self._auxiliary_output else None
        if not args and not kwargs:
            self._wired[wiring] = result
        return result


class _WsEndpoint:
    """One WebSocket route, its handler, and the serve/send wiring around it."""

    _INPUTS = {"event": TS[WsEvent], "frame": TS[WsInboundFrame]}

    def __init__(self, fn, route, path: str):
        self._fn = _as_wired(fn)
        self._route = _resolve_route(route, upgrade=True)
        self._path = path
        self.__name__ = getattr(fn, "__name__", "ws_endpoint")

        signature = _handler_signature(fn)
        self._inputs = tuple(
            name for name in self._INPUTS if name in signature.parameters
        )
        if not self._inputs:
            raise TypeError(
                "A WebSocket endpoint requires an 'event' or 'frame' time-series input"
            )
        for name in self._inputs:
            if signature.parameters[name].annotation != self._INPUTS[name]:
                raise TypeError(
                    f"A WebSocket endpoint's '{name}' must be {self._INPUTS[name]!r}"
                )

        fields = _bundle_field_types(signature.return_annotation)
        if (
            fields is None
            or fields.get("connection_id") != TS[int]
            or fields.get("frame") != TS[WsFrame]
        ):
            raise TypeError(
                "A WebSocket endpoint must return a TSB with 'connection_id: TS[int]' "
                "and 'frame: TS[WsFrame]' fields"
            )
        self._auxiliary_output = set(fields) != {"connection_id", "frame"}

        self.__signature__ = signature.replace(
            parameters=_extra_parameters(signature, set(self._INPUTS))
        )
        self._wired = weakref.WeakKeyDictionary()

    def __call__(self, *args, **kwargs):
        wiring = _current_wiring()
        if not args and not kwargs and wiring in self._wired:
            return self._wired[wiring]
        bound = self.__signature__.bind(*args, **kwargs)
        bound.apply_defaults()

        routed = web_ws_serve(self._route, path=self._path)
        served = {name: routed[name] for name in self._inputs}

        handled = self._fn(**served, **bound.arguments)
        web_ws_send(handled["connection_id"], handled["frame"], path=self._path)

        result = handled if self._auxiliary_output else None
        if not args and not kwargs:
            self._wired[wiring] = result
        return result


def http_endpoint(route, path: str = "", auth=None):
    """Declare an HTTP route answered by a handler graph or node.

    ``route`` is a :class:`WebRoute` or a bare pattern (which becomes a ``GET``
    route).  The handler takes ``request: TS[HttpServerRequest]`` and returns
    either ``TS[HttpResponse]`` or a ``TSB`` whose ``response`` field carries
    it; in the second form the whole bundle is returned to the caller so its
    auxiliary outputs stay wirable.

    ``auth`` is a graph ``TS[HttpServerRequest] -> TS[AuthResult]``.  Refused
    requests are answered from the verdict and never reach the handler.
    """

    def decorate(fn):
        return _HttpEndpoint(fn, route, path, auth)

    return decorate


def ws_endpoint(route, path: str = ""):
    """Declare a WebSocket upgrade route answered by a handler graph or node.

    The handler takes ``event: TS[WsEvent]`` and/or ``frame:
    TS[WsInboundFrame]`` and returns a ``TSB`` carrying ``connection_id`` and
    ``frame``; extra fields make it an auxiliary-output handler and are
    returned to the caller.
    """

    def decorate(fn):
        return _WsEndpoint(fn, route, path)

    return decorate
