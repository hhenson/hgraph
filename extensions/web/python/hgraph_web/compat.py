"""Released ``hgraph.adaptors.tornado`` server surface over the native transport.

The decorator, signature, and value contracts of the released tornado HTTP and
WebSocket *server* adaptors are preserved here and lowered onto the native web
service (RFC 0024, "Compatibility and migration").  Tornado is not imported and
is not required.  One native server is registered per port at the service path
``web-compat-{port}``; each handler's tornado URL pattern becomes a
:class:`~hgraph_web.WebRoute` pattern, and the legacy request/response dicts are
built in-graph from :class:`~hgraph_web.HttpServerRequest` /
:class:`~hgraph_web.WsEvent`.

Parity notes
------------

Reproduced defects (the released behaviour *is* the contract here, and only
here — the primary API keeps ordered, duplicate-preserving sequences):

* **Headers collapse to one dict entry per name.**  Duplicate request headers
  are joined with ``","`` and the name is rewritten to tornado's
  ``Http-Header-Case``, exactly as ``tornado.httputil.HTTPHeaders`` does.  The
  duplicates cannot be recovered from the compat request.
* **Multi-valued query parameters are concatenated with no separator**
  (``?a=1&a=2`` arrives as ``{"a": "12"}``), reproducing the released
  ``"".join(...)``.  Names and values are percent- and plus-decoded first, as
  tornado's ``parse_qs_bytes`` does.
* **Bodies are ``str``**, strict UTF-8 decoded, and only exist on the POST and
  PUT leaves.
* **``url`` is the registered route pattern, not the requested path.**  The
  captures arrive separately in ``url_parsed_args``.
* **Request keys are never released.**  The released adaptor never removes a
  request id from its keyed time-series dictionary, so a handler graph instance
  (and its key) survives for the life of the graph.  Preserved because the
  released per-request handler-state semantics depend on it.
* **``auth`` is ``("Anonymous", "Anonymous")``**, the value the released
  ``BaseHandler.prepare`` installs when no authentication callback is set.

Deliberate deviations:

* **Routing is specificity-ordered, not registration-ordered.**  Tornado tries
  patterns in registration order; the native route table always prefers the
  more specific pattern (literal before capture before rest).  Where the
  released precedence agreed with specificity the behaviour is identical.
* **Only whole-segment captures translate.**  ``(.*)``/``(.+)`` in the final
  position becomes a rest capture (``/*arg0``), any other group becomes a
  single-segment capture (``/{argN}``); a group that does not occupy a whole
  segment, a named group (``(?P<x>...)``), a non-capturing group, or a regex
  metacharacter in the literal text is rejected at wiring time rather than
  silently mis-translated.  A ``.`` in literal text is treated as a literal
  dot.
* **A trailing ``(.*)`` does not match the empty remainder.**  Tornado matches
  ``/echo/`` with an empty capture; the native rest segment requires at least
  one segment, so ``/echo/`` is a 404.
* **A mid-pattern ``(.*)`` captures one segment**, not several: only the final
  group can span segments.
* **An optional separator before a trailing capture serves two routes.**  The
  released REST route is spelled ``"{url}/?(.*)"``; one native pattern cannot
  express it, so compat serves ``{url}/*arg0`` and ``{url}``, and pads
  ``url_parsed_args`` to the tornado group count so the bare route still
  reports the empty capture the released handler saw.  ``{url}/`` is not
  served, and no other optional character is accepted.
* **``HEAD``, ``PATCH``, ``OPTIONS`` and ``TRACE`` are answered 405** by the
  compat layer itself (matching tornado's unimplemented-method response,
  including its HTML body) without invoking the handler.  ``GET``, ``POST``,
  ``PUT`` and ``DELETE`` reach the handler — the released request union has a
  leaf for exactly those four methods.
* **Responses default to ``Content-Type: text/html; charset=UTF-8``** when the
  handler sets no content type, which is tornado's ``RequestHandler`` default.
  Tornado's ``Server``/``Date`` headers are the native transport's business.
* **A rejected WebSocket closes with code 1000.**  Both transports complete
  the upgrade before the graph sees the connection, so a ``connect_response``
  of ``False`` closes an accepted socket in either — but the released handler
  closed with no code at all, where compat sends a normal closure.
  ``connect_response`` of ``True`` is a no-op.
* **Handler state is keyed per request id, and per connection id for
  WebSockets**, as released — but a handler graph created for a key sends its
  first service request on the following engine cycle (the nested-client
  hand-off described in the services design record).

The advanced tier is reproduced too: ``http_server_adaptor`` /
``http_server_adaptor_impl`` and ``websocket_server_adaptor`` with its helper
and implementation registration tokens accept the released explicit wiring
(``from_graph``/``to_graph`` clients discovered off the wiring context), lowered
so each route owns a native serve stream instead of a shared queue partitioned
by request URL.  Handlers do not go through the adaptor at all, so a graph may
mix both without one seeing the other's routes.

Not reproduced (import these from the released package if you need them):

* ``rest_handler`` and the ``Rest*`` request/response values are not defined
  here; the released ``hgraph.adaptors.tornado._rest_handler`` keeps them and
  now lowers them onto this module's ``http_server_handler``.
* The HTTP and WebSocket *client* adaptors (``http_client_adaptor``,
  ``websocket_client_adaptor``, ``Credentials``) — use
  :func:`hgraph_web.web_http_request` and :func:`hgraph_web.web_ws_connect`.
* ``BaseHandler.set_auth_callback`` / ``set_auth_callback_async``: there is no
  authentication hook, and ``auth`` is always the anonymous pair.
* ``HttpResponse.write``, which took a tornado stream.

Additions (not part of the released surface):

* :func:`compat_service_path`, the service path of the native server backing a
  port, so a graph can observe :func:`hgraph_web.web_server_stats` for it.
* ``register_http_server_adaptor(port, config=...)`` and its WebSocket twin
  accept an optional :class:`~hgraph_web.WebServerConfig`; its ``port`` is
  replaced by the registered port.  Without one, the server is configured with
  a 250ms shutdown drain rather than the transport's 5s default: the released
  teardown cancelled outstanding requests at once, so the longer drain reads as
  a graph that will not stop.  A supplied config is used as given.
"""

import email.utils
import inspect
import weakref
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from http.cookies import CookieError, SimpleCookie
from typing import Generic, TypeVar
from urllib.parse import unquote_to_bytes

import _hgraph
from frozendict import frozendict

from hgraph import (
    CompoundScalar,
    REMOVE_IF_EXISTS,
    TS,
    TSB,
    TSD,
    TimeSeriesSchema,
    adaptor,
    adaptor_impl,
    combine,
    compute_node,
    from_graph,
    graph,
    map_,
    merge,
    register_adaptor,
    to_graph,
)
from hgraph._types import _TsExpr
from hgraph._wiring import _GraphFn, _PyNode
from hgraph._wiring._core import _current_wiring, _wiring_stack
from hgraph._wiring._operator import _is_hidden_node_parameter

from . import (
    HttpMethod,
    HttpResponse as WebHttpResponse,
    HttpServerRequest as WebHttpServerRequest,
    WebHeader,
    WebRoute,
    WebServerConfig,
    WsConnectionState,
    WsEvent,
    WsFrame,
    WsFrameKind,
    WsInboundFrame,
    register_web_server,
    web_respond,
    web_serve,
    web_ws_send,
    web_ws_serve,
)

__all__ = (
    "HttpDeleteRequest",
    "HttpGetRequest",
    "HttpPostRequest",
    "HttpPutRequest",
    "HttpRequest",
    "HttpResponse",
    "WebSocketClientRequest",
    "WebSocketConnectRequest",
    "WebSocketResponse",
    "WebSocketServerRequest",
    "compat_service_path",
    "http_server_adaptor",
    "http_server_adaptor_impl",
    "http_server_handler",
    "register_http_server_adaptor",
    "register_websocket_server_adaptor",
    "websocket_server_adaptor",
    "websocket_server_adaptor_helper",
    "websocket_server_adaptor_impl",
    "websocket_server_handler",
)


# The released ``BaseHandler.prepare`` installs this pair whenever no
# authentication callback is registered, and compat has no callback hook.
_ANONYMOUS_USER = ("Anonymous", "Anonymous")
# The released adaptor cancelled outstanding work the moment its routes
# stopped, so the transport's multi-second shutdown drain would show up as a
# stalled graph teardown that no released user ever saw.  This keeps a flush
# window without the stall; pass a config to choose your own.
_COMPAT_DRAIN_MS = 250
_DEFAULT_CONTENT_TYPE = "text/html; charset=UTF-8"
_COMPAT_STATE_KEY = "hgraph-web.tornado-compatibility-state"

# Tornado routes a request first and dispatches on the method second: a handler
# that implements none of HEAD/PATCH/OPTIONS/TRACE still owns those methods on
# its pattern and answers them 405.
_HANDLED_METHODS = (
    HttpMethod.GET,
    HttpMethod.POST,
    HttpMethod.PUT,
    HttpMethod.DELETE,
)
_REJECTED_METHODS = (
    HttpMethod.HEAD,
    HttpMethod.PATCH,
    HttpMethod.OPTIONS,
    HttpMethod.TRACE,
)
_ALLOW_HEADER = "GET, POST, PUT, DELETE"
_METHOD_NOT_ALLOWED_BODY = (
    b"<html><title>405: Method Not Allowed</title>"
    b"<body>405: Method Not Allowed</body></html>"
)


# ---------------------------------------------------------------------------
# The released values.  Their schemas are re-declared rather than imported: the
# released module imports tornado at module scope, and the whole point of this
# layer is to run without it.


@dataclass(frozen=True)
class HttpRequest(CompoundScalar, abstract=True):
    """Base request value; construct one of the concrete method leaves."""

    url: str
    url_parsed_args: tuple[str, ...] = ()
    query: dict[str, str] = frozendict()
    headers: dict[str, str] = frozendict()
    cookies: dict[str, dict[str, object]] = frozendict()
    auth: object = None
    connect_timeout: float = 20.0
    request_timeout: float = 20.0


@dataclass(frozen=True)
class HttpGetRequest(HttpRequest):
    pass


@dataclass(frozen=True)
class HttpDeleteRequest(HttpRequest):
    pass


@dataclass(frozen=True)
class HttpPutRequest(HttpRequest):
    body: str = ""


@dataclass(frozen=True)
class HttpPostRequest(HttpRequest):
    body: str = ""


@dataclass(frozen=True)
class HttpResponse(CompoundScalar):
    status_code: int
    headers: dict[str, str] = frozendict()
    cookies: dict[str, dict[str, object]] = frozendict()
    body: bytes = b""

    def __repr__(self) -> str:
        return (
            f"HttpResponse(status_code={self.status_code}, headers={self.headers}, "
            f"cookies={self.cookies}, body_length={len(self.body)})"
        )


STR_OR_BYTES = TypeVar("STR_OR_BYTES", bytes, str)


@dataclass(frozen=True)
class WebSocketConnectRequest(CompoundScalar):
    url: str
    url_parsed_args: tuple[str, ...] = ()
    headers: dict[str, str] = frozendict()
    cookies: dict[str, dict[str, object]] = frozendict()
    auth: object = None


class WebSocketServerRequest(TimeSeriesSchema, Generic[STR_OR_BYTES]):
    connect_request: TS[WebSocketConnectRequest]
    messages: TS[tuple[STR_OR_BYTES, ...]]


class WebSocketClientRequest(TimeSeriesSchema, Generic[STR_OR_BYTES]):
    connect_request: TS[WebSocketConnectRequest]
    message: TS[STR_OR_BYTES]


class WebSocketResponse(TimeSeriesSchema, Generic[STR_OR_BYTES]):
    connect_response: TS[bool]
    message: TS[STR_OR_BYTES]


_METHOD_LEAVES = {
    HttpMethod.GET: HttpGetRequest,
    HttpMethod.DELETE: HttpDeleteRequest,
    HttpMethod.POST: HttpPostRequest,
    HttpMethod.PUT: HttpPutRequest,
}


def compat_service_path(port: int) -> str:
    """The native web service path backing the compatibility server on ``port``."""

    return f"web-compat-{port}"


# ---------------------------------------------------------------------------
# Tornado pattern -> native route pattern.


def _pattern_error(pattern: str, reason: str) -> ValueError:
    return ValueError(f"HTTP handler url {pattern!r} {reason}")


def _read_group(pattern: str, start: int) -> tuple[str, int]:
    """Return the body of the group opening at ``start`` and the index after it."""

    depth = 0
    index = start
    in_class = False
    while index < len(pattern):
        character = pattern[index]
        if character == "\\":
            index += 2
            continue
        if in_class:
            in_class = character != "]"
        elif character == "[":
            in_class = True
        elif character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth == 0:
                return pattern[start + 1 : index], index + 1
        index += 1
    raise _pattern_error(pattern, "has an unbalanced group")


_LITERAL_METACHARACTERS = frozenset("[]{}*+?|^$")
# A trailing ``(.*)`` is tornado's idiom for "the rest of the path"; every other
# group matches within one segment.
_REST_BODIES = frozenset({".*", ".*?", ".+", ".+?"})


@dataclass(frozen=True)
class _CompatRoute:
    """The native routes one tornado URL pattern serves.

    ``patterns`` is usually one route.  ``rest_handler`` spells its URL as
    ``"{url}/?(.*)"`` — an optional separator before the trailing capture —
    which one native pattern cannot express, so that form serves two: the
    captured route and the bare one.  ``captures`` is the number of groups the
    tornado pattern declares, which is what ``url_parsed_args`` must always
    carry however few the matched route filled in.
    """

    patterns: tuple[str, ...]
    captures: int


def _translate_pattern(pattern: str) -> _CompatRoute:
    """Translate a tornado URL pattern into the native routes it serves."""

    body = pattern
    if body.startswith("^"):
        body = body[1:]
    if body.endswith("$") and not body.endswith("\\$"):
        body = body[:-1]
    if not body.startswith("/"):
        raise _pattern_error(pattern, "must start with '/'")

    translated: list[str] = []
    optional: list[str] | None = None
    captures = 0
    index = 0
    while index < len(body):
        character = body[index]
        if character == "\\":
            if index + 1 >= len(body):
                raise _pattern_error(pattern, "ends with a trailing escape")
            translated.append(body[index + 1])
            index += 2
            continue
        if character == "(":
            group, index = _read_group(body, index)
            if group.startswith("?"):
                raise _pattern_error(
                    pattern,
                    "uses a named or non-capturing group; the released handler "
                    "only receives positional captures",
                )
            if translated and translated[-1] != "/":
                raise _pattern_error(
                    pattern, "must spell a capture as a whole path segment"
                )
            if index < len(body) and body[index] != "/":
                raise _pattern_error(
                    pattern, "must spell a capture as a whole path segment"
                )
            name = f"arg{captures}"
            captures += 1
            rest = group in _REST_BODIES and index == len(body)
            translated.append(f"*{name}" if rest else f"{{{name}}}")
            continue
        if character == "?":
            # Only the separator before a trailing capture may be optional:
            # that is the one regex option the released REST route relies on.
            if translated[-1:] != ["/"] or optional is not None:
                raise _pattern_error(
                    pattern,
                    "may only make the separator before a trailing capture optional",
                )
            optional = translated[:-1]
            index += 1
            continue
        if character in _LITERAL_METACHARACTERS:
            raise _pattern_error(
                pattern,
                f"contains the unsupported regular-expression character {character!r}",
            )
        translated.append(character)
        index += 1

    patterns = ["".join(translated)]
    if optional is not None:
        if captures != 1 or not patterns[0].endswith(("*arg0", "{arg0}")):
            raise _pattern_error(
                pattern,
                "may only make the separator before a trailing capture optional",
            )
        patterns.append("".join(optional))
    return _CompatRoute(tuple(patterns), captures)


# ---------------------------------------------------------------------------
# Legacy request and response shapes.


def _normalize_header_name(name: str) -> str:
    return "-".join(part.capitalize() for part in name.split("-"))


def _collapse_headers(headers) -> frozendict:
    collapsed: dict[str, str] = {}
    for header in headers:
        name = _normalize_header_name(header.name)
        previous = collapsed.get(name)
        collapsed[name] = header.value if previous is None else f"{previous},{header.value}"
    return frozendict(collapsed)


def _decode_query_component(text: str) -> str:
    return unquote_to_bytes(text.replace("+", " ")).decode("utf-8")


def _collapse_query(params) -> frozendict:
    collapsed: dict[str, str] = {}
    for param in params:
        name = _decode_query_component(param.name)
        collapsed[name] = collapsed.get(name, "") + _decode_query_component(param.value)
    return frozendict(collapsed)


def _parse_cookies(header: str) -> frozendict:
    jar = SimpleCookie()
    values: dict[str, str] = {}
    for chunk in header.split(";"):
        key, separator, value = chunk.partition("=")
        if not separator:
            key, value = "", chunk
        key, value = key.strip(), value.strip()
        if key or value:
            values[key] = jar.value_decode(value)[0]

    cookies: dict[str, frozendict] = {}
    for key, value in values.items():
        try:
            jar[key] = value
        except CookieError:
            continue
        morsel = jar[key]
        cookies[key] = frozendict({"value": morsel.value, **dict(morsel.items())})
    return frozendict(cookies)


def _capture_index(param) -> int:
    try:
        return int(param.name.removeprefix("arg"))
    except ValueError:
        return 0


def _parsed_args(path_params, captures: int) -> tuple[str, ...]:
    """The captures in declaration order, padded to the pattern's group count.

    A tornado group that matched nothing still arrives as an empty string; the
    bare route of an optional-separator pattern fills none of them.
    """

    values = tuple(param.value for param in sorted(path_params, key=_capture_index))
    return values + ("",) * (captures - len(values))


def _compat_request(server_request, url: str, captures: int) -> HttpRequest:
    inbound = server_request.request
    headers = _collapse_headers(inbound.headers)
    arguments = dict(
        url=url,
        url_parsed_args=_parsed_args(inbound.path_params, captures),
        query=_collapse_query(inbound.query),
        headers=headers,
        cookies=_parse_cookies(headers.get("Cookie", "")),
        auth=_ANONYMOUS_USER,
    )
    leaf = _METHOD_LEAVES[inbound.method]
    if leaf in (HttpPostRequest, HttpPutRequest):
        return leaf(**arguments, body=inbound.body.decode("utf-8"))
    return leaf(**arguments)


def _format_expiry(expires) -> str:
    if isinstance(expires, str):
        return expires
    if isinstance(expires, datetime):
        return email.utils.formatdate(expires.timestamp(), usegmt=True)
    return email.utils.formatdate(float(expires), usegmt=True)


def _set_cookie_header(name: str, value) -> str:
    """Render one released response cookie the way ``set_cookie`` did."""

    attributes = {"value": value} if isinstance(value, str) else dict(value)
    jar = SimpleCookie()
    jar[name] = attributes.pop("value")
    morsel = jar[name]
    domain = attributes.pop("domain", None)
    expires = attributes.pop("expires", None)
    path = attributes.pop("path", "/")
    expires_days = attributes.pop("expires_days", None)
    if domain:
        morsel["domain"] = domain
    if expires_days is not None and not expires:
        expires = datetime.now(timezone.utc) + timedelta(days=expires_days)
    if expires:
        morsel["expires"] = _format_expiry(expires)
    if path:
        morsel["path"] = path
    for key, item in attributes.items():
        if key == "max_age":
            key = "max-age"
        if key in ("httponly", "secure") and not item:
            continue
        morsel[key] = item
    return morsel.OutputString()


def _native_response(response: HttpResponse) -> WebHttpResponse:
    headers = [WebHeader(name, value) for name, value in response.headers.items()]
    headers.extend(
        WebHeader("Set-Cookie", _set_cookie_header(name, value))
        for name, value in response.cookies.items()
    )
    if not any(header.name.lower() == "content-type" for header in headers):
        headers.append(WebHeader("Content-Type", _DEFAULT_CONTENT_TYPE))
    return WebHttpResponse(
        status=response.status_code,
        headers=tuple(headers),
        body=response.body,
    )


# One node, not one per method plus a merge: the released request union is a
# polymorphic bundle, and merging is expressed as a fixed list of its inputs,
# which that union cannot be an element of.
@compute_node(valid=())
def _to_compat_requests(
    get: TS[WebHttpServerRequest],
    post: TS[WebHttpServerRequest],
    put: TS[WebHttpServerRequest],
    delete: TS[WebHttpServerRequest],
    url: str,
    captures: int,
) -> TSD[int, TS[HttpRequest]]:
    requests = {}
    for served in (get, post, put, delete):
        if served.modified:
            value = served.value
            requests[value.request_id] = _compat_request(value, url, captures)
    return requests or None


@compute_node
def _server_request_id(request: TS[WebHttpServerRequest]) -> TS[int]:
    return request.value.request_id


@compute_node
def _method_not_allowed(request: TS[WebHttpServerRequest]) -> TS[WebHttpResponse]:
    return WebHttpResponse(
        status=405,
        headers=(
            WebHeader("Allow", _ALLOW_HEADER),
            WebHeader("Content-Type", _DEFAULT_CONTENT_TYPE),
        ),
        body=_METHOD_NOT_ALLOWED_BODY,
    )


@compute_node
def _to_native_response(response: TS[HttpResponse]) -> TS[WebHttpResponse]:
    return _native_response(response.value)


@compute_node(active=("response",), valid=("request_id", "response"))
def _response_key(request_id: TS[int], response: TS[WebHttpResponse]) -> TS[int]:
    """Re-tick the request id whenever its response ticks.

    The respond service request is one bundle: both fields must tick together
    or the transport is handed a request id with no response beside it.
    """

    return request_id.value


# ---------------------------------------------------------------------------
# WebSocket conversions.


@compute_node
def _ws_connections(
    event: TS[WsEvent], url: str, captures: int
) -> TSD[int, TS[WebSocketConnectRequest]]:
    value = event.value
    if value.state != WsConnectionState.OPEN:
        return {value.connection_id: REMOVE_IF_EXISTS}
    inbound = value.request.request
    headers = _collapse_headers(inbound.headers)
    return {
        value.connection_id: WebSocketConnectRequest(
            url=url,
            url_parsed_args=_parsed_args(inbound.path_params, captures),
            headers=headers,
            cookies=_parse_cookies(headers.get("Cookie", "")),
            auth=_ANONYMOUS_USER,
        )
    }


# The close event has to erase the message key as well as the connection key:
# the handler instance lives for as long as either key does, so releasing only
# one of them would leave a connection's graph running after it closed.
@compute_node(valid=())
def _ws_text_messages(
    frame: TS[WsInboundFrame], event: TS[WsEvent]
) -> TSD[int, TS[tuple[str, ...]]]:
    messages = {}
    if event.modified and event.value.state != WsConnectionState.OPEN:
        messages[event.value.connection_id] = REMOVE_IF_EXISTS
    if frame.modified:
        value = frame.value
        payload = value.frame
        if payload.kind is WsFrameKind.TEXT:
            messages[value.connection_id] = (payload.text,)
        elif payload.kind is WsFrameKind.BINARY:
            messages[value.connection_id] = (payload.data.decode(),)
    return messages or None


@compute_node(valid=())
def _ws_binary_messages(
    frame: TS[WsInboundFrame], event: TS[WsEvent]
) -> TSD[int, TS[tuple[bytes, ...]]]:
    messages = {}
    if event.modified and event.value.state != WsConnectionState.OPEN:
        messages[event.value.connection_id] = REMOVE_IF_EXISTS
    if frame.modified:
        value = frame.value
        payload = value.frame
        if payload.kind is WsFrameKind.BINARY:
            messages[value.connection_id] = (payload.data,)
        elif payload.kind is WsFrameKind.TEXT:
            messages[value.connection_id] = (payload.text.encode(),)
    return messages or None


@compute_node(valid=())
def _ws_text_frame(message: TS[str], connect_response: TS[bool]) -> TS[WsFrame]:
    if message.modified:
        return WsFrame.text_frame(message.value)
    if connect_response.modified and not connect_response.value:
        return WsFrame.close_frame()
    return None


@compute_node(valid=())
def _ws_binary_frame(message: TS[bytes], connect_response: TS[bool]) -> TS[WsFrame]:
    if message.modified:
        return WsFrame.binary_frame(message.value)
    if connect_response.modified and not connect_response.value:
        return WsFrame.close_frame()
    return None


@compute_node(active=("frame",), valid=("connection_id", "frame"))
def _ws_send_key(connection_id: TS[int], frame: TS[WsFrame]) -> TS[int]:
    return connection_id.value


# ---------------------------------------------------------------------------
# Wiring-owned registration state.


@dataclass
class _CompatState:
    servers: dict = field(default_factory=dict)
    http_port: int = None
    ws_port: int = None

    def register(self, port: int, config: WebServerConfig) -> str:
        path = compat_service_path(port)
        if port not in self.servers:
            server_config = (
                WebServerConfig(port=port, shutdown_drain_timeout_ms=_COMPAT_DRAIN_MS)
                if config is None
                else replace(config, port=port)
            )
            register_web_server(server_config, path=path)
            self.servers[port] = server_config
        elif config is not None and replace(config, port=port) != self.servers[port]:
            raise ValueError(
                f"the compatibility web server on port {port} is already "
                "registered with a different configuration"
            )
        return path


def _compat_state() -> _CompatState:
    # Adaptor implementations are wired in their own context, so the
    # registration state hangs off the application's root wiring and is shared
    # by handlers, registrations, and implementations alike.
    _current_wiring()
    return _wiring_stack[0]._acquire_extension_state(_COMPAT_STATE_KEY, _CompatState)


def _serving_path(port: int, what: str) -> str:
    if port is None:
        raise RuntimeError(
            f"register_{what}_server_adaptor must be called before wiring "
            f"{what} handlers"
        )
    return compat_service_path(port)


# ---------------------------------------------------------------------------
# HTTP handlers.


def _handler_parameters(signature):
    return tuple(
        parameter
        for name, parameter in signature.parameters.items()
        if name != "request" and not _is_hidden_node_parameter(parameter)
    )


def _ts_expr(handle) -> _TsExpr:
    return _TsExpr(handle, repr(handle))


def _bundle_field_types(annotation) -> dict[str, _TsExpr] | None:
    if not isinstance(annotation, _TsExpr) or not annotation.handle.is_tsb:
        return None
    return {
        name: _ts_expr(field_type)
        for name, field_type in _hgraph.ts_field_types(annotation.handle)
    }


def _keyed_bundle_field_types(annotation) -> dict[str, _TsExpr] | None:
    if not isinstance(annotation, _TsExpr) or not annotation.handle.is_tsd:
        return None
    element_type = _hgraph.tsd_element_ts(annotation.handle)
    if not element_type.is_tsb:
        return None
    return {
        name: _ts_expr(field_type)
        for name, field_type in _hgraph.ts_field_types(element_type)
    }


def _serve(route: _CompatRoute, method: HttpMethod, path: str, upgrade: bool = False):
    """One request stream for a method across every pattern of ``route``.

    The transport delivers one request per engine cycle across all routes, and
    a path matches at most one of a route's patterns, so merging their streams
    cannot lose a request.
    """

    served = [
        web_serve(WebRoute(method, pattern), path=path)["request"]
        for pattern in route.patterns
    ]
    return served[0] if len(served) == 1 else merge(*served)


def _serve_requests(url: str, route: _CompatRoute, path: str):
    """Serve one tornado route: the four handled methods, keyed by request id."""

    streams = [_serve(route, method, path) for method in _HANDLED_METHODS]
    for method in _REJECTED_METHODS:
        request = _serve(route, method, path)
        web_respond(
            _server_request_id(request), _method_not_allowed(request), path=path
        )
    return _to_compat_requests(*streams, url=url, captures=route.captures)


def _wire_responses(responses, path: str) -> None:
    @graph
    def respond(key: TS[int], response: TS[HttpResponse]) -> None:
        native = _to_native_response(response)
        web_respond(_response_key(key, native), native, path=path)

    map_(respond, responses)


class _HttpServerHandler:
    def __init__(self, fn, url: str):
        # A handler is a graph or a Python-authored node; a bare function is
        # treated as a graph, matching the released decorator.
        self._fn = fn if isinstance(fn, (_GraphFn, _PyNode)) else graph(fn)
        self.url = url
        self.__name__ = getattr(fn, "__name__", "http_server_handler")
        self.route = _translate_pattern(url)

        target = getattr(fn, "fn", fn)
        signature = inspect.signature(target, eval_str=True)
        request = signature.parameters.get("request")
        if request is None:
            raise TypeError("HTTP handler requires a 'request' time-series input")

        single_request = TS[HttpRequest]
        batch_request = TSD[int, TS[HttpRequest]]
        if request.annotation == single_request:
            self._single = True
        elif request.annotation == batch_request:
            self._single = False
        else:
            raise TypeError(
                "HTTP handler request must be TS[HttpRequest] or "
                "TSD[int, TS[HttpRequest]]"
            )

        expected_output = TS[HttpResponse] if self._single else TSD[int, TS[HttpResponse]]
        output = signature.return_annotation
        self._auxiliary_output = False
        if output != expected_output:
            fields = _bundle_field_types(output)
            if fields is not None and fields.get("response") == expected_output:
                self._auxiliary_output = True
            elif not self._single:
                fields = _keyed_bundle_field_types(output)
                if fields is not None and fields.get("response") == TS[HttpResponse]:
                    self._auxiliary_output = True
        if output != expected_output and not self._auxiliary_output:
            raise TypeError(
                f"HTTP handler output must be {expected_output!r}, a TSB with a "
                f"'response' field of that type, or a keyed TSD of response bundles"
            )

        parameters = _handler_parameters(signature)
        self.__signature__ = signature.replace(parameters=parameters)
        self.auto_wire = not self._auxiliary_output and all(
            parameter.default is not inspect.Parameter.empty
            for parameter in parameters
        )
        self._wired = weakref.WeakKeyDictionary()

    def __call__(self, *args, **kwargs):
        wiring = _current_wiring()
        if not args and not kwargs and wiring in self._wired:
            return self._wired[wiring]
        path = _serving_path(_compat_state().http_port, "http")
        bound = self.__signature__.bind(*args, **kwargs)
        bound.apply_defaults()

        requests = _serve_requests(self.url, self.route, path)
        if self._single:
            responses = map_(self._fn, requests, *bound.args, **bound.kwargs)
        else:
            responses = self._fn(request=requests, **bound.arguments)
        _wire_responses(
            responses.response if self._auxiliary_output else responses, path
        )
        if not args and not kwargs:
            self._wired[wiring] = responses
        return responses


_HTTP_SERVER_HANDLERS: dict[str, _HttpServerHandler] = {}


def http_server_handler(fn=None, *, url: str):
    """Declare an HTTP route handled by a graph or Python-authored node.

    A handler accepts either one ``TS[HttpRequest]`` and returns one
    ``TS[HttpResponse]``, or accepts and returns the corresponding keyed
    ``TSD`` forms. The output may instead be a ``TSB`` with a field named
    ``response`` of the primary response type; keyed batch handlers may also
    return a ``TSD`` of those bundles. Auxiliary-output handlers are wired
    explicitly so their full output remains observable. Response-only handlers
    without additional required inputs are wired by
    :func:`register_http_server_adaptor`.
    """
    if fn is None:
        return lambda decorated: http_server_handler(decorated, url=url)

    handler = _HttpServerHandler(fn, url)
    _HTTP_SERVER_HANDLERS[url] = handler
    return handler


@adaptor
def http_server_adaptor(
    response: TSD[int, TS[HttpResponse]],
    path: str,
) -> TSD[int, TS[HttpRequest]]:
    """Expose a graph request/response stream as an HTTP route."""


def _adaptor_routes(interface, clients, ordering, label: str, generic: bool):
    """The released client walk: validate, dedup to base routes, then order.

    Ordering follows handler declaration order as it did on tornado, where it
    selected route precedence.  Native routing is specificity-ordered, so here
    it only fixes the order routes are wired in.
    """

    endpoints = set()
    routes = []
    seen = set()
    for endpoint, type_map, _node, receive in clients:
        if type_map and not generic:
            raise TypeError(f"{label} server adaptor does not support generic bindings")
        if (endpoint, receive) in endpoints:
            raise ValueError(
                f"duplicate {label} adaptor client for {endpoint!r} and direction {receive}")
        endpoints.add((endpoint, receive))
        base = endpoint.removesuffix("/from_graph").removesuffix("/to_graph")
        if base not in seen:
            routes.append(base)
            seen.add(base)
    priority = {route: index for index, route in enumerate(ordering)}
    routes.sort(key=lambda base: priority.get(
        interface.path_from_full_path(base), len(priority)))
    return routes


@adaptor_impl(interfaces=())
def http_server_adaptor_impl(path: str, port: int = 80) -> None:
    """Serve every requested HTTP route from the compatibility server.

    Each route owns its own native serve stream, so the released
    queue/partition/merge plumbing has no counterpart here.
    """
    from hgraph import WiringGraphContext

    routes = _adaptor_routes(
        http_server_adaptor,
        WiringGraphContext.instance().registered_service_clients(http_server_adaptor),
        _HTTP_SERVER_HANDLERS,
        label="HTTP",
        generic=False,
    )
    service_path = _compat_state().register(port, None)
    for base in routes:
        url = http_server_adaptor.path_from_full_path(base)
        requests = _serve_requests(url, _translate_pattern(url), service_path)
        to_graph(http_server_adaptor, requests, path=base)
        _wire_responses(from_graph(http_server_adaptor, path=base), service_path)


class _HttpServerAdaptorHelperRegistration:
    """Compatibility marker for explicitly wired HTTP handler graphs.

    Handlers reach the transport directly, so the marker deliberately leaves
    ownership with the shared implementation instead of registering a second
    implementation per route.
    """

    __name__ = "http_server_adaptor_helper"

    @staticmethod
    def _register_adaptor(path, *, resolution_dict=None, port=80):
        del path, port
        if resolution_dict:
            raise TypeError("http_server_adaptor_helper is not generic")


_http_server_adaptor_helper = _HttpServerAdaptorHelperRegistration()


def register_http_server_adaptor(port: int, config: WebServerConfig = None) -> None:
    """Register the HTTP server implementation and wire automatic handlers."""
    state = _compat_state()
    if state.http_port is not None and state.http_port != port:
        raise ValueError("one wiring graph cannot register the HTTP server on two ports")
    state.register(port, config)
    state.http_port = port
    register_adaptor(None, _http_server_adaptor_helper, port=port)
    register_adaptor("http_server_adaptor", http_server_adaptor_impl, port=port)
    for _url, handler in tuple(_HTTP_SERVER_HANDLERS.items()):
        if handler.auto_wire:
            handler()


# ---------------------------------------------------------------------------
# WebSocket handlers.


class _WebSocketServerHandler:
    def __init__(self, fn, url: str):
        self._fn = fn if isinstance(fn, (_GraphFn, _PyNode)) else graph(fn)
        self.url = url
        self.__name__ = getattr(fn, "__name__", "websocket_server_handler")
        self.route = _translate_pattern(url)

        target = getattr(fn, "fn", fn)
        signature = inspect.signature(target, eval_str=True)
        request = signature.parameters.get("request")
        if request is None:
            raise TypeError("WebSocket handler requires a 'request' time-series input")

        self._single = None
        self.message_type = None
        for message_type in (str, bytes):
            single = TSB[WebSocketServerRequest[message_type]]
            batch = TSD[int, single]
            if request.annotation == single:
                self._single = True
                self.message_type = message_type
                expected_output = TSB[WebSocketResponse[message_type]]
                break
            if request.annotation == batch:
                self._single = False
                self.message_type = message_type
                expected_output = TSD[int, TSB[WebSocketResponse[message_type]]]
                break
        if self.message_type is None:
            raise TypeError(
                "WebSocket handler request must be TSB[WebSocketServerRequest[str|bytes]] "
                "or its keyed TSD form"
            )
        if signature.return_annotation != expected_output:
            raise TypeError(
                f"WebSocket handler output must be {expected_output!r} with the same message type"
            )

        parameters = _handler_parameters(signature)
        self.__signature__ = signature.replace(parameters=parameters)
        self.auto_wire = all(
            parameter.default is not inspect.Parameter.empty
            for parameter in parameters
        )
        self._wired = weakref.WeakKeyDictionary()

    def __call__(self, *args, **kwargs):
        wiring = _current_wiring()
        if not args and not kwargs and wiring in self._wired:
            return self._wired[wiring]
        path = _serving_path(_compat_state().ws_port, "websocket")
        bound = self.__signature__.bind(*args, **kwargs)
        bound.apply_defaults()

        requests = _serve_connections(self.url, self.route, path, self.message_type)
        if self._single:
            responses = map_(self._fn, requests, *bound.args, **bound.kwargs)
        else:
            responses = self._fn(request=requests, **bound.arguments)
        _wire_ws_responses(responses, path, self.message_type)
        if not args and not kwargs:
            self._wired[wiring] = responses
        return responses


def _serve_connections(url: str, route: _CompatRoute, path: str, message_type: type):
    served = [
        web_ws_serve(WebRoute(HttpMethod.GET, pattern, upgrade=True), path=path)
        for pattern in route.patterns
    ]
    events = served[0]["event"] if len(served) == 1 else merge(
        *(entry["event"] for entry in served))
    frames = served[0]["frame"] if len(served) == 1 else merge(
        *(entry["frame"] for entry in served))
    connections = _ws_connections(events, url=url, captures=route.captures)
    to_messages = _ws_text_messages if message_type is str else _ws_binary_messages
    messages = to_messages(frames, events)
    request_bundle = TSB[WebSocketServerRequest[message_type]]

    @graph
    def make_request(
        connect_request: TS[WebSocketConnectRequest],
        messages: TS[tuple[message_type, ...]],
    ) -> request_bundle:
        return combine[request_bundle](
            connect_request=connect_request,
            messages=messages,
        )

    return map_(make_request, connections, messages)


def _wire_ws_responses(responses, path: str, message_type: type) -> None:
    response_bundle = TSB[WebSocketResponse[message_type]]
    to_frame = _ws_text_frame if message_type is str else _ws_binary_frame

    @graph
    def send(key: TS[int], response: response_bundle) -> None:
        frame = to_frame(response["message"], response["connect_response"])
        web_ws_send(_ws_send_key(key, frame), frame, path=path)

    map_(send, responses)


_WEBSOCKET_SERVER_HANDLERS: dict[str, _WebSocketServerHandler] = {}


def websocket_server_handler(fn=None, *, url: str):
    """Declare a typed WebSocket route handled by a graph or Python node."""
    if fn is None:
        return lambda decorated: websocket_server_handler(decorated, url=url)
    handler = _WebSocketServerHandler(fn, url)
    _WEBSOCKET_SERVER_HANDLERS[url] = handler
    return handler


@adaptor
def websocket_server_adaptor(
    response: TSD[int, TSB[WebSocketResponse[STR_OR_BYTES]]],
    path: str,
) -> TSD[int, TSB[WebSocketServerRequest[STR_OR_BYTES]]]:
    """Expose a typed graph request/response stream as a WebSocket route."""


def _wire_websocket_message_type(port: int, message_type: type, entries) -> None:
    interface = websocket_server_adaptor[STR_OR_BYTES:message_type]
    service_path = _compat_state().register(port, None)
    for base in entries:
        url = interface.path_from_full_path(base)
        requests = _serve_connections(
            url, _translate_pattern(url), service_path, message_type)
        to_graph(interface, requests, path=base)
        _wire_ws_responses(
            from_graph(interface, path=base), service_path, message_type)


@adaptor_impl(interfaces=())
def _websocket_server_catch_all(path: str, port: int = 80) -> None:
    from hgraph import WiringGraphContext
    from hgraph.reflection import resolved_type

    grouped = {str: [], bytes: []}
    for client in WiringGraphContext.instance().registered_service_clients(
            websocket_server_adaptor):
        grouped[resolved_type(client[1][STR_OR_BYTES])].append(client)
    for message_type, clients in grouped.items():
        if not clients:
            continue
        interface = websocket_server_adaptor[STR_OR_BYTES:message_type]
        routes = _adaptor_routes(
            interface,
            clients,
            _WEBSOCKET_SERVER_HANDLERS,
            label="WebSocket",
            generic=True,
        )
        _wire_websocket_message_type(port, message_type, routes)


class _WebSocketServerAdaptorHelperRegistration:
    """Compatibility marker for explicitly wired WebSocket handler graphs.

    Specialized graph-side clients are discovered by the shared catch-all, so
    the marker must not compete with it for ownership.
    """

    __name__ = "websocket_server_adaptor_helper"

    @staticmethod
    def _register_adaptor(path, *, resolution_dict=None, port=80):
        del path, port
        if resolution_dict:
            raise TypeError("websocket_server_adaptor_helper resolves from its handlers")


websocket_server_adaptor_helper = _WebSocketServerAdaptorHelperRegistration()


def register_websocket_server_adaptor(
    port: int, config: WebServerConfig = None
) -> None:
    """Register all declared WebSocket routes on ``port``."""
    state = _compat_state()
    if state.ws_port is not None and state.ws_port != port:
        raise ValueError(
            "one wiring graph cannot register the WebSocket server on two ports"
        )
    state.register(port, config)
    state.ws_port = port
    register_adaptor(None, websocket_server_adaptor_helper, port=port)
    register_adaptor(
        "websocket_server_adaptor", _websocket_server_catch_all, port=port)
    for _url, handler in tuple(_WEBSOCKET_SERVER_HANDLERS.items()):
        if handler.auto_wire:
            handler()


class _WebSocketServerAdaptorImplementation:
    """Public registration token for the shared typed server implementation."""

    __name__ = "websocket_server_adaptor_impl"

    @staticmethod
    def _register_adaptor(path, *, resolution_dict=None, port=80):
        del path
        if resolution_dict:
            raise TypeError(
                "websocket_server_adaptor_impl resolves message types from its handlers"
            )
        register_websocket_server_adaptor(port)


websocket_server_adaptor_impl = _WebSocketServerAdaptorImplementation()
