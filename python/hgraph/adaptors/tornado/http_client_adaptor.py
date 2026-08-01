"""Tornado HTTP client implemented as a Python service adaptor."""

import base64
import logging
import os
import re
import socket
import time
from urllib.parse import urlencode, urlparse

from frozendict import frozendict
from tornado.httpclient import AsyncHTTPClient, HTTPError

from hgraph import (
    TS,
    TSD,
    push_queue,
    service_adaptor,
    service_adaptor_impl,
    sink_node,
)

from ._tornado_web import TornadoWeb
from .http_server_adaptor import (
    HttpDeleteRequest,
    HttpGetRequest,
    HttpPostRequest,
    HttpPutRequest,
    HttpRequest,
    HttpResponse,
)

logger = logging.getLogger(__name__)


class Credentials:
    def __init__(self, username, password):
        self.username = username
        self.password = password

    def __repr__(self) -> str:
        return "credentials"

    __str__ = __repr__


@service_adaptor
def http_client_adaptor(
    request: TS[HttpRequest], path: str = "http_client"
) -> TS[HttpResponse]:
    """Send a GET, POST, PUT, or DELETE request and emit its response."""


def _request_method_and_body(request: HttpRequest) -> tuple[str, str | None]:
    if isinstance(request, HttpGetRequest):
        return "GET", None
    if isinstance(request, HttpPostRequest):
        return "POST", request.body
    if isinstance(request, HttpPutRequest):
        return "PUT", request.body
    if isinstance(request, HttpDeleteRequest):
        return "DELETE", None
    raise TypeError(f"unsupported HTTP request type: {type(request).__name__}")


async def _handle_auth(response, request: HttpRequest, client):
    import spnego

    auth_header = response.headers.get("www-authenticate")
    if not auth_header:
        raise HTTPError(401, "missing www-authenticate header")

    auth_header = auth_header.lower()
    if "negotiate" in auth_header:
        scheme = "Negotiate"
        protocol = "kerberos"
        username = None
        password = None
    elif "ntlm" in auth_header:
        scheme = "NTLM"
        protocol = "ntlm"
        if request.auth is not None and isinstance(request.auth, Credentials):
            username = request.auth.username
            password = request.auth.password
        else:
            raise HTTPError(
                401,
                "NTLM Authentication on non-windows hosts is not supported without supplying credentials",
            )
    else:
        raise HTTPError(401, "unhandled protocol")

    parsed_url = urlparse(response.request.url)
    host = parsed_url.hostname
    try:
        info = socket.getaddrinfo(host, None, 0, 0, 0, socket.AI_CANONNAME)
        host = info[0][3]
    except socket.gaierror as error:
        logger.info("Skipping canonicalization of name %s due to error: %s", host, error)

    ctx = spnego.client(
        username=username,
        password=password,
        hostname=host,
        service="HTTP",
        channel_bindings=None,
        context_req=spnego.ContextReq.sequence_detect
        | spnego.ContextReq.mutual_auth,
        protocol=protocol,
    )

    for _ in range(2):
        auth_req = re.search(f"{scheme}\\s*([^,]*)", auth_header, re.I)
        if auth_req is None:
            raise HTTPError(401, "No auth token found")

        gss_r = ctx.step(in_token=base64.b64decode(auth_req[1]))
        response.request.headers["Authorization"] = f"{scheme} {base64.b64encode(gss_r).decode()}"
        set_cookie = response.headers.get("set-cookie")
        if set_cookie is not None:
            response.request.headers["Cookie"] = set_cookie

        response2 = await client.fetch(response.request, raise_error=False)
        if response2.code != 401:
            final = response2.headers.get("WWW-Authenticate")
            if final is not None:
                try:
                    scheme_match = re.search(f"{scheme}\\s*([^,]*)", final, re.I)
                    if scheme_match is not None:
                        token = scheme_match[1]
                    else:
                        base64_pattern = r"(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?"
                        matches = re.findall(base64_pattern, final)
                        if matches and len(matches[0]) > 8:
                            token = matches[0]
                        else:
                            raise HTTPError(401, f"No valid auth token found in header: {final}")
                    ctx.step(in_token=base64.b64decode(token))
                except spnego.exceptions.SpnegoError:
                    logger.error("authenticate_server(): ctx step() failed:")
                    raise HTTPError(401, "Kerberos Authentication failed")
            return response2
        response = response2
        auth_header = response.headers.get("www-authenticate")

    raise HTTPError(401, f"Kerberos Authentication failed: {response}")


async def _handle_auth_win(response, request: HttpRequest, client):
    del request
    import win32security
    import sspi
    import sspicon
    import pywintypes

    auth_header = response.headers.get("www-authenticate")
    if not auth_header:
        raise HTTPError(401, "missing www-authenticate header")

    auth_header = auth_header.lower()
    if "negotiate" in auth_header:
        scheme = "Negotiate"
    elif "ntlm" in auth_header:
        scheme = "NTLM"
    else:
        raise HTTPError(401, "unhandled protocol")

    parsed_url = urlparse(response.request.url)
    host = parsed_url.hostname
    try:
        info = socket.getaddrinfo(host, None, 0, 0, 0, socket.AI_CANONNAME)
        host = info[0][3]
    except socket.gaierror as error:
        logger.info("Skipping canonicalization of name %s due to error: %s", host, error)

    pkg_info = win32security.QuerySecurityPackageInfo(scheme)
    clientauth = sspi.ClientAuth(
        scheme,
        targetspn=f"HTTP/{host}",
        auth_info=None,
        scflags=sspicon.ISC_REQ_MUTUAL_AUTH,
        datarep=sspicon.SECURITY_NETWORK_DREP,
    )
    sec_buffer = win32security.PySecBufferDescType()

    set_cookie = response.headers.get("set-cookie")
    if set_cookie is not None:
        response.request.headers["Cookie"] = set_cookie

    try:
        _, auth = clientauth.authorize(sec_buffer)
        data = base64.b64encode(auth[0].Buffer).decode("ASCII")
        response.request.headers["Authorization"] = f"{scheme} {data}"
    except pywintypes.error as error:
        logger.error("Error calling %s: %s", error[1], error[2], exc_info=error)
        return response

    response2 = await client.fetch(response.request, raise_error=False)
    if response2.code != 401:
        final = response2.headers.get("WWW-Authenticate")
        if final is not None:
            try:
                if scheme in final:
                    challenge = [
                        value[len(scheme) + 1:]
                        for item in final.split(",")
                        if scheme in (value := item.strip())
                    ]
                else:
                    challenge = [item.strip() for item in final.split(",")]
                if len(challenge) > 1:
                    raise HTTPError(401, f"Received more than one {scheme} challenge from server")
                tokenbuf = win32security.PySecBufferType(pkg_info["MaxToken"], sspicon.SECBUFFER_TOKEN)
                tokenbuf.Buffer = base64.b64decode(challenge[0])
                sec_buffer.append(tokenbuf)
                clientauth.authorize(sec_buffer)
            except TypeError:
                pass
        return response2

    set_cookie = response2.headers.get("set-cookie")
    if set_cookie is not None:
        response2.request.headers["Cookie"] = set_cookie
    challenge = [
        value[len(scheme) + 1:]
        for item in response2.headers.get("WWW-Authenticate", "").split(",")
        if scheme in (value := item.strip())
    ]
    if len(challenge) > 1:
        raise HTTPError(401, f"Received more than one {scheme} challenge from server")
    if not challenge:
        final = response2.headers.get("WWW-Authenticate", "")
        matches = re.findall(
            r"(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?",
            final,
        )
        if matches:
            challenge = [matches[0]]
        else:
            raise HTTPError(401, f"Could not find any {scheme} challenge in WWW-Authenticate header: {final}")

    tokenbuf = win32security.PySecBufferType(pkg_info["MaxToken"], sspicon.SECBUFFER_TOKEN)
    tokenbuf.Buffer = base64.b64decode(challenge[0])
    sec_buffer.append(tokenbuf)
    try:
        _, auth = clientauth.authorize(sec_buffer)
        data = base64.b64encode(auth[0].Buffer).decode("ASCII")
        response2.request.headers["Authorization"] = f"{scheme} {data}"
    except pywintypes.error as error:
        logger.error("Error calling %s: %s", error[1], error[2], exc_info=error)
        return response2
    return await client.fetch(response2.request, raise_error=False)


@service_adaptor_impl(interfaces=http_client_adaptor)
def http_client_adaptor_impl(
    request: TSD[int, TS[HttpRequest]],
    path: str = "http_client",
    use_curl: bool = False,
    max_clients: int = 50,
) -> TSD[int, TS[HttpResponse]]:
    """Multiplex graph clients over Tornado's asynchronous HTTP client."""
    if use_curl:
        AsyncHTTPClient.configure(
            "tornado.curl_httpclient.CurlAsyncHTTPClient",
            max_clients=max_clients,
        )

    sender_ref = {}

    @push_queue(TSD[int, TS[HttpResponse]])
    def from_web(sender) -> None:
        sender_ref["sender"] = sender

    async def make_http_request(
        request_id: int,
        request_value: HttpRequest,
        sender,
    ) -> None:
        started = time.perf_counter_ns()
        try:
            method, body = _request_method_and_body(request_value)
            query = urlencode(request_value.query)
            url = f"{request_value.url}?{query}" if query else request_value.url
            client = AsyncHTTPClient()
            response = await client.fetch(
                url,
                method=method,
                headers=request_value.headers,
                body=body,
                connect_timeout=request_value.connect_timeout,
                request_timeout=request_value.request_timeout,
                raise_error=False,
            )
            if (
                response.code == 401
                and response.headers.get("www-authenticate") is not None
            ):
                try:
                    if os.name == "nt":
                        response = await _handle_auth_win(
                            response,
                            request_value,
                            client,
                        )
                    else:
                        response = await _handle_auth(
                            response,
                            request_value,
                            client,
                        )
                except HTTPError as error:
                    message = getattr(error, "message", None) or str(error)
                    sender(
                        {
                            request_id: HttpResponse(
                                status_code=error.code,
                                body=message.encode(),
                            )
                        }
                    )
                    return
            result = HttpResponse(
                status_code=response.code,
                headers=frozendict(response.headers.items()),
                body=response.body,
            )
        except Exception as error:
            logger.exception("HTTP request %s failed", request_id)
            result = HttpResponse(status_code=400, body=str(error).encode())

        logger.debug(
            "HTTP request %s completed in %d ms",
            request_id,
            (time.perf_counter_ns() - started) // 1_000_000,
        )
        sender({request_id: result})

    @sink_node
    def to_web(
        requests: TSD[int, TS[HttpRequest]],
    ) -> None:
        sender = sender_ref["sender"]
        loop = TornadoWeb.get_loop()
        for request_id, request_value in requests.modified_items():
            loop.add_callback(
                make_http_request,
                request_id,
                request_value.value,
                sender,
            )

    @to_web.start
    def to_web_start() -> None:
        TornadoWeb.start_loop()

    @to_web.stop
    def to_web_stop() -> None:
        try:
            TornadoWeb.stop_loop()
        finally:
            sender_ref.clear()

    to_web(request)
    return from_web()


__all__ = (
    "Credentials",
    "http_client_adaptor",
    "http_client_adaptor_impl",
)
