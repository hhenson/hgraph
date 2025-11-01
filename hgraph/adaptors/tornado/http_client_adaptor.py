import base64
import os
import re
import socket
import time
from collections import namedtuple
from logging import getLogger
from typing import Callable
from urllib.parse import urlencode, urlparse

from frozendict import frozendict as fd
from tornado.httpclient import AsyncHTTPClient, HTTPError

from hgraph import service_adaptor, TS, service_adaptor_impl, TSD, push_queue, GlobalState, sink_node
from hgraph.adaptors.tornado._tornado_web import TornadoWeb
from hgraph.adaptors.tornado.http_server_adaptor import (
    HttpRequest,
    HttpResponse,
    HttpPostRequest,
    HttpGetRequest,
    HttpPutRequest,
    HttpDeleteRequest,
)

logger = getLogger(__name__)


class Credentials:
    def __init__(self, username, password):
        self.username = username
        self.password = password

    def __repr__(self):
        return f"credentials"

    def __str__(self):
        return f"credentials"


authorization_match = re.compile(r"'Authorization':\s*'(?:Bearer|Basic|Digest|Negotiate|NTLM)\s+[^']+'")


@service_adaptor
def http_client_adaptor(request: TS[HttpRequest], path: str = "http_client") -> TS[HttpResponse]:
    """
    Sends requests to a remote server. The result is returned as an HttpResponse object.
    Supports Get, Put, Delete and Post requests.
    To use the adaptor you need to register the service impl. The default provided implementation is
    ``http_client_adaptor_impl``.
    """


@service_adaptor_impl(interfaces=http_client_adaptor)
def http_client_adaptor_impl(
    request: TSD[int, TS[HttpRequest]], path: str = "http_client", use_curl: bool = False, max_clients: int = 50
) -> TSD[int, TS[HttpResponse]]:
    """
    The client adaptor is responsible for making HTTP requests to a remote server. This implementation is able to
    support NTLM and Kerberos authentication.
    """
    if use_curl:
        AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient", max_clients=max_clients)

    logger.info("Starting client adaptor on path: '%s'", path)

    @push_queue(TSD[int, TS[HttpResponse]])
    def from_web(sender, path: str = "http_client") -> TSD[int, TS[HttpResponse]]:
        GlobalState.instance()[f"http_client_adaptor://{path}/queue"] = sender
        return None

    async def handle_auth_win(response, request, client):
        import win32security
        import sspi
        import sspicon
        import pywintypes
        import sspicon

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
            logger.info(f"Skipping canonicalization of name {host} due to error: {error}")

        targetspn = f"HTTP/{host}"
        scflags = sspicon.ISC_REQ_MUTUAL_AUTH

        pkg_info = win32security.QuerySecurityPackageInfo(scheme)
        clientauth = sspi.ClientAuth(
            scheme, targetspn=targetspn, auth_info=None, scflags=scflags, datarep=sspicon.SECURITY_NETWORK_DREP
        )
        sec_buffer = win32security.PySecBufferDescType()

        # handling HTTPS connection will need peercert handling here

        set_cookie = response.headers.get("set-cookie")
        if set_cookie is not None:
            response.request.headers["Cookie"] = set_cookie

        try:
            err, auth = clientauth.authorize(sec_buffer)
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
                    challenge = [v[len(scheme) + 1 :] for val in final.split(",") if scheme in (v := val.strip())]
                    if len(challenge) > 1:
                        raise HTTPError(401, f"Received more than one {scheme} challenge from server")

                    tokenbuf = win32security.PySecBufferType(pkg_info["MaxToken"], sspicon.SECBUFFER_TOKEN)
                    tokenbuf.Buffer = base64.b64decode(challenge[0])
                    sec_buffer.append(tokenbuf)
                    err, auth = clientauth.authorize(sec_buffer)
                    logger.debug(
                        "Kerberos Authentication succeeded - error=%s authenticated=%s", err, clientauth.authenticated
                    )
                except TypeError:
                    pass

            return response2

        set_cookie = response2.headers.get("set-cookie")
        if set_cookie is not None:
            response2.request.headers["Cookie"] = set_cookie

        challenge = [
            v[len(scheme) + 1 :]
            for val in response2.headers.get("WWW-Authenticate", "").split(",")
            if scheme in (v := val.strip())
        ]
        if len(challenge) > 1:
            raise HTTPError(401, f"Received more than one {scheme} challenge from server")
        elif len(challenge) == 0:
            import re

            base64_pattern = r"(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?"
            matches = re.findall(base64_pattern, final)
            if matches:
                challenge = [matches[0]]
            else:
                raise HTTPError(401, f"Could not find any {scheme} challenge in WWW-Authenticate header: {final}")

        tokenbuf = win32security.PySecBufferType(pkg_info["MaxToken"], sspicon.SECBUFFER_TOKEN)
        tokenbuf.Buffer = base64.b64decode(challenge[0])
        sec_buffer.append(tokenbuf)

        try:
            err, auth = clientauth.authorize(sec_buffer)
            data = base64.b64encode(auth[0].Buffer).decode("ASCII")
            response2.request.headers["Authorization"] = f"{scheme} {data}"
        except pywintypes.error as error:
            logger.error("Error calling %s: %s", error[1], error[2], exc_info=error)
            return response2

        response3 = await client.fetch(response2.request, raise_error=False)

        return response3

    async def handle_auth(response, request, client):
        import spnego

        auth_header = response.headers.get("www-authenticate")
        if not auth_header:
            raise HTTPError(401, "missing www-authenticate header")

        # Parse auth header to handle case when both NTLM and Negotiate are present
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
                    401, "NTLM Authentication on non-windows hosts is not supported without supplying credentials"
                )
        else:
            raise HTTPError(401, "unhandled protocol")

        parsed_url = urlparse(response.request.url)
        host = parsed_url.hostname
        try:
            info = socket.getaddrinfo(host, None, 0, 0, 0, socket.AI_CANONNAME)
            host = info[0][3]
        except socket.gaierror as error:
            logger.info(f"Skipping canonicalization of name {host} due to error: {error}")

        ctx = spnego.client(
            username=username,
            password=password,
            hostname=host,
            service="HTTP",
            channel_bindings=None,
            context_req=spnego.ContextReq.sequence_detect | spnego.ContextReq.mutual_auth,
            protocol=protocol,
        )

        for _ in range(2):
            auth_req = re.search(f"{scheme}\\s*([^,]*)", auth_header, re.I)
            if auth_req is None:
                raise HTTPError(401, "No auth token found")

            gss_r = ctx.step(in_token=base64.b64decode(auth_req[1]))
            response.request.headers["Authorization"] = f"{scheme} {base64.b64encode(gss_r).decode()}"

            # handling HTTPS connection will need peercert handling here

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
                            # Try to find any Base64-encoded token in the header
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
            else:
                response = response2
                auth_header = response.headers.get("www-authenticate")

        raise HTTPError(401, f"Kerberos Authentication failed: {response}")

    async def make_http_request(id: int, request: HttpRequest, sender: Callable):
        start_time = time.perf_counter_ns()
        try:
            client = AsyncHTTPClient(force_instance=True)
            if request.query:
                url = f"{request.url}?{urlencode(request.query)}"
            else:
                url = request.url

            log_url = authorization_match.sub("'Authorization': '[REDACTED]'", url)

            timeouts = {"connect_timeout": request.connect_timeout, "request_timeout": request.request_timeout}
            if isinstance(request, HttpGetRequest):
                logger.debug("[GET][%i][%s]", id, log_url)
                response = await client.fetch(url, method="GET", headers=request.headers, raise_error=False, **timeouts)
            elif isinstance(request, HttpPostRequest):
                logger.debug("[POST][%i][%s] body: %s", id, log_url, request.body)
                response = await client.fetch(
                    url, method="POST", headers=request.headers, body=request.body, raise_error=False, **timeouts
                )
            elif isinstance(request, HttpPutRequest):
                logger.debug("[PUT][%i][%s] body: %s", id, log_url, request.body)
                response = await client.fetch(
                    url, method="PUT", headers=request.headers, body=request.body, raise_error=False, **timeouts
                )
            elif isinstance(request, HttpDeleteRequest):
                logger.debug("[DELETE][%i][%s]", id, log_url)
                response = await client.fetch(
                    url, method="DELETE", headers=request.headers, raise_error=False, **timeouts
                )
            else:
                logger.error("Bad request received: %s", request)
                response = namedtuple("HttpResponse_", ["code", "headers", "body"])(
                    400, fd(), b"Incorrect request type provided"
                )

            if response.code == 401 and response.headers.get("WWW-Authenticate") is not None:
                logger.debug("[AUTH] requesting authentication")
                try:
                    if os.name == "nt":
                        response = await handle_auth_win(response, request, client)
                    else:
                        response = await handle_auth(response, request, client)
                except HTTPError as e:
                    logger.error("[AUTH] authentication failed: %s", e)
                    sender({id: HttpResponse(status_code=e.code, body=e.message.encode())})
                    return

        except Exception as e:
            logger.error("request %i failed : %s", id, e)
            sender({id: HttpResponse(status_code=400, body=str(e).encode())})
            return

        logger.info("request %i succeeded in %i ms", id, int((time.perf_counter_ns() - start_time) / 1000000))
        sender({id: HttpResponse(status_code=response.code, headers=response.headers, body=response.body)})

    @sink_node
    def to_web(request: TSD[int, TS[HttpRequest]]):
        sender = GlobalState.instance()[f"http_client_adaptor://{path}/queue"]

        for i, r in request.modified_items():
            TornadoWeb.get_loop().add_callback(make_http_request, i, r.value, sender)

    @to_web.start
    def to_web_start():
        TornadoWeb.start_loop()

    @to_web.stop
    def to_web_stop():
        TornadoWeb.stop_loop()

    to_web(request)
    return from_web()
