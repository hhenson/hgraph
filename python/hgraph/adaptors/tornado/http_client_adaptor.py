"""The released HTTP client surface, wired to native libcurl by hgraph-web."""

try:
    from hgraph_web.compat_client import (
        Credentials,
        http_client_adaptor,
        http_client_adaptor_impl,
    )
except ModuleNotFoundError as error:
    if error.name != "hgraph_web":
        raise
    raise ModuleNotFoundError(
        "hgraph.adaptors.tornado sends HTTP through the optional 'hgraph-web' "
        "distribution; install it with `pip install hgraph-web`",
        name="hgraph_web",
    ) from error

__all__ = ("Credentials", "http_client_adaptor", "http_client_adaptor_impl")
