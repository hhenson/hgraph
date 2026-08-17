"""Compatibility surface for the C++-first web transports.

The implementation is supplied by the optional :mod:`hgraph_web`
extension. Keeping this shim in the core distribution avoids two wheels
installing files into the same ``hgraph`` package.

The released :mod:`hgraph.adaptors.tornado` package is unchanged and keeps
working without the extension installed; the authoring names re-exported
here land with the extension's service surface (RFC 0024).
"""

try:
    import hgraph_web as _hgraph_web  # noqa: F401
except ModuleNotFoundError as error:
    if error.name != "hgraph_web":
        raise
    raise ModuleNotFoundError(
        "hgraph.adaptors.web requires the optional 'hgraph-web' "
        "distribution; install it with `pip install hgraph-web` and use "
        "`import hgraph_web` for new code",
        name="hgraph_web",
    ) from error

__all__ = ()
