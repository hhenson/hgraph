"""Compatibility boundary for the retired private inspector value walker.

Released hgraph's public ``inspector()`` implementation imported this module,
but the function itself accepts live runtime objects and was never a public
debug export.  hg_cpp renders values inside ``GraphDiagnostics`` callbacks so
the Python presentation never retains or re-walks those objects.
"""


def inspector_read_value(*_args, **_kwargs):
    raise RuntimeError(
        "inspector_read_value() is private runtime-object machinery; "
        "use inspector() or GraphDiagnostics(capture_values=True)"
    )
