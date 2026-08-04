from inspect import cleandoc
from textwrap import indent

from hgraph.numpy_._constants import ARRAY

__all__ = ["extract_type_from_array", "extract_dimensions_from_array", "add_docs"]


def extract_type_from_array(a: type[ARRAY]) -> type:
    return a.__args__[0]


def extract_dimensions_from_array(a: ARRAY) -> tuple[int, ...]:
    return tuple(a.SIZE for a in a.__args__[1:])


def add_docs(from_fn):
    """
    Copies the wrapped function's documentation onto the node.

    The original documentation is emitted as a literal block. The functions being wrapped are third-party
    (numpy), so their doc-strings are written in numpydoc rather than reStructuredText; parsing them inline
    produces a page full of "Unexpected section title" errors and drags their doctests into our test run.
    Quoting them keeps the content intact and leaves the rendering well-formed.
    """

    def _add_docs(to_fn):
        original = indent(cleandoc(from_fn.__doc__ or ""), "    ")
        to_fn.__doc__ = (
            f"Wraps the function: '**{from_fn.__name__}**' as a node.\n"
            "\n"
            "Below is the original documentation of the function, see the main function signature for"
            " the time-series types expected (and supported inputs).\n"
            "\n"
            "**Original documentation:**\n"
            "\n"
            "::\n"
            "\n"
            f"{original}\n"
        )
        return to_fn

    return _add_docs
