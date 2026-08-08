from hgraph._types import _ArrayType


def add_docs(from_fn):
    """Copy a wrapped NumPy callable's documentation onto a wiring callable."""
    def decorate(to_fn):
        to_fn.__doc__ = (
            f"Wraps the function: '**{from_fn.__name__}**' as a node.\n\n"
            "Below is the original documentation of the function.\n\n"
            f"**Original documentation:**\n\n{from_fn.__doc__}"
        )
        return to_fn

    return decorate


def extract_type_from_array(array_type):
    if not isinstance(array_type, _ArrayType):
        raise TypeError(f"expected an Array annotation, got {array_type!r}")
    return array_type.element


def extract_dimensions_from_array(array_type) -> tuple[int, ...]:
    if not isinstance(array_type, _ArrayType):
        raise TypeError(f"expected an Array annotation, got {array_type!r}")
    return array_type.dimensions


__all__ = ("extract_type_from_array", "extract_dimensions_from_array", "add_docs")
