from hgraph.numpy._constants import ARRAY

__all__ = ["extract_type_from_array", "extract_dimensions_from_array"]


def extract_type_from_array(a: type[ARRAY]) -> type:
    return a.__args__[0]


def extract_dimensions_from_array(a: ARRAY) -> tuple[int, ...]:
    return tuple(a.SIZE for a in a.__args__[1:])
