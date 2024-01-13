import itertools
from typing import TypeVar

__all__ = ("clone_typevar", "nth", "take")


def clone_typevar(tp: TypeVar, name: str) -> TypeVar:
    """Creates a copy of a typevar and sets the name to the copies name"""
    if tp.__constraints__:
        rv = TypeVar(name, *tp.__constraints__, covariant=tp.__covariant__, contravariant=tp.__contravariant__)
    else:
        rv = TypeVar(name, bound=tp.__bound__, covariant=tp.__covariant__,
                 contravariant=tp.__contravariant__)
    return rv


class Sentinel:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f"Sentinel({self.name})"

    def __str__(self):
        return self.name


def nth(iterable, n):
    """Very trivial implementation of nth, to avoid the need to import more-itertools"""
    return next(itertools.islice(iterable, n, n+1))


def take(n, iterable):
    """Very trivial implementation of take, to avoid the need to import more-itertools"""
    return itertools.islice(iterable, n)