from typing import TypeVar, Callable


__all__ = ("map",)


SWITCH_SIGNATURE = TypeVar("SWITCH_SIGNATURE", bound=Callable)


def map(func: SWITCH_SIGNATURE, **kwargs):
    """

    """