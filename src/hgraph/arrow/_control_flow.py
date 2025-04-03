from typing import Generic, Callable

from hgraph import switch_
from hgraph.arrow import null
from hgraph.arrow._arrow import _Arrow, A, B, arrow, i

__all__ = ["if_", "if_then"]


class if_:
    """
    If some condition, then, otherwise, for example:

    ::

        eval_([1, 2]) | if_( i // const_(2) >> gt ).then(lambda x: x+1).otherwise(lambda x: 1-x)
    """

    def __init__(self, condition: _Arrow[A, bool]):
        self.condition = arrow(condition, __name__=f"if_({condition})")

    def then(self, then_fn: Callable[[A], B]) -> "if_then[A, B]":
        then_fn = arrow(then_fn)
        return if_then(then_fn, __if__=self.condition, __name__=f"{self.condition}.then({then_fn})")


class if_then:
    """
    Consumes the first of the tuple (must be a TS[bool]) and provides the second item to the resultant conditions
    of then and otherwise. The result of then or otherwise is returned.

    The usage is:

    ::

        eval_(True, 1) | if_then(lambda x: x*2).otherwise(lambda x: x+2)
    """

    def __init__(self, then_fn: Callable[[A], B], __if__=None, __name__=None):
        self._if = __if__
        self.then_fn = arrow(then_fn, __name__=__name__ or f"if_then({then_fn})")

    def __rshift__(self, other: _Arrow[A, B]) -> B:
        # If we chain before we are done, then call otherwise with null and continue
        return self.otherwise(null) >> other

    def __call__(self, pair):
        # If we are called then we need to finalise and go
        self.otherwise(null)(pair)

    def otherwise(self, else_fn: Callable[[A], B], __name__=None) -> B:

        then_otherwise = arrow(
            _IfThenOtherwise(
                self.then_fn,
                else_fn
            ),
            __name__=__name__ or f"{self.then_fn}.otherwise({__name__ or else_fn})"
        )

        if self._if is not None:
            return arrow(self._if / i >> then_otherwise, __name__=str(then_otherwise))
        else:
            return then_otherwise


class _IfThenOtherwise:

    def __init__(self, then_fn, else_fn):
        self.then_fn = then_fn
        self.else_fn = else_fn

    def __call__(self, pair):
        then_fn_ = self.then_fn
        else_fn_ = self.else_fn
        return switch_(
            pair[0],
            {
                True: lambda v: then_fn_(v),
                False: lambda v: else_fn_(v),
            },
            pair[1],
        )
