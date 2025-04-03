from functools import partial
from typing import Generic, Callable

from hgraph import switch_, feedback, lag, default, sample, gate, modified
from hgraph.arrow import null
from hgraph.arrow._arrow import _Arrow, A, B, arrow, i, _make_tuple

__all__ = ["if_", "if_then", "fb"]


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
            _IfThenOtherwise(self.then_fn, else_fn),
            __name__=__name__ or f"{self.then_fn}.otherwise({__name__ or else_fn})",
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


_FB_CACHE = None


def _get_fb_cache():
    global _FB_CACHE
    if _FB_CACHE is None:
        _FB_CACHE = {}
    return _FB_CACHE


class fb:
    """
    A feed-back function, when the label is used initially, it registers
    a feed-back object, called it will instantiate the feed-back.
    This can only be used at the same layer in the graph, i.e. it can't be
    used in a higher-order arrow function unless it was also initiated in the
    function supplied.

    For now assume that once the passiveness is set it is sticky.
    The initial state is feedbacks are passive.

    The first time a feedback is used it must declare its type.
    This is done using:

    ::

        fb["my_label": TS[int]]

    """

    def __class_getitem__(cls, item):
        fb_cache = _get_fb_cache()
        item = item if isinstance(item, tuple) else (item,)
        first = item[0]
        label = first if (is_str := isinstance(first, str)) else first.start
        if is_str:
            # If f is defined then assume we are now applying the value
            # Here, fan_out, use i for left and then return just left

            @arrow(__name__=f"fb[{item}]")
            def _feedback_wrapper(x):
                if (f := fb_cache.get(label)) is None:
                    raise ValueError(f"No feedback registered for label: {label}")
                del fb_cache[label]
                f(x)
                return x

            return _feedback_wrapper
        else:
            tp = first.stop
            d, p = _extract_fb_items(item[1:])
            from hgraph._wiring import passive

            @arrow(__name__=f"fb[{item}]")
            def _feedback_wrapper(x):
                fb_cache[label] = (f := feedback(tp, default=d))
                v = gate(modified(x), f(), -1) if p else f()
                return _make_tuple(x, v)

            return _feedback_wrapper

    def __init__(self):
        raise TypeError("fb cannot be instantiated, use fb['label': <Type>] to declare a feedback")


def _extract_fb_items(item):
    p = True
    d = None
    for i in item:
        if i.start == "passive":
            p = i.stop
        elif i.start == "default":
            d = i.stop
    return d, p
