"""Arrow control flow — port of upstream ``hgraph.arrow._control_flow``.

Dialect notes: the ``if_then`` input-shape validation is structural (pair
detection) rather than type-metadata-based; the never-ticking ``otherwise``
branch types its ``nothing`` source from the then-branch port (which, unlike
upstream, is not pruned from the false branch — harmless dead wiring)."""
from typing import Callable, Mapping

import hgraph as hg

from .._wiring import WiringPort, _unwrap, wire
from ._arrow import _Arrow, arrow, i, make_pair, _pair_elements

__all__ = ["if_", "if_then", "fb", "switch_", "map_", "reduce"]


class if_:
    """``if_(condition).then(fn1).otherwise(fn2)`` — see upstream README."""

    def __init__(self, condition):
        self.condition = arrow(condition, __name__=f"if_({condition})")

    def then(self, then_fn):
        then_fn = arrow(then_fn)
        return if_then(then_fn, __if__=self.condition,
                       __name__=f"{self.condition}.then({then_fn})")


class if_then:
    """Consumes a pair of (TS[bool], value); applies ``then``/``otherwise``
    to the value according to the condition."""

    def __init__(self, then_fn, __if__=None, __name__=None):
        self._if = __if__
        self.__name__ = __name__ or f"if_then({then_fn})"
        self.then_fn = arrow(then_fn, __name__=self.__name__)

    def __rshift__(self, other):
        return self.otherwise(None) >> other

    def __call__(self, pair):
        return self.otherwise(None)(pair)

    def otherwise(self, else_fn, __name__=None):
        then_otherwise = arrow(
            _IfThenOtherwise(self.then_fn, else_fn),
            __name__=__name__ or f"{self.then_fn}.otherwise({__name__ or else_fn or 'None'})",
        )
        if self._if is not None:
            return arrow(self._if / i >> then_otherwise, __name__=str(then_otherwise))
        return then_otherwise


class _IfThenOtherwise:

    def __init__(self, then_fn, else_fn):
        self.then_fn = then_fn
        self.else_fn = else_fn

    def __call__(self, pair):
        if _pair_elements(pair) is None:
            raise TypeError(
                "if_then requires a pair input of (TS[bool], TIME_SERIES_TYPE)")
        then_fn_ = self.then_fn
        else_fn_ = self.else_fn
        switches = {True: lambda v: then_fn_(v)}
        if else_fn_ is not None:
            switches[False] = lambda v: else_fn_(v)
        else:
            # Type the never-ticking branch from the then result (a lambda:
            # the wired-fn bridge accepts lambdas and decorated graphs only).
            switches[False] = (
                lambda v: wire("nothing", output_type=_unwrap(then_fn_(v)).ts_type))
        return hg.switch_(pair[0], switches, pair[1])


_FB_CACHE = None


def _get_fb_cache():
    global _FB_CACHE
    if _FB_CACHE is None:
        _FB_CACHE = {}
    return _FB_CACHE


class fb:
    """``fb["label": TS[int], "default": 0]`` initiates a feedback (emitting
    a pair of input and feedback value); ``fb["label"]`` consumes into it."""

    def __class_getitem__(cls, item):
        fb_cache = _get_fb_cache()
        item = item if isinstance(item, tuple) else (item,)
        first = item[0]
        label = first if (is_str := isinstance(first, str)) else first.start
        if is_str:

            @arrow(__name__=f"fb[{item}]")
            def _feedback_wrapper(x):
                if (f := fb_cache.get(label)) is None:
                    raise ValueError(f"No feedback registered for label: {label}")
                del fb_cache[label]
                f(x)
                return x

            return _feedback_wrapper

        tp = first.stop
        d, p = _extract_fb_items(item[1:])

        @arrow(__name__=f"fb[{item}]")
        def _feedback_wrapper(x):
            fb_cache[label] = (f := hg.feedback(tp, d))
            v = hg.gate(hg.modified(x), f(), -1) if p else f()
            return make_pair(x, v)

        return _feedback_wrapper

    def __init__(self):
        raise TypeError(
            "fb cannot be instantiated, use fb['label': <Type>] to declare a feedback")


def _extract_fb_items(item):
    p = True
    d = None
    for entry in item:
        if entry.start == "passive":
            p = entry.stop
        elif entry.start == "default":
            d = entry.stop
    return d, p


def switch_(options: Mapping, __name__=None):
    """Pattern-matched flow: the pair's first selects the branch, the second
    is the branch input."""

    @arrow(__name__=__name__ or f"switch_({list(options.keys())})")
    def wrapper(pair):
        return hg.switch_(pair[0], options, pair[1])

    return wrapper


def map_(fn):
    """Apply the arrow ``fn`` to each element of a TSL/TSD collection."""

    @arrow(__name__=f"map_({fn})")
    def wrapper(collection):
        return hg.map_(lambda x: fn(x), collection)

    return wrapper


def reduce(fn, zero, is_associative: bool = True):
    """Reduce a TSL/TSD collection with an arrow function over pairs."""
    fn = arrow(fn)

    @arrow(__name__=f"reduce({fn})")
    def wrapper(x):
        return hg.reduce(lambda lhs, rhs: fn(make_pair(lhs, rhs)), x, zero,
                         is_associative=is_associative)

    return wrapper
