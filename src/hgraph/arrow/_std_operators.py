import hgraph
from hgraph import OUT
from hgraph.arrow import arrow

__all__ = (
    "eq_", "lt_", "gt_", "le_", "ge_", "ne_", "and_", "or_", "not_", "add_", "sub_", "mul_", "div_", "mod_", "pow_",
    "floordiv_", "neg_", "pos_", "abs_", "const_", "apply_", "binary_op"
)


def binary_op(fn):
    """
    A simple arrow wrapper of an HGraph function that takes two args as input.
    Useful to wrap binary operators.
    """
    _f = lambda pair: fn(pair[0], pair[1])
    return arrow(_f)


"""
The basic operators as per Python.
"""

@arrow
def eq_(pair):
    return pair[0] == pair[1]


@arrow
def lt_(pair):
    return pair[0] < pair[1]


@arrow
def gt_(pair):
    return pair[0] > pair[1]


@arrow
def le_(pair):
    return pair[0] <= pair[1]


@arrow
def ge_(pair):
    return pair[0] >= pair[1]


@arrow
def ne_(pair):
    return pair[0] != pair[1]


@arrow
def and_(pair):
    return hgraph.and_(pair[0], pair[1])


@arrow
def or_(pair):
    return hgraph.or_(pair[0], pair[1])


@arrow
def not_(pair):
    return hgraph.not_(pair[0])


@arrow
def add_(pair):
    return pair[0] + pair[1]


@arrow
def sub_(pair):
    return pair[0] - pair[1]


@arrow
def mul_(pair):
    return pair[0] * pair[1]


@arrow
def div_(pair):
    return pair[0] / pair[1]


@arrow
def mod_(pair):
    return pair[0] % pair[1]


@arrow
def pow_(pair):
    return pair[0] ** pair[1]


@arrow
def floordiv_(pair):
    return pair[0] // pair[1]


@arrow
def neg_(pair):
    return -pair[0]


@arrow
def pos_(pair):
    return +pair[0]


@arrow
def abs_(pair):
    return abs(pair[0])


def const_(first, second=None, tp=None):
    """
    Inject a constant value into the arrow flow.
    This takes much the same for of eval_, except this is considered
    part of the evaluation flow. This effectively creates a collection of const
    values.

    Usage:

    ::

        ... >> i / const_(1) >> ...

    In the above pattern we expand the flow to include a second with the
    const value.
    """
    from hgraph.arrow._arrow import _build_inputs
    return arrow(
        lambda _, first_=first, second_=second, tp_=tp: _build_inputs(first_, second=second_, type_map=tp_),
        __name__=f"{first}" if second is None else f"{first}, {second}",
    )


def apply_(tp: OUT):
    """
    Applies the function in the first element to the value in the second element.
    The tp is the output type of the function.
    """
    from hgraph import apply

    return arrow(lambda pair, tp_=tp: apply[tp_](pair[0], pair[1]))
