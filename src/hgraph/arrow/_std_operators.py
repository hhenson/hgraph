from hgraph import OUT
from hgraph.arrow import arrow

__all__ = (
    "const_", "apply_", "c"
)

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

c = const_  # Use c when it will not be confusing.

def apply_(tp: OUT):
    """
    Applies the function in the first element to the value in the second element.
    The tp is the output type of the function.
    """
    from hgraph import apply

    return arrow(lambda pair, tp_=tp: apply[tp_](pair[0], pair[1]))
