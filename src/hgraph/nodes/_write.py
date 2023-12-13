from hgraph import sink_node, TS
from hgraph.nodes import format_ts

# NOTE: When using un-typed arguments such as present below, it is not possible to use the @graph decorate (or any of
#       the node decorators). Instead, we use a raw Python function and then perform the actions to produce
#       code that can be correctly typed.


__all__ = ("write", "write_str")


def write(format_str: str, *args, **kwargs):
    """
    A sink node that will write the formatted string to the std out.
    This should be generally be used for debugging purposes and not be present in production code, instead use the
    log nodes for writing in a production context.

    :param format_str: The format string as defined in format
    :param args: The time-series enumerated inputs
    :param kwargs: The named time-series inputs
    """
    write_str(format_ts(format_str, *args, **kwargs))


@sink_node
def write_str(ts: TS[str]):
    """
    A sink node that will write the string time-series to the standard out.

    :param ts: The string to write to the std out.
    """
    print(ts.value)
