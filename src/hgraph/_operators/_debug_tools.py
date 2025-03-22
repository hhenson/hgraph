from hgraph._wiring import operator
from hgraph._types._time_series_types import TIME_SERIES_TYPE

__all__ = ("DebugContext", "debug_print")


class DebugContext:
    """
    A mechanism to support leaving useful debug code in place but will occur no runtime cost if the debug logic is not
    requested. This will only wire in the logic when the debug flag is set to True. By default, it is set to False.

    An additional prefix can be supplied to the context, this will be pre-pended to all debug prints to provide
    additional context.
    """

    _CURRENT: "DebugContext" = None

    def __init__(self, prefix: str = "", debug: bool = True):
        self.prefix = prefix
        self.debug = debug
        self._previous_context: "DebugContext" = None

    def __enter__(self):
        self._previous_context = DebugContext._CURRENT
        DebugContext._CURRENT = self

    def __exit__(self, exc_type, exc_val, exc_tb):
        DebugContext._CURRENT = self._previous_context

    @staticmethod
    def instance() -> "DebugContext":
        return DebugContext._CURRENT

    @staticmethod
    def print(
        label: str,
        ts: TIME_SERIES_TYPE,
        print_delta: bool = True,
        sample: int = -1,
    ):
        """If debug is True, this will call debug_print with the supplied parameters"""
        if (self := DebugContext.instance()) is not None and self.debug:
            space = " " if self.prefix and not label.startswith("[") else ""
            debug_print(f"{self.prefix}{space}{label}", ts, print_delta=print_delta, sample=sample)


@operator
def debug_print(label: str, ts: TIME_SERIES_TYPE, print_delta: bool = True, sample: int = -1):
    """
    Use this to help debug code, this will print the value of the supplied time-series to the standard out.
    It will include the engine time in the print. Do not leave these lines in production code.

    :param label: The label to print before the value
    :param ts: The time-series to print
    :param print_delta: If true, print the delta value, otherwise print the value
    :param sample: Only print an output for every sample number of ticks.
    """
