from hgraph import TS


__all__ = ("format_ts",)


def format_ts(format_str: str, *args, **kwargs) -> TS[str]:
    """
    Writes the contents of the time-series values provided (in args / kwargs) to a string using the format string
    provided. The kwargs will be used as named inputs to the format string and args as enumerated args.
    :param format_str: A standard python format string (using {}). When converted to C++ this will use the c++ fmt
                       specifications.
    :param args: Time series args
    :param kwargs: Time series kwargs
    :return:
    """
    raise NotImplementedError()