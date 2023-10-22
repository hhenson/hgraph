from hg import generator, SCALAR, TIME_SERIES_TYPE, ExecutionContext, MIN_TD


@generator
def replay(values: SCALAR, tp: type[TIME_SERIES_TYPE], context: ExecutionContext) -> TIME_SERIES_TYPE:
    """
    This will replay a sequence of values, a None value will be ignored (skip the tick).
    The type of the elements of the sequence must be a delta value of the time series type.

    # TODO: At some point it would be useful to support a time-indexed collection of values to provide
    # More complex replay scenarios.
    """
    count = 1
    for value in values:
        next_engine_time = context.current_engine_time + MIN_TD * count
        if value is None:
            count += 1
            continue
        yield next_engine_time, value
        count = 1
