from datetime import date

from hgraph import compute_node, TS, TSL, Size, explode, graph, day_of_month, month_of_year, year

__all__ = []


@compute_node(overloads=explode)
def explode_date_impl(ts: TS[date], _output: TSL[TS[int], Size[3]] = None) -> TSL[TS[int], Size[3]]:
    if _output.valid:
        out = {}
        dt = ts.value
        day, month, year = dt.day, dt.month, dt.year
        if _output[2].value != day:
            out[2] = day
        if _output[1].value != month:
            out[1] = month
        if _output[0].value != year:
            out[0] = year
        return out
    else:
        dt = ts.value
        return (dt.year, dt.month, dt.day)


@graph(overloads=day_of_month)
def day_of_month_impl(ts: TS[date]) -> TS[int]:
    """The day of the moth of the given date."""
    return explode(ts)[2]


@graph(overloads=month_of_year)
def month_of_year_impl(ts: TS[date]) -> TS[int]:
    """The month of the year of the given date."""
    return explode(ts)[1]


@graph(overloads=year)
def year_impl(ts: TS[date]) -> TS[int]:
    """The year of the given date."""
    return explode(ts)[0]
