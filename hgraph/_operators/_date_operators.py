from datetime import date

from hgraph._wiring._decorators import operator
from hgraph._types import TS, TSL, Size

__all__ = ("day_of_month", "month_of_year", "year", "explode")


@operator
def day_of_month(ts: TS[date]) -> TS[int]:
    """The day of the moth of the given date."""


@operator
def month_of_year(ts: TS[date]) -> TS[int]:
    """The month of the year of the given date."""


@operator
def year(ts: TS[date]) -> TS[int]:
    """The year of the given date."""


@operator
def explode(ts: TS[date]) -> TSL[TS[int], Size[3]]:
    """The year, month and day of the given date."""
