"""Generated typing declarations for operators exposed lazily by ``hgraph``.

Each overload, default, variadic tail, keyword-only boundary, and output
comes from native registry metadata. Regenerate with
``tools/api_inventory.py``; runtime dispatch remains registry-owned."""

from __future__ import annotations
# mypy: disable-error-code="overload-cannot-match,overload-overlap"

from datetime import (date as _date, datetime as _datetime, time as _time,
                      timedelta as _timedelta)
from typing import (Any as _Any, Callable as _Callable, Protocol as _Protocol,
                    Self as _Self, overload as _overload)

from _hgraph import (AmbiguousTimePolicy as _AmbiguousTimePolicy,
                     CivilDateRange as _CivilDateRange,
                     CivilDateTime as _CivilDateTime, InstantRange as _InstantRange,
                     MonthEndPolicy as _MonthEndPolicy,
                     NonexistentTimePolicy as _NonexistentTimePolicy,
                     Period as _Period, ZoneId as _ZoneId,
                     ZonedDateTime as _ZonedDateTime)
from ._compat import CmpResult as _CmpResult, DivideByZero as _DivideByZero
from ._wiring import WiringPort as _WiringPort

class _abs__Operator(_Protocol):
    """``abs_`` — the ``abs`` operator (``abs(ts) -> OUT``).

    Accepted native overloads:

    - ``abs_(ts: TS[int]) -> TS[int]``
    - ``abs_(ts: TS[float]) -> TS[float]``
    - ``abs_(ts: TS[timedelta]) -> TS[timedelta]``
    - ``abs_(ts: TSL[TIME_SERIES_TYPE, SIZE]) -> OUT``
    - ``abs_(ts: TIME_SERIES_TYPE) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | _timedelta) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

abs_: _abs__Operator

class _add__Operator(_Protocol):
    """``add_`` — the ``+`` operator. Operands and result may all differ (``lhs + rhs -> OUT``).

    Accepted native overloads:

    - ``add_(lhs: TS[int], rhs: TS[int]) -> TS[int]``
    - ``add_(lhs: TS[float], rhs: TS[float]) -> TS[float]``
    - ``add_(lhs: TS[str], rhs: TS[str]) -> TS[str]``
    - ``add_(lhs: TS[timedelta], rhs: TS[timedelta]) -> TS[timedelta]``
    - ``add_(lhs: TS[int], rhs: TS[float]) -> TS[float]``
    - ``add_(lhs: TS[float], rhs: TS[int]) -> TS[float]``
    - ``add_(lhs: TS[datetime], rhs: TS[timedelta]) -> TS[datetime]``
    - ``add_(lhs: TS[timedelta], rhs: TS[datetime]) -> TS[datetime]``
    - ``add_(lhs: TS[date], rhs: TS[timedelta]) -> TS[date]``
    - ``add_(lhs: TS[period], rhs: TS[period]) -> TS[period]``
    - ``add_(lhs: TS[civil_datetime], rhs: TS[timedelta]) -> TS[civil_datetime]``
    - ``add_(lhs: TS[date], rhs: TS[time]) -> TS[civil_datetime]``
    - ``add_(lhs: TS[zoned_datetime], rhs: TS[timedelta]) -> TS[zoned_datetime]``
    - ``add_(lhs: TS[timedelta], rhs: TS[zoned_datetime]) -> TS[zoned_datetime]``
    - ``add_(lhs: TS[date], rhs: TS[period], month_end_policy: month_end_policy = ...) -> TS[date]``
    - ``add_(lhs: TS[civil_datetime], rhs: TS[period], month_end_policy: month_end_policy = ...) -> TS[civil_datetime]``
    - ``add_(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``add_(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TIME_SERIES_TYPE_1) -> OUT``
    - ``add_(lhs: TIME_SERIES_TYPE, rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``add_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE_1) -> OUT``
    - ``add_(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[SCALAR]``
    - ``add_(lhs: TS[SCALAR], rhs: TS[SCALAR], __strict__: bool = ...) -> OUT``
    - ``add_(lhs: TS[SCALAR], rhs: TS[SCALAR_1]) -> OUT``
    - ``add_(lhs: TSS[K], rhs: TS[K]) -> TSS[K]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | str, rhs: _WiringPort | str) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _timedelta, rhs: _WiringPort | _timedelta) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _datetime, rhs: _WiringPort | _timedelta) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _timedelta, rhs: _WiringPort | _datetime) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _date, rhs: _WiringPort | _timedelta) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _Period, rhs: _WiringPort | _Period) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _CivilDateTime, rhs: _WiringPort | _timedelta) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _date, rhs: _WiringPort | _time) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _ZonedDateTime, rhs: _WiringPort | _timedelta) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _timedelta, rhs: _WiringPort | _ZonedDateTime) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _date, rhs: _WiringPort | _Period, month_end_policy: _MonthEndPolicy = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _CivilDateTime, rhs: _WiringPort | _Period, month_end_policy: _MonthEndPolicy = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object, __strict__: bool = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

add_: _add__Operator

class _all__Operator(_Protocol):
    """``all_`` — graph ``all``: ``True`` when every boolean input is ``True`` (variadic).

    Accepted native overloads:

    - ``all_(*args: TS[bool]) -> TS[bool]``
    - ``all_(arg: TSD[K, TS[bool]]) -> TS[bool]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, *args: _WiringPort | bool) -> _WiringPort: ...
    @_overload
    def __call__(self, arg: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

all_: _all__Operator

class _and__Operator(_Protocol):
    """``and_`` — the ``and`` operator (truthy combination), yielding ``TS<Bool>``.

    Accepted native overloads:

    - ``and_(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[bool]``
    - ``and_(lhs: TS[bool], rhs: TS[bool]) -> TS[bool]``
    - ``and_(lhs: TS[int], rhs: TS[int]) -> TS[bool]``
    - ``and_(lhs: TS[float], rhs: TS[float]) -> TS[bool]``
    - ``and_(lhs: TS[str], rhs: TS[str]) -> TS[bool]``
    - ``and_(lhs: TS[int], rhs: TS[float]) -> TS[bool]``
    - ``and_(lhs: TS[float], rhs: TS[int]) -> TS[bool]``
    - ``and_(lhs: TSS[K], rhs: TSS[K]) -> TS[bool]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | bool, rhs: _WiringPort | bool) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | str, rhs: _WiringPort | str) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | int) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

and_: _and__Operator

class _any__Operator(_Protocol):
    """``any_`` — graph ``any``: ``True`` when any boolean input is ``True`` (variadic).

    Accepted native overloads:

    - ``any_(*args: TS[bool]) -> TS[bool]``
    - ``any_(arg: TSD[K, TS[bool]]) -> TS[bool]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, *args: _WiringPort | bool) -> _WiringPort: ...
    @_overload
    def __call__(self, arg: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

any_: _any__Operator

class _apply_Operator(_Protocol):
    """``apply`` — invoke a ticking runtime callable and publish its result.

    Accepted native overloads:

    - ``apply(fn: TS[callable], *args: TIME_SERIES_TYPE, **kwargs: time-series) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, fn: _WiringPort | object, *args: _WiringPort | object, **kwargs: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

apply: _apply_Operator

class _as_array_Operator(_Protocol):
    """Convert a fixed tick window into a shaped array, optionally padding with ``zero``.

    Accepted native overloads:

    - ``as_array(tsw: TIME_SERIES_TYPE) -> OUT``
    - ``as_array(tsw: TIME_SERIES_TYPE, zero: TIME_SERIES_TYPE_1) -> OUT``
    - ``as_array(tsw: TIME_SERIES_TYPE, zero: SCALAR) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, tsw: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, tsw: _WiringPort | object, zero: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, tsw: _WiringPort | object, zero: object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

as_array: _as_array_Operator

class _assert__Operator(_Protocol):
    """``assert_`` — assert ``condition`` holds, raising ``error_msg`` otherwise (a sink).

    Accepted native overloads:

    - ``assert_(condition: TS[bool], error_msg: str) -> None``
    - ``assert_(condition: TS[bool], error_msg: str, *args: TIME_SERIES_TYPE, **kwargs: time-series) -> None``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, condition: _WiringPort | bool, error_msg: str) -> None: ...
    @_overload
    def __call__(self, condition: _WiringPort | bool, error_msg: str, *args: _WiringPort | object, **kwargs: _WiringPort | object) -> None: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

assert_: _assert__Operator

class _at_zone_Operator(_Protocol):
    """``at_zone`` — represent an instant in the supplied time zone.

    Accepted native overloads:

    - ``at_zone(instant: TS[datetime], zone: TS[zone_id]) -> TS[zoned_datetime]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, instant: _WiringPort | _datetime, zone: _WiringPort | _ZoneId) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

at_zone: _at_zone_Operator

class _batch_Operator(_Protocol):
    """``batch`` — like ``gate`` but releases queued ticks in batches with ``delay`` between them.

    Accepted native overloads:

    - ``batch(condition: TS[bool], ts: TIME_SERIES_TYPE, delay: timedelta, buffer_length: int = ...) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, condition: _WiringPort | bool, ts: _WiringPort | object, delay: _timedelta, buffer_length: int = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

batch: _batch_Operator

class _bit_and_Operator(_Protocol):
    """``bit_and`` — the ``&`` operator (``lhs & rhs -> OUT``).

    Accepted native overloads:

    - ``bit_and(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[SCALAR]``
    - ``bit_and(lhs: TS[int], rhs: TS[int]) -> TS[int]``
    - ``bit_and(lhs: TS[bool], rhs: TS[bool]) -> TS[bool]``
    - ``bit_and(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``bit_and(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TIME_SERIES_TYPE_1) -> OUT``
    - ``bit_and(lhs: TIME_SERIES_TYPE, rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``bit_and(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE_1) -> OUT``
    - ``bit_and(*ts: TIME_SERIES_TYPE) -> OUT``
    - ``bit_and(lhs: TSD[K, V], rhs: TSD[K, V]) -> TSD[K, V]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | bool, rhs: _WiringPort | bool) -> _WiringPort: ...
    @_overload
    def __call__(self, *ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

bit_and: _bit_and_Operator

class _bit_or_Operator(_Protocol):
    """``bit_or`` — the ``|`` operator (``lhs | rhs -> OUT``).

    Accepted native overloads:

    - ``bit_or(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[SCALAR]``
    - ``bit_or(lhs: TS[int], rhs: TS[int]) -> TS[int]``
    - ``bit_or(lhs: TS[bool], rhs: TS[bool]) -> TS[bool]``
    - ``bit_or(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``bit_or(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TIME_SERIES_TYPE_1) -> OUT``
    - ``bit_or(lhs: TIME_SERIES_TYPE, rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``bit_or(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE_1) -> OUT``
    - ``bit_or(*ts: TIME_SERIES_TYPE) -> OUT``
    - ``bit_or(lhs: TSD[K, V], rhs: TSD[K, V]) -> TSD[K, V]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | bool, rhs: _WiringPort | bool) -> _WiringPort: ...
    @_overload
    def __call__(self, *ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

bit_or: _bit_or_Operator

class _bit_xor_Operator(_Protocol):
    """``bit_xor`` — the ``^`` operator (``lhs ^ rhs -> OUT``).

    Accepted native overloads:

    - ``bit_xor(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[SCALAR]``
    - ``bit_xor(lhs: TS[int], rhs: TS[int]) -> TS[int]``
    - ``bit_xor(lhs: TS[bool], rhs: TS[bool]) -> TS[bool]``
    - ``bit_xor(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``bit_xor(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TIME_SERIES_TYPE_1) -> OUT``
    - ``bit_xor(lhs: TIME_SERIES_TYPE, rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``bit_xor(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE_1) -> OUT``
    - ``bit_xor(*ts: TIME_SERIES_TYPE) -> OUT``
    - ``bit_xor(lhs: TSD[K, V], rhs: TSD[K, V]) -> TSD[K, V]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | bool, rhs: _WiringPort | bool) -> _WiringPort: ...
    @_overload
    def __call__(self, *ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

bit_xor: _bit_xor_Operator

class _call_Operator(_Protocol):
    """``call`` — invoke a ticking runtime callable for side effects.

    Accepted native overloads:

    - ``call(fn: TS[callable], *args: TIME_SERIES_TYPE, **kwargs: time-series) -> None``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, fn: _WiringPort | object, *args: _WiringPort | object, **kwargs: _WiringPort | object) -> None: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

call: _call_Operator

class _clip_Operator(_Protocol):
    """``clip`` — clip ``ts`` into the ``[min, max]`` range.

    Accepted native overloads:

    - ``clip(ts: TS[float], min: float, max: float) -> TS[float]``
    - ``clip(ts: TS[int], min: int, max: int) -> TS[int]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | float, min: float, max: float) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | int, min: int, max: int) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

clip: _clip_Operator

class _cmp__Operator(_Protocol):
    """``cmp_`` — three-way comparison; returns ``LT`` / ``EQ`` / ``GT`` in one step.

    Accepted native overloads:

    - ``cmp_(lhs: TS[int], rhs: TS[int]) -> TS[CmpResult]``
    - ``cmp_(lhs: TS[float], rhs: TS[float]) -> TS[CmpResult]``
    - ``cmp_(lhs: TS[str], rhs: TS[str]) -> TS[CmpResult]``
    - ``cmp_(lhs: TS[date], rhs: TS[date]) -> TS[CmpResult]``
    - ``cmp_(lhs: TS[datetime], rhs: TS[datetime]) -> TS[CmpResult]``
    - ``cmp_(lhs: TS[timedelta], rhs: TS[timedelta]) -> TS[CmpResult]``
    - ``cmp_(lhs: TS[bool], rhs: TS[bool]) -> TS[CmpResult]``
    - ``cmp_(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[CmpResult]``
    - ``cmp_(lhs: TS[int], rhs: TS[float]) -> TS[CmpResult]``
    - ``cmp_(lhs: TS[float], rhs: TS[int]) -> TS[CmpResult]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | str, rhs: _WiringPort | str) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _date, rhs: _WiringPort | _date) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _datetime, rhs: _WiringPort | _datetime) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _timedelta, rhs: _WiringPort | _timedelta) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | bool, rhs: _WiringPort | bool) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | int) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

cmp_: _cmp__Operator

class _collapse_keys_Operator(_Protocol):
    """``collapse_keys`` — flatten nested TSD keys into tuple keys.

    Accepted native overloads:

    - ``collapse_keys(ts: TIME_SERIES_TYPE) -> OUT``
    - ``collapse_keys(ts: TSD[K, V]) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

collapse_keys: _collapse_keys_Operator

class _combine_cs_Operator(_Protocol):
    """``combine_cs`` — assemble a compound-scalar (Bundle) value from field ports (the runtime half of ``combine[TS[CS]](field=...)``).

    Accepted native overloads:

    - ``combine_cs(ts: TIME_SERIES_TYPE) -> OUT``
    - ``combine_cs(ts: TIME_SERIES_TYPE, __strict__: bool) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, __strict__: bool) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

combine_cs: _combine_cs_Operator

class _combine_json_Operator(_Protocol):
    """Dynamic-JSON tree operators (design record: parity_matrix.rst, ruling 2026-07-06 — the tree is a C++ value; python is sugar).

    Accepted native overloads:

    - ``combine_json(**kwargs: time-series) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, **kwargs: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

combine_json: _combine_json_Operator

class _combine_map_Operator(_Protocol):
    """``combine_map`` — build a mapping value from key and value time-series.

    Accepted native overloads:

    - ``combine_map(keys: TIME_SERIES_TYPE, values: TIME_SERIES_TYPE_1) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, keys: _WiringPort | object, values: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

combine_map: _combine_map_Operator

class _combine_tsd_Operator(_Protocol):
    """``combine_tsd`` — build a TSD from keys + element ports (hgraph's combine[TSD] family; TSD.from_ts wires this).

    Accepted native overloads:

    - ``combine_tsd(keys: TIME_SERIES_TYPE, values: TIME_SERIES_TYPE_1, __strict__: bool = ...) -> OUT``
    - ``combine_tsd(keys: SCALAR, values: TIME_SERIES_TYPE, __strict__: bool = ...) -> OUT``
    - ``combine_tsd(keys: TIME_SERIES_TYPE, values: TIME_SERIES_TYPE_1) -> OUT``
    - ``combine_tsd(keys: SCALAR, *values: V, __strict__: bool = ...) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, keys: _WiringPort | object, values: _WiringPort | object, __strict__: bool = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, keys: object, values: _WiringPort | object, __strict__: bool = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, keys: _WiringPort | object, values: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, keys: object, *values: _WiringPort | object, __strict__: bool = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

combine_tsd: _combine_tsd_Operator

class _combine_tss_from_tsl_Operator(_Protocol):
    """Packed-TSL kernel behind combine[TSS](a, b, ...).

    Accepted native overloads:

    - ``combine_tss_from_tsl(ts: TSL[TS[SCALAR], SIZE]) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

combine_tss_from_tsl: _combine_tss_from_tsl_Operator

class _compare_Operator(_Protocol):
    """``compare`` — the backtesting comparison sink (COMPARE mode): records per-tick equality of ``lhs`` vs ``rhs`` through the registered frame store (P6) under ``fq_recordable_id.__compare__``.

    Accepted native overloads:

    - ``compare(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE, recordable_id: str) -> None``
    - ``compare(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE, recordable_id: str = ...) -> None``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object, recordable_id: str) -> None: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object, recordable_id: str = ...) -> None: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

compare: _compare_Operator

class _concat_Operator(_Protocol):
    """``concat(ts1, ts2)`` — append rows from two frames with the same schema.

    Accepted native overloads:

    - ``concat(ts1: TS[Frame[SCALAR]], ts2: TS[Frame[SCALAR]]) -> TS[Frame[SCALAR]]``
    - ``concat(ts1: TS[Frame[SCALAR, SCALAR_1]], ts2: TS[Frame[SCALAR, SCALAR_1]]) -> TS[Frame[SCALAR, SCALAR_1]]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts1: _WiringPort | object, ts2: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

concat: _concat_Operator

class _const_Operator(_Protocol):
    """``const_`` — a source that emits a configured ``value`` once at the start cycle, or ``delay`` after it. The output type is the registered ``TS`` of the value's type, or an explicit output schema at the wiring site (``wire<const_, TSS<Int>>(w, set_value)``). Two arities: ``const(value)`` (tick at start) and ``const(value, delay)`` (tick at ``start_time + delay``).

    Accepted native overloads:

    - ``const(value: SCALAR) -> OUT``
    - ``const(value: SCALAR, delay: timedelta) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, value: object) -> _WiringPort: ...
    @_overload
    def __call__(self, value: object, delay: _timedelta) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

const: _const_Operator

class _contains__Operator(_Protocol):
    """``contains_`` — the ``in`` operator: ``item in ts`` -> ``TS<Bool>``.

    Accepted native overloads:

    - ``contains_(ts: TS[str], item: TS[str]) -> TS[bool]``
    - ``contains_(ts: TSS[K], item: TS[K]) -> TS[bool]``
    - ``contains_(ts: TSS[K], item: TSS[K]) -> TS[bool]``
    - ``contains_(ts: TSD[K, V], item: TS[K]) -> TS[bool]``
    - ``contains_(ts: TIME_SERIES_TYPE, item: TIME_SERIES_TYPE_1) -> TS[bool]``
    - ``contains_(ts: TS[SCALAR], item: TS[SCALAR_1]) -> TS[bool]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | str, item: _WiringPort | str) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, item: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

contains_: _contains__Operator

class _convert_zone_Operator(_Protocol):
    """``convert_zone`` — view a zoned datetime in another zone without changing its instant.

    Accepted native overloads:

    - ``convert_zone(value: TS[zoned_datetime], zone: TS[zone_id]) -> TS[zoned_datetime]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, value: _WiringPort | _ZonedDateTime, zone: _WiringPort | _ZoneId) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

convert_zone: _convert_zone_Operator

class _corrcoef_Operator(_Protocol):
    """Native correlation coefficients, with an optional second array.

    Accepted native overloads:

    - ``corrcoef(x: TIME_SERIES_TYPE) -> OUT``
    - ``corrcoef(x: TIME_SERIES_TYPE, y: TIME_SERIES_TYPE_1) -> OUT``
    - ``corrcoef(x: TIME_SERIES_TYPE, rowvar: bool) -> OUT``
    - ``corrcoef(x: TIME_SERIES_TYPE, y: TIME_SERIES_TYPE_1, rowvar: bool) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, x: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, x: _WiringPort | object, y: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, x: _WiringPort | object, rowvar: bool) -> _WiringPort: ...
    @_overload
    def __call__(self, x: _WiringPort | object, y: _WiringPort | object, rowvar: bool) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

corrcoef: _corrcoef_Operator

class _count_Operator(_Protocol):
    """``count`` — a running count of the ticks of ``ts`` (optional ``reset`` signal).

    Accepted native overloads:

    - ``count(ts: SIGNAL) -> TS[int]``
    - ``count(ts: SIGNAL, reset: SIGNAL) -> TS[int]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort, reset: _WiringPort) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

count: _count_Operator

class _cumsum_Operator(_Protocol):
    """Native cumulative sum. The optional scalar axis is supplied as a second argument.

    Accepted native overloads:

    - ``cumsum(a: TIME_SERIES_TYPE) -> OUT``
    - ``cumsum(a: TIME_SERIES_TYPE, axis: int) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, a: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, a: _WiringPort | object, axis: int) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

cumsum: _cumsum_Operator

class _day_Operator(_Protocol):
    """``day`` — the day-of-month attribute of a date or datetime.

    Accepted native overloads:

    - ``day(ts: TS[date]) -> TS[int]``
    - ``day(ts: TS[datetime]) -> TS[int]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | _date) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | _datetime) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

day: _day_Operator

class _day_of_month_Operator(_Protocol):
    """``day_of_month`` — the day-of-month of a ``TS<Date>``.

    Accepted native overloads:

    - ``day_of_month(ts: TS[date]) -> TS[int]``
    - ``day_of_month(ts: TS[datetime]) -> TS[int]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | _date) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | _datetime) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

day_of_month: _day_of_month_Operator

class _days_Operator(_Protocol):
    """hgraph's timedelta ATTRIBUTES (port.days / .seconds / .microseconds) and ``total_seconds()`` — issue #82. Python's normalization: ``days`` floors toward -inf; ``seconds`` / ``microseconds`` are the non-negative remainders.

    Accepted native overloads:

    - ``days(ts: TS[timedelta]) -> TS[int]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | _timedelta) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

days: _days_Operator

class _debug_print_Operator(_Protocol):
    """``debug_print`` — print ``label: value`` on each tick of ``ts`` (a diagnostic sink). (Python also takes ``print_delta`` / ``sample`` — not yet modelled.)

    Accepted native overloads:

    - ``debug_print(label: str, ts: TIME_SERIES_TYPE, sample: int = ...) -> None``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, label: str, ts: _WiringPort | object, sample: int = ...) -> None: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

debug_print: _debug_print_Operator

class _dedup_Operator(_Protocol):
    """``dedup`` — drop consecutive duplicate values.

    Accepted native overloads:

    - ``dedup(ts: TS[SCALAR]) -> TS[SCALAR]``
    - ``dedup(ts: TS[float], abs_tol: TS[float] = ...) -> TS[float]``
    - ``dedup(ts: TSD[K, V]) -> OUT``
    - ``dedup(ts: TSS[K]) -> TSS[K]``
    - ``dedup(ts: TSL[TIME_SERIES_TYPE, SIZE]) -> OUT``
    - ``dedup(ts: TIME_SERIES_TYPE) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | float, abs_tol: _WiringPort | float = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

dedup: _dedup_Operator

class _default_Operator(_Protocol):
    """``default_`` — pass ``ts`` through, substituting ``default_value`` while ``ts`` is invalid.

    Accepted native overloads:

    - ``default(ts: TIME_SERIES_TYPE, default_value: TIME_SERIES_TYPE) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object, default_value: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

default: _default_Operator

class _dereference_Operator(_Protocol):
    """``dereference`` — materialize ``REF[TSB[...]]`` or ``REF[TSL[...]]`` as the same container shape whose children reference the corresponding children of the referenced container.

    Accepted native overloads:

    - ``dereference(tsb: REF[TIME_SERIES_TYPE]) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, tsb: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

dereference: _dereference_Operator

class _diff_Operator(_Protocol):
    """``diff`` — the difference between the current and previous value of ``ts``.

    Accepted native overloads:

    - ``diff(ts: TS[int]) -> TS[int]``
    - ``diff(ts: TS[float]) -> TS[float]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | float) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

diff: _diff_Operator

class _difference_Operator(_Protocol):
    """``difference_`` — set difference (``lhs`` minus the rest).

    Accepted native overloads:

    - ``difference(*ts: TIME_SERIES_TYPE) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, *ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

difference: _difference_Operator

class _div__Operator(_Protocol):
    """``div_`` — the ``/`` (true division) operator (``lhs / rhs -> OUT``). Implementations may take an optional ``Scalar<"divide_by_zero", DivideByZero>`` wiring-time policy.

    Accepted native overloads:

    - ``div_(lhs: TS[int], rhs: TS[int]) -> TS[float]``
    - ``div_(lhs: TS[float], rhs: TS[float]) -> TS[float]``
    - ``div_(lhs: TS[int], rhs: TS[float]) -> TS[float]``
    - ``div_(lhs: TS[float], rhs: TS[int]) -> TS[float]``
    - ``div_(lhs: TS[int], rhs: TS[int], divide_by_zero: DivideByZero = ...) -> TS[float]``
    - ``div_(lhs: TS[timedelta], rhs: TS[int]) -> TS[timedelta]``
    - ``div_(lhs: TS[timedelta], rhs: TS[float]) -> TS[timedelta]``
    - ``div_(lhs: TS[float], rhs: TS[float], divide_by_zero: DivideByZero = ...) -> TS[float]``
    - ``div_(lhs: TS[int], rhs: TS[float], divide_by_zero: DivideByZero = ...) -> TS[float]``
    - ``div_(lhs: TS[float], rhs: TS[int], divide_by_zero: DivideByZero = ...) -> TS[float]``
    - ``div_(lhs: TS[timedelta], rhs: TS[timedelta]) -> TS[float]``
    - ``div_(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``div_(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TIME_SERIES_TYPE_1) -> OUT``
    - ``div_(lhs: TIME_SERIES_TYPE, rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``div_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE_1) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int, divide_by_zero: _DivideByZero = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _timedelta, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _timedelta, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | float, divide_by_zero: _DivideByZero = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | float, divide_by_zero: _DivideByZero = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | int, divide_by_zero: _DivideByZero = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _timedelta, rhs: _WiringPort | _timedelta) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

div_: _div__Operator

class _divmod__Operator(_Protocol):
    """``divmod_`` — the ``divmod`` operator. Result is a 2-element list ``(quotient, remainder)``.

    Accepted native overloads:

    - ``divmod_(lhs: TS[int], rhs: TS[int]) -> TSL[TS[int], 2]``
    - ``divmod_(lhs: TS[float], rhs: TS[float]) -> TSL[TS[float], 2]``
    - ``divmod_(lhs: TS[int], rhs: TS[float]) -> TSL[TS[float], 2]``
    - ``divmod_(lhs: TS[float], rhs: TS[int]) -> TSL[TS[float], 2]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | int) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

divmod_: _divmod__Operator

class _downcast__Operator(_Protocol):
    """``downcast_`` — downcast a ``TS`` value to a (checked) derived type.

    Accepted native overloads:

    - ``downcast_(ts: TIME_SERIES_TYPE) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

downcast_: _downcast__Operator

class _drop_Operator(_Protocol):
    """``drop`` — drop the first ``count`` ticks of ``ts``, then forward the rest.

    Accepted native overloads:

    - ``drop(ts: TIME_SERIES_TYPE, count: int) -> TIME_SERIES_TYPE``
    - ``drop(ts: TIME_SERIES_TYPE, period: timedelta) -> TIME_SERIES_TYPE``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | object, count: int) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, period: _timedelta) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

drop: _drop_Operator

class _eq__Operator(_Protocol):
    """``eq_`` — the ``==`` operator.

    Accepted native overloads:

    - ``eq_(lhs: TS[bool], rhs: TS[bool]) -> TS[bool]``
    - ``eq_(lhs: TS[int], rhs: TS[int]) -> TS[bool]``
    - ``eq_(lhs: TS[str], rhs: TS[str]) -> TS[bool]``
    - ``eq_(lhs: TS[date], rhs: TS[date]) -> TS[bool]``
    - ``eq_(lhs: TS[datetime], rhs: TS[datetime]) -> TS[bool]``
    - ``eq_(lhs: TS[timedelta], rhs: TS[timedelta]) -> TS[bool]``
    - ``eq_(lhs: TS[float], rhs: TS[float], epsilon: float = ...) -> TS[bool]``
    - ``eq_(lhs: TS[int], rhs: TS[float], epsilon: float = ...) -> TS[bool]``
    - ``eq_(lhs: TS[float], rhs: TS[int], epsilon: float = ...) -> TS[bool]``
    - ``eq_(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> TS[bool]``
    - ``eq_(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[bool]``
    - ``eq_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE_1) -> TS[bool]``
    - ``eq_(lhs: TSS[K], rhs: TSS[K]) -> TS[bool]``
    - ``eq_(lhs: TSD[K, V], rhs: TSD[K, V]) -> TS[bool]``
    - ``eq_(lhs: TSD[K, TS[float]], rhs: TSD[K, TS[float]], epsilon: TS[float]) -> TS[bool]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | bool, rhs: _WiringPort | bool) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | str, rhs: _WiringPort | str) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _date, rhs: _WiringPort | _date) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _datetime, rhs: _WiringPort | _datetime) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _timedelta, rhs: _WiringPort | _timedelta) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | float, epsilon: float = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | float, epsilon: float = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | int, epsilon: float = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object, epsilon: _WiringPort | float) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

eq_: _eq__Operator

class _evaluation_time_in_range_Operator(_Protocol):
    """``evaluation_time_in_range`` — where the evaluation time sits relative to [start, end]: LT / EQ / GT, self-scheduling at the boundaries (datetime / date / daily-recurring time overloads).

    Accepted native overloads:

    - ``evaluation_time_in_range(start_time: TS[datetime], end_time: TS[datetime]) -> TS[CmpResult]``
    - ``evaluation_time_in_range(start_time: TS[date], end_time: TS[date]) -> TS[CmpResult]``
    - ``evaluation_time_in_range(start_time: TS[time], end_time: TS[time]) -> TS[CmpResult]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, start_time: _WiringPort | _datetime, end_time: _WiringPort | _datetime) -> _WiringPort: ...
    @_overload
    def __call__(self, start_time: _WiringPort | _date, end_time: _WiringPort | _date) -> _WiringPort: ...
    @_overload
    def __call__(self, start_time: _WiringPort | _time, end_time: _WiringPort | _time) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

evaluation_time_in_range: _evaluation_time_in_range_Operator

class _ewma_Operator(_Protocol):
    """``ewma`` — an exponential moving average of ``ts`` with smoothing ``alpha``.

    Accepted native overloads:

    - ``ewma(ts: TS[float], alpha: float) -> TS[float]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | float, alpha: float) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

ewma: _ewma_Operator

class _explode_Operator(_Protocol):
    """``explode`` — split a date into a fixed list of year, month, and day.

    Accepted native overloads:

    - ``explode(ts: TS[date]) -> TSL[TS[int], 3]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | _date) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

explode: _explode_Operator

class _filter__Operator(_Protocol):
    """``filter_`` — suppress ticks of ``ts`` while ``condition`` is ``False``.

    Accepted native overloads:

    - ``filter_(condition: TS[bool], ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, condition: _WiringPort | bool, ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

filter_: _filter__Operator

class _filter_cs_Operator(_Protocol):
    """Filter a frame by the set fields of one compound scalar value.

    Accepted native overloads:

    - ``filter_cs(ts: TS[Frame[SCALAR]], predicate: TS[SCALAR_1]) -> TS[Frame[SCALAR]]``
    - ``filter_cs(ts: TS[Frame[SCALAR, SCALAR_1]], predicate: TS[SCALAR_2]) -> TS[Frame[SCALAR, SCALAR_1]]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object, predicate: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

filter_cs: _filter_cs_Operator

class _filter_frame_Operator(_Protocol):
    """Filter a frame by the currently valid fields of a structural TSB.

    Accepted native overloads:

    - ``filter_frame(ts: TS[Frame[SCALAR]], predicate: TIME_SERIES_TYPE) -> TS[Frame[SCALAR]]``
    - ``filter_frame(ts: TS[Frame[SCALAR, SCALAR_1]], predicate: TIME_SERIES_TYPE) -> TS[Frame[SCALAR, SCALAR_1]]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object, predicate: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

filter_frame: _filter_frame_Operator

class _filter_tsd_by_matches_Operator(_Protocol):
    """``filter_tsd_by_matches`` — keep the TSD entries whose per-key boolean match is TRUE (the runtime half of ``filter_by``; the match dictionary is produced by ``map_`` over the caller's expression).

    Accepted native overloads:

    - ``filter_tsd_by_matches(ts: TSD[K, V], matches: TSD[K, TS[bool]]) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object, matches: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

filter_tsd_by_matches: _filter_tsd_by_matches_Operator

class _flip_Operator(_Protocol):
    """``flip`` — swap keys and values of a dictionary.

    Accepted native overloads:

    - ``flip(ts: TIME_SERIES_TYPE) -> OUT``
    - ``flip(ts: TSD[K, TS[K_1]], unique: bool = ...) -> TSD[K_1, TS[K]]``
    - ``flip(ts: TSD[K, TS[K_1]], unique: bool = ...) -> TSD[K_1, TSS[K]]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, unique: bool = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

flip: _flip_Operator

class _flip_keys_Operator(_Protocol):
    """``flip_keys`` — invert the outer/inner keys of a nested ``TSD[K, TSD[K1, V]]``.

    Accepted native overloads:

    - ``flip_keys(ts: TIME_SERIES_TYPE) -> OUT``
    - ``flip_keys(ts: TSD[K, V]) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

flip_keys: _flip_keys_Operator

class _floordiv__Operator(_Protocol):
    """``floordiv_`` — the ``//`` (floor division) operator (``lhs // rhs -> OUT``).

    Accepted native overloads:

    - ``floordiv_(lhs: TS[int], rhs: TS[int]) -> TS[int]``
    - ``floordiv_(lhs: TS[float], rhs: TS[float]) -> TS[float]``
    - ``floordiv_(lhs: TS[int], rhs: TS[float]) -> TS[float]``
    - ``floordiv_(lhs: TS[float], rhs: TS[int]) -> TS[float]``
    - ``floordiv_(lhs: TS[int], rhs: TS[int], divide_by_zero: DivideByZero) -> TS[int]``
    - ``floordiv_(lhs: TS[float], rhs: TS[float], divide_by_zero: DivideByZero) -> TS[float]``
    - ``floordiv_(lhs: TS[int], rhs: TS[float], divide_by_zero: DivideByZero) -> TS[float]``
    - ``floordiv_(lhs: TS[float], rhs: TS[int], divide_by_zero: DivideByZero) -> TS[float]``
    - ``floordiv_(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``floordiv_(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TIME_SERIES_TYPE_1) -> OUT``
    - ``floordiv_(lhs: TIME_SERIES_TYPE, rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``floordiv_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE_1) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int, divide_by_zero: _DivideByZero) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | float, divide_by_zero: _DivideByZero) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | float, divide_by_zero: _DivideByZero) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | int, divide_by_zero: _DivideByZero) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

floordiv_: _floordiv__Operator

class _format__Operator(_Protocol):
    """``format_`` — format the supplied time-series values into a string using ``fmt`` (variadic args).

    Accepted native overloads:

    - ``format_(arg0: TS[str], *args: TIME_SERIES_TYPE, __sample__: int = ..., __strict__: bool = ..., **kwargs: time-series) -> TS[str]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, arg0: _WiringPort | str, *args: _WiringPort | object, __sample__: int = ..., __strict__: bool = ..., **kwargs: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

format_: _format__Operator

class _freeze_Operator(_Protocol):
    """``freeze`` — forward ``ts`` until ``predicate`` first holds, then passivate ``ts`` (stop forwarding).

    Accepted native overloads:

    - ``freeze(predicate: TS[bool], ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE``
    - ``freeze(predicate: callable, ts: TIME_SERIES_TYPE) -> OUT``
    - ``freeze(predicate: fn, ts: TIME_SERIES_TYPE) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, predicate: _WiringPort | bool, ts: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, predicate: _Callable[..., object], ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

freeze: _freeze_Operator

class _from_data_frame_Operator(_Protocol):
    """Data-frame convenience operators (design record: *Record/replay, tables and const_fn*, step 6): the same layout/codec machinery as ``to_table`` with a plain ``date`` column and no ``as_of``.

    ``from_data_frame[OUT](df, dt_col="date", key_col="key", value_col="value", offset=0)`` replays a frame VALUE by its date column (a pull source; TSD forms take the key from ``key_col``). ``to_data_frame(ts, ...)`` snapshots the time-series per tick into a one-tick frame whose columns come from the requested output ``Frame[Schema]``. ``group_by(ts, by)`` partitions a Frame-valued TS into ``TSD[key, TS[Frame]]`` by column name(s).

    Accepted native overloads:

    - ``from_data_frame(df: frame, dt_col: str = ..., key_col: str = ..., value_col: str = ..., offset: timedelta = ...) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, df: object, dt_col: str = ..., key_col: str = ..., value_col: str = ..., offset: _timedelta = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

from_data_frame: _from_data_frame_Operator

class _from_data_frame_batches_Operator(_Protocol):
    """Replay successively supplied frame batches without concatenating the source. Each batch must arrive no later than its first retained row.

    Accepted native overloads:

    - ``from_data_frame_batches(frames: TS[frame], dt_col: str = ..., key_col: str = ..., value_col: str = ..., offset: timedelta = ...) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, frames: _WiringPort | object, dt_col: str = ..., key_col: str = ..., value_col: str = ..., offset: _timedelta = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

from_data_frame_batches: _from_data_frame_batches_Operator

class _from_json_Operator(_Protocol):
    """``from_json`` — parse JSON text as the explicitly selected output schema.

    Accepted native overloads:

    - ``from_json(ts: TS[str]) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | str) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

from_json: _from_json_Operator

class _from_table_Operator(_Protocol):
    """``from_table`` — apply bitemporal tuple rows as deltas of the selected output schema.

    Accepted native overloads:

    - ``from_table(ts: TIME_SERIES_TYPE) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

from_table: _from_table_Operator

class _from_table_const_Operator(_Protocol):
    """``from_table_const`` — the const-evaluable read of a recorded FRAME value (the replay_const seam): extract the (last) row of a frame VALUE as the output type.

    Accepted native overloads:

    - ``from_table_const(value: frame) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, value: object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

from_table_const: _from_table_const_Operator

class _gate_Operator(_Protocol):
    """``gate`` — queue ticks while ``condition`` is ``False``, releasing them once it is ``True``.

    Accepted native overloads:

    - ``gate(condition: TS[bool], ts: TIME_SERIES_TYPE, buffer_length: int = ...) -> TIME_SERIES_TYPE``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, condition: _WiringPort | bool, ts: _WiringPort | object, buffer_length: int = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

gate: _gate_Operator

class _ge__Operator(_Protocol):
    """``ge_`` — the ``>=`` operator.

    Accepted native overloads:

    - ``ge_(lhs: TS[int], rhs: TS[int]) -> TS[bool]``
    - ``ge_(lhs: TS[float], rhs: TS[float]) -> TS[bool]``
    - ``ge_(lhs: TS[str], rhs: TS[str]) -> TS[bool]``
    - ``ge_(lhs: TS[date], rhs: TS[date]) -> TS[bool]``
    - ``ge_(lhs: TS[datetime], rhs: TS[datetime]) -> TS[bool]``
    - ``ge_(lhs: TS[timedelta], rhs: TS[timedelta]) -> TS[bool]``
    - ``ge_(lhs: TS[int], rhs: TS[float]) -> TS[bool]``
    - ``ge_(lhs: TS[float], rhs: TS[int]) -> TS[bool]``
    - ``ge_(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[bool]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | str, rhs: _WiringPort | str) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _date, rhs: _WiringPort | _date) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _datetime, rhs: _WiringPort | _datetime) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _timedelta, rhs: _WiringPort | _timedelta) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

ge_: _ge__Operator

class _get_item_Operator(_Protocol):
    """Apply a wiring-time integer or integer-tuple index to a shaped array.

    Accepted native overloads:

    - ``get_item(ts: TIME_SERIES_TYPE, idx: SCALAR) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object, idx: object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

get_item: _get_item_Operator

class _getattr__Operator(_Protocol):
    """``getattr_`` — the ``.`` (attribute access) operator: ``ts.attr``. ``attr`` is a wiring-time string; an optional ``default_value`` may be supplied by an implementation.

    Accepted native overloads:

    - ``getattr_(ts: REF[TIME_SERIES_TYPE], attr: str) -> OUT``
    - ``getattr_(ts: TIME_SERIES_TYPE, attr: str) -> OUT``
    - ``getattr_(ts: TSD[K, TIME_SERIES_TYPE], attr: str) -> OUT``
    - ``getattr_(ts: TS[SCALAR], attr: str) -> OUT``
    - ``getattr_(ts: TS[SCALAR], attr: str, default: SCALAR_1) -> OUT``
    - ``getattr_(ts: TS[Any], attr: str) -> TS[str]``
    - ``getattr_(ts: TS[COMPOUND_SCALAR], attr: str, default_value: TS[SCALAR] = ...) -> TS[SCALAR]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | object, attr: str) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, attr: str, default: object) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, attr: str, default_value: _WiringPort | object = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

getattr_: _getattr__Operator

class _getitem__Operator(_Protocol):
    """``getitem_`` — the ``[]`` operator: ``ts[key]``.

    Accepted native overloads:

    - ``getitem_(ts: TS[SCALAR], key: TS[K]) -> TS[SCALAR_1]``
    - ``getitem_(ts: TS[str], key: TS[int]) -> TS[str]``
    - ``getitem_(ts: TSL[TIME_SERIES_TYPE, SIZE], key: TS[int]) -> REF[TIME_SERIES_TYPE]``
    - ``getitem_(ts: TSD[K, V], key: TS[K]) -> REF[V]``
    - ``getitem_(ts: REF[TIME_SERIES_TYPE], key: str) -> OUT``
    - ``getitem_(ts: REF[TIME_SERIES_TYPE], key: int) -> OUT``
    - ``getitem_(ts: TIME_SERIES_TYPE, key: str) -> OUT``
    - ``getitem_(ts: TIME_SERIES_TYPE, key: int) -> OUT``
    - ``getitem_(ts: TSD[K, V], key: TSS[K]) -> OUT``
    - ``getitem_(ts: TS[SCALAR], key: TS[int]) -> OUT``
    - ``getitem_(ts: TS[SCALAR], key: int) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | object, key: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | str, key: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, key: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, key: str) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, key: int) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

getitem_: _getitem__Operator

class _group_by_Operator(_Protocol):
    """``group_by`` — partition a frame-valued time-series by one or more columns.

    Accepted native overloads:

    - ``group_by(ts: TS[SCALAR], by: SCALAR_1) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object, by: object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

group_by: _group_by_Operator

class _gt__Operator(_Protocol):
    """``gt_`` — the ``>`` operator.

    Accepted native overloads:

    - ``gt_(lhs: TS[int], rhs: TS[int]) -> TS[bool]``
    - ``gt_(lhs: TS[float], rhs: TS[float]) -> TS[bool]``
    - ``gt_(lhs: TS[str], rhs: TS[str]) -> TS[bool]``
    - ``gt_(lhs: TS[date], rhs: TS[date]) -> TS[bool]``
    - ``gt_(lhs: TS[datetime], rhs: TS[datetime]) -> TS[bool]``
    - ``gt_(lhs: TS[timedelta], rhs: TS[timedelta]) -> TS[bool]``
    - ``gt_(lhs: TS[int], rhs: TS[float]) -> TS[bool]``
    - ``gt_(lhs: TS[float], rhs: TS[int]) -> TS[bool]``
    - ``gt_(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[bool]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | str, rhs: _WiringPort | str) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _date, rhs: _WiringPort | _date) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _datetime, rhs: _WiringPort | _datetime) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _timedelta, rhs: _WiringPort | _timedelta) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

gt_: _gt__Operator

class _hour_Operator(_Protocol):
    """hgraph's datetime / time ATTRIBUTES (port.hour / .minute / .second / .microsecond) plus the datetime methods (``weekday()`` / ``isoweekday()`` / ``timestamp()``) — issue #82. The datetime overloads register under the existing ``year`` / ``month`` / ``day`` / ``weekday`` / ``isoweekday`` markers.

    Accepted native overloads:

    - ``hour(ts: TS[datetime]) -> TS[int]``
    - ``hour(ts: TS[time]) -> TS[int]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | _datetime) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | _time) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

hour: _hour_Operator

class _if__Operator(_Protocol):
    """``if_`` — route ``ts`` to a ``true`` / ``false`` bundle output by ``condition``.

    Accepted native overloads:

    - ``if_(condition: TS[bool], ts: REF[TIME_SERIES_TYPE]) -> TSB[true: REF[TIME_SERIES_TYPE], false: REF[TIME_SERIES_TYPE]]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, condition: _WiringPort | bool, ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

if_: _if__Operator

class _if_cmp_Operator(_Protocol):
    """``if_cmp`` — select ``lt`` / ``eq`` / ``gt`` according to a ``CmpResult``.

    Accepted native overloads:

    - ``if_cmp(cmp: TS[CmpResult], lt: REF[OUT], eq: REF[OUT], gt: REF[OUT]) -> REF[OUT]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, cmp: _WiringPort | _CmpResult, lt: _WiringPort | object, eq: _WiringPort | object, gt: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

if_cmp: _if_cmp_Operator

class _if_then_else_Operator(_Protocol):
    """``if_then_else`` — select ``true_value`` or ``false_value`` per ``condition``.

    Accepted native overloads:

    - ``if_then_else(condition: TS[bool], true_value: REF[TIME_SERIES_TYPE], false_value: REF[TIME_SERIES_TYPE]) -> REF[TIME_SERIES_TYPE]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, condition: _WiringPort | bool, true_value: _WiringPort | object, false_value: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

if_then_else: _if_then_else_Operator

class _if_true_Operator(_Protocol):
    """``if_true`` — tick ``True`` when ``condition`` ticks ``True`` (optional ``tick_once_only``).

    Accepted native overloads:

    - ``if_true(condition: TS[bool], tick_once_only: bool = ...) -> TS[bool]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, condition: _WiringPort | bool, tick_once_only: bool = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

if_true: _if_true_Operator

class _index_of_Operator(_Protocol):
    """``index_of`` — the index of ``item`` within ``ts`` -> ``TS<Int>``.

    Accepted native overloads:

    - ``index_of(ts: TSL[TS[SCALAR], SIZE], item: TS[SCALAR]) -> TS[int]``
    - ``index_of(ts: TS[SCALAR], item: TS[SCALAR_1]) -> TS[int]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object, item: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

index_of: _index_of_Operator

class _intersection_Operator(_Protocol):
    """``intersection_`` — set intersection of the inputs.

    Accepted native overloads:

    - ``intersection(*ts: TIME_SERIES_TYPE) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, *ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

intersection: _intersection_Operator

class _invert__Operator(_Protocol):
    """``invert_`` — the unary ``~`` (bitwise invert) operator (``~ts -> OUT``).

    Accepted native overloads:

    - ``invert_(ts: TS[int]) -> TS[int]``
    - ``invert_(ts: TS[bool]) -> TS[int]``
    - ``invert_(ts: TSL[TIME_SERIES_TYPE, SIZE]) -> OUT``
    - ``invert_(ts: TIME_SERIES_TYPE) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | bool) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

invert_: _invert__Operator

class _is_empty_Operator(_Protocol):
    """``is_empty`` — whether the time-series value is considered empty -> ``TS<Bool>``.

    Accepted native overloads:

    - ``is_empty(ts: TS[str]) -> TS[bool]``
    - ``is_empty(ts: TSS[K]) -> TS[bool]``
    - ``is_empty(ts: TSD[K, V]) -> TS[bool]``
    - ``is_empty(ts: TIME_SERIES_TYPE) -> TS[bool]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | str) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

is_empty: _is_empty_Operator

class _isoformat_Operator(_Protocol):
    """``isoformat`` — format a date, datetime, or time as an ISO 8601 string.

    Accepted native overloads:

    - ``isoformat(ts: TS[date]) -> TS[str]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | _date) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

isoformat: _isoformat_Operator

class _isoweekday_Operator(_Protocol):
    """``isoweekday`` — the ISO day of the week using Monday as one.

    Accepted native overloads:

    - ``isoweekday(ts: TS[date]) -> TS[int]``
    - ``isoweekday(ts: TS[datetime]) -> TS[int]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | _date) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | _datetime) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

isoweekday: _isoweekday_Operator

class _join_Operator(_Protocol):
    """Arrow-native equi-join. The namespace avoids colliding with the string join marker; both remain overloads in the public family.

    ``join`` — join several string time-series with ``separator`` (variadic).

    Accepted native overloads:

    - ``join(lhs: TS[Frame[SCALAR]], rhs: TS[Frame[SCALAR_1]], on: K, how: str = ..., suffix: str = ...) -> TS[Frame[OUT]]``
    - ``join(strings: TSL[TS[str], SIZE], separator: str, __strict__: bool = ...) -> TS[str]``
    - ``join(*ts: TS[str], separator: str, __strict__: bool = ...) -> OUT``
    - ``join(ts: TS[SCALAR], separator: str, __strict__: bool = ...) -> TS[str]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object, on: object, how: str = ..., suffix: str = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, strings: _WiringPort | object, separator: str, __strict__: bool = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, *ts: _WiringPort | str, separator: str, __strict__: bool = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, separator: str, __strict__: bool = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

join: _join_Operator

class _json_as_bool_Operator(_Protocol):
    """``json_as_bool`` — coerce a dynamic JSON leaf to a boolean.

    Accepted native overloads:

    - ``json_as_bool(ts: TIME_SERIES_TYPE) -> TS[bool]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

json_as_bool: _json_as_bool_Operator

class _json_as_float_Operator(_Protocol):
    """``json_as_float`` — coerce a dynamic JSON leaf to a floating-point value.

    Accepted native overloads:

    - ``json_as_float(ts: TIME_SERIES_TYPE) -> TS[float]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

json_as_float: _json_as_float_Operator

class _json_as_int_Operator(_Protocol):
    """``json_as_int`` — coerce a dynamic JSON leaf to an integer.

    Accepted native overloads:

    - ``json_as_int(ts: TIME_SERIES_TYPE) -> TS[int]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

json_as_int: _json_as_int_Operator

class _json_as_str_Operator(_Protocol):
    """``json_as_str`` — coerce a dynamic JSON leaf to a string.

    Accepted native overloads:

    - ``json_as_str(ts: TIME_SERIES_TYPE) -> TS[str]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

json_as_str: _json_as_str_Operator

class _json_decode_Operator(_Protocol):
    """``json_decode`` — parse JSON text into the dynamic JSON-tree value type.

    Accepted native overloads:

    - ``json_decode(ts: TS[str]) -> OUT``
    - ``json_decode(ts: TS[bytes]) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | str) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | bytes) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

json_decode: _json_decode_Operator

class _json_encode_Operator(_Protocol):
    """``json_encode`` — encode a dynamic JSON-tree value as JSON text.

    Accepted native overloads:

    - ``json_encode(ts: TIME_SERIES_TYPE) -> TS[str]``
    - ``json_encode(ts: TIME_SERIES_TYPE) -> TS[bytes]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

json_encode: _json_encode_Operator

class _keys__Operator(_Protocol):
    """``keys_`` — the keys of a dictionary (as a ``TSS`` / set).

    Accepted native overloads:

    - ``keys_(ts: TSD[K, V]) -> OUT``
    - ``keys_(ts: TSD[K, V]) -> TS[SCALAR]``
    - ``keys_(ts: TIME_SERIES_TYPE) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

keys_: _keys__Operator

class _lag_Operator(_Protocol):
    """``lag`` — delay delivery of ``ts`` by ``period`` ticks (or a time-delta).

    Accepted native overloads:

    - ``lag(ts: TIME_SERIES_TYPE, period: int) -> TIME_SERIES_TYPE``
    - ``lag(ts: TIME_SERIES_TYPE, period: timedelta) -> TIME_SERIES_TYPE``
    - ``lag(ts: TIME_SERIES_TYPE, period: int, proxy: SIGNAL) -> OUT``
    - ``lag(ts: TSD[K, V], period: int, proxy: SIGNAL) -> OUT``
    - ``lag(ts: TSL[V, SIZE], period: int, proxy: SIGNAL) -> OUT``
    - ``lag(ts: TIME_SERIES_TYPE, period: TS[timedelta]) -> TIME_SERIES_TYPE``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | object, period: int) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, period: _timedelta) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, period: int, proxy: _WiringPort) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, period: _WiringPort | _timedelta) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

lag: _lag_Operator

class _last_modified_date_Operator(_Protocol):
    """``last_modified_date`` — the date component of the last-modified time.

    Accepted native overloads:

    - ``last_modified_date(ts: SIGNAL) -> TS[date]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

last_modified_date: _last_modified_date_Operator

class _last_modified_time_Operator(_Protocol):
    """``last_modified_time`` — the evaluation time ``ts`` was last modified.

    Accepted native overloads:

    - ``last_modified_time(ts: SIGNAL) -> TS[datetime]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

last_modified_time: _last_modified_time_Operator

class _last_modified_wall_clock_time_Operator(_Protocol):
    """``last_modified_wall_clock_time`` — the wall-clock time ``ts`` was last modified.

    Accepted native overloads:

    - ``last_modified_wall_clock_time(ts: SIGNAL) -> TS[datetime]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

last_modified_wall_clock_time: _last_modified_wall_clock_time_Operator

class _le__Operator(_Protocol):
    """``le_`` — the ``<=`` operator.

    Accepted native overloads:

    - ``le_(lhs: TS[int], rhs: TS[int]) -> TS[bool]``
    - ``le_(lhs: TS[float], rhs: TS[float]) -> TS[bool]``
    - ``le_(lhs: TS[str], rhs: TS[str]) -> TS[bool]``
    - ``le_(lhs: TS[date], rhs: TS[date]) -> TS[bool]``
    - ``le_(lhs: TS[datetime], rhs: TS[datetime]) -> TS[bool]``
    - ``le_(lhs: TS[timedelta], rhs: TS[timedelta]) -> TS[bool]``
    - ``le_(lhs: TS[int], rhs: TS[float]) -> TS[bool]``
    - ``le_(lhs: TS[float], rhs: TS[int]) -> TS[bool]``
    - ``le_(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[bool]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | str, rhs: _WiringPort | str) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _date, rhs: _WiringPort | _date) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _datetime, rhs: _WiringPort | _datetime) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _timedelta, rhs: _WiringPort | _timedelta) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

le_: _le__Operator

class _len__Operator(_Protocol):
    """``len_`` — the ``len`` operator -> ``TS<Int>``.

    Accepted native overloads:

    - ``len_(ts: TS[SCALAR]) -> TS[int]``
    - ``len_(ts: TS[str]) -> TS[int]``
    - ``len_(ts: TSS[K]) -> TS[int]``
    - ``len_(ts: TSD[K, V]) -> TS[int]``
    - ``len_(ts: TSL[TIME_SERIES_TYPE, SIZE]) -> TS[int]``
    - ``len_(ts: TIME_SERIES_TYPE) -> TS[int]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | str) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

len_: _len__Operator

class _ln_Operator(_Protocol):
    """``ln`` — the natural logarithm of a ``TS<Float>`` value.

    Accepted native overloads:

    - ``ln(ts: TS[float]) -> TS[float]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | float) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

ln: _ln_Operator

class _log__Operator(_Protocol):
    """``log_`` — format and log the supplied values at ``level`` (a sink, with positional and named time-series arguments).

    Accepted native overloads:

    - ``log_(fmt: TS[str], *args: TIME_SERIES_TYPE, level: int = ..., sample_count: int = ..., **kwargs: time-series) -> None``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, fmt: _WiringPort | str, *args: _WiringPort | object, level: int = ..., sample_count: int = ..., **kwargs: _WiringPort | object) -> None: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

log_: _log__Operator

class _lshift__Operator(_Protocol):
    """``lshift_`` — the ``<<`` operator (``lhs << rhs -> OUT``).

    Accepted native overloads:

    - ``lshift_(lhs: TS[int], rhs: TS[int]) -> TS[int]``
    - ``lshift_(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``lshift_(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TIME_SERIES_TYPE_1) -> OUT``
    - ``lshift_(lhs: TIME_SERIES_TYPE, rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``lshift_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE_1) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

lshift_: _lshift__Operator

class _lt__Operator(_Protocol):
    """``lt_`` — the ``<`` operator.

    Accepted native overloads:

    - ``lt_(lhs: TS[int], rhs: TS[int]) -> TS[bool]``
    - ``lt_(lhs: TS[float], rhs: TS[float]) -> TS[bool]``
    - ``lt_(lhs: TS[str], rhs: TS[str]) -> TS[bool]``
    - ``lt_(lhs: TS[date], rhs: TS[date]) -> TS[bool]``
    - ``lt_(lhs: TS[datetime], rhs: TS[datetime]) -> TS[bool]``
    - ``lt_(lhs: TS[timedelta], rhs: TS[timedelta]) -> TS[bool]``
    - ``lt_(lhs: TS[int], rhs: TS[float]) -> TS[bool]``
    - ``lt_(lhs: TS[float], rhs: TS[int]) -> TS[bool]``
    - ``lt_(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[bool]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | str, rhs: _WiringPort | str) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _date, rhs: _WiringPort | _date) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _datetime, rhs: _WiringPort | _datetime) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _timedelta, rhs: _WiringPort | _timedelta) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

lt_: _lt__Operator

class _make_tsd_Operator(_Protocol):
    """Build/update one keyed TSD entry from a key and arbitrary time-series value.

    Three-input C++ wiring form with an explicit remove signal.

    Accepted native overloads:

    - ``make_tsd(key: TS[K], value: V) -> OUT``
    - ``make_tsd(key: TS[K], value: V, remove_key: TS[bool]) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, key: _WiringPort | object, value: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, key: _WiringPort | object, value: _WiringPort | object, remove_key: _WiringPort | bool) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

make_tsd: _make_tsd_Operator

class _match__Operator(_Protocol):
    """``match_`` — match a regex ``pattern`` against ``s``; result is a bundle (``is_match`` / ``groups``).

    Accepted native overloads:

    - ``match_(pattern: TS[str], s: TS[str]) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, pattern: _WiringPort | str, s: _WiringPort | str) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

match_: _match__Operator

class _max__Operator(_Protocol):
    """``max_`` — binary element-wise maximum. Collection / variadic forms are separate overloads.

    Accepted native overloads:

    - ``max_(ts: TS[SCALAR]) -> TS[SCALAR_1]``
    - ``max_(ts: TS[SCALAR], default_value: TS[SCALAR_1]) -> TS[SCALAR_1]``
    - ``max_(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[SCALAR]``
    - ``max_(ts: TS[SCALAR]) -> TS[SCALAR]``
    - ``max_(lhs: TS[SCALAR], rhs: TS[SCALAR], __strict__: bool = ...) -> TS[SCALAR]``
    - ``max_(*ts: TIME_SERIES_TYPE, __strict__: bool = ...) -> OUT``
    - ``max_(lhs: TS[int], rhs: TS[int]) -> TS[int]``
    - ``max_(lhs: TS[float], rhs: TS[float]) -> TS[float]``
    - ``max_(lhs: TS[str], rhs: TS[str]) -> TS[str]``
    - ``max_(lhs: TS[date], rhs: TS[date]) -> TS[date]``
    - ``max_(lhs: TS[datetime], rhs: TS[datetime]) -> TS[datetime]``
    - ``max_(lhs: TS[timedelta], rhs: TS[timedelta]) -> TS[timedelta]``
    - ``max_(lhs: TS[int], rhs: TS[float]) -> TS[float]``
    - ``max_(lhs: TS[float], rhs: TS[int]) -> TS[float]``
    - ``max_(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``max_(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TIME_SERIES_TYPE_1) -> OUT``
    - ``max_(lhs: TIME_SERIES_TYPE, rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``max_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE_1) -> OUT``
    - ``max_(*tsl: TS[SCALAR]) -> OUT``
    - ``max_(ts: TIME_SERIES_TYPE) -> OUT``
    - ``max_(ts: TSS[K], default_value: TS[K]) -> TS[K]``
    - ``max_(ts: TSS[K]) -> TS[K]``
    - ``max_(ts: TSD[K, TS[V]]) -> TS[V]``
    - ``max_(ts: TSL[TS[V], SIZE]) -> TS[V]``
    - ``max_(ts: TIME_SERIES_TYPE, default_value: SCALAR) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, default_value: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object, __strict__: bool = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, *ts: _WiringPort | object, __strict__: bool = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | str, rhs: _WiringPort | str) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _date, rhs: _WiringPort | _date) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _datetime, rhs: _WiringPort | _datetime) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _timedelta, rhs: _WiringPort | _timedelta) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, *tsl: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, default_value: object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

max_: _max__Operator

class _max_ts_list_Operator(_Protocol):
    """``max_ts_list`` — internal packed-list maximum used by public ``max_`` overloads.

    Accepted native overloads:

    - ``max_ts_list(tsl: TSL[TS[SCALAR], SIZE]) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, tsl: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

max_ts_list: _max_ts_list_Operator

class _mean_Operator(_Protocol):
    """``mean`` — running / element-wise mean.

    Accepted native overloads:

    - ``mean(ts: TS[SCALAR]) -> TS[SCALAR_1]``
    - ``mean(ts: TS[SCALAR], default_value: TS[SCALAR_1]) -> TS[SCALAR_1]``
    - ``mean(ts: TS[int]) -> TS[float]``
    - ``mean(ts: TS[float]) -> TS[float]``
    - ``mean(*ts: TIME_SERIES_TYPE) -> OUT``
    - ``mean(ts: TIME_SERIES_TYPE) -> OUT``
    - ``mean(ts: TSS[int]) -> TS[float]``
    - ``mean(ts: TSS[float]) -> TS[float]``
    - ``mean(ts: TSD[K, TS[int]]) -> TS[float]``
    - ``mean(ts: TSD[K, TS[float]]) -> TS[float]``
    - ``mean(ts: TSL[TS[int], SIZE]) -> TS[float]``
    - ``mean(ts: TSL[TS[float], SIZE]) -> TS[float]``
    - ``mean(lhs: TS[int], rhs: TS[int]) -> TS[float]``
    - ``mean(lhs: TS[float], rhs: TS[float]) -> TS[float]``
    - ``mean(lhs: TS[int], rhs: TS[float]) -> TS[float]``
    - ``mean(lhs: TS[float], rhs: TS[int]) -> TS[float]``
    - ``mean(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``mean(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE_1) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, default_value: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, *ts: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

mean: _mean_Operator

class _merge_Operator(_Protocol):
    """``merge`` — forward the first input that ticks in the current cycle.

    Accepted native overloads:

    - ``merge(*tsl: TIME_SERIES_TYPE) -> OUT``
    - ``merge(*tsl: TSD[K, V]) -> OUT``
    - ``merge(*tsl: TSD[K, V], disjoint: bool = ...) -> OUT``
    - ``merge(tsl: TIME_SERIES_TYPE) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, *tsl: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, *tsl: _WiringPort | object, disjoint: bool = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, tsl: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

merge: _merge_Operator

class _merge_tsd_disjoint_Operator(_Protocol):
    """Runtime half of merge(disjoint=True): leftmost-wins reference merge over a packed TSL of dictionaries.

    Accepted native overloads:

    - ``merge_tsd_disjoint(tsl: TSL[TSD[K, V], SIZE]) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, tsl: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

merge_tsd_disjoint: _merge_tsd_disjoint_Operator

class _microsecond_Operator(_Protocol):
    """``microsecond`` — the microsecond component of a datetime or time.

    Accepted native overloads:

    - ``microsecond(ts: TS[datetime]) -> TS[int]``
    - ``microsecond(ts: TS[time]) -> TS[int]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | _datetime) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | _time) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

microsecond: _microsecond_Operator

class _microseconds_Operator(_Protocol):
    """``microseconds`` — the non-negative microsecond remainder of a timedelta.

    Accepted native overloads:

    - ``microseconds(ts: TS[timedelta]) -> TS[int]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | _timedelta) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

microseconds: _microseconds_Operator

class _min__Operator(_Protocol):
    """``min_`` — binary element-wise minimum. Collection / variadic forms are separate overloads.

    Accepted native overloads:

    - ``min_(ts: TS[SCALAR]) -> TS[SCALAR_1]``
    - ``min_(ts: TS[SCALAR], default_value: TS[SCALAR_1]) -> TS[SCALAR_1]``
    - ``min_(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[SCALAR]``
    - ``min_(ts: TS[SCALAR]) -> TS[SCALAR]``
    - ``min_(lhs: TS[SCALAR], rhs: TS[SCALAR], __strict__: bool = ...) -> TS[SCALAR]``
    - ``min_(*ts: TIME_SERIES_TYPE, __strict__: bool = ...) -> OUT``
    - ``min_(lhs: TS[int], rhs: TS[int]) -> TS[int]``
    - ``min_(lhs: TS[float], rhs: TS[float]) -> TS[float]``
    - ``min_(lhs: TS[str], rhs: TS[str]) -> TS[str]``
    - ``min_(lhs: TS[date], rhs: TS[date]) -> TS[date]``
    - ``min_(lhs: TS[datetime], rhs: TS[datetime]) -> TS[datetime]``
    - ``min_(lhs: TS[timedelta], rhs: TS[timedelta]) -> TS[timedelta]``
    - ``min_(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``min_(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TIME_SERIES_TYPE_1) -> OUT``
    - ``min_(lhs: TIME_SERIES_TYPE, rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``min_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE_1) -> OUT``
    - ``min_(lhs: TS[int], rhs: TS[float]) -> TS[float]``
    - ``min_(lhs: TS[float], rhs: TS[int]) -> TS[float]``
    - ``min_(*tsl: TS[SCALAR]) -> OUT``
    - ``min_(ts: TIME_SERIES_TYPE) -> OUT``
    - ``min_(ts: TSS[K]) -> TS[K]``
    - ``min_(ts: TSS[K], default_value: TS[K]) -> TS[K]``
    - ``min_(ts: TSD[K, TS[V]]) -> TS[V]``
    - ``min_(ts: TSL[TS[V], SIZE]) -> TS[V]``
    - ``min_(ts: TIME_SERIES_TYPE, default_value: SCALAR) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, default_value: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object, __strict__: bool = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, *ts: _WiringPort | object, __strict__: bool = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | str, rhs: _WiringPort | str) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _date, rhs: _WiringPort | _date) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _datetime, rhs: _WiringPort | _datetime) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _timedelta, rhs: _WiringPort | _timedelta) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, *tsl: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, default_value: object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

min_: _min__Operator

class _min_ts_list_Operator(_Protocol):
    """Packed-TSL kernels behind the LIST-valued ``min_``/``max_`` overloads.

    Accepted native overloads:

    - ``min_ts_list(tsl: TSL[TS[SCALAR], SIZE]) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, tsl: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

min_ts_list: _min_ts_list_Operator

class _minute_Operator(_Protocol):
    """``minute`` — the minute component of a datetime or time.

    Accepted native overloads:

    - ``minute(ts: TS[datetime]) -> TS[int]``
    - ``minute(ts: TS[time]) -> TS[int]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | _datetime) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | _time) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

minute: _minute_Operator

class _mod__Operator(_Protocol):
    """``mod_`` — the ``%`` operator (``lhs % rhs -> OUT``).

    Accepted native overloads:

    - ``mod_(lhs: TS[int], rhs: TS[int]) -> TS[int]``
    - ``mod_(lhs: TS[float], rhs: TS[float]) -> TS[float]``
    - ``mod_(lhs: TS[int], rhs: TS[float]) -> TS[float]``
    - ``mod_(lhs: TS[float], rhs: TS[int]) -> TS[float]``
    - ``mod_(lhs: TS[int], rhs: TS[int], divide_by_zero: DivideByZero) -> TS[int]``
    - ``mod_(lhs: TS[float], rhs: TS[float], divide_by_zero: DivideByZero) -> TS[float]``
    - ``mod_(lhs: TS[int], rhs: TS[float], divide_by_zero: DivideByZero) -> TS[float]``
    - ``mod_(lhs: TS[float], rhs: TS[int], divide_by_zero: DivideByZero) -> TS[float]``
    - ``mod_(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``mod_(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TIME_SERIES_TYPE_1) -> OUT``
    - ``mod_(lhs: TIME_SERIES_TYPE, rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``mod_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE_1) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int, divide_by_zero: _DivideByZero) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | float, divide_by_zero: _DivideByZero) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | float, divide_by_zero: _DivideByZero) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | int, divide_by_zero: _DivideByZero) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

mod_: _mod__Operator

class _modified_Operator(_Protocol):
    """``modified`` — ``True`` in the cycle ``ts`` is modified (a live, ticking property).

    Accepted native overloads:

    - ``modified(ts: SIGNAL) -> TS[bool]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

modified: _modified_Operator

class _month_Operator(_Protocol):
    """hgraph's date ATTRIBUTES (port.month / .day / .weekday / .isoweekday).

    Accepted native overloads:

    - ``month(ts: TS[date]) -> TS[int]``
    - ``month(ts: TS[datetime]) -> TS[int]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | _date) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | _datetime) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

month: _month_Operator

class _month_of_year_Operator(_Protocol):
    """``month_of_year`` — the month-of-year of a ``TS<Date>``.

    Accepted native overloads:

    - ``month_of_year(ts: TS[date]) -> TS[int]``
    - ``month_of_year(ts: TS[datetime]) -> TS[int]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | _date) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | _datetime) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

month_of_year: _month_of_year_Operator

class _mul__Operator(_Protocol):
    """``mul_`` — the ``*`` operator (``lhs * rhs -> OUT``). (Python takes an optional ``__strict__`` flag.)

    Accepted native overloads:

    - ``mul_(lhs: TS[int], rhs: TS[int]) -> TS[int]``
    - ``mul_(lhs: TS[float], rhs: TS[float]) -> TS[float]``
    - ``mul_(lhs: TS[int], rhs: TS[float]) -> TS[float]``
    - ``mul_(lhs: TS[float], rhs: TS[int]) -> TS[float]``
    - ``mul_(lhs: TS[str], rhs: TS[int]) -> TS[str]``
    - ``mul_(lhs: TS[int], rhs: TS[str]) -> TS[str]``
    - ``mul_(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``mul_(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TIME_SERIES_TYPE_1) -> OUT``
    - ``mul_(lhs: TIME_SERIES_TYPE, rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``mul_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE_1) -> OUT``
    - ``mul_(lhs: TS[timedelta], rhs: TS[int]) -> TS[timedelta]``
    - ``mul_(lhs: TS[timedelta], rhs: TS[float]) -> TS[timedelta]``
    - ``mul_(lhs: TS[period], rhs: TS[int]) -> TS[period]``
    - ``mul_(lhs: TS[int], rhs: TS[period]) -> TS[period]``
    - ``mul_(lhs: TS[SCALAR], rhs: TS[int]) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | str, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | str) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _timedelta, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _timedelta, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _Period, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | _Period) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | int) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

mul_: _mul__Operator

class _ne__Operator(_Protocol):
    """``ne_`` — the ``!=`` operator.

    Accepted native overloads:

    - ``ne_(lhs: TS[bool], rhs: TS[bool]) -> TS[bool]``
    - ``ne_(lhs: TS[int], rhs: TS[int]) -> TS[bool]``
    - ``ne_(lhs: TS[float], rhs: TS[float]) -> TS[bool]``
    - ``ne_(lhs: TS[str], rhs: TS[str]) -> TS[bool]``
    - ``ne_(lhs: TS[date], rhs: TS[date]) -> TS[bool]``
    - ``ne_(lhs: TS[datetime], rhs: TS[datetime]) -> TS[bool]``
    - ``ne_(lhs: TS[timedelta], rhs: TS[timedelta]) -> TS[bool]``
    - ``ne_(lhs: TS[int], rhs: TS[float]) -> TS[bool]``
    - ``ne_(lhs: TS[float], rhs: TS[int]) -> TS[bool]``
    - ``ne_(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> TS[bool]``
    - ``ne_(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[bool]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | bool, rhs: _WiringPort | bool) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | str, rhs: _WiringPort | str) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _date, rhs: _WiringPort | _date) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _datetime, rhs: _WiringPort | _datetime) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _timedelta, rhs: _WiringPort | _timedelta) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

ne_: _ne__Operator

class _neg__Operator(_Protocol):
    """``neg_`` — the unary ``-`` operator (``-ts -> OUT``).

    Accepted native overloads:

    - ``neg_(ts: TS[int]) -> TS[int]``
    - ``neg_(ts: TS[float]) -> TS[float]``
    - ``neg_(ts: TS[timedelta]) -> TS[timedelta]``
    - ``neg_(ts: TS[period]) -> TS[period]``
    - ``neg_(ts: TSL[TIME_SERIES_TYPE, SIZE]) -> OUT``
    - ``neg_(ts: TIME_SERIES_TYPE) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | _timedelta) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | _Period) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

neg_: _neg__Operator

class _not__Operator(_Protocol):
    """``not_`` — the unary ``not`` operator, yielding ``TS<Bool>``.

    Accepted native overloads:

    - ``not_(ts: TS[bool]) -> TS[bool]``
    - ``not_(ts: TS[int]) -> TS[bool]``
    - ``not_(ts: TS[float]) -> TS[bool]``
    - ``not_(ts: TS[str]) -> TS[bool]``
    - ``not_(ts: TSS[K]) -> TS[bool]``
    - ``not_(ts: TSD[K, V]) -> TS[bool]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | bool) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | str) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

not_: _not__Operator

class _nothing_Operator(_Protocol):
    """``nothing`` — a source that never ticks, of the requested output type.

    Accepted native overloads:

    - ``nothing() -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

nothing: _nothing_Operator

class _np_std_Operator(_Protocol):
    """Population/sample standard deviation over a numeric shaped array.

    Accepted native overloads:

    - ``np_std(ts: TIME_SERIES_TYPE) -> TS[float]``
    - ``np_std(ts: TIME_SERIES_TYPE, ddof: int) -> TS[float]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, ddof: int) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

np_std: _np_std_Operator

class _null_sink_Operator(_Protocol):
    """``null_sink`` — consume ``ts`` and do nothing (a terminal sink).

    Accepted native overloads:

    - ``null_sink(ts: TIME_SERIES_TYPE) -> None``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object) -> None: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

null_sink: _null_sink_Operator

class _or__Operator(_Protocol):
    """``or_`` — the ``or`` operator, yielding ``TS<Bool>``.

    Accepted native overloads:

    - ``or_(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[bool]``
    - ``or_(lhs: TS[bool], rhs: TS[bool]) -> TS[bool]``
    - ``or_(lhs: TS[int], rhs: TS[int]) -> TS[bool]``
    - ``or_(lhs: TS[float], rhs: TS[float]) -> TS[bool]``
    - ``or_(lhs: TS[str], rhs: TS[str]) -> TS[bool]``
    - ``or_(lhs: TS[int], rhs: TS[float]) -> TS[bool]``
    - ``or_(lhs: TS[float], rhs: TS[int]) -> TS[bool]``
    - ``or_(lhs: TSS[K], rhs: TSS[K]) -> TS[bool]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | bool, rhs: _WiringPort | bool) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | str, rhs: _WiringPort | str) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | int) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

or_: _or__Operator

class _partition_Operator(_Protocol):
    """``partition`` — split a ``TSD[K, V]`` into ``TSD[K1, TSD[K, V]]`` using a mapping.

    Accepted native overloads:

    - ``partition(ts: TIME_SERIES_TYPE, partitions: TIME_SERIES_TYPE_1) -> OUT``
    - ``partition(ts: TSD[K, V], partitions: TSD[K, TS[K_1]]) -> TSD[K_1, TSD[K, V]]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object, partitions: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

partition: _partition_Operator

class _pct_change_Operator(_Protocol):
    """Fractional change from the immediately preceding value.

    Accepted native overloads:

    - ``pct_change(ts: TS[SCALAR]) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

pct_change: _pct_change_Operator

class _pos__Operator(_Protocol):
    """``pos_`` — the unary ``+`` operator (``+ts -> OUT``).

    Accepted native overloads:

    - ``pos_(ts: TS[int]) -> TS[int]``
    - ``pos_(ts: TS[float]) -> TS[float]``
    - ``pos_(ts: TS[timedelta]) -> TS[timedelta]``
    - ``pos_(ts: TSL[TIME_SERIES_TYPE, SIZE]) -> OUT``
    - ``pos_(ts: TIME_SERIES_TYPE) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | _timedelta) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

pos_: _pos__Operator

class _pow__Operator(_Protocol):
    """``pow_`` — the ``**`` operator (``lhs ** rhs -> OUT``).

    Accepted native overloads:

    - ``pow_(lhs: TS[int], rhs: TS[int]) -> TS[int]``
    - ``pow_(lhs: TS[float], rhs: TS[float]) -> TS[float]``
    - ``pow_(lhs: TS[int], rhs: TS[float]) -> TS[float]``
    - ``pow_(lhs: TS[float], rhs: TS[int]) -> TS[float]``
    - ``pow_(lhs: TS[int], rhs: TS[int], divide_by_zero: DivideByZero) -> TS[int]``
    - ``pow_(lhs: TS[float], rhs: TS[float], divide_by_zero: DivideByZero) -> TS[float]``
    - ``pow_(lhs: TS[int], rhs: TS[float], divide_by_zero: DivideByZero) -> TS[float]``
    - ``pow_(lhs: TS[float], rhs: TS[int], divide_by_zero: DivideByZero) -> TS[float]``
    - ``pow_(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``pow_(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TIME_SERIES_TYPE_1) -> OUT``
    - ``pow_(lhs: TIME_SERIES_TYPE, rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``pow_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE_1) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int, divide_by_zero: _DivideByZero) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | float, divide_by_zero: _DivideByZero) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | float, divide_by_zero: _DivideByZero) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | int, divide_by_zero: _DivideByZero) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

pow_: _pow__Operator

class _print__Operator(_Protocol):
    """``print_`` — format and write the supplied values to std-out (a sink, variadic args).

    Accepted native overloads:

    - ``print_(fmt: TS[str], *args: TIME_SERIES_TYPE, __std_out__: bool = ..., **kwargs: time-series) -> None``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, fmt: _WiringPort | str, *args: _WiringPort | object, __std_out__: bool = ..., **kwargs: _WiringPort | object) -> None: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

print_: _print__Operator

class _quantile_Operator(_Protocol):
    """Native scalar quantile over an array or time-series window.

    Accepted native overloads:

    - ``quantile(a: TIME_SERIES_TYPE, q: TS[float], method: str, keepdims: bool) -> TS[float]``
    - ``quantile(a: TIME_SERIES_TYPE, q: TS[float]) -> TS[float]``
    - ``quantile(a: TIME_SERIES_TYPE, q: TS[float], method: str) -> TS[float]``
    - ``quantile(a: TIME_SERIES_TYPE, q: TS[float], keepdims: bool) -> TS[float]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, a: _WiringPort | object, q: _WiringPort | float, method: str, keepdims: bool) -> _WiringPort: ...
    @_overload
    def __call__(self, a: _WiringPort | object, q: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, a: _WiringPort | object, q: _WiringPort | float, method: str) -> _WiringPort: ...
    @_overload
    def __call__(self, a: _WiringPort | object, q: _WiringPort | float, keepdims: bool) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

quantile: _quantile_Operator

class _race_Operator(_Protocol):
    """``race`` — forward the first valid input, falling through when it invalidates.

    Accepted native overloads:

    - ``race(*ts: TIME_SERIES_TYPE) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, *ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

race: _race_Operator

class _range_adjacent_Operator(_Protocol):
    """``range_adjacent`` — test whether endpoints coincide with exactly one of them closed.

    Accepted native overloads:

    - ``range_adjacent(lhs: TS[instant_range], rhs: TS[instant_range]) -> TS[bool]``
    - ``range_adjacent(lhs: TS[civil_date_range], rhs: TS[civil_date_range]) -> TS[bool]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | _InstantRange, rhs: _WiringPort | _InstantRange) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _CivilDateRange, rhs: _WiringPort | _CivilDateRange) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

range_adjacent: _range_adjacent_Operator

class _range_contains_Operator(_Protocol):
    """``range_contains`` — test whether a temporal range contains a value or another range.

    Accepted native overloads:

    - ``range_contains(range: TS[instant_range], value: TS[datetime]) -> TS[bool]``
    - ``range_contains(range: TS[civil_date_range], value: TS[date]) -> TS[bool]``
    - ``range_contains(range: TS[instant_range], value: TS[instant_range]) -> TS[bool]``
    - ``range_contains(range: TS[civil_date_range], value: TS[civil_date_range]) -> TS[bool]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, range: _WiringPort | _InstantRange, value: _WiringPort | _datetime) -> _WiringPort: ...
    @_overload
    def __call__(self, range: _WiringPort | _CivilDateRange, value: _WiringPort | _date) -> _WiringPort: ...
    @_overload
    def __call__(self, range: _WiringPort | _InstantRange, value: _WiringPort | _InstantRange) -> _WiringPort: ...
    @_overload
    def __call__(self, range: _WiringPort | _CivilDateRange, value: _WiringPort | _CivilDateRange) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

range_contains: _range_contains_Operator

class _range_difference_Operator(_Protocol):
    """``range_difference`` — subtract the right-hand range from the left-hand range.

    Accepted native overloads:

    - ``range_difference(lhs: TS[instant_range], rhs: TS[instant_range]) -> TS[instant_range_set]``
    - ``range_difference(lhs: TS[civil_date_range], rhs: TS[civil_date_range]) -> TS[civil_date_range_set]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | _InstantRange, rhs: _WiringPort | _InstantRange) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _CivilDateRange, rhs: _WiringPort | _CivilDateRange) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

range_difference: _range_difference_Operator

class _range_extent_Operator(_Protocol):
    """``range_extent`` — return the duration between an instant range's boundaries.

    Accepted native overloads:

    - ``range_extent(range: TS[instant_range]) -> TS[timedelta]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, range: _WiringPort | _InstantRange) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

range_extent: _range_extent_Operator

class _range_hull_Operator(_Protocol):
    """``range_hull`` — return the smallest range spanning both inputs.

    Accepted native overloads:

    - ``range_hull(lhs: TS[instant_range], rhs: TS[instant_range]) -> TS[instant_range]``
    - ``range_hull(lhs: TS[civil_date_range], rhs: TS[civil_date_range]) -> TS[civil_date_range]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | _InstantRange, rhs: _WiringPort | _InstantRange) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _CivilDateRange, rhs: _WiringPort | _CivilDateRange) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

range_hull: _range_hull_Operator

class _range_intersection_Operator(_Protocol):
    """``range_intersection`` — return the common portion of two temporal ranges.

    Accepted native overloads:

    - ``range_intersection(lhs: TS[instant_range], rhs: TS[instant_range]) -> TS[instant_range]``
    - ``range_intersection(lhs: TS[civil_date_range], rhs: TS[civil_date_range]) -> TS[civil_date_range]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | _InstantRange, rhs: _WiringPort | _InstantRange) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _CivilDateRange, rhs: _WiringPort | _CivilDateRange) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

range_intersection: _range_intersection_Operator

class _range_merge_Operator(_Protocol):
    """``range_merge`` — merge two mergeable ranges into one range.

    Accepted native overloads:

    - ``range_merge(lhs: TS[instant_range], rhs: TS[instant_range]) -> TS[instant_range]``
    - ``range_merge(lhs: TS[civil_date_range], rhs: TS[civil_date_range]) -> TS[civil_date_range]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | _InstantRange, rhs: _WiringPort | _InstantRange) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _CivilDateRange, rhs: _WiringPort | _CivilDateRange) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

range_merge: _range_merge_Operator

class _range_mergeable_Operator(_Protocol):
    """``range_mergeable`` — test whether two ranges overlap or are adjacent.

    Accepted native overloads:

    - ``range_mergeable(lhs: TS[instant_range], rhs: TS[instant_range]) -> TS[bool]``
    - ``range_mergeable(lhs: TS[civil_date_range], rhs: TS[civil_date_range]) -> TS[bool]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | _InstantRange, rhs: _WiringPort | _InstantRange) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _CivilDateRange, rhs: _WiringPort | _CivilDateRange) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

range_mergeable: _range_mergeable_Operator

class _range_overlaps_Operator(_Protocol):
    """``range_overlaps`` — test whether two temporal ranges share any included instant.

    Accepted native overloads:

    - ``range_overlaps(lhs: TS[instant_range], rhs: TS[instant_range]) -> TS[bool]``
    - ``range_overlaps(lhs: TS[civil_date_range], rhs: TS[civil_date_range]) -> TS[bool]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | _InstantRange, rhs: _WiringPort | _InstantRange) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _CivilDateRange, rhs: _WiringPort | _CivilDateRange) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

range_overlaps: _range_overlaps_Operator

class _range_shift_Operator(_Protocol):
    """``range_shift`` — move both range boundaries by a duration or calendar period.

    Accepted native overloads:

    - ``range_shift(range: TS[instant_range], delta: TS[timedelta], month_end_policy: month_end_policy = ...) -> TS[instant_range]``
    - ``range_shift(range: TS[civil_date_range], delta: TS[period], month_end_policy: month_end_policy = ...) -> TS[civil_date_range]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, range: _WiringPort | _InstantRange, delta: _WiringPort | _timedelta, month_end_policy: _MonthEndPolicy = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, range: _WiringPort | _CivilDateRange, delta: _WiringPort | _Period, month_end_policy: _MonthEndPolicy = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

range_shift: _range_shift_Operator

class _range_touches_Operator(_Protocol):
    """``range_touches`` — test whether finite endpoint values coincide, regardless of openness.

    Accepted native overloads:

    - ``range_touches(lhs: TS[instant_range], rhs: TS[instant_range]) -> TS[bool]``
    - ``range_touches(lhs: TS[civil_date_range], rhs: TS[civil_date_range]) -> TS[bool]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | _InstantRange, rhs: _WiringPort | _InstantRange) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _CivilDateRange, rhs: _WiringPort | _CivilDateRange) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

range_touches: _range_touches_Operator

class _range_union_Operator(_Protocol):
    """``range_union`` — return the normalized union of two temporal ranges.

    Accepted native overloads:

    - ``range_union(lhs: TS[instant_range], rhs: TS[instant_range]) -> TS[instant_range_set]``
    - ``range_union(lhs: TS[civil_date_range], rhs: TS[civil_date_range]) -> TS[civil_date_range_set]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | _InstantRange, rhs: _WiringPort | _InstantRange) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _CivilDateRange, rhs: _WiringPort | _CivilDateRange) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

range_union: _range_union_Operator

class _record_Operator(_Protocol):
    """``record`` — record ``ts`` under ``key`` (a sink).

    Accepted native overloads:

    - ``record(ts: TIME_SERIES_TYPE, key: str = ..., sparse: bool = ...) -> None``
    - ``record(ts: TIME_SERIES_TYPE, key: str = ..., recordable_id: str = ...) -> None``
    - ``record(ts: TIME_SERIES_TYPE, key: str, recordable_id: str = ...) -> None``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | object, key: str = ..., sparse: bool = ...) -> None: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, key: str = ..., recordable_id: str = ...) -> None: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, key: str, recordable_id: str = ...) -> None: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

record: _record_Operator

class _reduce_tsd_of_bundles_with_race_Operator(_Protocol):
    """``reduce_tsd_of_bundles_with_race`` — bundle-flavoured keyed race reduction.

    Accepted native overloads:

    - ``reduce_tsd_of_bundles_with_race(tsd: TSD[K, REF[TIME_SERIES_TYPE]]) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, tsd: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

reduce_tsd_of_bundles_with_race: _reduce_tsd_of_bundles_with_race_Operator

class _reduce_tsd_with_race_Operator(_Protocol):
    """``reduce_tsd_with_race(tsd=TSD<K, REF<OUT>>) -> REF<OUT>`` — keyed race (hgraph parity; ``reduce_tsd_of_bundles_with_race`` is the same erased implementation registered under the bundle-flavoured name).

    Accepted native overloads:

    - ``reduce_tsd_with_race(tsd: TSD[K, REF[TIME_SERIES_TYPE]]) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, tsd: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

reduce_tsd_with_race: _reduce_tsd_with_race_Operator

class _rekey_Operator(_Protocol):
    """``rekey`` — re-key the input dictionary using a mapping time-series.

    Accepted native overloads:

    - ``rekey(ts: TIME_SERIES_TYPE, new_keys: K) -> OUT``
    - ``rekey(ts: TSD[K, V], new_keys: TSD[K, TS[K_1]]) -> TSD[K_1, V]``
    - ``rekey(ts: TSD[K, V], new_keys: TSD[K, TSS[K_1]]) -> TSD[K_1, V]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object, new_keys: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

rekey: _rekey_Operator

class _replace_Operator(_Protocol):
    """``replace`` — replace ``pattern`` with ``repl`` in ``s``.

    Accepted native overloads:

    - ``replace(pattern: TS[str], repl: TS[str], s: TS[str]) -> TS[str]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, pattern: _WiringPort | str, repl: _WiringPort | str, s: _WiringPort | str) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

replace: _replace_Operator

class _replay_Operator(_Protocol):
    """``replay`` — replay a recorded series for ``key`` as the requested output type (a source).

    Accepted native overloads:

    - ``replay(key: str, recordable_id: str = ...) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, key: str, recordable_id: str = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

replay: _replay_Operator

class _replay_const_Operator(_Protocol):
    """``replay_const`` — replay the const value(s) at/under ``key`` valid up to the start time.

    Accepted native overloads:

    - ``replay_const(key: str, recordable_id: str = ..., tm: datetime = ...) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, key: str, recordable_id: str = ..., tm: _datetime = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

replay_const: _replay_const_Operator

class _replay_data_frame_Operator(_Protocol):
    """Replay a canonical bitemporal table frame through the native table protocol, selecting the latest as-of revision per partition.

    Accepted native overloads:

    - ``replay_data_frame(data_frame: frame, as_of_time: datetime = ...) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, data_frame: object, as_of_time: _datetime = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

replay_data_frame: _replay_data_frame_Operator

class _request_id_Operator(_Protocol):
    """``request_id`` — a process-unique identifier allocated at node start.

    Accepted native overloads:

    - ``request_id(hash: int) -> TS[int]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, hash: int) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

request_id: _request_id_Operator

class _resample_Operator(_Protocol):
    """``resample`` — re-tick ``ts`` at ``period``, even when the input does not tick.

    Accepted native overloads:

    - ``resample(ts: TIME_SERIES_TYPE, period: timedelta) -> TIME_SERIES_TYPE``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object, period: _timedelta) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

resample: _resample_Operator

class _resolve_civil_Operator(_Protocol):
    """``resolve_civil`` — resolve a local civil time using explicit daylight-saving policies.

    Accepted native overloads:

    - ``resolve_civil(local: TS[civil_datetime], zone: TS[zone_id], ambiguous: ambiguous_time_policy = ..., nonexistent: nonexistent_time_policy = ...) -> TS[zoned_datetime]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, local: _WiringPort | _CivilDateTime, zone: _WiringPort | _ZoneId, ambiguous: _AmbiguousTimePolicy = ..., nonexistent: _NonexistentTimePolicy = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

resolve_civil: _resolve_civil_Operator

class _rolling_average_Operator(_Protocol):
    """``rolling_average`` — the trailing average of ``ts`` by tick count or duration (hgraph's window helper: ``(sum(ts) - sum(lag(ts, period)))`` over the covered tick count).

    Accepted native overloads:

    - ``rolling_average(ts: TS[SCALAR], period: int, min_window_period: int = ...) -> OUT``
    - ``rolling_average(ts: TS[SCALAR], period: timedelta, min_window_period: timedelta = ...) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | object, period: int, min_window_period: int = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, period: _timedelta, min_window_period: _timedelta = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

rolling_average: _rolling_average_Operator

class _rolling_window_arrays_Operator(_Protocol):
    """Materialize a window's values and evaluation timestamps as shaped arrays.

    Accepted native overloads:

    - ``rolling_window_arrays(window: TIME_SERIES_TYPE) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, window: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

rolling_window_arrays: _rolling_window_arrays_Operator

class _round__Operator(_Protocol):
    """``round_`` — round a float to ``n_digits`` decimal places (python's correctly-rounded decimal semantics).

    Accepted native overloads:

    - ``round_(ts: TS[float], n_digits: TS[int]) -> TS[float]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | float, n_digits: _WiringPort | int) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

round_: _round__Operator

class _route_by_index_Operator(_Protocol):
    """``route_by_index`` — forward ``ts`` to the ``index``-th of a list of outputs.

    Accepted native overloads:

    - ``route_by_index(index: TS[int], ts: REF[TIME_SERIES_TYPE]) -> TSL[REF[TIME_SERIES_TYPE], SIZE]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, index: _WiringPort | int, ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

route_by_index: _route_by_index_Operator

class _rshift__Operator(_Protocol):
    """``rshift_`` — the ``>>`` operator (``lhs >> rhs -> OUT``).

    Accepted native overloads:

    - ``rshift_(lhs: TS[int], rhs: TS[int]) -> TS[int]``
    - ``rshift_(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``rshift_(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TIME_SERIES_TYPE_1) -> OUT``
    - ``rshift_(lhs: TIME_SERIES_TYPE, rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``rshift_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE_1) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

rshift_: _rshift__Operator

class _sample_Operator(_Protocol):
    """``sample`` — snap ``ts`` on each tick of ``signal``.

    Accepted native overloads:

    - ``sample(signal: SIGNAL, ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, signal: _WiringPort, ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

sample: _sample_Operator

class _schedule_Operator(_Protocol):
    """``schedule`` — a source ticking ``True`` every ``delay``.

    Accepted native overloads:

    - ``schedule(delay: timedelta, initial_delay: bool = ..., max_ticks: int = ...) -> TS[bool]``
    - ``schedule(delay: TS[timedelta], initial_delay: bool = ..., max_ticks: int = ...) -> TS[bool]``
    - ``schedule(delay: TS[timedelta], start: TS[datetime], initial_delay: bool = ..., max_ticks: int = ...) -> TS[bool]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, delay: _timedelta, initial_delay: bool = ..., max_ticks: int = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, delay: _WiringPort | _timedelta, initial_delay: bool = ..., max_ticks: int = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, delay: _WiringPort | _timedelta, start: _WiringPort | _datetime, initial_delay: bool = ..., max_ticks: int = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

schedule: _schedule_Operator

class _second_Operator(_Protocol):
    """``second`` — the second component of a datetime or time.

    Accepted native overloads:

    - ``second(ts: TS[datetime]) -> TS[int]``
    - ``second(ts: TS[time]) -> TS[int]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | _datetime) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | _time) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

second: _second_Operator

class _seconds_Operator(_Protocol):
    """``seconds`` — the non-negative whole-second remainder of a timedelta.

    Accepted native overloads:

    - ``seconds(ts: TS[timedelta]) -> TS[int]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | _timedelta) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

seconds: _seconds_Operator

class _setattr__Operator(_Protocol):
    """``setattr_`` — sets ``ts.attr`` to ``value`` and returns the updated bundle.

    Accepted native overloads:

    - ``setattr_(ts: TS[SCALAR], attr: str, value: TS[V]) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object, attr: str, value: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

setattr_: _setattr__Operator

class _sign_Operator(_Protocol):
    """``sign`` — Python-compatible numeric sign: ``-1`` for negative values, ``+1`` otherwise.

    Accepted native overloads:

    - ``sign(ts: TS[int]) -> TS[int]``
    - ``sign(ts: TS[float]) -> TS[float]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | float) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

sign: _sign_Operator

class _slice__Operator(_Protocol):
    """``slice_`` — ``drop`` + ``take`` + ``step`` combined over ``[start, stop)`` by ``step_size``.

    Accepted native overloads:

    - ``slice_(ts: TIME_SERIES_TYPE, start: int, stop: int, step_size: int) -> TIME_SERIES_TYPE``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object, start: int, stop: int, step_size: int) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

slice_: _slice__Operator

class _sorted__Operator(_Protocol):
    """``sorted_(ts, by, descending=false)`` — order a typed frame by one column.

    Accepted native overloads:

    - ``sorted_(ts: TS[Frame[SCALAR]], by: str, descending: bool = ...) -> TS[Frame[SCALAR]]``
    - ``sorted_(ts: TS[Frame[SCALAR, SCALAR_1]], by: str, descending: bool = ...) -> TS[Frame[SCALAR, SCALAR_1]]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object, by: str, descending: bool = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

sorted_: _sorted__Operator

class _split_Operator(_Protocol):
    """``split`` — split ``s`` over ``separator`` into the requested output shape.

    Fixed TSL output size is an output type decision, not an input-derived fact. Callers must supply the output schema explicitly, for example:

    ``wire<stdlib::split, TSL<TS<Str>, 2>>(w, s, Str{","})``.

    Accepted native overloads:

    - ``split(s: TS[str], separator: str) -> TSL[TS[str], SIZE]``
    - ``split(s: TS[str], separator: str) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, s: _WiringPort | str, separator: str) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

split: _split_Operator

class _std_Operator(_Protocol):
    """``std_`` — running / element-wise standard deviation. Window overloads accept ``ddof`` to use an ``N - ddof`` divisor.

    Accepted native overloads:

    - ``std(ts: TS[SCALAR]) -> TS[SCALAR_1]``
    - ``std(ts: TS[SCALAR], default_value: TS[SCALAR_1]) -> TS[SCALAR_1]``
    - ``std(ts: TS[int]) -> TS[float]``
    - ``std(ts: TS[float]) -> TS[float]``
    - ``std(ts: TIME_SERIES_TYPE) -> OUT``
    - ``std(ts: TSS[int]) -> TS[float]``
    - ``std(ts: TSS[float]) -> TS[float]``
    - ``std(ts: TSD[K, TS[int]]) -> TS[float]``
    - ``std(ts: TSD[K, TS[float]]) -> TS[float]``
    - ``std(ts: TSL[TS[int], SIZE]) -> TS[float]``
    - ``std(ts: TSL[TS[float], SIZE]) -> TS[float]``
    - ``std(lhs: TS[int], rhs: TS[int]) -> TS[float]``
    - ``std(lhs: TS[float], rhs: TS[float]) -> TS[float]``
    - ``std(lhs: TS[int], rhs: TS[float]) -> TS[float]``
    - ``std(lhs: TS[float], rhs: TS[int]) -> TS[float]``
    - ``std(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``std(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE_1) -> OUT``
    - ``std(ts: TIME_SERIES_TYPE, ddof: int) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, default_value: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, ddof: int) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

std: _std_Operator

class _step_Operator(_Protocol):
    """``step`` — forward every ``step_size``-th tick of ``ts``.

    Accepted native overloads:

    - ``step(ts: TIME_SERIES_TYPE, step_size: int) -> TIME_SERIES_TYPE``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object, step_size: int) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

step: _step_Operator

class _stop_engine_Operator(_Protocol):
    """``stop_engine`` — request an orderly engine stop after this cycle.

    Accepted native overloads:

    - ``stop_engine(ts: SIGNAL, msg: str = ...) -> None``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort, msg: str = ...) -> None: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

stop_engine: _stop_engine_Operator

class _str__Operator(_Protocol):
    """``str_`` — convert the incoming time-series to its ``TS<Str>`` representation.

    Accepted native overloads:

    - ``str_(ts: TIME_SERIES_TYPE) -> TS[str]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

str_: _str__Operator

class _sub__Operator(_Protocol):
    """``sub_`` — the ``-`` operator (``lhs - rhs -> OUT``).

    Accepted native overloads:

    - ``sub_(lhs: TS[int], rhs: TS[int]) -> TS[int]``
    - ``sub_(lhs: TS[float], rhs: TS[float]) -> TS[float]``
    - ``sub_(lhs: TS[timedelta], rhs: TS[timedelta]) -> TS[timedelta]``
    - ``sub_(lhs: TS[int], rhs: TS[float]) -> TS[float]``
    - ``sub_(lhs: TS[float], rhs: TS[int]) -> TS[float]``
    - ``sub_(lhs: TS[datetime], rhs: TS[timedelta]) -> TS[datetime]``
    - ``sub_(lhs: TS[datetime], rhs: TS[datetime]) -> TS[timedelta]``
    - ``sub_(lhs: TS[date], rhs: TS[date]) -> TS[timedelta]``
    - ``sub_(lhs: TS[period], rhs: TS[period]) -> TS[period]``
    - ``sub_(lhs: TS[civil_datetime], rhs: TS[timedelta]) -> TS[civil_datetime]``
    - ``sub_(lhs: TS[civil_datetime], rhs: TS[civil_datetime]) -> TS[timedelta]``
    - ``sub_(lhs: TS[zoned_datetime], rhs: TS[timedelta]) -> TS[zoned_datetime]``
    - ``sub_(lhs: TS[date], rhs: TS[period], month_end_policy: month_end_policy = ...) -> TS[date]``
    - ``sub_(lhs: TS[civil_datetime], rhs: TS[period], month_end_policy: month_end_policy = ...) -> TS[civil_datetime]``
    - ``sub_(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``sub_(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TIME_SERIES_TYPE_1) -> OUT``
    - ``sub_(lhs: TIME_SERIES_TYPE, rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``sub_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE_1) -> OUT``
    - ``sub_(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[SCALAR]``
    - ``sub_(lhs: TS[SCALAR], rhs: TS[SCALAR_1], cmp: callable = ...) -> TS[SCALAR]``
    - ``sub_(lhs: TS[SCALAR], rhs: TS[SCALAR_1]) -> OUT``
    - ``sub_(lhs: TS[str], rhs: TS[str]) -> OUT``
    - ``sub_(lhs: TSS[K], rhs: TS[K]) -> TSS[K]``
    - ``sub_(*ts: TIME_SERIES_TYPE) -> OUT``
    - ``sub_(lhs: TSD[K, V], rhs: TSD[K, V]) -> TSD[K, V]``
    - ``sub_(lhs: TS[date], rhs: TS[timedelta]) -> TS[date]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _timedelta, rhs: _WiringPort | _timedelta) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _datetime, rhs: _WiringPort | _timedelta) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _datetime, rhs: _WiringPort | _datetime) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _date, rhs: _WiringPort | _date) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _Period, rhs: _WiringPort | _Period) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _CivilDateTime, rhs: _WiringPort | _timedelta) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _CivilDateTime, rhs: _WiringPort | _CivilDateTime) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _ZonedDateTime, rhs: _WiringPort | _timedelta) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _date, rhs: _WiringPort | _Period, month_end_policy: _MonthEndPolicy = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _CivilDateTime, rhs: _WiringPort | _Period, month_end_policy: _MonthEndPolicy = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object, cmp: _Callable[..., object] = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | str, rhs: _WiringPort | str) -> _WiringPort: ...
    @_overload
    def __call__(self, *ts: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | _date, rhs: _WiringPort | _timedelta) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

sub_: _sub__Operator

class _substr_Operator(_Protocol):
    """``substr`` — extract a substring of ``s`` between ``start`` and ``end``.

    Accepted native overloads:

    - ``substr(s: TS[str], start: TS[int], end: TS[int]) -> TS[str]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, s: _WiringPort | str, start: _WiringPort | int, end: _WiringPort | int) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

substr: _substr_Operator

class _sum__Operator(_Protocol):
    """``sum_`` — running / element-wise sum.

    Accepted native overloads:

    - ``sum_(ts: TS[SCALAR]) -> TS[SCALAR_1]``
    - ``sum_(ts: TS[SCALAR], default_value: TS[SCALAR_1]) -> TS[SCALAR_1]``
    - ``sum_(ts: TS[int]) -> TS[int]``
    - ``sum_(ts: TS[float]) -> TS[float]``
    - ``sum_(ts: TS[int], reset: TS[bool]) -> TS[int]``
    - ``sum_(ts: TS[float], reset: TS[bool]) -> TS[float]``
    - ``sum_(*ts: TIME_SERIES_TYPE) -> OUT``
    - ``sum_(ts: TIME_SERIES_TYPE) -> OUT``
    - ``sum_(ts: TSS[int]) -> TS[int]``
    - ``sum_(ts: TSS[float]) -> TS[float]``
    - ``sum_(ts: TSD[K, TS[int]]) -> TS[int]``
    - ``sum_(ts: TSD[K, TS[float]]) -> TS[float]``
    - ``sum_(ts: TSL[TS[int], SIZE]) -> TS[int]``
    - ``sum_(ts: TSL[TS[float], SIZE]) -> TS[float]``
    - ``sum_(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``sum_(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TIME_SERIES_TYPE_1) -> OUT``
    - ``sum_(lhs: TIME_SERIES_TYPE, rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``sum_(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE_1) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, default_value: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | int, reset: _WiringPort | bool) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | float, reset: _WiringPort | bool) -> _WiringPort: ...
    @_overload
    def __call__(self, *ts: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

sum_: _sum__Operator

class _symmetric_difference_Operator(_Protocol):
    """``symmetric_difference_`` — set symmetric difference of the inputs.

    Accepted native overloads:

    - ``symmetric_difference(*ts: TIME_SERIES_TYPE) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, *ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

symmetric_difference: _symmetric_difference_Operator

class _take_Operator(_Protocol):
    """``take`` — forward only the first ``count`` ticks of ``ts``.

    Accepted native overloads:

    - ``take(ts: TIME_SERIES_TYPE, count: int) -> TIME_SERIES_TYPE``
    - ``take(ts: TIME_SERIES_TYPE, reset: SIGNAL, count: int) -> TIME_SERIES_TYPE``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | object, count: int) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, reset: _WiringPort, count: int) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

take: _take_Operator

class _temporal_bucket_Operator(_Protocol):
    """``temporal_bucket`` — return the fixed-width instant range containing a value.

    Accepted native overloads:

    - ``temporal_bucket(value: TS[datetime], width: TS[timedelta], origin: datetime = ...) -> TS[instant_range]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, value: _WiringPort | _datetime, width: _WiringPort | _timedelta, origin: _datetime = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

temporal_bucket: _temporal_bucket_Operator

class _temporal_ceil_Operator(_Protocol):
    """``temporal_ceil`` — round a temporal value up to a quantum boundary.

    Accepted native overloads:

    - ``temporal_ceil(value: TS[timedelta], quantum: TS[timedelta]) -> TS[timedelta]``
    - ``temporal_ceil(value: TS[datetime], quantum: TS[timedelta], origin: datetime = ...) -> TS[datetime]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, value: _WiringPort | _timedelta, quantum: _WiringPort | _timedelta) -> _WiringPort: ...
    @_overload
    def __call__(self, value: _WiringPort | _datetime, quantum: _WiringPort | _timedelta, origin: _datetime = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

temporal_ceil: _temporal_ceil_Operator

class _temporal_floor_Operator(_Protocol):
    """``temporal_floor`` — round a temporal value down to a quantum boundary.

    Accepted native overloads:

    - ``temporal_floor(value: TS[timedelta], quantum: TS[timedelta]) -> TS[timedelta]``
    - ``temporal_floor(value: TS[datetime], quantum: TS[timedelta], origin: datetime = ...) -> TS[datetime]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, value: _WiringPort | _timedelta, quantum: _WiringPort | _timedelta) -> _WiringPort: ...
    @_overload
    def __call__(self, value: _WiringPort | _datetime, quantum: _WiringPort | _timedelta, origin: _datetime = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

temporal_floor: _temporal_floor_Operator

class _temporal_round_Operator(_Protocol):
    """``temporal_round`` — round a temporal value to its nearest quantum boundary.

    Accepted native overloads:

    - ``temporal_round(value: TS[timedelta], quantum: TS[timedelta]) -> TS[timedelta]``
    - ``temporal_round(value: TS[datetime], quantum: TS[timedelta], origin: datetime = ...) -> TS[datetime]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, value: _WiringPort | _timedelta, quantum: _WiringPort | _timedelta) -> _WiringPort: ...
    @_overload
    def __call__(self, value: _WiringPort | _datetime, quantum: _WiringPort | _timedelta, origin: _datetime = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

temporal_round: _temporal_round_Operator

class _throttle_Operator(_Protocol):
    """``throttle`` — limit the tick rate of ``ts`` to ``period``.

    Accepted native overloads:

    - ``throttle(ts: TIME_SERIES_TYPE, period: TS[timedelta], delay_first_tick: bool = ...) -> TIME_SERIES_TYPE``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object, period: _WiringPort | _timedelta, delay_first_tick: bool = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

throttle: _throttle_Operator

class _timestamp_Operator(_Protocol):
    """``timestamp`` — FRACTIONAL seconds since the Unix epoch (Python's ``datetime.timestamp()`` returns a float). hgraph datetimes are UTC by convention, so this is the UTC epoch count (upstream's naive ``datetime.timestamp()`` is local-tz dependent; recorded deviation).

    Accepted native overloads:

    - ``timestamp(ts: TS[datetime]) -> TS[float]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | _datetime) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

timestamp: _timestamp_Operator

class _to_civil_Operator(_Protocol):
    """``to_civil`` — extract the local civil date and time from a zoned datetime.

    Accepted native overloads:

    - ``to_civil(value: TS[zoned_datetime]) -> TS[civil_datetime]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, value: _WiringPort | _ZonedDateTime) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

to_civil: _to_civil_Operator

class _to_data_frame_Operator(_Protocol):
    """``to_data_frame`` — snapshot each time-series tick as a typed frame.

    Accepted native overloads:

    - ``to_data_frame(ts: TSD[K, V], dt_col: str = ..., key_col: str = ..., value_col: str = ...) -> OUT``
    - ``to_data_frame(ts: TIME_SERIES_TYPE, dt_col: str = ..., key_col: str = ..., value_col: str = ...) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object, dt_col: str = ..., key_col: str = ..., value_col: str = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

to_data_frame: _to_data_frame_Operator

class _to_instant_Operator(_Protocol):
    """``to_instant`` — extract the absolute instant from a zoned datetime.

    Accepted native overloads:

    - ``to_instant(value: TS[zoned_datetime]) -> TS[datetime]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, value: _WiringPort | _ZonedDateTime) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

to_instant: _to_instant_Operator

class _to_json_Operator(_Protocol):
    """JSON serialization operators (design record: *Record/replay, tables and const_fn*, step 1). The wire format is the Python one — see ``types/value/json_codec.h``.

    ``to_json(ts, delta=false)`` serialises the time-series VALUE per tick; with ``delta=true`` it serialises the canonical per-tick delta value (``capture_delta``) instead — the canonical delta *is* the recorded delta wire form.

    ``from_json`` parses into the resolved output type and applies the parsed value as the tick's delta: ``wire<from_json, TS<MySchema>>(w, ts)``.

    Accepted native overloads:

    - ``to_json(ts: TIME_SERIES_TYPE, delta: bool = ...) -> TS[str]``
    - ``to_json(ts: TIME_SERIES_TYPE, delta: bool) -> TS[str]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | object, delta: bool = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, delta: bool) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

to_json: _to_json_Operator

class _to_table_Operator(_Protocol):
    """Table serialization operators (design record: *Record/replay, tables and const_fn*, P4 + step 6). ``to_table`` is the Python-parity TUPLE-ROW protocol: each tick converts to bitemporal row values ``[date, as_of, {removed, *keys}(per TSD level), *value columns]`` — ``TS<tuple[...]>`` for single-row types, ``TS<tuple[tuple[...], ...]>`` for partitioned (TSD) or multi-row (``Frame``-valued) types; unset cells are tuple field validity (Python ``None``). The output schema is computed from the resolved input at wiring. ``mode`` is a ``ToTableMode`` enum time-series (Tick/Sample/Snap) defaulting to Tick.

    ``from_table`` reverses it, applying each row as the tick's delta at the resolved output (supplied at the wiring site: ``wire<from_table, TS<MySchema>>(w, ts)``); removed flags map to TSD key removals. The record/replay backends bypass both and drive the Arrow serializer ops directly (``types/value/table_codec.h``).

    Accepted native overloads:

    - ``to_table(ts: TIME_SERIES_TYPE, mode: TS[SCALAR] = ...) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object, mode: _WiringPort | object = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

to_table: _to_table_Operator

class _to_window_Operator(_Protocol):
    """``to_window`` — convert ``ts`` into a ``TSW`` time-series window of ``period`` ticks, valid once ``min_window_period`` ticks arrived. An optional ``reset`` SIGNAL overload clears retained ticks before admitting a source tick from the same evaluation cycle.

    Accepted native overloads:

    - ``to_window(ts: TS[SCALAR], period: int, min_window_period: int = ...) -> OUT``
    - ``to_window(ts: TS[SCALAR], period: int, min_window_period: int = ..., reset: SIGNAL) -> OUT``
    - ``to_window(ts: TS[SCALAR], period: timedelta, min_window_period: timedelta = ...) -> OUT``
    - ``to_window(ts: TS[SCALAR], period: timedelta, min_window_period: timedelta = ..., reset: SIGNAL) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | object, period: int, min_window_period: int = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, period: int, min_window_period: int, reset: _WiringPort) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, period: int, *, min_window_period: int = ..., reset: _WiringPort) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, period: _timedelta, min_window_period: _timedelta = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, period: _timedelta, min_window_period: _timedelta, reset: _WiringPort) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, period: _timedelta, *, min_window_period: _timedelta = ..., reset: _WiringPort) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

to_window: _to_window_Operator

class _total_seconds_Operator(_Protocol):
    """``total_seconds`` — convert a timedelta to fractional seconds.

    Accepted native overloads:

    - ``total_seconds(ts: TS[timedelta]) -> TS[float]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | _timedelta) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

total_seconds: _total_seconds_Operator

class _type__Operator(_Protocol):
    """``type_`` — the (python) type of the time-series value.

    Accepted native overloads:

    - ``type_(ts: TIME_SERIES_TYPE) -> TS[Any]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

type_: _type__Operator

class _uncollapse_keys_Operator(_Protocol):
    """``uncollapse_keys`` — the inverse of ``collapse_keys`` (optional ``remove_empty`` flag).

    Accepted native overloads:

    - ``uncollapse_keys(ts: TIME_SERIES_TYPE) -> OUT``
    - ``uncollapse_keys(ts: TSD[K, V], remove_empty: bool = ...) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, remove_empty: bool = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

uncollapse_keys: _uncollapse_keys_Operator

class _ungroup_Operator(_Protocol):
    """Concatenate the valid values of a keyed frame collection.

    Ungroup while materializing a scalar or tuple key into columns.

    Accepted native overloads:

    - ``ungroup(ts: TIME_SERIES_TYPE) -> TS[Frame[OUT]]``
    - ``ungroup(ts: TIME_SERIES_TYPE, key_col: SCALAR) -> TS[Frame[OUT]]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, key_col: object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

ungroup: _ungroup_Operator

class _union_Operator(_Protocol):
    """``union_`` — set union of the inputs.

    Accepted native overloads:

    - ``union(*ts: TIME_SERIES_TYPE) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, *ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

union: _union_Operator

class _unpartition_Operator(_Protocol):
    """``unpartition`` — merge a nested ``TSD[K1, TSD[K, V]]`` back into ``TSD[K, V]``.

    Accepted native overloads:

    - ``unpartition(ts: TSD[K_1, TSD[K, V]]) -> TSD[K, V]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

unpartition: _unpartition_Operator

class _until_true_Operator(_Protocol):
    """``until_true`` — emit ``False`` until ``predicate`` first holds, then ``True`` (and passivate ``ts``).

    Accepted native overloads:

    - ``until_true(ts: TS[bool]) -> TS[bool]``
    - ``until_true(predicate: callable, ts: TIME_SERIES_TYPE) -> TS[bool]``
    - ``until_true(predicate: fn, ts: TIME_SERIES_TYPE) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | bool) -> _WiringPort: ...
    @_overload
    def __call__(self, predicate: _Callable[..., object], ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

until_true: _until_true_Operator

class _valid_Operator(_Protocol):
    """``valid`` — ``True`` while ``ts`` is valid, ``False`` otherwise.

    Accepted native overloads:

    - ``valid(ts: TIME_SERIES_TYPE) -> TS[bool]``
    - ``valid(ts: REF[TIME_SERIES_TYPE]) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

valid: _valid_Operator

class _values__Operator(_Protocol):
    """``values_`` — the values of a dictionary.

    Accepted native overloads:

    - ``values_(ts: TSD[K, V]) -> TSS[SCALAR]``
    - ``values_(ts: TIME_SERIES_TYPE) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

values_: _values__Operator

class _var_Operator(_Protocol):
    """``var_`` — running / element-wise variance.

    Accepted native overloads:

    - ``var(ts: TS[SCALAR]) -> TS[SCALAR_1]``
    - ``var(ts: TS[SCALAR], default_value: TS[SCALAR_1]) -> TS[SCALAR_1]``
    - ``var(ts: TS[int]) -> TS[float]``
    - ``var(ts: TS[float]) -> TS[float]``
    - ``var(ts: TSS[int]) -> TS[float]``
    - ``var(ts: TSS[float]) -> TS[float]``
    - ``var(ts: TSD[K, TS[int]]) -> TS[float]``
    - ``var(ts: TSD[K, TS[float]]) -> TS[float]``
    - ``var(ts: TSL[TS[int], SIZE]) -> TS[float]``
    - ``var(ts: TSL[TS[float], SIZE]) -> TS[float]``
    - ``var(lhs: TS[int], rhs: TS[int]) -> TS[float]``
    - ``var(lhs: TS[float], rhs: TS[float]) -> TS[float]``
    - ``var(lhs: TS[int], rhs: TS[float]) -> TS[float]``
    - ``var(lhs: TS[float], rhs: TS[int]) -> TS[float]``
    - ``var(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE_1, SIZE]) -> OUT``
    - ``var(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE_1) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, default_value: _WiringPort | object) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | int, rhs: _WiringPort | float) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | float, rhs: _WiringPort | int) -> _WiringPort: ...
    @_overload
    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

var: _var_Operator

class _weekday_Operator(_Protocol):
    """``weekday`` — the day of the week using Monday as zero.

    Accepted native overloads:

    - ``weekday(ts: TS[date]) -> TS[int]``
    - ``weekday(ts: TS[datetime]) -> TS[int]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | _date) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | _datetime) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

weekday: _weekday_Operator

class _window_Operator(_Protocol):
    """``window`` — buffer the last ``period`` values; result is a bundle (buffer + timestamps).

    Accepted native overloads:

    - ``window(ts: TS[SCALAR], period: int, min_window_period: int = ...) -> OUT``
    - ``window(ts: TS[SCALAR], period: timedelta, min_window_period: timedelta = ...) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | object, period: int, min_window_period: int = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, period: _timedelta, min_window_period: _timedelta = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

window: _window_Operator

class _with_columns_Operator(_Protocol):
    """Replace/add structural columns and project to the output row schema.

    Accepted native overloads:

    - ``with_columns(ts: TS[Frame[SCALAR]], columns: TIME_SERIES_TYPE) -> TS[Frame[OUT]]``
    - ``with_columns(ts: TS[Frame[SCALAR, SCALAR_1]], columns: TIME_SERIES_TYPE) -> TS[Frame[OUT, SCALAR_1]]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object, columns: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

with_columns: _with_columns_Operator

class _year_Operator(_Protocol):
    """``year`` — the year of a ``TS<Date>``.

    Accepted native overloads:

    - ``year(ts: TS[date]) -> TS[int]``
    - ``year(ts: TS[datetime]) -> TS[int]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | _date) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | _datetime) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

year: _year_Operator

class _zero_Operator(_Protocol):
    """``zero_`` — the zero source for a requested output type and operation (mirrors Python ``zero(tp, op)``: the value depends on both — e.g. ``add_`` -> 0 but ``mul_`` -> 1). ``op`` is the wired function (``fn<add_>()``).

    Accepted native overloads:

    - ``zero(op: fn) -> TS[int]``
    - ``zero(op: fn) -> TS[float]``
    - ``zero(op: fn) -> TS[str]``
    - ``zero(op: fn) -> TSD[K, V]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, op: _Callable[..., object]) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

zero: _zero_Operator


__all__ = (
    "abs_",
    "add_",
    "all_",
    "and_",
    "any_",
    "apply",
    "as_array",
    "assert_",
    "at_zone",
    "batch",
    "bit_and",
    "bit_or",
    "bit_xor",
    "call",
    "clip",
    "cmp_",
    "collapse_keys",
    "combine_cs",
    "combine_json",
    "combine_map",
    "combine_tsd",
    "combine_tss_from_tsl",
    "compare",
    "concat",
    "const",
    "contains_",
    "convert_zone",
    "corrcoef",
    "count",
    "cumsum",
    "day",
    "day_of_month",
    "days",
    "debug_print",
    "dedup",
    "default",
    "dereference",
    "diff",
    "difference",
    "div_",
    "divmod_",
    "downcast_",
    "drop",
    "eq_",
    "evaluation_time_in_range",
    "ewma",
    "explode",
    "filter_",
    "filter_cs",
    "filter_frame",
    "filter_tsd_by_matches",
    "flip",
    "flip_keys",
    "floordiv_",
    "format_",
    "freeze",
    "from_data_frame",
    "from_data_frame_batches",
    "from_json",
    "from_table",
    "from_table_const",
    "gate",
    "ge_",
    "get_item",
    "getattr_",
    "getitem_",
    "group_by",
    "gt_",
    "hour",
    "if_",
    "if_cmp",
    "if_then_else",
    "if_true",
    "index_of",
    "intersection",
    "invert_",
    "is_empty",
    "isoformat",
    "isoweekday",
    "join",
    "json_as_bool",
    "json_as_float",
    "json_as_int",
    "json_as_str",
    "json_decode",
    "json_encode",
    "keys_",
    "lag",
    "last_modified_date",
    "last_modified_time",
    "last_modified_wall_clock_time",
    "le_",
    "len_",
    "ln",
    "log_",
    "lshift_",
    "lt_",
    "make_tsd",
    "match_",
    "max_",
    "max_ts_list",
    "mean",
    "merge",
    "merge_tsd_disjoint",
    "microsecond",
    "microseconds",
    "min_",
    "min_ts_list",
    "minute",
    "mod_",
    "modified",
    "month",
    "month_of_year",
    "mul_",
    "ne_",
    "neg_",
    "not_",
    "nothing",
    "np_std",
    "null_sink",
    "or_",
    "partition",
    "pct_change",
    "pos_",
    "pow_",
    "print_",
    "quantile",
    "race",
    "range_adjacent",
    "range_contains",
    "range_difference",
    "range_extent",
    "range_hull",
    "range_intersection",
    "range_merge",
    "range_mergeable",
    "range_overlaps",
    "range_shift",
    "range_touches",
    "range_union",
    "record",
    "reduce_tsd_of_bundles_with_race",
    "reduce_tsd_with_race",
    "rekey",
    "replace",
    "replay",
    "replay_const",
    "replay_data_frame",
    "request_id",
    "resample",
    "resolve_civil",
    "rolling_average",
    "rolling_window_arrays",
    "round_",
    "route_by_index",
    "rshift_",
    "sample",
    "schedule",
    "second",
    "seconds",
    "setattr_",
    "sign",
    "slice_",
    "sorted_",
    "split",
    "std",
    "step",
    "stop_engine",
    "str_",
    "sub_",
    "substr",
    "sum_",
    "symmetric_difference",
    "take",
    "temporal_bucket",
    "temporal_ceil",
    "temporal_floor",
    "temporal_round",
    "throttle",
    "timestamp",
    "to_civil",
    "to_data_frame",
    "to_instant",
    "to_json",
    "to_table",
    "to_window",
    "total_seconds",
    "type_",
    "uncollapse_keys",
    "ungroup",
    "union",
    "unpartition",
    "until_true",
    "valid",
    "values_",
    "var",
    "weekday",
    "window",
    "with_columns",
    "year",
    "zero",
)
