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
from ._table import ToTableMode as _ToTableMode
from ._wiring import WiringPort as _WiringPort

class _abs__Operator(_Protocol):
    """Return the absolute magnitude of a numeric value, duration, or compatible collection. Python's ``abs(ts)`` syntax wires this operator.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[int]``, ``TS[float]``, ``TS[timedelta]``, ``TSL[TIME_SERIES_TYPE, SIZE]``, ``TIME_SERIES_TYPE``
       Input whose sign is removed.

    Returns
    ~~~~~~~

    The non-negative magnitude with the overload-selected schema.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       magnitude = abs(change)

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
    """Add two time-series values using the overload selected for their schemas. Supports numeric promotion, string concatenation, temporal arithmetic, collection broadcasting, keyed-set insertion, and runtime-checked concatenation of dynamic JSON arrays. Python's ``lhs + rhs`` syntax wires this operator.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[int]``, ``TS[float]``, ``TS[str]``, ``TS[timedelta]``, ``TS[datetime]``, ``TS[date]``, ``TS[period]``, ``TS[civil_datetime]``, ``TS[zoned_datetime]``, ``TSL[TIME_SERIES_TYPE, SIZE]``, ``TIME_SERIES_TYPE``, ``TS[SCALAR]``, ``TSS[K]``
       Left-hand value. A tick triggers a new result once the overload's validity requirements are met.

    ``rhs`` : time-series; ``TS[int]``, ``TS[float]``, ``TS[str]``, ``TS[timedelta]``, ``TS[datetime]``, ``TS[period]``, ``TS[time]``, ``TS[zoned_datetime]``, ``TSL[TIME_SERIES_TYPE_1, SIZE]``, ``TIME_SERIES_TYPE_1``, ``TS[SCALAR]``, ``TS[SCALAR_1]``, ``TS[K]``
       Right-hand value; compatible plain values are lifted to constants.

    ``month_end_policy`` : scalar; ``month_end_policy``
       Policy used when adding a calendar period to a date whose day does not exist in the target month. Optional in overloads that show ``= ...``.

    ``__strict__`` : scalar; ``bool``
       When true, wait until both operands are valid. Non-strict overloads may forward the valid operand when the other is absent. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    The sum, with its schema selected from both operand schemas. For ``TS[JSON]`` operands both values must be arrays at runtime.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       total = lhs + rhs  # equivalent to hg.add_(lhs, rhs)

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
    """Return true when every supplied boolean value is true. Variadic and keyed-dictionary forms recompute when any member changes.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``*args`` : time-series; ``TS[bool]``
       Boolean inputs to test.

    ``arg`` : time-series; ``TSD[K, TS[bool]]``
       Keyed boolean collection accepted by collection overloads.

    Returns
    ~~~~~~~

    Boolean conjunction of all current inputs.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       ready = hg.all_(has_price, has_quantity, is_open)

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
    """Return the boolean conjunction of two current values using their truth semantics. This is an eager graph operator: both ports are wired, unlike Python's scalar short-circuit ``and``.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[SCALAR]``, ``TS[bool]``, ``TS[int]``, ``TS[float]``, ``TS[str]``, ``TSS[K]``
       Left-hand truth-valued input.

    ``rhs`` : time-series; ``TS[SCALAR]``, ``TS[bool]``, ``TS[int]``, ``TS[float]``, ``TS[str]``, ``TSS[K]``
       Right-hand truth-valued input.

    Returns
    ~~~~~~~

    True only when both values are truthy.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       ready = hg.and_(has_data, market_open)

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
    """Return true when at least one supplied boolean value is true. Variadic and keyed-dictionary forms recompute when any member changes.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``*args`` : time-series; ``TS[bool]``
       Boolean inputs to test.

    ``arg`` : time-series; ``TSD[K, TS[bool]]``
       Keyed boolean collection accepted by collection overloads.

    Returns
    ~~~~~~~

    Boolean disjunction of all current inputs.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       has_alert = hg.any_(price_alert, risk_alert)

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
    """Invoke the latest runtime callable when it or one of its inputs ticks. This differs from higher-order graph operators: ``fn`` is a value available at runtime, not a graph callable compiled at wiring time.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``fn`` : time-series; ``TS[callable]``
       Time series carrying the callable.

    ``*args`` : time-series; ``TIME_SERIES_TYPE``
       Positional values supplied to the callable.

    ``**kwargs`` : time-series; ``time-series``
       Named values supplied to the callable.

    Returns
    ~~~~~~~

    The callable's result.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.apply(runtime_function, lhs, rhs)

    Accepted native overloads:

    - ``apply(fn: TS[callable], *args: TIME_SERIES_TYPE, **kwargs: time-series) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, fn: _WiringPort | object, *args: _WiringPort | object, **kwargs: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

apply: _apply_Operator

class _assert__Operator(_Protocol):
    """Raise ``AssertionError`` when a ticking condition is false. Additional overloads format the error message from live arguments.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``condition`` : time-series; ``TS[bool]``
       Boolean stream to enforce.

    ``error_msg`` : scalar; ``str``
       Wiring-time error message or format string.

    ``*args`` : time-series; ``TIME_SERIES_TYPE``
       Values used to format the message.

    ``**kwargs`` : time-series; ``time-series``
       Named values used to format the message.

    Returns
    ~~~~~~~

    No output.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       hg.assert_(quantity >= 0, "quantity must be non-negative")

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
    """Represent an absolute instant in a time zone without changing that instant.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``instant`` : time-series; ``TS[datetime]``
       Absolute UTC-line timestamp.

    ``zone`` : time-series; ``TS[zone_id]``
       IANA time-zone identifier used for local representation.

    Returns
    ~~~~~~~

    A zoned datetime carrying both instant and zone.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       local_view = hg.at_zone(instant, hg.ZoneId("Europe/London"))

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
    """Queue ticks while ``condition`` is false, then drain them in batches separated by ``delay``. Raises if a positive ``buffer_length`` is exceeded.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``condition`` : time-series; ``TS[bool]``
       False to buffer and true to begin draining.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``
       Stream to buffer.

    ``delay`` : scalar; ``timedelta``
       Engine-time interval between released batches.

    ``buffer_length`` : scalar; ``int``
       Maximum number of queued values. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    Buffered values released in delayed batches.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       paced = hg.batch(is_ready, updates, timedelta(milliseconds=10), buffer_length=1000)

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
    """Apply bitwise AND or the corresponding structural collection operation.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[SCALAR]``, ``TS[int]``, ``TS[bool]``, ``TSL[TIME_SERIES_TYPE, SIZE]``, ``TIME_SERIES_TYPE``, ``TSD[K, V]``
       Left-hand input.

    ``rhs`` : time-series; ``TS[SCALAR]``, ``TS[int]``, ``TS[bool]``, ``TSL[TIME_SERIES_TYPE_1, SIZE]``, ``TIME_SERIES_TYPE_1``, ``TSD[K, V]``
       Right-hand input.

    ``*ts`` : time-series; ``TIME_SERIES_TYPE_2``
       The primary time-series input.

    Returns
    ~~~~~~~

    ``lhs & rhs`` with overload-selected structure.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       common_flags = flags & allowed

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
    """Apply bitwise OR or the corresponding structural collection operation.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[SCALAR]``, ``TS[int]``, ``TS[bool]``, ``TSL[TIME_SERIES_TYPE, SIZE]``, ``TIME_SERIES_TYPE``, ``TSD[K, V]``
       Left-hand input.

    ``rhs`` : time-series; ``TS[SCALAR]``, ``TS[int]``, ``TS[bool]``, ``TSL[TIME_SERIES_TYPE_1, SIZE]``, ``TIME_SERIES_TYPE_1``, ``TSD[K, V]``
       Right-hand input.

    ``*ts`` : time-series; ``TIME_SERIES_TYPE_2``
       The primary time-series input.

    Returns
    ~~~~~~~

    ``lhs | rhs`` with overload-selected structure.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       combined_flags = flags | defaults

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
    """Apply bitwise exclusive OR or the corresponding structural operation.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[SCALAR]``, ``TS[int]``, ``TS[bool]``, ``TSL[TIME_SERIES_TYPE, SIZE]``, ``TIME_SERIES_TYPE``, ``TSD[K, V]``
       Left-hand input.

    ``rhs`` : time-series; ``TS[SCALAR]``, ``TS[int]``, ``TS[bool]``, ``TSL[TIME_SERIES_TYPE_1, SIZE]``, ``TIME_SERIES_TYPE_1``, ``TSD[K, V]``
       Right-hand input.

    ``*ts`` : time-series; ``TIME_SERIES_TYPE_2``
       The primary time-series input.

    Returns
    ~~~~~~~

    ``lhs ^ rhs`` with overload-selected structure.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       changed_flags = before ^ after

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
    """Invoke a runtime callable for side effects and discard its return value.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``fn`` : time-series; ``TS[callable]``
       Time series carrying the callable.

    ``*args`` : time-series; ``TIME_SERIES_TYPE``
       Positional values supplied to the callable.

    ``**kwargs`` : time-series; ``time-series``
       Named values supplied to the callable.

    Returns
    ~~~~~~~

    No output.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       hg.call(runtime_callback, event)

    Accepted native overloads:

    - ``call(fn: TS[callable], *args: TIME_SERIES_TYPE, **kwargs: time-series) -> None``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, fn: _WiringPort | object, *args: _WiringPort | object, **kwargs: _WiringPort | object) -> None: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

call: _call_Operator

class _cmp__Operator(_Protocol):
    """Compare two values once and classify the result as ``LT``, ``EQ``, or ``GT``. This is useful with ``if_cmp`` when three branches must share one comparison.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[int]``, ``TS[float]``, ``TS[str]``, ``TS[date]``, ``TS[datetime]``, ``TS[timedelta]``, ``TS[bool]``, ``TS[SCALAR]``
       Left-hand value.

    ``rhs`` : time-series; ``TS[int]``, ``TS[float]``, ``TS[str]``, ``TS[date]``, ``TS[datetime]``, ``TS[timedelta]``, ``TS[bool]``, ``TS[SCALAR]``
       Right-hand value.

    Returns
    ~~~~~~~

    A ``CmpResult`` classification.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       ordering = hg.cmp_(lhs, rhs)

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
    """Flatten both key levels of a nested dictionary into tuple keys. @note Cost: O(total entries) per tick (delta-driven for TSD).

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``, ``TSD[K, V]``
       ``TSD[K, TSD[K1, V]]`` input.

    Returns
    ~~~~~~~

    ``TSD[tuple[K, K1], V]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       flat = hg.collapse_keys(nested)

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

class _compare_Operator(_Protocol):
    """Compare two streams during backtesting and record their per-tick equality result. This sink is active in compare mode and stores results beneath the recordable id.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TIME_SERIES_TYPE``
       Actual or newly computed stream.

    ``rhs`` : time-series; ``TIME_SERIES_TYPE``
       Expected or reference stream.

    ``recordable_id`` : scalar; ``str``
       Optional explicit identity; context supplies it when omitted.

    ``model`` : scalar; ``str``
       Optional per-call backend id (``"memory"``, ``"testing"``, or an extension id such as ``"hgraph.persistence.frame"``; legacy model names are translated); an empty value inherits the graph configuration. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    No output.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       hg.compare(actual, expected, recordable_id="pricing")

    Accepted native overloads:

    - ``compare(lhs: TIME_SERIES_TYPE, rhs: TIME_SERIES_TYPE, recordable_id: str, model: str = ...) -> None``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, lhs: _WiringPort | object, rhs: _WiringPort | object, recordable_id: str, model: str = ...) -> None: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

compare: _compare_Operator

class _concat_Operator(_Protocol):
    """Append rows from two frames with the same row schema.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts1`` : time-series; ``TS[Frame[SCALAR]]``, ``TS[Frame[SCALAR, SCALAR_1]]``
       First frame.

    ``ts2`` : time-series; ``TS[Frame[SCALAR]]``, ``TS[Frame[SCALAR, SCALAR_1]]``
       Frame appended after ``ts1``.

    Returns
    ~~~~~~~

    The vertically concatenated frame.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       all_rows = hg.concat(primary_rows, secondary_rows)

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
    """Create a source that emits one configured value and then becomes passive. With no delay it ticks at graph start; otherwise it ticks at ``start_time + delay``. Subscript ``const`` when the output shape cannot be inferred from the Python value.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``value`` : scalar; ``SCALAR``, ``py_object``
       Python value adapted to the selected time-series schema.

    ``delay`` : scalar; ``timedelta``
       Optional engine-time delay before the single tick.

    Returns
    ~~~~~~~

    A source of the inferred or explicitly selected output type.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       answer = hg.const[TS[int]](42)
       delayed = hg.const("ready", delay=timedelta(seconds=1))

    Accepted native overloads:

    - ``const(value: SCALAR) -> OUT``
    - ``const(value: SCALAR, delay: timedelta) -> OUT``
    - ``const(value: py_object) -> OUT``
    - ``const(value: py_object, delay: timedelta) -> OUT``

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
    """Test membership of an item in the current collection value. Call the operator directly because Python's ``in`` syntax coerces its result to a scalar boolean and cannot represent a wiring port. @note Cost: O(n) scan per tick for list/tuple inputs; O(1) for sets/dicts.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[str]``, ``TSS[K]``, ``TSD[K, V]``, ``TIME_SERIES_TYPE``, ``TS[SCALAR]``, ``TS[SCALAR_2]``
       Collection or mapping to search.

    ``item`` : time-series; ``TS[str]``, ``TS[K]``, ``TSS[K]``, ``TIME_SERIES_TYPE_1``, ``TS[SCALAR_1]``, ``TS[SCALAR_3]``
       Candidate member or key.

    Returns
    ~~~~~~~

    True while ``item`` is contained in ``ts``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       subscribed = hg.contains_(subscriptions, symbol)

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
    """Change the display zone of a zoned datetime while preserving its absolute instant.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``value`` : time-series; ``TS[zoned_datetime]``
       Zoned datetime to convert.

    ``zone`` : time-series; ``TS[zone_id]``
       Destination IANA time zone.

    Returns
    ~~~~~~~

    The same instant represented in ``zone``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       new_york_time = hg.convert_zone(london_time, hg.ZoneId("America/New_York"))

    Accepted native overloads:

    - ``convert_zone(value: TS[zoned_datetime], zone: TS[zone_id]) -> TS[zoned_datetime]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, value: _WiringPort | _ZonedDateTime, zone: _WiringPort | _ZoneId) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

convert_zone: _convert_zone_Operator

class _datepart_Operator(_Protocol):
    """``datepart`` — truncate a datetime to midnight while retaining datetime type.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[datetime]``
       The primary time-series input.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[datetime]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.datepart(ts)

    Accepted native overloads:

    - ``datepart(ts: TS[datetime]) -> TS[datetime]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | _datetime) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

datepart: _datepart_Operator

class _day_Operator(_Protocol):
    """``day`` — the day-of-month attribute of a date or datetime.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[date]``, ``TS[datetime]``
       The primary time-series input.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[int]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.day(ts)

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[date]``, ``TS[datetime]``
       The primary time-series input.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[int]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.day_of_month(ts)

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[timedelta]``
       The primary time-series input.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[int]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.days(ts)

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
    """Print a labelled representation of each source tick for graph diagnostics.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``label`` : scalar; ``str``
       Wiring-time prefix identifying the stream.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``
       Value printed when it ticks.

    ``sample`` : scalar; ``int``
       Emit one diagnostic line for every nth source tick. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    No output; this is a diagnostic sink.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       hg.debug_print("price", price)

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
    """Suppress an input tick when its value compares equal to the last emitted value. The first valid value always passes through.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[SCALAR]``, ``TS[float]``, ``TSD[K, V]``, ``TSS[K]``, ``TSL[TIME_SERIES_TYPE, SIZE]``, ``TIME_SERIES_TYPE``
       Stream to de-duplicate.

    ``abs_tol`` : time-series; ``TS[float]``
       The abs tol value used by the selected overload. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    ``ts`` with consecutive equal values removed.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       changes_only = hg.dedup(status)

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
    """Forward ``ts`` when it is valid and otherwise expose ``default_value``. Once the primary input becomes valid it takes precedence; later invalidation makes the fallback visible again.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``
       Preferred input.

    ``default_value`` : time-series; ``TIME_SERIES_TYPE``
       Fallback input used while ``ts`` is invalid.

    Returns
    ~~~~~~~

    A port that is valid whenever either input supplies a value.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       price_or_zero = hg.default(price, 0.0)

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
    """Materialize a reference to a structured time series as a structural port. Fields or list elements remain references to the original sources rather than copied scalar snapshots.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``tsb`` : time-series; ``REF[TIME_SERIES_TYPE]``
       Reference to a bundle or fixed time-series list.

    Returns
    ~~~~~~~

    A structural port whose children preserve reference semantics.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       live_bundle = hg.dereference(bundle_ref)

    Accepted native overloads:

    - ``dereference(tsb: REF[TIME_SERIES_TYPE]) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, tsb: _WiringPort | object) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

dereference: _dereference_Operator

class _difference_Operator(_Protocol):
    """Remove every member of the later sets from the first set.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``*ts`` : time-series; ``TIME_SERIES_TYPE``
       Ordered set inputs; the first is the minuend.

    Returns
    ~~~~~~~

    Members present only in the first input.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       available = hg.difference(all_symbols, suspended_symbols)

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
    """Divide ``lhs`` by ``rhs`` using true-division semantics. The scalar ``divide_by_zero`` choice is fixed at wiring time, so the selected behaviour adds no per-tick policy dispatch.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[int]``, ``TS[float]``, ``TS[timedelta]``, ``TSL[TIME_SERIES_TYPE, SIZE]``, ``TIME_SERIES_TYPE``
       Dividend.

    ``rhs`` : time-series; ``TS[int]``, ``TS[float]``, ``TS[timedelta]``, ``TSL[TIME_SERIES_TYPE_1, SIZE]``, ``TIME_SERIES_TYPE_1``
       Divisor.

    ``divide_by_zero`` : scalar; ``DivideByZero``
       Behaviour for a zero divisor: raise, emit NaN or infinity, emit zero or one, or suppress the tick. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    The quotient, normally promoted to a floating-point schema.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       ratio = hg.div_(numerator, denominator, divide_by_zero=hg.DivideByZero.NAN)

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
    """Compute floor quotient and remainder together.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[int]``, ``TS[float]``
       Dividend.

    ``rhs`` : time-series; ``TS[int]``, ``TS[float]``
       Divisor.

    Returns
    ~~~~~~~

    A two-element time-series list containing quotient then remainder.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       quotient_and_remainder = hg.divmod_(items, batch_size)

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

class _drop_Operator(_Protocol):
    """Suppress the first ``count`` source ticks and forward every later tick.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[SCALAR]``, ``TIME_SERIES_TYPE``
       Stream whose prefix is removed.

    ``count`` : scalar; ``int``
       Non-negative number of ticks to discard, fixed at wiring time. Optional in overloads that show ``= ...``.

    ``period`` : scalar; ``timedelta``
       Tick count or elapsed interval controlling the temporal operation.

    Returns
    ~~~~~~~

    ``ts`` without its first ``count`` ticks.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       after_warmup = hg.drop(updates, 10)

    Accepted native overloads:

    - ``drop(ts: TS[SCALAR], count: int = ...) -> TS[SCALAR]``
    - ``drop(ts: TIME_SERIES_TYPE, count: int = ...) -> TIME_SERIES_TYPE``
    - ``drop(ts: TIME_SERIES_TYPE, period: timedelta) -> TIME_SERIES_TYPE``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | object, count: int = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, period: _timedelta) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

drop: _drop_Operator

class _eq__Operator(_Protocol):
    """Compare two current values for equality. Python's ``lhs == rhs`` syntax wires this operator, including structural overloads for hgraph collections.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[bool]``, ``TS[int]``, ``TS[str]``, ``TS[date]``, ``TS[datetime]``, ``TS[timedelta]``, ``TS[float]``, ``TSL[TIME_SERIES_TYPE, SIZE]``, ``TS[SCALAR]``, ``TIME_SERIES_TYPE``, ``TSS[K]``, ``TSD[K, V]``, ``TSD[K, TS[float]]``
       Left-hand value.

    ``rhs`` : time-series; ``TS[bool]``, ``TS[int]``, ``TS[str]``, ``TS[date]``, ``TS[datetime]``, ``TS[timedelta]``, ``TS[float]``, ``TSL[TIME_SERIES_TYPE_1, SIZE]``, ``TS[SCALAR]``, ``TIME_SERIES_TYPE_1``, ``TSS[K]``, ``TSD[K, V]``, ``TSD[K, TS[float]]``
       Right-hand value.

    ``epsilon`` : scalar, time-series; ``float``, ``TS[float]``
       The epsilon value used by the selected overload. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    True when the values compare equal.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       unchanged = current == previous

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``start_time`` : time-series; ``TS[datetime]``, ``TS[date]``, ``TS[time]``
       The start time value used by the selected overload.

    ``end_time`` : time-series; ``TS[datetime]``, ``TS[date]``, ``TS[time]``
       The end time value used by the selected overload.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[CmpResult]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.evaluation_time_in_range(start_time, end_time)

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

class _explode_Operator(_Protocol):
    """``explode`` — split a date into a fixed list of year, month, and day.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[date]``
       The primary time-series input.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TSL[TS[int], 3]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.explode(ts)

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
    """Forward source ticks only while the latest ``condition`` value is true. When the condition changes from false to true, the operator publishes the latest source value if the source changed while the gate was closed. A condition-only tick does not emit when there is no blocked change to replay.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``condition`` : time-series; ``TS[bool]``
       Boolean gate controlling whether source ticks pass.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``
       Stream to filter.

    Returns
    ~~~~~~~

    Source ticks accepted while ``condition`` is true, plus the latest blocked value when the condition reopens.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       positive_prices = hg.filter_(price > 0.0, price)

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
    """Keep frame rows matching the populated fields of one compound-scalar predicate.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[Frame[SCALAR]]``, ``TS[Frame[SCALAR, SCALAR_2]]``
       Frame-valued input.

    ``predicate`` : time-series; ``TS[SCALAR_1]``
       Compound scalar whose populated fields define equality filters.

    Returns
    ~~~~~~~

    The matching rows with the original frame schema.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       matching = hg.filter_cs(rows, filter_value)

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
    """Keep frame rows matching every currently valid field of a structural predicate. Invalid predicate fields are ignored rather than compared.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[Frame[SCALAR]]``, ``TS[Frame[SCALAR, SCALAR_1]]``
       Frame-valued input.

    ``predicate`` : time-series; ``TIME_SERIES_TYPE``
       Structural bundle of column equality filters.

    Returns
    ~~~~~~~

    The matching rows with the original frame schema.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       matching = hg.filter_frame(rows, filters)

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

class _flip_Operator(_Protocol):
    """Invert a keyed dictionary so each value becomes a key. Duplicate values require ``unique=False``, which collects their original keys in a time-series set instead of choosing one arbitrarily. @note Cost: O(n) rebuild per tick.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``, ``TSD[K, TS[K_1]]``
       Keyed scalar values to invert.

    ``unique`` : scalar; ``bool``
       Assert values are unique when true; collect duplicates when false. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    An inverted keyed dictionary.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       symbols_by_sector = hg.flip(sector_by_symbol, unique=False)

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
    """Swap outer and inner keys of a nested keyed dictionary while preserving values. @note Cost: O(outer×inner) rebuild per tick.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``, ``TSD[K, V]``
       ``TSD[K, TSD[K1, V]]`` input.

    Returns
    ~~~~~~~

    ``TSD[K1, TSD[K, V]]`` with both key levels inverted.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       by_metric_then_symbol = hg.flip_keys(by_symbol_then_metric)

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
    """Divide ``lhs`` by ``rhs`` and round the quotient toward negative infinity. Python's ``lhs // rhs`` syntax wires this operator.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[int]``, ``TS[float]``, ``TSL[TIME_SERIES_TYPE, SIZE]``, ``TIME_SERIES_TYPE``
       Dividend.

    ``rhs`` : time-series; ``TS[int]``, ``TS[float]``, ``TSL[TIME_SERIES_TYPE_1, SIZE]``, ``TIME_SERIES_TYPE_1``
       Divisor.

    ``divide_by_zero`` : scalar; ``DivideByZero``
       Policy controlling the result when the divisor is zero.

    Returns
    ~~~~~~~

    The floor-division quotient.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       whole_batches = items // batch_size

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
    """Format positional and named time-series values with a Python-style format string. ``__sample__`` can reduce output frequency, while ``__strict__`` controls whether every referenced input must be valid before formatting.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``arg0`` : time-series; ``TS[str]``
       The arg0 value used by the selected overload.

    ``__sample__`` : scalar; ``int``
       Emit every nth formatted tick; one emits every tick. Optional in overloads that show ``= ...``.

    ``__strict__`` : scalar; ``bool``
       When true, wait for every supplied value to be valid. Optional in overloads that show ``= ...``.

    ``*args`` : time-series; ``TIME_SERIES_TYPE``
       Positional values used by the format string.

    ``**kwargs`` : time-series; ``time-series``
       Named values used by the format string.

    Returns
    ~~~~~~~

    The formatted string.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       message = hg.format_("{symbol}: {price:.2f}", symbol=symbol, price=price)

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
    """Forward source ticks until ``predicate`` first ticks true, then retain the last value and passivate the source permanently.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``predicate`` : time-series, scalar; ``TS[bool]``, ``callable``, ``fn``
       Boolean stream that freezes the output when true.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``
       Stream to forward until frozen.

    Returns
    ~~~~~~~

    The source stream up to the freeze point, retaining its last value.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       final_price = hg.freeze(done, price)

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
    """Replay rows from a wiring-time frame according to a timestamp column. Scalar output reads ``value_col``; keyed output additionally uses ``key_col``.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``df`` : scalar; ``frame``
       Frame containing replay rows.

    ``dt_col`` : scalar; ``str``
       Timestamp column controlling tick times. Optional in overloads that show ``= ...``.

    ``key_col`` : scalar; ``str``
       Key column used by keyed output schemas. Optional in overloads that show ``= ...``.

    ``value_col`` : scalar; ``str``
       Value column used by scalar output schemas. Optional in overloads that show ``= ...``.

    ``offset`` : scalar; ``timedelta``
       Duration added to every replay timestamp. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    A source of the explicitly selected time-series schema.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       prices = hg.from_data_frame[TSD[str, TS[float]]](frame, dt_col="date", key_col="symbol")

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
    """Replay successively supplied frame batches without concatenating the source. Each batch must arrive no later than its first retained row, preserving bounded memory while maintaining global timestamp order.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``frames`` : time-series; ``TS[frame]``
       Stream of frame batches.

    ``dt_col`` : scalar; ``str``
       Timestamp column controlling tick times. Optional in overloads that show ``= ...``.

    ``key_col`` : scalar; ``str``
       Key column used by keyed output schemas. Optional in overloads that show ``= ...``.

    ``value_col`` : scalar; ``str``
       Value column used by scalar output schemas. Optional in overloads that show ``= ...``.

    ``offset`` : scalar; ``timedelta``
       Duration added to every replay timestamp. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    A source of the explicitly selected time-series schema.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       prices = hg.from_data_frame_batches[TSD[str, TS[float]]](batches, dt_col="date")

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
    """Parse JSON text directly into an explicitly selected time-series schema. Each parsed value is applied as that tick's delta, so collection removals and structural updates follow the target type's normal delta semantics.

    Being a delta, an absent member is UNCHANGED rather than removed: a bare ``[..]`` for a ``TSS`` adds its members, and removal needs the explicit ``{"added": [..], "removed": [..]}`` form. A ``null`` element of a ``TSL`` array means that element does not tick.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[str]``
       JSON text.

    Returns
    ~~~~~~~

    Parsed values in the selected output schema.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       prices = hg.from_json[TSD[str, TS[float]]](payload)

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``
       The primary time-series input.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``OUT``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.from_table(ts)

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``value`` : scalar; ``frame``
       Value used to construct or update the output.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``OUT``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.from_table_const(value)

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
    """Queue source ticks while ``condition`` is false and release them in order after it becomes true. A positive ``buffer_length`` raises on overflow; ``-1`` keeps only the most recent gated value.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``condition`` : time-series; ``TS[bool]``
       False to buffer and true to release/forward.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``
       Stream passing through the gate.

    ``buffer_length`` : scalar; ``int``
       Maximum queued ticks, or ``-1`` for last-value mode. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    The original ticks, delayed while the gate is closed.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       released = hg.gate(is_ready, updates, buffer_length=1000)

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
    """Test whether ``lhs`` sorts after or equals ``rhs``.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[int]``, ``TS[float]``, ``TS[str]``, ``TS[date]``, ``TS[datetime]``, ``TS[timedelta]``, ``TS[SCALAR]``
       Left-hand value.

    ``rhs`` : time-series; ``TS[int]``, ``TS[float]``, ``TS[str]``, ``TS[date]``, ``TS[datetime]``, ``TS[timedelta]``, ``TS[SCALAR]``
       Right-hand value.

    Returns
    ~~~~~~~

    The result of ``lhs >= rhs``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       reached_limit = value >= limit

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

class _getattr__Operator(_Protocol):
    """Project a named field or attribute from a structured time-series value. Attribute selection is a wiring-time operation because it determines the output schema; normal ``port.field`` syntax delegates to this operator.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``REF[TIME_SERIES_TYPE]``, ``TIME_SERIES_TYPE``, ``TSD[K, TIME_SERIES_TYPE]``, ``TS[SCALAR]``, ``TS[SCALAR_1]``, ``TS[Frame[SCALAR_3]]``, ``TS[Frame[SCALAR_3, SCALAR_4]]``, ``TS[Any]``, ``TS[COMPOUND_SCALAR]``
       Structured input.

    ``attr`` : scalar; ``str``
       Field name fixed when the graph is wired.

    ``default`` : scalar; ``SCALAR_2``
       Fallback used when no more specific value is available.

    ``default_value`` : time-series; ``TS[SCALAR]``
       Optional fallback for overloads that support missing attributes. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    A port carrying the selected field or fallback.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       price = quote.price  # equivalent to hg.getattr_(quote, "price")

    Accepted native overloads:

    - ``getattr_(ts: REF[TIME_SERIES_TYPE], attr: str) -> OUT``
    - ``getattr_(ts: TIME_SERIES_TYPE, attr: str) -> OUT``
    - ``getattr_(ts: TSD[K, TIME_SERIES_TYPE], attr: str) -> OUT``
    - ``getattr_(ts: TS[SCALAR], attr: str) -> OUT``
    - ``getattr_(ts: TS[SCALAR], attr: str, default: SCALAR_1) -> OUT``
    - ``getattr_(ts: TS[Frame[SCALAR]], attr: str) -> OUT``
    - ``getattr_(ts: TS[Frame[SCALAR, SCALAR_1]], attr: str) -> OUT``
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
    """Select an item from a collection-valued time series. Python's ``ts[key]`` syntax wires this operator; live keys retarget the output when they change.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[SCALAR]``, ``TS[str]``, ``TSL[TIME_SERIES_TYPE, SIZE]``, ``TSD[K, V]``, ``REF[TIME_SERIES_TYPE_1]``, ``TIME_SERIES_TYPE_1``, ``TS[SCALAR_1]``, ``TS[Frame[SCALAR_2]]``, ``TS[Frame[SCALAR_2, SCALAR_3]]``
       Collection, mapping, list, bundle, or other indexable input.

    ``key`` : time-series, scalar; ``TS[K]``, ``TS[int]``, ``str``, ``int``, ``TSS[K]``
       Index, key, or slice selector supported by the chosen overload.

    Returns
    ~~~~~~~

    The selected item as a time-series port.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       selected_price = prices[symbol]

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
    - ``getitem_(ts: TS[Frame[SCALAR]], key: str) -> OUT``
    - ``getitem_(ts: TS[Frame[SCALAR, SCALAR_1]], key: str) -> OUT``
    - ``getitem_(ts: TS[Frame[SCALAR]], key: int) -> OUT``
    - ``getitem_(ts: TS[Frame[SCALAR, SCALAR_1]], key: int) -> OUT``
    - ``getitem_(ts: TS[Frame[SCALAR]], key: TS[int]) -> OUT``
    - ``getitem_(ts: TS[Frame[SCALAR, SCALAR_1]], key: TS[int]) -> OUT``

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
    """Partition each frame into a keyed dictionary of frames by one or more columns. @note Cost: O(rows×groups) per tick (linear bucket probe).

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[SCALAR]``
       Frame-valued input.

    ``by`` : scalar; ``SCALAR_1``
       Wiring-time column name or tuple of column names forming the key.

    Returns
    ~~~~~~~

    A keyed dictionary whose values contain rows for each distinct key.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       by_symbol = hg.group_by(rows, by="symbol")

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
    """Test whether ``lhs`` sorts after ``rhs``.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[int]``, ``TS[float]``, ``TS[str]``, ``TS[date]``, ``TS[datetime]``, ``TS[timedelta]``, ``TS[SCALAR]``
       Left-hand value.

    ``rhs`` : time-series; ``TS[int]``, ``TS[float]``, ``TS[str]``, ``TS[date]``, ``TS[datetime]``, ``TS[timedelta]``, ``TS[SCALAR]``
       Right-hand value.

    Returns
    ~~~~~~~

    The result of ``lhs > rhs``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       above_limit = value > limit

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[datetime]``, ``TS[time]``
       The primary time-series input.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[int]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.hour(ts)

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
    """Route each source tick to the ``true`` or ``false`` field of a bundle according to the latest condition. The non-selected output does not receive that tick.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``condition`` : time-series; ``TS[bool]``
       Boolean route selector.

    ``ts`` : time-series; ``REF[TIME_SERIES_TYPE]``
       Stream to route.

    Returns
    ~~~~~~~

    A two-field bundle containing the mutually exclusive routed outputs.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       routed = hg.if_(is_buy, order)
       buys, sells = routed.true, routed.false

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
    """Select one of three value streams from a three-way comparison result.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``cmp`` : time-series; ``TS[CmpResult]``
       ``LT``, ``EQ``, or ``GT`` selector, typically produced by ``cmp_``.

    ``lt`` : time-series; ``REF[OUT]``
       Value selected for ``LT``.

    ``eq`` : time-series; ``REF[OUT]``
       Value selected for ``EQ``.

    ``gt`` : time-series; ``REF[OUT]``
       Value selected for ``GT``.

    Returns
    ~~~~~~~

    The branch selected by ``cmp``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       label = hg.if_cmp(hg.cmp_(lhs, rhs), "low", "equal", "high")

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
    """Select between two value streams using the latest boolean condition. A tick from the active branch is forwarded; a tick from the inactive branch is not. Python scalar branch values are lifted to constant sources.  This preserves the nominal type of ``Enum``, ``IntEnum``, and ``StrEnum`` members, so callers do not need to wrap them in typed ``const`` nodes.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``condition`` : time-series; ``TS[bool]``
       Boolean selector.

    ``true_value`` : time-series; ``REF[TIME_SERIES_TYPE]``
       Value exposed while ``condition`` is true.

    ``false_value`` : time-series; ``REF[TIME_SERIES_TYPE]``
       Value exposed while ``condition`` is false.

    Returns
    ~~~~~~~

    The currently selected branch.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       effective = hg.if_then_else(use_live, live_value, fallback_value)

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
    """Emit true whenever ``condition`` ticks with a true value. False values are suppressed rather than emitted. One-shot mode passivates the input after the first true tick.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``condition`` : time-series; ``TS[bool]``
       Boolean stream to observe.

    ``tick_once_only`` : scalar; ``bool``
       When true, emit at most one tick. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    A true-valued signal for qualifying condition ticks.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       first_ready = hg.if_true(is_ready, tick_once_only=True)

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
    """Locate an item within an ordered collection. @note Cost: O(n) scan per tick.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TSL[TS[SCALAR], SIZE]``, ``TS[SCALAR]``
       Ordered collection to search.

    ``item`` : time-series; ``TS[SCALAR]``, ``TS[SCALAR_1]``
       Value whose position is requested.

    Returns
    ~~~~~~~

    Zero-based index according to the selected overload's missing-item policy.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       position = hg.index_of(priority_order, symbol)

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
    """Keep members present in every input set.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``*ts`` : time-series; ``TIME_SERIES_TYPE``
       Set-valued or variadic set inputs.

    Returns
    ~~~~~~~

    The common members of all inputs.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       common_symbols = hg.intersection(listed, liquid)

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
    """Apply bitwise or collection inversion selected by the input schema. Python's ``~ts`` syntax wires this operator.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[int]``, ``TS[bool]``, ``TSL[TIME_SERIES_TYPE, SIZE]``, ``TIME_SERIES_TYPE``
       Integer, boolean, or compatible collection input.

    Returns
    ~~~~~~~

    The overload-selected inverted value.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       inverted_mask = ~mask

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
    """Test the selected type's empty-value semantics.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[str]``, ``TSS[K]``, ``TSD[K, V]``, ``TIME_SERIES_TYPE``
       Collection, string, mapping, or other supported value.

    Returns
    ~~~~~~~

    True when the current value contains no elements or content.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       no_orders = hg.is_empty(active_orders)

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[date]``
       The primary time-series input.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[str]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.isoformat(ts)

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[date]``, ``TS[datetime]``
       The primary time-series input.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[int]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.isoweekday(ts)

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
    """Join two frames by equality of one or more key columns. This is part of the public ``join`` overload family alongside string joining. @note Cost: a full arrow hash join per tick.

    Join current string inputs with a fixed separator.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[Frame[SCALAR]]``
       Left frame.

    ``rhs`` : time-series; ``TS[Frame[SCALAR_1]]``
       Right frame.

    ``on`` : scalar; ``K``
       Wiring-time join column or columns.

    ``how`` : scalar; ``str``
       Join policy such as inner, left, right, or full where supported. Optional in overloads that show ``= ...``.

    ``suffix`` : scalar; ``str``
       Suffix applied to colliding right-hand column names. Optional in overloads that show ``= ...``.

    ``strings`` : time-series; ``TSL[TS[str], SIZE]``
       Collection or variadic string inputs, kept in argument order.

    ``separator`` : scalar; ``str``
       Wiring-time text inserted between adjacent values.

    ``__strict__`` : scalar; ``bool``
       Validity policy. When true, all required inputs must be valid; non-strict overloads may use the valid subset. Optional in overloads that show ``= ...``.

    ``*ts`` : time-series; ``TS[str]``, ``TS[SCALAR_2]``
       The primary time-series input.

    Returns
    ~~~~~~~

    A frame with the explicitly resolved joined row schema.

    The joined string, updated when an input changes.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       enriched = hg.join(trades, instruments, on="instrument_id", how="left")

    .. code-block:: python

       full_name = hg.join(first_name, last_name, separator=" ")

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
    """Coerce a dynamic JSON scalar leaf to a boolean.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``
       JSON scalar value.

    Returns
    ~~~~~~~

    Boolean representation.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       enabled = hg.json_as_bool(json_value)

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
    """Coerce a dynamic JSON scalar leaf to floating point.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``
       JSON scalar value.

    Returns
    ~~~~~~~

    Floating-point representation.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       price = hg.json_as_float(json_value)

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
    """Coerce a dynamic JSON scalar leaf to an integer.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``
       JSON scalar value.

    Returns
    ~~~~~~~

    Integer representation, raising for incompatible JSON shapes or values.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       count = hg.json_as_int(json_value)

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
    """Coerce a dynamic JSON scalar leaf to a string.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``
       JSON scalar value.

    Returns
    ~~~~~~~

    String representation.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       label = hg.json_as_str(json_value)

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
    """Parse JSON text into hgraph's dynamic JSON-tree value.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[str]``, ``TS[bytes]``
       JSON text.

    Returns
    ~~~~~~~

    Dynamic JSON value preserving object, array, scalar, and null structure.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       json_value = hg.json_decode(text)

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
    """Encode a dynamic JSON-tree value as standards-compliant JSON text.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``
       Dynamic JSON value.

    Returns
    ~~~~~~~

    Compact JSON string.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       text = hg.json_encode(json_value)

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
    """Project the current keys of a mapping or keyed time-series dictionary. Key additions and removals are emitted incrementally for ``TSD`` inputs.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TSD[K, V]``, ``TIME_SERIES_TYPE``
       Mapping or keyed dictionary.

    Returns
    ~~~~~~~

    Its keys as a set-valued time series.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       symbols = hg.keys_(prices)

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
    """Delay every input tick by a tick count or elapsed duration. Tick-count lag replays the value after that many later source ticks; duration lag schedules it for ``input_time + period``. @note Cost: O(delta) per tick; retains up to ``period`` pending deltas.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``, ``TSD[K, V]``, ``TSL[V, SIZE]``
       Stream to delay.

    ``period`` : scalar, time-series; ``int``, ``timedelta``, ``TS[timedelta]``
       Positive tick count or duration selected at wiring time.

    ``proxy`` : time-series; ``SIGNAL``
       Optional proxy stream whose count defines progress for proxy-lag overloads.

    Returns
    ~~~~~~~

    The original values with delayed tick times.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       previous = hg.lag(price, 1)
       delayed = hg.lag(price, timedelta(seconds=5))

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``SIGNAL``
       The primary time-series input.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[date]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.last_modified_date(ts)

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
    """Report the engine evaluation time of each source modification.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``SIGNAL``
       Signal whose modification time is observed.

    Returns
    ~~~~~~~

    The current graph evaluation timestamp whenever ``ts`` ticks.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       event_time = hg.last_modified_time(price)

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
    """Report wall-clock time when each source modification is evaluated. Unlike ``last_modified_time``, this observes real elapsed time rather than the graph's logical evaluation clock.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``SIGNAL``
       Signal whose modification time is observed.

    Returns
    ~~~~~~~

    Wall-clock timestamp for each source tick.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       received_at = hg.last_modified_wall_clock_time(price)

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
    """Test whether ``lhs`` sorts before or equals ``rhs``.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[int]``, ``TS[float]``, ``TS[str]``, ``TS[date]``, ``TS[datetime]``, ``TS[timedelta]``, ``TS[SCALAR]``
       Left-hand value.

    ``rhs`` : time-series; ``TS[int]``, ``TS[float]``, ``TS[str]``, ``TS[date]``, ``TS[datetime]``, ``TS[timedelta]``, ``TS[SCALAR]``
       Right-hand value.

    Returns
    ~~~~~~~

    The result of ``lhs <= rhs``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       within_limit = value <= limit

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
    """Return the current number of elements in a collection-valued input.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[SCALAR]``, ``TS[str]``, ``TSS[K]``, ``TSD[K, V]``, ``TSL[TIME_SERIES_TYPE, SIZE]``, ``TIME_SERIES_TYPE_1``, ``TS[SCALAR_1]``
       Collection, mapping, string, or supported structural value.

    Returns
    ~~~~~~~

    An integer that updates when collection length changes.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       active_count = hg.len_(active_orders)

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
    """Compute the natural logarithm of a floating-point time-series value.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[float]``
       Positive floating-point input.

    Returns
    ~~~~~~~

    The base-e logarithm of ``ts``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       log_price = hg.ln(price)

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
    """Format time-series values and send them to the configured logger.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``fmt`` : time-series; ``TS[str]``
       Python-style format string.

    ``level`` : scalar; ``int``
       Wiring-time logging severity. Optional in overloads that show ``= ...``.

    ``sample_count`` : scalar; ``int``
       Emit one message for every ``sample_count`` qualifying ticks. Optional in overloads that show ``= ...``.

    ``*args`` : time-series; ``TIME_SERIES_TYPE``
       Positional and packed named values used by the format string.

    ``**kwargs`` : time-series; ``time-series``
       Additional named time-series inputs.

    Returns
    ~~~~~~~

    No output.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       hg.log_("price={:.2f}", price, level=logging.INFO)

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
    """Shift integer bits left by the current right-hand value.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[int]``, ``TSL[TIME_SERIES_TYPE, SIZE]``, ``TIME_SERIES_TYPE``
       Integer value to shift.

    ``rhs`` : time-series; ``TS[int]``, ``TSL[TIME_SERIES_TYPE_1, SIZE]``, ``TIME_SERIES_TYPE_1``
       Non-negative shift distance.

    Returns
    ~~~~~~~

    ``lhs << rhs``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       mask = value << bit_count

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
    """Test whether ``lhs`` sorts before ``rhs`` using the selected value semantics.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[int]``, ``TS[float]``, ``TS[str]``, ``TS[date]``, ``TS[datetime]``, ``TS[timedelta]``, ``TS[SCALAR]``
       Left-hand value.

    ``rhs`` : time-series; ``TS[int]``, ``TS[float]``, ``TS[str]``, ``TS[date]``, ``TS[datetime]``, ``TS[timedelta]``, ``TS[SCALAR]``
       Right-hand value.

    Returns
    ~~~~~~~

    The result of ``lhs < rhs``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       below_limit = value < limit

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
    """Build a keyed dictionary from a live key and one arbitrary time-series value. A key change removes the old entry and publishes the current value under the new key.

    Three-input C++ wiring form with an explicit remove signal.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``key`` : time-series; ``TS[K]``
       Key under which the value is exposed.

    ``value`` : time-series; ``V``
       Time-series value stored at that key.

    ``remove_key`` : time-series; ``TS[bool]``
       Optional boolean stream that removes the active key when true.

    Returns
    ~~~~~~~

    A keyed dictionary containing at most the active entry.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       one_price = hg.make_tsd(symbol, price)

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
    """Match each string against a regular expression and expose both success and captures.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``pattern`` : time-series; ``TS[str]``
       Regular-expression pattern; changing it recompiles the active match.

    ``s`` : time-series; ``TS[str]``
       String to test.

    Returns
    ~~~~~~~

    A bundle containing ``is_match`` and the captured ``groups``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       match = hg.match_(r"([A-Z]+)-(\\d+)", code)

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
    """Select maxima according to input shape and arity. Unary scalar input produces a running maximum; unary collection input reduces its current values; multiple inputs select element-wise maxima. A reset restarts running state and ``default_value`` covers empty collections.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``*ts`` : time-series; ``TS[SCALAR]``, ``TIME_SERIES_TYPE``, ``TIME_SERIES_TYPE_3``, ``TSS[K]``, ``TSD[K, TS[V]]``, ``TSL[TS[V], SIZE]``, ``TS[Series[SCALAR_3]]``
       Scalar, collection, or variadic values.

    ``default_value`` : time-series, scalar; ``TS[SCALAR_1]``, ``TS[K]``, ``SCALAR_2``
       Value used when an input collection is empty.

    ``lhs`` : time-series; ``TS[SCALAR]``, ``TS[int]``, ``TS[float]``, ``TS[str]``, ``TS[date]``, ``TS[datetime]``, ``TS[timedelta]``, ``TSL[TIME_SERIES_TYPE_1, SIZE]``, ``TIME_SERIES_TYPE_1``
       Left-hand value in binary overloads.

    ``rhs`` : time-series; ``TS[SCALAR]``, ``TS[int]``, ``TS[float]``, ``TS[str]``, ``TS[date]``, ``TS[datetime]``, ``TS[timedelta]``, ``TSL[TIME_SERIES_TYPE_2, SIZE]``, ``TIME_SERIES_TYPE_2``
       Right-hand value in binary overloads.

    ``__strict__`` : scalar; ``bool``
       When true, every variadic input must be valid. Optional in overloads that show ``= ...``.

    ``*tsl`` : time-series; ``TS[SCALAR]``
       The collection or variadic sequence of time-series inputs.

    Returns
    ~~~~~~~

    The running, reduced, or element-wise maximum.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       running_high = hg.max_(price, reset=session_start)
       highest_price = hg.max_(prices_by_venue)

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
    - ``max_(ts: TS[Series[SCALAR]]) -> OUT``

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

class _mean_Operator(_Protocol):
    """Calculate a mean according to input shape and arity. Unary scalar input produces a running mean; unary collection input averages its current members; multiple inputs are averaged element by element.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``*ts`` : time-series; ``TS[SCALAR]``, ``TS[int]``, ``TS[float]``, ``TIME_SERIES_TYPE``, ``TIME_SERIES_TYPE_1``, ``TSS[int]``, ``TSS[float]``, ``TSD[K, TS[int]]``, ``TSD[K, TS[float]]``, ``TSL[TS[int], SIZE]``, ``TSL[TS[float], SIZE]``
       Value, collection, or variadic inputs to average.

    ``default_value`` : time-series; ``TS[SCALAR_1]``
       Fallback used when a collection has no values to average.

    ``lhs`` : time-series; ``TS[int]``, ``TS[float]``, ``TSL[TIME_SERIES_TYPE_2, SIZE]``, ``TIME_SERIES_TYPE_2``
       The left-hand operand.

    ``rhs`` : time-series; ``TS[int]``, ``TS[float]``, ``TSL[TIME_SERIES_TYPE_3, SIZE]``, ``TIME_SERIES_TYPE_3``
       The right-hand operand.

    Returns
    ~~~~~~~

    The overload-selected mean, promoted to floating point where required.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       running_average = hg.mean(price)
       cross_sectional_average = hg.mean(prices_by_symbol)

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
    """Forward the first input modified in each evaluation cycle. Input order is the tie-breaker when several streams tick together. For keyed dictionaries, distinct keys from all ticking inputs are combined; the leftmost input wins a same-key conflict.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``*tsl`` : time-series; ``TIME_SERIES_TYPE``, ``TSD[K, V]``
       Ordered input streams.

    ``disjoint`` : scalar; ``bool``
       When true, selects the faster TSD path and promises that input dictionaries have no overlapping keys. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    A stream containing the cycle's first modified value, or merged TSD delta.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       preferred_tick = hg.merge(primary, secondary)
       combined_books = hg.merge(bids, asks, disjoint=True)

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

class _microsecond_Operator(_Protocol):
    """``microsecond`` — the microsecond component of a datetime or time.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[datetime]``, ``TS[time]``
       The primary time-series input.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[int]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.microsecond(ts)

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[timedelta]``
       The primary time-series input.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[int]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.microseconds(ts)

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
    """Select minima according to input shape and arity. Unary scalar input produces a running minimum; unary collection input reduces its current values; multiple inputs select element-wise minima. A reset restarts running state and ``default_value`` covers empty collections.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``*ts`` : time-series; ``TS[SCALAR]``, ``TIME_SERIES_TYPE``, ``TIME_SERIES_TYPE_3``, ``TSS[K]``, ``TSD[K, TS[V]]``, ``TSL[TS[V], SIZE]``, ``TS[Series[SCALAR_3]]``
       Scalar, collection, or variadic values.

    ``default_value`` : time-series, scalar; ``TS[SCALAR_1]``, ``TS[K]``, ``SCALAR_2``
       Value used when an input collection is empty.

    ``lhs`` : time-series; ``TS[SCALAR]``, ``TS[int]``, ``TS[float]``, ``TS[str]``, ``TS[date]``, ``TS[datetime]``, ``TS[timedelta]``, ``TSL[TIME_SERIES_TYPE_1, SIZE]``, ``TIME_SERIES_TYPE_1``
       Left-hand value in binary overloads.

    ``rhs`` : time-series; ``TS[SCALAR]``, ``TS[int]``, ``TS[float]``, ``TS[str]``, ``TS[date]``, ``TS[datetime]``, ``TS[timedelta]``, ``TSL[TIME_SERIES_TYPE_2, SIZE]``, ``TIME_SERIES_TYPE_2``
       Right-hand value in binary overloads.

    ``__strict__`` : scalar; ``bool``
       When true, every variadic input must be valid. Optional in overloads that show ``= ...``.

    ``*tsl`` : time-series; ``TS[SCALAR]``
       The collection or variadic sequence of time-series inputs.

    Returns
    ~~~~~~~

    The running, reduced, or element-wise minimum.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       running_low = hg.min_(price, reset=session_start)
       lowest_price = hg.min_(prices_by_venue)

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
    - ``min_(ts: TS[Series[SCALAR]]) -> OUT``

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

class _minute_Operator(_Protocol):
    """``minute`` — the minute component of a datetime or time.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[datetime]``, ``TS[time]``
       The primary time-series input.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[int]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.minute(ts)

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
    """Return the remainder paired with floor division of ``lhs`` by ``rhs``. Python's ``lhs % rhs`` syntax wires this operator.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[int]``, ``TS[float]``, ``TSL[TIME_SERIES_TYPE, SIZE]``, ``TIME_SERIES_TYPE``
       Dividend.

    ``rhs`` : time-series; ``TS[int]``, ``TS[float]``, ``TSL[TIME_SERIES_TYPE_1, SIZE]``, ``TIME_SERIES_TYPE_1``
       Divisor.

    ``divide_by_zero`` : scalar; ``DivideByZero``
       Policy controlling the result when the divisor is zero.

    Returns
    ~~~~~~~

    The remainder, with the divisor's sign under Python semantics.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       bucket_offset = sequence % bucket_count

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
    """Produce a live boolean pulse describing source modification in the current cycle.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``SIGNAL``
       Signal whose tick timing is observed; its value is ignored.

    Returns
    ~~~~~~~

    True in a modification cycle and false in subsequent evaluation cycles.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       price_ticked = hg.modified(price)

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[date]``, ``TS[datetime]``
       The primary time-series input.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[int]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.month(ts)

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[date]``, ``TS[datetime]``
       The primary time-series input.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[int]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.month_of_year(ts)

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
    """Multiply two values, including numeric promotion and collection broadcasting. Python's ``lhs * rhs`` syntax uses the normal strict overload. Call ``mul_`` directly to select non-strict validity behaviour where it is supported.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[int]``, ``TS[float]``, ``TS[str]``, ``TSL[TIME_SERIES_TYPE, SIZE]``, ``TIME_SERIES_TYPE``, ``TS[timedelta]``, ``TS[period]``, ``TS[SCALAR]``
       Left-hand multiplicand.

    ``rhs`` : time-series; ``TS[int]``, ``TS[float]``, ``TS[str]``, ``TSL[TIME_SERIES_TYPE_1, SIZE]``, ``TIME_SERIES_TYPE_1``, ``TS[period]``
       Right-hand multiplicand.

    Returns
    ~~~~~~~

    The product selected from the operand schemas.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       scaled = hg.mul_(price, quantity, __strict__=True)

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
    """Compare two current values for inequality. Python's ``lhs != rhs`` syntax wires it.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[bool]``, ``TS[int]``, ``TS[float]``, ``TS[str]``, ``TS[date]``, ``TS[datetime]``, ``TS[timedelta]``, ``TSL[TIME_SERIES_TYPE, SIZE]``, ``TS[SCALAR]``
       Left-hand value.

    ``rhs`` : time-series; ``TS[bool]``, ``TS[int]``, ``TS[float]``, ``TS[str]``, ``TS[date]``, ``TS[datetime]``, ``TS[timedelta]``, ``TSL[TIME_SERIES_TYPE_1, SIZE]``, ``TS[SCALAR]``
       Right-hand value.

    Returns
    ~~~~~~~

    True when the values do not compare equal.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       changed = current != previous

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
    """Negate each input value. Python's ``-ts`` syntax wires this operator.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[int]``, ``TS[float]``, ``TS[timedelta]``, ``TS[period]``, ``TSL[TIME_SERIES_TYPE, SIZE]``, ``TIME_SERIES_TYPE``
       Numeric, duration, or compatible collection input.

    Returns
    ~~~~~~~

    The additive inverse of ``ts``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       debit = -credit

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
    """Negate the truth value of each input tick.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[bool]``, ``TS[int]``, ``TS[float]``, ``TS[str]``, ``TSS[K]``, ``TSD[K, V]``
       Truth-valued input.

    Returns
    ~~~~~~~

    Boolean logical negation.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       closed = hg.not_(market_open)

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
    """Create an invalid source of an explicitly selected type that never ticks. This is useful as an empty branch or optional graph input without inventing a value.

    Returns
    ~~~~~~~

    A permanently invalid port of the selected type.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       absent = hg.nothing[TS[int]]()

    Accepted native overloads:

    - ``nothing() -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

nothing: _nothing_Operator

class _null_sink_Operator(_Protocol):
    """Consume a stream without producing output or side effects. Use this to make an otherwise unused branch part of the executable graph.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``
       Stream to keep connected and active.

    Returns
    ~~~~~~~

    No output.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       hg.null_sink(background_updates)

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
    """Return the boolean disjunction of two current values using their truth semantics. Both input ports remain wired and active.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[SCALAR]``, ``TS[bool]``, ``TS[int]``, ``TS[float]``, ``TS[str]``, ``TSS[K]``
       Left-hand truth-valued input.

    ``rhs`` : time-series; ``TS[SCALAR]``, ``TS[bool]``, ``TS[int]``, ``TS[float]``, ``TS[str]``, ``TSS[K]``
       Right-hand truth-valued input.

    Returns
    ~~~~~~~

    True when either value is truthy.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       alert = hg.or_(price_alert, risk_alert)

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
    """Partition a keyed dictionary into nested dictionaries using a live key-to-group map. Mapping changes move an entry between partitions without changing its inner key. @note Cost: O(n×groups) per tick.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``, ``TSD[K, V]``
       Keyed values to partition.

    ``partitions`` : time-series; ``TIME_SERIES_TYPE_1``, ``TSD[K, TS[K_1]]``
       Mapping from each input key to its outer partition key.

    Returns
    ~~~~~~~

    ``TSD[K1, TSD[K, V]]`` grouped by the mapped partition.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       prices_by_sector = hg.partition(prices, sector_by_symbol)

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

class _pos__Operator(_Protocol):
    """Apply unary plus to each input value. Python's ``+ts`` syntax wires this operator and preserves the resolved value schema.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[int]``, ``TS[float]``, ``TS[timedelta]``, ``TSL[TIME_SERIES_TYPE, SIZE]``, ``TIME_SERIES_TYPE``
       Numeric or compatible collection input.

    Returns
    ~~~~~~~

    The positive form of ``ts``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       normalized = +value

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
    """Raise ``lhs`` to the power ``rhs``. Python's ``lhs ** rhs`` syntax wires this operator; overloads select integer or floating-point result semantics.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[int]``, ``TS[float]``, ``TSL[TIME_SERIES_TYPE, SIZE]``, ``TIME_SERIES_TYPE``
       Base value.

    ``rhs`` : time-series; ``TS[int]``, ``TS[float]``, ``TSL[TIME_SERIES_TYPE_1, SIZE]``, ``TIME_SERIES_TYPE_1``
       Exponent.

    ``divide_by_zero`` : scalar; ``DivideByZero``
       Policy used by overloads where a negative exponent would divide by a zero base.

    Returns
    ~~~~~~~

    ``lhs`` raised to ``rhs``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       squared = values ** 2

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
    """Format time-series values and write a line to standard output when they tick.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``fmt`` : time-series; ``TS[str]``
       Python-style format string, supplied as a port or liftable value.

    ``__std_out__`` : scalar; ``bool``
       The std out value used by the selected overload. Optional in overloads that show ``= ...``.

    ``*args`` : time-series; ``TIME_SERIES_TYPE``
       Positional and packed named values referenced by the format string.

    ``**kwargs`` : time-series; ``time-series``
       Additional named time-series inputs.

    Returns
    ~~~~~~~

    No output.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       hg.print_("{}: {:.2f}", symbol, price)

    Accepted native overloads:

    - ``print_(fmt: TS[str], *args: TIME_SERIES_TYPE, __std_out__: bool = ..., **kwargs: time-series) -> None``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, fmt: _WiringPort | str, *args: _WiringPort | object, __std_out__: bool = ..., **kwargs: _WiringPort | object) -> None: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

print_: _print__Operator

class _race_Operator(_Protocol):
    """Expose the first valid input in argument order, independently of which input ticks. If the selected input invalidates, the output falls through to the next valid input.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``*ts`` : time-series; ``TIME_SERIES_TYPE``
       Ordered candidate streams.

    Returns
    ~~~~~~~

    The first currently valid candidate.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       effective_price = hg.race(live_price, cached_price, fallback_price)

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[instant_range]``, ``TS[civil_date_range]``
       The left-hand operand.

    ``rhs`` : time-series; ``TS[instant_range]``, ``TS[civil_date_range]``
       The right-hand operand.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[bool]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.range_adjacent(lhs, rhs)

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``range`` : time-series; ``TS[instant_range]``, ``TS[civil_date_range]``
       The range value used by the selected overload.

    ``value`` : time-series; ``TS[datetime]``, ``TS[date]``, ``TS[instant_range]``, ``TS[civil_date_range]``
       Value used to construct or update the output.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[bool]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.range_contains(range, value)

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[instant_range]``, ``TS[civil_date_range]``
       The left-hand operand.

    ``rhs`` : time-series; ``TS[instant_range]``, ``TS[civil_date_range]``
       The right-hand operand.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[instant_range_set]``, ``TS[civil_date_range_set]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.range_difference(lhs, rhs)

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``range`` : time-series; ``TS[instant_range]``
       The range value used by the selected overload.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[timedelta]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.range_extent(range)

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[instant_range]``, ``TS[civil_date_range]``
       The left-hand operand.

    ``rhs`` : time-series; ``TS[instant_range]``, ``TS[civil_date_range]``
       The right-hand operand.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[instant_range]``, ``TS[civil_date_range]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.range_hull(lhs, rhs)

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[instant_range]``, ``TS[civil_date_range]``
       The left-hand operand.

    ``rhs`` : time-series; ``TS[instant_range]``, ``TS[civil_date_range]``
       The right-hand operand.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[instant_range]``, ``TS[civil_date_range]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.range_intersection(lhs, rhs)

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[instant_range]``, ``TS[civil_date_range]``
       The left-hand operand.

    ``rhs`` : time-series; ``TS[instant_range]``, ``TS[civil_date_range]``
       The right-hand operand.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[instant_range]``, ``TS[civil_date_range]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.range_merge(lhs, rhs)

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[instant_range]``, ``TS[civil_date_range]``
       The left-hand operand.

    ``rhs`` : time-series; ``TS[instant_range]``, ``TS[civil_date_range]``
       The right-hand operand.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[bool]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.range_mergeable(lhs, rhs)

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[instant_range]``, ``TS[civil_date_range]``
       The left-hand operand.

    ``rhs`` : time-series; ``TS[instant_range]``, ``TS[civil_date_range]``
       The right-hand operand.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[bool]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.range_overlaps(lhs, rhs)

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
    """Move both finite range boundaries by a duration or calendar period. Open/closed boundary flags and unbounded endpoints are preserved.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``range`` : time-series; ``TS[instant_range]``, ``TS[civil_date_range]``
       Temporal range to move.

    ``delta`` : time-series; ``TS[timedelta]``, ``TS[period]``
       Fixed duration or calendar-relative period added to each finite endpoint.

    ``month_end_policy`` : scalar; ``month_end_policy``
       Policy for calendar shifts whose target month lacks the source day. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    The shifted range.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       next_month = hg.range_shift(window, hg.Period(months=1))

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[instant_range]``, ``TS[civil_date_range]``
       The left-hand operand.

    ``rhs`` : time-series; ``TS[instant_range]``, ``TS[civil_date_range]``
       The right-hand operand.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[bool]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.range_touches(lhs, rhs)

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[instant_range]``, ``TS[civil_date_range]``
       The left-hand operand.

    ``rhs`` : time-series; ``TS[instant_range]``, ``TS[civil_date_range]``
       The right-hand operand.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[instant_range_set]``, ``TS[civil_date_range_set]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.range_union(lhs, rhs)

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
    """Persist source ticks through the active record/replay backend. The effective location combines graph recordable context with ``key``.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``
       Stream to record.

    ``key`` : scalar; ``str``
       Wiring-time name within the current recordable context. Optional in overloads that show ``= ...``.

    ``sparse`` : scalar; ``bool``
       The sparse value used by the selected overload. Optional in overloads that show ``= ...``.

    ``model`` : scalar; ``str``
       Optional per-call backend id (``"memory"``, ``"testing"``, or an extension id such as ``"hgraph.persistence.frame"``; legacy model names are translated); an empty value inherits the graph configuration. Optional in overloads that show ``= ...``.

    ``recordable_id`` : scalar; ``str``
       Optional explicit identity; context supplies it when omitted. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    No output.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       hg.record(price, key="price")

    Accepted native overloads:

    - ``record(ts: TIME_SERIES_TYPE, key: str = ..., sparse: bool = ..., model: str = ...) -> None``
    - ``record(ts: TIME_SERIES_TYPE, key: str = ..., recordable_id: str = ..., model: str = ...) -> None``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | object, key: str = ..., sparse: bool = ..., model: str = ...) -> None: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, key: str = ..., recordable_id: str = ..., model: str = ...) -> None: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

record: _record_Operator

class _reduce_tsd_of_bundles_with_race_Operator(_Protocol):
    """Bundle-specialized keyed race reduction with the same first-valid and fall-through semantics as ``reduce_tsd_with_race``.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``tsd`` : time-series; ``TSD[K, REF[TIME_SERIES_TYPE]]``
       Keyed bundle references considered in deterministic key order.

    Returns
    ~~~~~~~

    A reference to the first currently valid bundle.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       active_bundle = hg.reduce_tsd_of_bundles_with_race(candidates)

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
    """Select the first valid referenced value in key iteration order. The output falls through when the selected entry is removed or invalidated.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``tsd`` : time-series; ``TSD[K, REF[TIME_SERIES_TYPE]]``
       Keyed references considered in deterministic key order.

    Returns
    ~~~~~~~

    A reference to the first currently valid entry.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       active = hg.reduce_tsd_with_race(candidates)

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
    """Replace input dictionary keys according to a live key mapping. Entries without a usable target key are omitted; mapping changes move the corresponding current value to its new key. @note Cost: O(n) rebuild per tick (scalar-map form); delta-driven for TSD.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``, ``TSD[K, V]``
       Keyed values to transform.

    ``new_keys`` : time-series; ``K``, ``TSD[K, TS[K_1]]``, ``TSD[K, TSS[K_1]]``
       Mapping from each existing key to its replacement.

    Returns
    ~~~~~~~

    The same values indexed by their mapped keys.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       by_identifier = hg.rekey(by_symbol, symbol_to_id)

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
    """Replace regular-expression matches in each input string.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``pattern`` : time-series; ``TS[str]``
       Pattern whose matches are replaced.

    ``repl`` : time-series; ``TS[str]``
       Replacement string, including supported capture references.

    ``s`` : time-series; ``TS[str]``
       Source string.

    Returns
    ~~~~~~~

    The transformed string.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       normalized = hg.replace(r"\\s+", "_", label)

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
    """Replay stored ticks for a key as an explicitly selected output type. Replay timing and availability follow the active record/replay mode and backend.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``key`` : scalar; ``str``
       Wiring-time name within the current recordable context.

    ``recordable_id`` : scalar; ``str``
       Optional explicit identity; context supplies it when omitted. Optional in overloads that show ``= ...``.

    ``model`` : scalar; ``str``
       Optional per-call backend id (``"memory"``, ``"testing"``, or an extension id such as ``"hgraph.persistence.frame"``; legacy model names are translated); an empty value inherits the graph configuration. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    A source reproducing the recorded stream.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       price = hg.replay[TS[float]](key="price")
       positions = hg.replay[TSD[str, TS[float]]](
           key="positions", partition_names=("symbol",))

    Accepted native overloads:

    - ``replay(key: str, recordable_id: str = ..., model: str = ...) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, key: str, recordable_id: str = ..., model: str = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

replay: _replay_Operator

class _replay_data_frame_Operator(_Protocol):
    """Replay a canonical bitemporal table frame, selecting the latest as-of revision for each partition before applying event-time deltas. @note Retained memory: the whole decoded tick list is held for the run.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``data_frame`` : scalar; ``frame``
       Canonical table frame to replay.

    ``as_of_time`` : scalar; ``datetime``
       Revision cutoff fixed at wiring time. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    A source of the explicitly selected table-compatible schema.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       positions = hg.replay_data_frame[TSD[str, TS[float]]](frame, as_of_time=cutoff)

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
    """Allocate a process-unique integer identifier when the node starts.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``hash`` : scalar; ``int``
       Required wiring-time namespace discriminator retained for service compatibility.

    Returns
    ~~~~~~~

    A single identifier tick suitable for correlating request/reply flows.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       correlation_id = hg.request_id(1)

    Accepted native overloads:

    - ``request_id(hash: int) -> TS[int]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, hash: int) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

request_id: _request_id_Operator

class _resolve_civil_Operator(_Protocol):
    """Resolve a timezone-free local civil datetime to an absolute zoned instant. Daylight-saving overlaps and gaps require explicit, fixed wiring-time policies so ambiguous data cannot silently select an instant.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``local`` : time-series; ``TS[civil_datetime]``
       Local date and time without an offset.

    ``zone`` : time-series; ``TS[zone_id]``
       IANA zone whose transition rules interpret ``local``.

    ``ambiguous`` : scalar; ``ambiguous_time_policy``
       Policy for a local time that occurs twice. Optional in overloads that show ``= ...``.

    ``nonexistent`` : scalar; ``nonexistent_time_policy``
       Policy for a local time skipped by a clock transition. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    The resolved zoned datetime.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       resolved = hg.resolve_civil(local, zone, ambiguous=hg.AmbiguousTimePolicy.EARLIEST,
                                   nonexistent=hg.NonexistentTimePolicy.NEXT_VALID)

    Accepted native overloads:

    - ``resolve_civil(local: TS[civil_datetime], zone: TS[zone_id], ambiguous: ambiguous_time_policy = ..., nonexistent: nonexistent_time_policy = ...) -> TS[zoned_datetime]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, local: _WiringPort | _CivilDateTime, zone: _WiringPort | _ZoneId, ambiguous: _AmbiguousTimePolicy = ..., nonexistent: _NonexistentTimePolicy = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

resolve_civil: _resolve_civil_Operator

class _round__Operator(_Protocol):
    """Round a floating-point value to ``n_digits`` decimal places using Python-compatible rounding semantics.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[float]``
       Values to round.

    ``n_digits`` : time-series; ``TS[int]``
       Decimal precision; this may itself vary as a time series.

    Returns
    ~~~~~~~

    The rounded floating-point value.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       display_price = hg.round_(price, 2)

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
    """Route each source tick to one element of an output list.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``index`` : time-series; ``TS[int]``
       Zero-based destination selected by its latest value.

    ``ts`` : time-series; ``REF[TIME_SERIES_TYPE]``
       Stream to route.

    Returns
    ~~~~~~~

    A time-series list whose selected element receives each source tick.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       partitions = hg.route_by_index(destination, event)

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
    """Shift integer bits right by the current right-hand value.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[int]``, ``TSL[TIME_SERIES_TYPE, SIZE]``, ``TIME_SERIES_TYPE``
       Integer value to shift.

    ``rhs`` : time-series; ``TS[int]``, ``TSL[TIME_SERIES_TYPE_1, SIZE]``, ``TIME_SERIES_TYPE_1``
       Non-negative shift distance.

    Returns
    ~~~~~~~

    ``lhs >> rhs``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       bucket = value >> bit_count

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
    """Sample the latest valid value of ``ts`` whenever ``signal`` ticks. Ticks from ``ts`` alone update the value available to sample but do not produce output.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``signal`` : time-series; ``SIGNAL``
       Trigger whose tick timing controls output; its value is ignored.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``
       Input whose latest value is sampled.

    Returns
    ~~~~~~~

    ``ts`` reticked at the signal's times.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       snapshot = hg.sample(clock, price)

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
    """Create a periodic ``True`` signal driven by engine time.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``delay`` : scalar, time-series; ``timedelta``, ``TS[timedelta]``
       Positive interval between ticks; time-series overloads allow it to change.

    ``initial_delay`` : scalar; ``bool``
       When true, wait one ``delay`` before the first tick; when false, tick at graph start. Optional in overloads that show ``= ...``.

    ``max_ticks`` : scalar; ``int``
       Optional upper bound after which the source becomes passive. Optional in overloads that show ``= ...``.

    ``start`` : time-series; ``TS[datetime]``
       Optional time-series start instant that re-bases the schedule grid.

    Returns
    ~~~~~~~

    A boolean signal ticking at the requested schedule.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       every_second = hg.schedule(timedelta(seconds=1))

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[datetime]``, ``TS[time]``
       The primary time-series input.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[int]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.second(ts)

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[timedelta]``
       The primary time-series input.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[int]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.seconds(ts)

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
    """Return a structured value with one field replaced by another time-series value.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[SCALAR]``
       Original structured input.

    ``attr`` : scalar; ``str``
       Field name fixed at wiring time.

    ``value`` : time-series; ``TS[V]``
       Replacement field value.

    Returns
    ~~~~~~~

    A structure with the same schema and updated field.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       repriced = hg.setattr_(quote, "price", new_price)

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
    """Classify the sign of a numeric time-series value.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[int]``, ``TS[float]``
       Numeric input.

    Returns
    ~~~~~~~

    ``-1`` for a negative value and ``+1`` otherwise, including zero.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       direction = hg.sign(change)

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
    """Slice a stream by tick position, combining prefix removal, truncation, and stride.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``
       Stream to slice.

    ``start`` : scalar; ``int``
       Zero-based inclusive first tick position.

    ``stop`` : scalar; ``int``
       Zero-based exclusive stop position.

    ``step_size`` : scalar; ``int``
       Positive stride between forwarded ticks.

    Returns
    ~~~~~~~

    Ticks at positions described by ``slice(start, stop, step_size)``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       middle_even_ticks = hg.slice_(updates, start=2, stop=10, step_size=2)

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
    """Order rows in each frame by one column. @note Cost: a full sort per tick.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[Frame[SCALAR]]``, ``TS[Frame[SCALAR, SCALAR_1]]``
       Frame-valued input.

    ``by`` : scalar; ``str``
       Wiring-time sort column.

    ``descending`` : scalar; ``bool``
       Reverse the ascending order when true. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    A frame with the same row schema in sorted order.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       ranked = hg.sorted_(rows, by="price", descending=True)

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``s`` : time-series; ``TS[str]``
       String to split.

    ``separator`` : scalar; ``str``
       Wiring-time separator; it is fixed for the graph's lifetime.

    Returns
    ~~~~~~~

    The explicitly selected tuple, list, set, or other supported string collection.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       fields = hg.split[TS[tuple[str, ...]]](line, separator=",")

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

class _step_Operator(_Protocol):
    """Forward one source tick for each ``step_size`` input ticks.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``
       Stream to subsample.

    ``step_size`` : scalar; ``int``
       Positive stride fixed at wiring time.

    Returns
    ~~~~~~~

    Every ``step_size``-th tick of ``ts``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       every_tenth = hg.step(updates, 10)

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
    """Request a graceful graph-engine stop when the trigger ticks. The current evaluation cycle completes before execution terminates.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``SIGNAL``
       Trigger signal; its value is ignored.

    ``msg`` : scalar; ``str``
       Optional diagnostic reason attached to the stop request. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    No output.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       hg.stop_engine(done, msg="processing complete")

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
    """Convert each valid input value to its string representation.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``
       Values to render.

    Returns
    ~~~~~~~

    A ``TS[str]`` that ticks when ``ts`` ticks.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       label = hg.str_(value)

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
    """Subtract ``rhs`` from ``lhs`` using the overload selected for their schemas. Numeric, temporal, collection, and keyed-set forms are supported; Python's ``lhs - rhs`` syntax wires this operator.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``lhs`` : time-series; ``TS[int]``, ``TS[float]``, ``TS[timedelta]``, ``TS[datetime]``, ``TS[date]``, ``TS[period]``, ``TS[civil_datetime]``, ``TS[zoned_datetime]``, ``TSL[TIME_SERIES_TYPE, SIZE]``, ``TIME_SERIES_TYPE``, ``TS[SCALAR]``, ``TS[str]``, ``TSS[K]``, ``TSD[K, V]``
       Value from which ``rhs`` is subtracted.

    ``rhs`` : time-series; ``TS[int]``, ``TS[float]``, ``TS[timedelta]``, ``TS[datetime]``, ``TS[date]``, ``TS[period]``, ``TS[civil_datetime]``, ``TSL[TIME_SERIES_TYPE_1, SIZE]``, ``TIME_SERIES_TYPE_1``, ``TS[SCALAR]``, ``TS[SCALAR_1]``, ``TS[str]``, ``TS[K]``, ``TSD[K, V]``
       Value to subtract; compatible plain values are lifted to constants.

    ``month_end_policy`` : scalar; ``month_end_policy``
       Policy used when subtracting a calendar period near month end. Optional in overloads that show ``= ...``.

    ``cmp`` : scalar; ``callable``
       The cmp value used by the selected overload. Optional in overloads that show ``= ...``.

    ``*ts`` : time-series; ``TIME_SERIES_TYPE_2``
       The primary time-series input.

    Returns
    ~~~~~~~

    The difference, with its schema selected from both operand schemas.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       change = lhs - rhs  # equivalent to hg.sub_(lhs, rhs)

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
    """Extract the slice ``s[start:end]`` using live start and end positions.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``s`` : time-series; ``TS[str]``
       Source string.

    ``start`` : time-series; ``TS[int]``
       Inclusive starting index.

    ``end`` : time-series; ``TS[int]``
       Exclusive ending index.

    Returns
    ~~~~~~~

    The selected substring.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       prefix = hg.substr(code, 0, 3)

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
    """Sum numeric values according to input shape and arity. Unary scalar input produces a running sum; unary collection input reduces its current members; multiple inputs are added element by element. An optional reset clears running state before admitting a same-cycle source tick.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``*ts`` : time-series; ``TS[SCALAR]``, ``TS[int]``, ``TS[float]``, ``TIME_SERIES_TYPE``, ``TIME_SERIES_TYPE_1``, ``TSS[int]``, ``TSS[float]``, ``TSD[K, TS[int]]``, ``TSD[K, TS[float]]``, ``TSL[TS[int], SIZE]``, ``TSL[TS[float], SIZE]``
       Value, collection, or variadic inputs to sum.

    ``default_value`` : time-series; ``TS[SCALAR_1]``
       Value emitted when the primary input has no usable value.

    ``reset`` : time-series; ``TS[bool]``
       Optional signal that resets a running sum to its identity.

    ``lhs`` : time-series; ``TSL[TIME_SERIES_TYPE_2, SIZE]``, ``TIME_SERIES_TYPE_2``
       The left-hand operand.

    ``rhs`` : time-series; ``TSL[TIME_SERIES_TYPE_3, SIZE]``, ``TIME_SERIES_TYPE_3``
       The right-hand operand.

    Returns
    ~~~~~~~

    The running, reduced, or element-wise sum selected by overload resolution.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       running_total = hg.sum_(amount, reset=session_start)
       basket_total = hg.sum_(prices)

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
    """Keep members present in an odd number of input sets, excluding shared members.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``*ts`` : time-series; ``TIME_SERIES_TYPE``
       Set-valued or variadic set inputs.

    Returns
    ~~~~~~~

    The symmetric difference of the inputs.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       changed_membership = hg.symmetric_difference(before, after)

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
    """Forward the first ``count`` source ticks, then passivate the input.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[SCALAR]``, ``TIME_SERIES_TYPE``
       Stream to truncate.

    ``count`` : scalar; ``int``
       Non-negative number of ticks to forward, fixed at wiring time. Optional in overloads that show ``= ...``.

    ``reset`` : time-series; ``SIGNAL``
       Optional signal that clears the operator's accumulated state when it ticks.

    Returns
    ~~~~~~~

    At most the first ``count`` ticks of ``ts``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       first_ten = hg.take(updates, 10)

    Accepted native overloads:

    - ``take(ts: TS[SCALAR], count: int = ...) -> TS[SCALAR]``
    - ``take(ts: TIME_SERIES_TYPE, count: int = ...) -> TIME_SERIES_TYPE``
    - ``take(ts: TIME_SERIES_TYPE, reset: SIGNAL, count: int = ...) -> TIME_SERIES_TYPE``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    @_overload
    def __call__(self, ts: _WiringPort | object, count: int = ...) -> _WiringPort: ...
    @_overload
    def __call__(self, ts: _WiringPort | object, reset: _WiringPort, count: int = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

take: _take_Operator

class _temporal_bucket_Operator(_Protocol):
    """Return the half-open fixed-width range containing an instant.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``value`` : time-series; ``TS[datetime]``
       Instant to classify.

    ``width`` : time-series; ``TS[timedelta]``
       Positive bucket duration.

    ``origin`` : scalar; ``datetime``
       Optional origin anchoring bucket boundaries. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    The containing ``[start, end)`` instant range.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       minute_bucket = hg.temporal_bucket(instant, timedelta(minutes=1))

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
    """Round a temporal value up to the following fixed-duration boundary. An exact boundary remains unchanged.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``value`` : time-series; ``TS[timedelta]``, ``TS[datetime]``
       Instant or supported temporal value to round.

    ``quantum`` : time-series; ``TS[timedelta]``
       Positive fixed duration defining the boundary grid.

    ``origin`` : scalar; ``datetime``
       Optional origin from which boundaries are measured. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    The least boundary not before ``value``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       next_minute = hg.temporal_ceil(instant, timedelta(minutes=1))

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
    """Round a temporal value down to the preceding fixed-duration boundary.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``value`` : time-series; ``TS[timedelta]``, ``TS[datetime]``
       Instant or supported temporal value to round.

    ``quantum`` : time-series; ``TS[timedelta]``
       Positive fixed duration defining the boundary grid.

    ``origin`` : scalar; ``datetime``
       Optional origin from which boundaries are measured. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    The greatest boundary not after ``value``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       minute = hg.temporal_floor(instant, timedelta(minutes=1))

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
    """Round a temporal value to the nearest fixed-duration boundary.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``value`` : time-series; ``TS[timedelta]``, ``TS[datetime]``
       Instant or supported temporal value to round.

    ``quantum`` : time-series; ``TS[timedelta]``
       Positive fixed duration defining the boundary grid.

    ``origin`` : scalar; ``datetime``
       Optional origin from which boundaries are measured. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    The nearest boundary using the selected tie behaviour.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       nearest_minute = hg.temporal_round(instant, timedelta(minutes=1))

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
    """Limit output frequency while preserving the latest pending source value. Unlike ``hgraph_analytics.resample``, no output is produced during an interval with no source tick. @note Cost: O(delta) per tick; retains the pending deltas of one period.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``
       Stream whose tick rate is limited.

    ``period`` : time-series; ``TS[timedelta]``
       Minimum elapsed time between output ticks; it may vary at runtime.

    ``delay_first_tick`` : scalar; ``bool``
       The delay first tick value used by the selected overload. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    A rate-limited stream of the latest source values.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       limited = hg.throttle(updates, timedelta(milliseconds=100))

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[datetime]``
       The primary time-series input.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[float]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.timestamp(ts)

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``value`` : time-series; ``TS[zoned_datetime]``
       Value used to construct or update the output.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[civil_datetime]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.to_civil(value)

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
    """Snapshot each input tick into a typed one-tick frame. The explicitly selected ``Frame`` schema determines column types and names.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TSD[K, V]``, ``TIME_SERIES_TYPE``
       Stream to snapshot.

    ``dt_col`` : scalar; ``str``
       Optional evaluation-time column name. Optional in overloads that show ``= ...``.

    ``key_col`` : scalar; ``str``
       Key column name for keyed inputs. Optional in overloads that show ``= ...``.

    ``value_col`` : scalar; ``str``
       Value column name for scalar inputs. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    A frame-valued time series representing each input delta.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       rows = hg.to_data_frame[TS[Frame[PriceRow]]](prices, key_col="symbol")

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``value`` : time-series; ``TS[zoned_datetime]``
       Value used to construct or update the output.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[datetime]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.to_instant(value)

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
    """Serialize each time-series tick as JSON text. Full-value mode encodes the current value; delta mode encodes the canonical per-tick delta used by record/replay and preserves removals explicitly.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``
       Value or structure to encode.

    ``delta`` : scalar; ``bool``
       When true, encode only the current tick's canonical delta. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    JSON text for each source tick.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       payload = hg.to_json(prices, delta=True)

    Accepted native overloads:

    - ``to_json(ts: TIME_SERIES_TYPE, delta: bool = ...) -> TS[str]``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object, delta: bool = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

to_json: _to_json_Operator

class _to_table_Operator(_Protocol):
    """Table serialization operators (design record: *Record/replay, tables and const_fn*, P4 + step 6). ``to_table`` is the Python-parity TUPLE-ROW protocol: each tick converts to bitemporal row values ``[date, as_of, {removed, *keys}(per TSD level), *value columns]`` — ``TS<tuple[...]>`` for single-row types, ``TS<tuple[tuple[...], ...]>`` for partitioned (TSD) or multi-row (``Frame``-valued) types; unset cells are tuple field validity (Python ``None``). The output schema is computed from the resolved input at wiring. ``mode`` is a ``ToTableMode`` enum time-series (Tick/Sample/Snap) defaulting to Tick.

    ``from_table`` reverses it, applying each row as the tick's delta at the resolved output (supplied at the wiring site: ``wire<from_table, TS<MySchema>>(w, ts)``); removed flags map to TSD key removals. The record/replay backends bypass tuple materialisation and drive the selected ``TableTypeOps`` into Arrow builders directly.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``
       Time-series value or structure to flatten.

    ``mode`` : time-series; ``TS[ToTableMode]``
       Tick emits deltas, Sample emits complete modified entries, and Snap emits a complete snapshot when the source ticks. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    One row per scalar tick, or a tuple of rows for partitioned and frame-valued inputs.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       rows = hg.to_table(positions, hg.ToTableMode.Sample)

    Accepted native overloads:

    - ``to_table(ts: TIME_SERIES_TYPE, mode: TS[ToTableMode] = ...) -> OUT``

    Time-series parameters accept wiring ports and compatible plain
    values that can be lifted to constant sources. Generic names use
    the public Python vocabulary: ``SCALAR``, ``TIME_SERIES_TYPE``,
    ``SIZE``, ``OUT``, ``K`` and ``V``."""

    def __call__(self, ts: _WiringPort | object, mode: _WiringPort | _ToTableMode = ...) -> _WiringPort: ...
    def __getitem__(self, item: _Any, /) -> _Self: ...

to_table: _to_table_Operator

class _to_window_Operator(_Protocol):
    """Convert a stream into a typed trailing ``TSW`` window. The output becomes valid after ``min_window_period`` values. When ``reset`` and the source tick together, retained values are cleared before the new tick is added. Wiring rejects a non-positive period, a negative minimum, or a minimum greater than the period. @note Cost: O(1) append/evict per tick; O(W) retained by the TSW itself. Aggregates over the window (``min_`` / ``max_`` / ``sum_`` / ``mean`` / ``std``) recompute in O(W) per window tick — recorded beside their kernels.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[SCALAR]``
       Values admitted to the window.

    ``period`` : scalar; ``int``, ``timedelta``
       Maximum number of retained ticks, fixed at wiring time.

    ``min_window_period`` : scalar; ``int``, ``timedelta``
       Minimum retained count required for validity. Optional in overloads that show ``= ...``.

    ``reset`` : time-series; ``SIGNAL``
       Optional signal that clears retained ticks.

    Returns
    ~~~~~~~

    A typed time-series window over the most recent values.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       recent = hg.to_window(price, period=20, min_window_period=5, reset=session_start)

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[timedelta]``
       The primary time-series input.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[float]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.total_seconds(ts)

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
    """Report the Python runtime type of each input value.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``
       Values to inspect.

    Returns
    ~~~~~~~

    A time series of Python type objects.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       value_type = hg.type_(value)

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
    """Expand tuple keys into the two levels of a nested keyed dictionary. @note Cost: O(n) regroup per tick.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``, ``TSD[K, V]``
       ``TSD[tuple[K, K1], V]`` input.

    ``remove_empty`` : scalar; ``bool``
       When true, remove an outer entry after its last inner key is removed. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    ``TSD[K, TSD[K1, V]]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       nested = hg.uncollapse_keys(flat, remove_empty=True)

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
    """Concatenate every currently valid frame in a keyed collection.

    Ungroup while materializing a scalar or tuple key into columns.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``
       Keyed collection of same-schema frames.

    ``key_col`` : scalar; ``SCALAR``
       The key col value used by the selected overload.

    Returns
    ~~~~~~~

    One frame containing rows from all valid keyed values.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       rows = hg.ungroup(rows_by_symbol)

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
    """Combine all members present in any input set.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``*ts`` : time-series; ``TIME_SERIES_TYPE``
       Set-valued or variadic set inputs.

    Returns
    ~~~~~~~

    A set containing each distinct member from any input.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       all_symbols = hg.union(primary_symbols, secondary_symbols)

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
    """Flatten partitioned dictionaries by removing the outer partition key. Inner keys are expected to be unique across partitions; conflicting updates follow the selected overload's deterministic ordering.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TSD[K_1, TSD[K, V]]``
       Nested partitioned dictionary.

    Returns
    ~~~~~~~

    The merged inner keyed dictionary.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       prices = hg.unpartition(prices_by_sector)

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
    """Evaluate a predicate until it first succeeds, emitting false for failed ticks and true for the first successful tick before passivating the input.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[bool]``, ``TIME_SERIES_TYPE``
       Input tested by the selected predicate overload.

    ``predicate`` : scalar; ``callable``, ``fn``
       Predicate callable or expression used by applicable overloads.

    Returns
    ~~~~~~~

    A boolean stream ending with the first true result.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       reached_target = hg.until_true(lambda value: value >= target, price)

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
    """Track whether an input currently has a usable value. This emits on validity transitions rather than mirroring every source tick.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TIME_SERIES_TYPE``, ``REF[TIME_SERIES_TYPE]``
       Input whose validity is observed.

    Returns
    ~~~~~~~

    True while valid and false while invalid.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       has_price = hg.valid(price)

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
    """Project dictionary values in the representation selected by the overload.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TSD[K, V]``, ``TIME_SERIES_TYPE``
       Mapping or keyed time-series dictionary.

    Returns
    ~~~~~~~

    A tuple, list, or keyed value collection corresponding to ``ts``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       current_prices = hg.values_(prices)

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

class _weekday_Operator(_Protocol):
    """``weekday`` — the day of the week using Monday as zero.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[date]``, ``TS[datetime]``
       The primary time-series input.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[int]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.weekday(ts)

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
    """Retain a trailing tick-count or time-duration buffer and expose both values and their evaluation timestamps. Tick windows use a circular buffer; duration windows evict entries older than the requested horizon. @note Cost: O(W) per tick (the value/time bundle is rebuilt) and O(W) retained in private state. Deprecated-parity shape — prefer ``to_window``, whose TSW substrate appends/evicts in O(1).

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[SCALAR]``
       Values to retain.

    ``period`` : scalar; ``int``, ``timedelta``
       Maximum tick count or elapsed horizon.

    ``min_window_period`` : scalar; ``int``, ``timedelta``
       Optional minimum population before output becomes valid. Optional in overloads that show ``= ...``.

    Returns
    ~~~~~~~

    A bundle containing the trailing values and corresponding timestamps.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       recent = hg.window(price, 20)

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
    """Replace or add columns from a structural input and project to the requested row schema.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[Frame[SCALAR]]``, ``TS[Frame[SCALAR, SCALAR_1]]``
       Original frame.

    ``columns`` : time-series; ``TIME_SERIES_TYPE``
       Bundle or mapping of replacement column values.

    Returns
    ~~~~~~~

    A frame with the explicitly selected output row schema.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       enriched = hg.with_columns[TS[Frame[EnrichedRow]]](rows, columns)

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

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``ts`` : time-series; ``TS[date]``, ``TS[datetime]``
       The primary time-series input.

    Returns
    ~~~~~~~

    A wired output with one of the overload-selected shapes: ``TS[int]``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       result = hg.year(ts)

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
    """Produce the identity value for an operator and explicitly selected output type. For example addition uses zero while multiplication uses one; collection overloads choose the corresponding empty or identity value.

    Parameters
    ~~~~~~~~~~

    Time-series inputs are live graph edges. Wiring-time scalar choices
    are fixed when the graph is built.

    ``op`` : scalar; ``fn``
       Operator whose identity is required. This choice is fixed at wiring time.

    Returns
    ~~~~~~~

    A constant source of the selected type's identity for ``op``.

    Python example
    ~~~~~~~~~~~~~~

    .. code-block:: python

       additive_identity = hg.zero[TS[int]](hg.add_)

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
    "assert_",
    "at_zone",
    "batch",
    "bit_and",
    "bit_or",
    "bit_xor",
    "call",
    "cmp_",
    "collapse_keys",
    "compare",
    "concat",
    "const",
    "contains_",
    "convert_zone",
    "datepart",
    "day",
    "day_of_month",
    "days",
    "debug_print",
    "dedup",
    "default",
    "dereference",
    "difference",
    "div_",
    "divmod_",
    "drop",
    "eq_",
    "evaluation_time_in_range",
    "explode",
    "filter_",
    "filter_cs",
    "filter_frame",
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
    "mean",
    "merge",
    "microsecond",
    "microseconds",
    "min_",
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
    "null_sink",
    "or_",
    "partition",
    "pos_",
    "pow_",
    "print_",
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
    "replay_data_frame",
    "request_id",
    "resolve_civil",
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
    "weekday",
    "window",
    "with_columns",
    "year",
    "zero",
)
