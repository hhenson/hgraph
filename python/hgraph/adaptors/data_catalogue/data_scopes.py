import copy
import random
from abc import ABC, abstractmethod
from collections.abc import Iterable
from datetime import date, datetime, timedelta, timezone

__all__ = (
    "Scope",
    "OptionalScope",
    "StringScope",
    "BooleanScope",
    "IntegerScope",
    "DateScope",
    "DateTimeScope",
    "AsofDateTimeScope",
    "MinDateTimeScope",
    "MaxDateTimeScope",
    "RankingScope",
    "EmailScope",
    "PollingScope",
    "FixedDelayRetryOptions",
    "ExponentialBackoffRetryOptions",
    "RetryOptions",
    "RetryScope",
    "StringSequenceScope",
)


class Scope(ABC):
    @abstractmethod
    def in_scope(self, value: object) -> bool:
        raise NotImplementedError

    def adjust(self, value: object) -> object:
        return value

    def default(self) -> object:
        return None


class OptionalScope(Scope):
    def __init__(self, scope: Scope):
        self._scope = scope

    def in_scope(self, value):
        return value is None or self._scope.in_scope(value)

    def adjust(self, value):
        return self._scope.adjust(value) if value is not None else None


class StringScope(Scope):
    def in_scope(self, value):
        return type(value) is str


class BooleanScope(Scope):
    def __init__(self, default=False):
        self._default = default

    def in_scope(self, value):
        return type(value) is bool

    def default(self):
        return self._default


class IntegerScope(Scope):
    def __init__(self, default=0):
        self._default = default

    def in_scope(self, value):
        return type(value) is int

    def default(self):
        return self._default


class DateScope(Scope):
    def in_scope(self, value):
        if isinstance(value, (date, datetime)):
            return True
        try:
            datetime.strptime(value, "%Y-%m-%d")
            return True
        except (TypeError, ValueError):
            return False

    def adjust(self, value):
        if type(value) is datetime:
            return value.date()
        if type(value) is str:
            try:
                return datetime.strptime(value, "%Y-%m-%d")
            except ValueError:
                pass
        return value


class DateTimeScope(Scope):
    _FORMATS = ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S")

    def in_scope(self, value):
        if isinstance(value, (date, datetime)):
            return True
        return any(self._parse(value, format_) is not None for format_ in self._FORMATS)

    @staticmethod
    def _parse(value, format_):
        try:
            return datetime.strptime(value, format_)
        except (TypeError, ValueError):
            return None

    def adjust(self, value):
        if type(value) is date:
            return datetime(value.year, value.month, value.day)
        if type(value) is str:
            for format_ in reversed(self._FORMATS):
                if (parsed := self._parse(value, format_)) is not None:
                    return parsed
        return value


class AsofDateTimeScope(DateTimeScope):
    def default(self):
        return datetime.now(timezone.utc).replace(tzinfo=None)


class MinDateTimeScope(DateTimeScope):
    def adjust(self, value):
        return super().adjust(value if isinstance(value, (date, datetime)) else min(value))


class MaxDateTimeScope(DateTimeScope):
    def adjust(self, value):
        return super().adjust(value if isinstance(value, (date, datetime)) else max(value))


class RankingScope(Scope):
    def in_scope(self, value):
        return isinstance(value, Iterable) and all(len(item) == 2 for item in value)

    def adjust(self, value):
        return ", ".join(
            f" ({index}, '{source}', '{prefix}')"
            for index, (source, prefix) in enumerate(value)
        )


class PollingScope(Scope):
    def __init__(self, default=timedelta(minutes=1)):
        self._default = default

    def default(self):
        return self._default

    def in_scope(self, value):
        return isinstance(value, timedelta) or value is self._default


class RetryOptions(ABC):
    @abstractmethod
    def create(self):
        raise NotImplementedError

    @abstractmethod
    def next(self, when: datetime):
        raise NotImplementedError


class FixedDelayRetryOptions(RetryOptions):
    def __init__(self, delay: timedelta, max_retries: int = 5):
        self.delay = delay
        self.max_retries = max_retries

    def create(self):
        return copy.copy(self)

    def next(self, when):
        retry = getattr(self, "retries", 0)
        if retry >= self.max_retries:
            return None
        self.retries = retry + 1
        return when + self.delay


class ExponentialBackoffRetryOptions(RetryOptions):
    def __init__(
        self,
        delay: timedelta,
        factor: float = 2.0,
        initial_delay: bool = True,
        max_delay: timedelta = timedelta(minutes=30),
        randomise: bool = True,
    ):
        self.delay = delay
        self.factor = factor
        self.max_delay = max_delay
        self.initial_delay = initial_delay
        self.randomise = randomise

    def create(self):
        return copy.copy(self)

    def next(self, when):
        previous = getattr(self, "last_delay", None)
        if previous is None:
            if not self.initial_delay:
                self.initial_delay = True
                return when
            self.last_delay = self.delay
        else:
            factor = self.factor
            if self.randomise:
                factor = 1 + (self.factor - 1) * (random.random() + 0.5)
            self.last_delay = min(
                timedelta(seconds=previous.total_seconds() * factor),
                self.max_delay,
            )
        return when + self.last_delay


class RetryScope(Scope):
    def __init__(self, default):
        self._default = default

    def default(self):
        return self._default

    def in_scope(self, value):
        return isinstance(value, RetryOptions)


class EmailScope(StringScope):
    def __init__(self, domain="bamfunds.com"):
        self.domain = domain

    def adjust(self, value):
        return value if "@" in value else f"{value}@{self.domain}"


class StringSequenceScope(Scope):
    def in_scope(self, value):
        return isinstance(value, Iterable) and all(isinstance(item, str) for item in value)

    def adjust(self, value):
        return ",".join(f"'{item}'" for item in value)
