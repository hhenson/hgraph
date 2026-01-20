from abc import abstractmethod
import copy
from datetime import date, datetime, timedelta, UTC
import random
from typing import Iterable


__all__ = [
    "Scope",
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
    "StringSequenceScope"
]

# Scope serves two purposes:
# 1) Potentially define shards of data,
# 2) Define schema of parameters to the query/filters so,
#    in_scope() is to find the right shard,
#    default and adjust are for processing "filter" values.
class Scope:
    @abstractmethod
    def in_scope(self, value: object) -> bool: ...

    def adjust(self, value: object) -> object:
        return value

    def default(self) -> object:
        return None


class StringScope(Scope):
    def in_scope(self, value: object) -> bool:
        return value.__class__ is str

    def adjust(self, value: object) -> object:
        return value


class BooleanScope(Scope):
    def __init__(self, default=False):
        self._default = default

    def in_scope(self, value: object) -> bool:
        return value.__class__ is bool

    def adjust(self, value: object) -> object:
        return value

    def default(self) -> object:
        return self._default


class IntegerScope(Scope):
    def __init__(self, default=0):
        self._default = default

    def in_scope(self, value: object) -> bool:
        return value.__class__ is int

    def adjust(self, value: object) -> object:
        return value

    def default(self) -> object:
        return self._default


class DateScope(Scope):
    def in_scope(self, value: object) -> bool:
        if isinstance(value, (date, datetime)):
            return True
        try: 
            datetime.strptime(value, "%Y-%m-%d")
            return True
        except:
            pass

        return False

    def adjust(self, value: object) -> object:
        if type(value) is datetime:
            return value.date()
        
        if type(value) is str:
            try:
                dt = datetime.strptime(value, "%Y-%m-%d")
                return dt
            except:
                pass

        return value


class DateTimeScope(Scope):
    def in_scope(self, value: object) -> bool:
        if isinstance(value, (date, datetime)):
            return True
        try: 
            datetime.strptime(value, "%Y-%m-%d")
            return True
        except:
            pass

        try:
            datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            return True
        except:
            pass

        return False

    def adjust(self, value: object) -> object:
        if type(value) is date:
            return datetime(value.year, value.month, value.day)
        
        if type(value) is str:
            try:
                dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                return dt
            except:
                pass

            try:
                dt = datetime.strptime(value, "%Y-%m-%d")
                return dt
            except:
                pass

        return value


class AsofDateTimeScope(DateTimeScope):
    def default(self) -> object:
        return datetime.now(UTC).replace(tzinfo=None)


class MinDateTimeScope(DateTimeScope):

    def adjust(self, value: object) -> object:
        if isinstance(value, (date, datetime)):
            return super(MinDateTimeScope, self).adjust(value)
        else:
            return super(MinDateTimeScope, self).adjust(min(value))


class MaxDateTimeScope(DateTimeScope):

    def adjust(self, value: object) -> object:
        if isinstance(value, (date, datetime)):
            return super(MaxDateTimeScope, self).adjust(value)
        else:
            return super(MaxDateTimeScope, self).adjust(max(value))


class RankingScope(Scope):
    def in_scope(self, value: object) -> bool:
        return isinstance(value, Iterable) and all(len(v) == 2 for v in value)

    def adjust(self, value: object) -> object:
        return ", ".join(
            f" ({i}, '{source}', '{prefix}')"
            for i, (source, prefix) in enumerate(value)
        )


class PollingScope(Scope):
    def __init__(self, default=timedelta(minutes=1)):
        self._default = default

    def default(self) -> object:
        return self._default

    def in_scope(self, value: object) -> bool:
        return isinstance(value, timedelta) or value is self._default

    def adjust(self, value: object) -> object:
        return value


class RetryOptions:
    @abstractmethod
    def create(self) -> object:
        """create retry state object"""
        
    @abstractmethod
    def next(self, time: datetime) -> datetime:
        """get next retry time based on current time, or None if no more retries"""


class FixedDelayRetryOptions(RetryOptions):
    def __init__(self, delay: timedelta, max_retries: int = 5):
        self.delay = delay
        self.max_retries = max_retries

    def create(self) -> object:
        return copy.copy(self)
    
    def next(self, time: datetime) -> datetime:
        retry = getattr(self, "retries", 0)
        if retry >= self.max_retries:
            return None
        self.retries = retry + 1
        return time + self.delay


class ExponentialBackoffRetryOptions(RetryOptions):
    def __init__(self, 
                 delay: timedelta, 
                 factor: float = 2.0, 
                 initial_delay: bool = True,
                 max_delay: timedelta = timedelta(minutes=30), 
                 randomise: bool = True):
        self.delay = delay
        self.factor = factor
        self.max_delay = max_delay
        self.initial_delay = initial_delay
        self.randomise = randomise

    def create(self) -> object:
        return copy.copy(self)
    
    def next(self, time: datetime) -> datetime:
        prev = getattr(self, "last_delay", None)
        if prev is None:
            if not self.initial_delay:
                self.initial_delay = True
                return time
            else:
                self.last_delay = self.delay
        else:
            factor = self.factor if not self.randomise else (1 + ((self.factor - 1) * (random.random() + 0.5)))
            self.last_delay = min(timedelta(seconds=prev.total_seconds() * factor), self.max_delay)
            
        return time + self.last_delay


class RetryScope(Scope):
    def __init__(self, default):
        self._default = default

    def default(self) -> object:
        return self._default

    def in_scope(self, value: object) -> bool:
        return isinstance(value, RetryOptions)

    def adjust(self, value: object) -> object:
        return value


class EmailScope(StringScope):
    def __init__(self, domain="bamfunds.com"):
        self.domain = domain

    def adjust(self, value: object) -> object:
        return value if "@" in value else f"{value}@{self.domain}"


class StringSequenceScope(Scope):
    def in_scope(self, value: object) -> bool:
        return isinstance(value, Iterable) and all(isinstance(v, str) for v in value)

    def adjust(self, value: object) -> object:
        return ",".join(f"'{v}'" for v in value)
