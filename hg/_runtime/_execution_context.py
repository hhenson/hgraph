from abc import abstractmethod
from datetime import datetime, timedelta

__all__ = ("ExecutionContext",)


class ExecutionContext:

    @property
    @abstractmethod
    def current_engine_time(self) -> datetime:
        """
        The current engine time for this evaluation cycle
        """

    @current_engine_time.setter
    @abstractmethod
    def current_engine_time(self, value: datetime):
        """
        Set the current engine time for this evaluation cycle
        """

    @property
    @abstractmethod
    def wall_clock_time(self) -> datetime:
        """
        The current wall clock time, in a realtime engine, this is the actual wall clock time, in a back test engine
        this is the current engine time + engine lag.
        """

    @property
    @abstractmethod
    def engine_lag(self) -> timedelta:
        """
        The lag between the current engine time and the current wall clock time.
        """

    @property
    @abstractmethod
    def proposed_next_engine_time(self) -> datetime:
        """
        The proposed next engine time, this is the time that the engine will advance to after the current evaluation.
        """

    @abstractmethod
    def mark_push_has_pending_values(self):
        """
        Mark that there are pending changes for push nodes.
        """

    @property
    @abstractmethod
    def push_has_pending_values(self) -> bool:
        """
        Returns True if there are any pending changes for push nodes.
        """

    @abstractmethod
    def reset_push_has_pending_values(self):
        """
        Reset the push_has_pending_values property.
        """

    @abstractmethod
    def wait_until_proposed_engine_time(self, proposed_engine_time: datetime):
        """
        Wait until the proposed engine time is reached. In the case of a runtime context, this may end early if a
        push node is scheduled whilst waiting for the proposed engine time.
        """

    @abstractmethod
    def update_next_proposed_time(self, next_time: datetime):
        """
        This provides hints to the execution context as to the next potential update time. This method will
        ensure the smallest next meaningful engine time is returned at the end of the evaluation cycle when
        proposed_next_engine_time is called.
        """

    @property
    @abstractmethod
    def next_cycle_engine_time(self) -> datetime:
        """
        The next meaningful engine time after the current evaluation cycle.
        """

    @abstractmethod
    def request_engine_stop(self):
        """
        Request the engine to evaluation.
        """

    @property
    @abstractmethod
    def is_stop_requested(self) -> bool:
        """
        Returns True if the engine has been requested to stop.
        """