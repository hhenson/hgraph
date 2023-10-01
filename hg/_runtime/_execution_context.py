from abc import abstractmethod
from datetime import datetime, timedelta


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
    def wait_until_proposed_engine_time(self, proposed_engine_time: datetime) -> datetime:
        """
        Wait until the proposed engine time is reached. In the case of a runtime context, this may end early if a
        push node is scheduled whilst waiting for the proposed engine time.
        """
