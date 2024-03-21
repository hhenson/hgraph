import sys
from abc import ABC
from datetime import datetime, timedelta
from threading import Condition

from hgraph._runtime._constants import MAX_DT, MIN_TD
from hgraph._runtime._evaluation_clock import EngineEvaluationClock


class BaseEvaluationClock(EngineEvaluationClock, ABC):
    """
    Base class for the master engine evaluation clock.
    """

    def __init__(self, start_time: datetime):
        self._evaluation_time = start_time
        self._next_scheduled_evaluation_time: datetime = MAX_DT

    @property
    def evaluation_time(self) -> datetime:
        return self._evaluation_time

    @evaluation_time.setter
    def evaluation_time(self, value: datetime):
        self._evaluation_time = value
        self._next_scheduled_evaluation_time: datetime = MAX_DT

    @property
    def next_cycle_evaluation_time(self) -> datetime:
        return self._evaluation_time + MIN_TD

    @property
    def next_scheduled_evaluation_time(self) -> datetime:
        return self._next_scheduled_evaluation_time

    def update_next_scheduled_evaluation_time(self, scheudled_time: datetime):
        if scheudled_time == self._evaluation_time:
            return  # This will be evaluated in the current cycle, nothing to do.
        self._next_scheduled_evaluation_time = max(self.next_cycle_evaluation_time,
                                                   min(self._next_scheduled_evaluation_time, scheudled_time))


class SimulationEvaluationClock(BaseEvaluationClock):
    """
    The simulation base clock.
    """

    def __init__(self, current_time: datetime):
        super().__init__(current_time)
        self._system_clock_at_start_of_evaluation = datetime.utcnow()

    @property
    def evaluation_time(self) -> datetime:
        # This is required in order to implement the setter behaviour.
        return self._evaluation_time

    @evaluation_time.setter
    def evaluation_time(self, value: datetime):
        self._evaluation_time = value
        self._system_clock_at_start_of_evaluation = datetime.utcnow()
        self._next_scheduled_evaluation_time = MAX_DT

    @property
    def now(self) -> datetime:
        """
        This provides a loose representation of now in simulation mode by
        adding the cycle time to the evaluation time. In simulation mode
        now is not necessarily a monotonic increasing value.
        """
        return self._evaluation_time + self.cycle_time

    @property
    def cycle_time(self) -> timedelta:
        """The time since the current evaluation time was set and now."""
        return datetime.utcnow() - self._system_clock_at_start_of_evaluation

    def advance_to_next_scheduled_time(self):
        self.evaluation_time = self.next_scheduled_evaluation_time

    def mark_push_node_requires_scheduling(self):
        raise NotImplementedError()

    @property
    def push_node_requires_scheduling(self) -> bool:
        return False

    def reset_push_node_requires_scheduling(self):
        raise NotImplementedError()


class RealTimeEvaluationClock(BaseEvaluationClock):
    """
    The realtime base clock.
    """

    def __init__(self, start_time: datetime):
        super().__init__(start_time)
        self._push_node_requires_scheduling = False
        self._push_node_requires_scheduling_condition = Condition()
        self._ready_to_push: bool = False

    @property
    def now(self) -> datetime:
        """This represents the current system clock time."""
        return datetime.utcnow()

    @property
    def cycle_time(self) -> timedelta:
        """
        The cycle time here represents the time elapsed
        since the evaluation time and the current system clock.
        """
        return datetime.utcnow() - self._evaluation_time

    def mark_push_node_requires_scheduling(self):
        with self._push_node_requires_scheduling_condition:
            self._push_node_requires_scheduling = True
            self._push_node_requires_scheduling_condition.notify_all()

    def push_node_requires_scheduling(self) -> bool:
        if not self._ready_to_push:
            return False
        with self._push_node_requires_scheduling_condition:
            return self._push_node_requires_scheduling

    def advance_to_next_scheduled_time(self):
        next_scheduled_time = self.next_scheduled_evaluation_time
        self._ready_to_push = False  # We only let push values to be introduced when there are no PULL entries left
        #print(f"RealTimeEvaluationClock.advance_to_next_scheduled_time: {next_scheduled_time}", file=sys.stderr)
        with self._push_node_requires_scheduling_condition:
            while datetime.utcnow() < next_scheduled_time and not self._push_node_requires_scheduling:
                sleep_time = (next_scheduled_time - datetime.utcnow()).total_seconds()
                #print(f"RealTimeEvaluationClock.advance_to_next_scheduled_time: sleeping for {sleep_time}", file=sys.stderr)
                self._push_node_requires_scheduling_condition.wait(min(sleep_time, 10)) # wake up regularly so sleep_time is not 100 years
                self._ready_to_push = True
            # It could be that a push node has triggered
        #print(f"RealTimeEvaluationClock.advance_to_next_scheduled_time: setting evaluation time to {next_scheduled_time}", file=sys.stderr)
        self.evaluation_time = min(next_scheduled_time, max(self.next_cycle_evaluation_time, datetime.utcnow()))

    def reset_push_node_requires_scheduling(self):
        """
        Reset the push_has_pending_values property.
        """
        with self._push_node_requires_scheduling_condition:
            self._push_node_requires_scheduling = False
