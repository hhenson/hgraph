from abc import abstractmethod
from datetime import datetime, timedelta

__all__ = ("EvaluationClock", "EngineEvaluationClock")


class EvaluationClock:
    """
    The evaluation clock provides a view on time in the currently evaluating graph.
    Time in the graph is dependent on the mode of execution. In simulation mode, the clock presented is simulated to
    allow for processing historical data in rapid time. In realtime mode, the clock represent the system clock (time
    is always presented in UTC). By using the clock abstraction to query time-related information it is possible
    to ensure that logic can be written independently of how the system chooses to move through time.
    """

    @property
    @abstractmethod
    def evaluation_time(self) -> datetime:
        """
        The time of the source event initiating this evaluation cycle. This time remains the same for each node
        processed until the graph completes one complete step through the nodes in the graph.
        """

    @property
    @abstractmethod
    def now(self) -> datetime:
        """
        This represents the current time, in a real time engine, this will represent the system clock. In simulation
        mode this will represent the evaluation_time + lag.
        """

    @property
    @abstractmethod
    def cycle_time(self) -> timedelta:
        """
        The amount of time spend in computation since the beginning of the evaluation of the graph till the point
        where this property is called.
        """

    @property
    @abstractmethod
    def next_cycle_evaluation_time(self) -> datetime:
        """
        The next smallest evaluation time that can be scheduled. This is a convenience method and is equivalent
        to evaluation_time + MIN_TD
        """


class EngineEvaluationClock(EvaluationClock):
    """
    Extends the evaluation clock to provide additional functionality for the engine.
    The engine needs to be able to interact with the clock. The user api is limited
    to the EvaluationClock interface.
    """

    @property
    @abstractmethod
    def evaluation_time(self) -> datetime:
        """
        The time of the source event initiating this evaluation cycle. This time remains the same for each node
        processed until the graph completes one complete step through the nodes in the graph.
        """

    @evaluation_time.setter
    @abstractmethod
    def evaluation_time(self, value: datetime):
        """
        Set the evaluation time. This should only be done by the graph engine.
        """

    @property
    @abstractmethod
    def next_scheduled_evaluation_time(self) -> datetime:
        """
        The next smallest evaluation time that a node is currently scheduled
        to be evaluated.
        """

    @abstractmethod
    def update_next_scheduled_evaluation_time(self, next_time: datetime):
        """
        This will ensure the next scheduled time is updated to reflect the
        earliest time that a node is scheduled to evaluate.
        """

    @abstractmethod
    def advance_to_next_scheduled_time(self):
        """
        Advance the clock to the next scheduled time. For a simulation clock,
        this will set the evaluation time to the next scheduled time and return.
        For a real time clock, this will block until the next scheduled time
        or the engine is notified by a push node that will cause the engine
        to set the evaluation time ot the lesser of now or the next scheduled
        time.
        """

    @abstractmethod
    def mark_push_node_requires_scheduling(self):
        """
        Push nodes are not scheduled using time as is the case for pull source nodes.
        For push nodes, when an event arrives, the engine will schedule
        the node to be evaluated at the next soonest time, effectively interleaving
        push events with what ever scheduled events are being evaluated at the
        time.
        This method is only ever called in a real-time graph as simulation graphs
        are not allowed to have push nodes.
        """

    @property
    @abstractmethod
    def push_node_requires_scheduling(self) -> bool:
        """
        True if there are any push nodes have requested to be scheduled, False
        otherwise.
        """

    @abstractmethod
    def reset_push_node_requires_scheduling(self):
        """
        Reset the push_has_pending_values property.
        """


class EngineEvaluationClockDelegate(EngineEvaluationClock):
    """Support adding new logic to the engine evaluation clock."""

    def __init__(self, engine_evaluation_clock: EngineEvaluationClock):
        self._engine_evaluation_clock = engine_evaluation_clock

    @property
    def evaluation_time(self) -> datetime:
        return self._engine_evaluation_clock.evaluation_time

    @evaluation_time.setter
    def evaluation_time(self, value: datetime):
        self._engine_evaluation_clock.evaluation_time = value

    @property
    def next_scheduled_evaluation_time(self) -> datetime:
        return self._engine_evaluation_clock.next_scheduled_evaluation_time

    def update_next_scheduled_evaluation_time(self, next_time: datetime):
        self._engine_evaluation_clock.update_next_scheduled_evaluation_time(next_time)

    def advance_to_next_scheduled_time(self):
        self._engine_evaluation_clock.advance_to_next_scheduled_time()

    def mark_push_node_requires_scheduling(self):
        self._engine_evaluation_clock.mark_push_node_requires_scheduling()

    @property
    def push_node_requires_scheduling(self) -> bool:
        return self._engine_evaluation_clock.push_node_requires_scheduling

    def reset_push_node_requires_scheduling(self):
        self._engine_evaluation_clock.reset_push_node_requires_scheduling()

    @property
    def now(self) -> datetime:
        return self._engine_evaluation_clock.now

    @property
    def cycle_time(self) -> timedelta:
        return self._engine_evaluation_clock.cycle_time

    @property
    def next_cycle_evaluation_time(self) -> datetime:
        return self._engine_evaluation_clock.next_cycle_evaluation_time
