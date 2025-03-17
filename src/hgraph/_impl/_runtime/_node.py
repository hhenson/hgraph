import functools
import threading
from abc import ABC, abstractmethod
from collections import deque
from contextlib import ExitStack, nullcontext
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, Mapping, TYPE_CHECKING, Callable, Any, Iterator

from sortedcontainers import SortedList

from hgraph._operators import get_fq_recordable_id
from hgraph._runtime._constants import MIN_TD
from hgraph._impl._types._tss import PythonSetDelta, Removed
from hgraph._runtime._constants import MIN_DT, MAX_DT, MIN_ST
from hgraph._runtime._evaluation_clock import EngineEvaluationClock
from hgraph._runtime._graph import Graph
from hgraph._runtime._lifecycle import start_guard, stop_guard
from hgraph._runtime._node import NodeSignature, Node, NodeScheduler, NodeDelegate
from hgraph._types import TimeSeriesDictOutput
from hgraph._types._tsb_meta_data import HgTSBTypeMetaData
from hgraph._types._tsd_meta_data import HgTSDTypeMetaData
from hgraph._types._tsd_type import REMOVE, REMOVE_IF_EXISTS
from hgraph._types._tsl_meta_data import HgTSLTypeMetaData
from hgraph._types._tss_meta_data import HgTSSTypeMetaData

if TYPE_CHECKING:
    from hgraph._types._ts_type import TimeSeriesInput, TimeSeriesOutput
    from hgraph._types._tsb_type import TimeSeriesBundleInput
    from hgraph._types._scalar_types import SCALAR
    from hgraph._types._recordable_state import RECORDABLE_STATE


__all__ = (
    "NodeImpl",
    "NodeSchedulerImpl",
    "GeneratorNodeImpl",
    "PythonPushQueueNodeImpl",
    "PythonLastValuePullNodeImpl",
    "BaseNodeImpl",
)


class BaseNodeImpl(Node, ABC):

    def __init__(
        self, node_ndx: int, owning_graph_id: tuple[int, ...], signature: NodeSignature, scalars: Mapping[str, Any]
    ):
        super().__init__()
        self._node_ndx: int = node_ndx
        self._owning_graph_id: tuple[int, ...] = owning_graph_id
        self._signature: NodeSignature = signature
        self._scalars: Mapping[str, Any] = scalars
        self._graph: Graph | None = None
        self._input: Optional["TimeSeriesBundleInput"] = None
        self._output: Optional["TimeSeriesOutput"] = None
        self._error_output: Optional["TimeSeriesOutput"] = None
        self._scheduler: Optional["NodeSchedulerImpl"] = None
        self._kwargs: dict[str, Any] | None = None
        self._start_inputs: list["TimeSeriesInput"] = []
        self._recordable_state: Optional["RECORDABLE_STATE"] = None

    @property
    def node_ndx(self) -> int:
        return self._node_ndx

    @property
    def owning_graph_id(self) -> tuple[int, ...]:
        return self._owning_graph_id

    @property
    def signature(self) -> NodeSignature:
        return self._signature

    @property
    def scalars(self) -> Mapping[str, Any]:
        return self._scalars

    @functools.cached_property
    def node_id(self) -> tuple[int, ...]:
        """Computed once and then cached"""
        return self.owning_graph_id + tuple([self.node_ndx])

    @property
    def graph(self) -> "Graph":
        return self._graph

    @graph.setter
    def graph(self, value: "Graph"):
        self._graph = value

    @property
    def input(self) -> Optional["TimeSeriesBundleInput"]:
        return self._input

    @input.setter
    def input(self, value: "TimeSeriesBundleInput"):
        self._input = value
        if self._kwargs is not None:
            self._initialise_kwargs()

    @property
    def output(self) -> Optional["TimeSeriesOutput"]:
        return self._output

    @output.setter
    def output(self, value: "TimeSeriesOutput"):
        self._output = value
        if self._kwargs is not None:
            self._initialise_kwargs()

    @property
    def recordable_state(self) -> Optional["RECORDABLE_STATE"]:
        return self._recordable_state

    @recordable_state.setter
    def recordable_state(self, value: Optional["RECORDABLE_STATE"]):
        self._recordable_state = value
        if self._kwargs is not None:
            self._initialise_kwargs()

    @property
    def error_output(self) -> Optional["TimeSeriesOutput"]:
        return self._error_output

    @error_output.setter
    def error_output(self, value: "TimeSeriesOutput"):
        self._error_output = value
        # We may need to update kwargs if we choose to let error outputs be made available to the user.

    @property
    def inputs(self) -> Optional[Mapping[str, "TimeSeriesInput"]]:
        # noinspection PyTypeChecker
        return {k: self.input[k] for k in self.signature.time_series_inputs}

    @property
    def start_inputs(self) -> list["TimeSeriesInput"]:
        return self._start_inputs

    @property
    def scheduler(self) -> "NodeScheduler":
        if self._scheduler is None:
            self._scheduler = NodeSchedulerImpl(self)
        return self._scheduler

    def initialise(self):
        """Nothing to do"""

    def _initialise_kwargs(self):
        from hgraph._types._scalar_type_meta_data import Injector

        extras = {k: s(self) for k, s in self.scalars.items() if isinstance(s, Injector)}
        self._kwargs = {
            k: v for k, v in {**(self.input or {}), **self.scalars, **extras}.items() if k in self.signature.args
        }

    def _initialise_inputs(self):
        if self.input:
            for i in self._start_inputs:
                i.start()
            for k, ts in self.input.items():
                ts: TimeSeriesInput
                if self.signature.active_inputs is None or k in self.signature.active_inputs:
                    ts.make_active()

    def _initialise_state(self):
        if self.recordable_state is not None:
            from hgraph._operators._record_replay import RecordReplayContext, RecordReplayEnum, replay_const
            mode = RecordReplayContext.instance().mode
            if RecordReplayEnum.RECOVER in mode:
                # TODO: make recordable_id unique by using parent node context information.
                from hgraph._operators._to_table import get_as_of
                recordable_id = get_fq_recordable_id(self.graph.traits, self.signature.record_replay_id)
                self.recordable_state.value = replay_const(
                    "__state__",
                    self.signature.recordable_state.tsb_type.py_type,
                    recordable_id=recordable_id,
                    tm = (clock := self.graph.evaluation_clock).evaluation_time - MIN_TD,  # We want the state just before now
                    as_of = get_as_of(clock),
                ).value

    def do_eval(self): ...

    def eval(self):
        scheduled = False if self._scheduler is None else self._scheduler.is_scheduled_now
        eval = True
        if self.input:
            # Perform validity check of inputs
            args = (
                self.signature.valid_inputs
                if self.signature.valid_inputs is not None
                else self.signature.time_series_inputs.keys()
            )
            if not all(self.input[k].valid for k in args):
                eval = False  # We should look into caching the result of this check.
                # This check could perhaps be set on a separate call?
            else:
                all_valid = self.signature.all_valid_inputs
                if not all(self.input[k].all_valid for k in ([] if all_valid is None else all_valid)):
                    eval = False  # This really could do with some optimisation as on large collections this will be expensive!
                elif self.signature.uses_scheduler:
                    # It is possible we have scheduled and then remove the schedule,
                    # so we need to check that something has caused this to be scheduled.
                    if not scheduled and not any(
                        self.input[k].modified for k in self.signature.time_series_inputs.keys()
                    ):
                        eval = False

        if eval:
            with ExitStack() if self.signature.context_inputs else nullcontext() as stack:
                if self.signature.context_inputs:
                    for context in self.signature.context_inputs:
                        if self.input[context].valid:
                            stack.enter_context(self.input[context].value)

                if self.error_output:
                    try:
                        self.do_eval()
                    except Exception as e:
                        from hgraph._types._error_type import NodeError

                        self.error_output.apply_result(NodeError.capture_error(e, self))
                else:
                    self.do_eval()

        if scheduled:
            self._scheduler.advance()
        elif self.scheduler.requires_scheduling:
            self.graph.schedule_node(self.node_ndx, self.scheduler.next_scheduled_time)

    @abstractmethod
    def do_start(self): ...

    @start_guard
    def start(self):
        self._initialise_kwargs()
        self._initialise_inputs()
        self._initialise_state()
        self.do_start()
        if self._scheduler is not None:
            if self._scheduler.pop_tag("start", None) is not None:
                self.notify()
                if not self.signature.uses_scheduler:
                    self._scheduler = None
            else:
                self._scheduler.advance()

    @abstractmethod
    def do_stop(self): ...

    @stop_guard
    def stop(self):
        self.do_stop()
        if self.input is not None:
            self.input.un_bind_output()
        if self._scheduler is not None:
            self._scheduler.reset()

    def dispose(self):
        self._kwargs = None  # For neatness purposes only, not required here.

    def notify(self, modified_time: datetime = None):
        """Notify the graph that this node needs to be evaluated."""
        if self.is_started or self.is_starting:
            self.graph.schedule_node(
                self.node_ndx,
                modified_time if modified_time is not None else self.graph.evaluation_clock.evaluation_time,
            )
        else:
            self.scheduler.schedule(when=MIN_ST, tag="start")

    def notify_next_cycle(self):
        if self.is_started or self.is_starting:
            self.graph.schedule_node(self.node_ndx, self.graph.evaluation_clock.next_cycle_evaluation_time)
        else:
            self.notify()


class NodeImpl(BaseNodeImpl):
    """
    Provide a basic implementation of the Node as a reference implementation.
    """

    def __init__(
        self,
        node_ndx: int,
        owning_graph_id: tuple[int, ...],
        signature: NodeSignature,
        scalars: Mapping[str, Any],
        eval_fn: Callable = None,
        start_fn: Callable = None,
        stop_fn: Callable = None,
    ):
        super().__init__(node_ndx, owning_graph_id, signature, scalars)
        self.eval_fn: Callable = eval_fn
        self.start_fn: Callable = start_fn
        self.stop_fn: Callable = stop_fn

    def do_eval(self):
        out = self.eval_fn(**self._kwargs)
        if out is not None:
            self.output.apply_result(out)

    def do_start(self):
        if self.start_fn is not None:
            from inspect import signature

            self.start_fn(**{k: self._kwargs[k] for k in (signature(self.start_fn).parameters.keys())})

    def do_stop(self):
        from inspect import signature

        if self.stop_fn is not None:
            self.stop_fn(**{k: self._kwargs[k] for k in (signature(self.stop_fn).parameters.keys())})

    def __repr__(self):
        if self.signature.label:
            return f"{self.signature.wiring_path_name}.{self.signature.label}"
        else:
            return f"{self.signature.wiring_path_name}.{self.signature.name}"


class NodeSchedulerImpl(NodeScheduler):

    def __init__(self, node: NodeImpl):
        self._node = node
        self._scheduled_events: SortedList[tuple[datetime, str]] = SortedList[tuple[datetime, str]]()
        self._tags: dict[str, datetime] = {}
        self._alarm_tags: dict[str, datetime] = {}
        self._last_scheduled_time: datetime = MIN_DT

    @property
    def next_scheduled_time(self) -> datetime:
        return self._scheduled_events[0][0] if self._scheduled_events else MIN_DT

    @property
    def requires_scheduling(self) -> bool:
        return bool(self._scheduled_events)

    @property
    def is_scheduled(self) -> bool:
        return bool(self._scheduled_events) or bool(self._alarm_tags)

    @property
    def is_scheduled_now(self) -> bool:
        return bool(
            self._scheduled_events and self._scheduled_events[0][0] == self._node.graph.evaluation_clock.evaluation_time
        )

    def has_tag(self, tag: str) -> bool:
        return tag in self._tags

    def pop_tag(self, tag: str, default=None) -> datetime:
        if tag in self._tags:
            dt = self._tags.pop(tag)
            self._scheduled_events.remove((dt, tag))
            return dt
        else:
            return default

    def schedule(self, when: datetime | timedelta, tag: str = None, on_wall_clock: bool = False):
        from hgraph import RealTimeEvaluationClock

        original_time = None
        if tag is not None and tag in self._tags:
            original_time = self.next_scheduled_time
            self._scheduled_events.remove((self._tags[tag], tag))

        if on_wall_clock and isinstance(clock := self._node.graph.evaluation_clock, RealTimeEvaluationClock):
            alarm_tag = f"{id(self)}:{tag}"
            clock.set_alarm(when, alarm_tag, lambda et: self._on_alarm(et, tag))
            self._alarm_tags[alarm_tag] = when
            return

        if type(when) is timedelta:
            when = self._node.graph.evaluation_clock.evaluation_time + when
        if when > (
            self._node.graph.evaluation_clock.evaluation_time if (is_started := self._node.is_started) else MIN_DT
        ):
            self._tags[tag] = when
            current_first = self._scheduled_events[0][0] if self._scheduled_events else MAX_DT
            self._scheduled_events.add((when, "" if tag is None else tag))
            if is_started and current_first > (next_ := self.next_scheduled_time):
                force_set = original_time is not None and original_time < when
                self._node.graph.schedule_node(self._node.node_ndx, next_, force_set)

    def _on_alarm(self, when: datetime, tag: str):
        self._tags[tag] = when
        self._alarm_tags.pop(f"{id(self)}:{tag}")
        self._scheduled_events.add((when, tag))
        self._node.graph.schedule_node(self._node.node_ndx, when)

    def un_schedule(self, tag: str = None):
        if tag is not None:
            if tag in self._tags:
                self._scheduled_events.remove((self._tags[tag], tag))
                del self._tags[tag]
        elif self._scheduled_events:
            self._scheduled_events.pop(0)

    def reset(self):
        self._scheduled_events.clear()
        self._tags.clear()
        for alarm in self._alarm_tags:
            self._node.graph.evaluation_clock.cancel_alarm(alarm)

    def advance(self):
        until = self._node.graph.evaluation_clock.evaluation_time
        while self._scheduled_events and (ev := self._scheduled_events[0])[0] <= until:
            if ev[1]:
                self._tags.pop(ev[1])
            self._scheduled_events.pop(0)
        if self._scheduled_events:
            self._node.graph.schedule_node(self._node.node_ndx, self._scheduled_events[0][0])


class GeneratorNodeImpl(NodeImpl):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generator: Iterator | None = None
        self.next_value: object = None

    @start_guard
    def start(self):
        self._initialise_kwargs()
        self.generator = self.eval_fn(**self._kwargs)
        self.graph.schedule_node(self.node_ndx, self.graph.evaluation_clock.evaluation_time)

    def eval(self):
        et = self.graph.evaluation_clock.evaluation_time

        while v := next(self.generator, None):
            time, out = v
            if time.__class__ is timedelta:
                time = et + time
                break
            if time >= et:
                break
        if v is None:
            time, out = None, None
        elif out is not None and time is not None and time <= et:
            if self.output.last_modified_time == time:
                raise ValueError(f"Duplicate time produced by generator: [{time}] - {out}")
            self.output.apply_result(out)
            self.next_value = None
            self.eval()  # We are going to apply now! Prepare next step,
            return
            # This should ultimately either produce no result or a result that is to be scheduled

        if self.next_value is not None:
            self.output.apply_result(self.next_value)
            self.next_value = None

        if time is not None and out is not None:
            self.next_value = out
            self.graph.schedule_node(self.node_ndx, time)


@dataclass
class PythonPushQueueNodeImpl(NodeImpl):  # Node

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.receiver: "_SenderReceiverState" = None
        self.messages_queued = 0
        self.messages_dequeued = 0

    @start_guard
    def start(self):
        self._initialise_kwargs()
        self.receiver = self.graph.receiver
        self.eval_fn(lambda m: self.enqueue_message(m), **self._kwargs)
        self.elide = self.scalars.get("elide", False)
        self.batch = self.scalars.get("batch", False)

    def enqueue_message(self, message):
        self.messages_queued += 1
        self.receiver((self.node_ndx, message))

    def apply_message(self, message) -> bool:
        """
        Attempt to apply the message to the output, if the application is successful returns True
        else returns False to indicate the application was not possible.
        """
        if self.batch:
            if isinstance(self.output, TimeSeriesDictOutput):
                for k, v in message.items():
                    if v is not REMOVE and v is not REMOVE_IF_EXISTS:
                        output = self.output.get_or_create(k)
                        if output.modified:
                            output.value = output.value + (v,)
                        else:
                            output.value = (v,)
                    else:
                        self.output.pop(k)
            else:
                if self.output.modified:
                    self.output.value = self.output.value + (message,)
                else:
                    self.output.value = (message,)

            self.messages_dequeued += 1
            return True
        
        if self.elide or self.output.can_apply_result(message):
            self.output.apply_result(message)
            self.messages_dequeued += 1
            return True
        
        return False

    @property
    def messages_in_queue(self):
        return self.messages_queued - self.messages_dequeued


@dataclass
class _SenderReceiverState:
    lock: threading.RLock = field(default_factory=threading.RLock)
    queue: deque = field(default_factory=deque)
    evaluation_clock: EngineEvaluationClock = None
    stopped: bool = False

    def __call__(self, value):
        self.enqueue(value)

    def enqueue(self, value):
        with self.lock:
            if self.stopped:
                raise RuntimeError("Cannot enqueue into a stopped receiver")
            self.queue.append(value)
            self.evaluation_clock.mark_push_node_requires_scheduling()

    def dequeue(self):
        with self.lock:
            return self.queue.popleft() if self.queue else None

    def __bool__(self):
        with self.lock:
            return bool(self.queue)

    def __enter__(self):
        self.lock.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.lock.release()


class PythonLastValuePullNodeImpl(NodeImpl):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._delta_value: Optional[Any] = None
        self._delta_combine_fn: Callable[[Any, Any], Any] = {
            HgTSSTypeMetaData: PythonLastValuePullNodeImpl._combine_tss_delta,
            HgTSDTypeMetaData: PythonLastValuePullNodeImpl._combine_tsd_delta,
            HgTSBTypeMetaData: PythonLastValuePullNodeImpl._combine_tsb_delta,
            HgTSLTypeMetaData: PythonLastValuePullNodeImpl._combine_tsl_delta_value,
        }.get(type(self.signature.time_series_output), lambda old_delta, new_delta: new_delta)
        if self.scalars:
            self._delta_value = self.scalars["default"]
            self.notify()

    def copy_from_input(self, output: "TimeSeriesOutput"):
        self._delta_value = (
            output.delta_value
            if self._delta_value is None
            else self._delta_combine_fn(self._delta_value, output.delta_value)
        )
        self.notify_next_cycle()  # If we are copying the value now, then we expect it to be used in the next cycle

    def apply_value(self, new_value: "SCALAR"):
        try:
            self._delta_value = (
                new_value if self._delta_value is None else self._delta_combine_fn(self._delta_value, new_value)
            )
        except Exception as e:
            raise TypeError(
                f"Cannot apply value {new_value} of type {new_value.__class__.__name__} to {self}: {e}"
            ) from e

        self.notify_next_cycle()

    def eval(self):
        if self._delta_value is not None:
            self.output.value = self._delta_value
            self._delta_value = None

    @staticmethod
    def _combine_tss_delta(old_delta: PythonSetDelta | set, new_delta: PythonSetDelta | set) -> PythonSetDelta:
        """We get TimeSeriesSetDelta from output or set into apply_resul"""

        if type(old_delta) is set and type(new_delta) is set:
            return new_delta | old_delta

        if isinstance(old_delta, set):
            old_delta = PythonSetDelta(
                added={i for i in old_delta if type(i) is not Removed},
                removed={i for i in old_delta if type(i) is Removed},
            )
        if isinstance(new_delta, set):
            new_delta = PythonSetDelta(
                added={i for i in new_delta if type(i) is not Removed},
                removed={i for i in new_delta if type(i) is Removed},
            )

        # Only add items that have not subsequently been removed plus the new added items less the "re-added elements"
        added = (old_delta.added - new_delta.removed) | (new_delta.added - old_delta.removed)
        removed = (old_delta.removed - new_delta.added) | (new_delta.removed - old_delta.added)
        # Only remove elements that have not been recently added and don't remove old removes that have been re-added
        return PythonSetDelta(added=added, removed=removed)

    @staticmethod
    def _combine_tsd_delta(old_delta: Mapping, new_delta: Mapping) -> Mapping:
        # REMOVES are tracked inside-of the dict, so if we re-add a removed element, the union operator
        # Will take care of changing the remove to a modify operation. If we remove a new added element, the remove
        # will overwrite the remove operation, and it will become an update.
        return old_delta | new_delta

    @staticmethod
    def _combine_tsb_delta(old_delta: Mapping, new_delta: Mapping) -> Mapping:
        return old_delta | new_delta

    @staticmethod
    def _combine_tsl_delta_value(old_delta: Mapping, new_delta: Mapping) -> Mapping:
        return old_delta | new_delta


class SkipEvalDelegate(NodeDelegate):
    def eval(self):
        """Don't evaluate"""
        ...
