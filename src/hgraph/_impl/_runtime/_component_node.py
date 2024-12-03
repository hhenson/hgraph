from datetime import datetime
from string import Formatter
from typing import Mapping, Any, Callable, List

from hgraph import GlobalState, replay_const, recover_ts, get_fq_recordable_id, get_as_of, EngineEvaluationClock, \
    HgTSWTypeMetaData, MIN_DT, MIN_TD, MIN_ST, EvaluationEngine
from hgraph._builder._graph_builder import GraphBuilder
from hgraph._impl._runtime._nested_evaluation_engine import (
    PythonNestedNodeImpl,
    NestedEvaluationEngine,
    NestedEngineEvaluationClock,
)
from hgraph._impl._runtime._node import NodeImpl
from hgraph._runtime._graph import Graph
from hgraph._runtime._node import NodeSignature, Node, ComponentNode

__all__ = ("PythonComponentNodeImpl",)


class PythonComponentEvaluationEngine(NestedEvaluationEngine):

    def __init__(self, engine: EvaluationEngine, evaluation_clock: EngineEvaluationClock):
        super().__init__(engine, evaluation_clock)
        self._before_evaluation_notification: List[callable] = []
        self._after_evaluation_notification: List[callable] = []
        self._recovering: bool = False

    def mark_recovering(self):
        self._recovering = True

    def mark_recovered(self):
        self._recovering = False

    def add_before_evaluation_notification(self, fn: callable):
        if self._recovering:
            self._before_evaluation_notification.append(fn)
        else:
            super().add_before_evaluation_notification(fn)

    def add_after_evaluation_notification(self, fn: callable):
        if self._recovering:
            self._after_evaluation_notification.append(fn)
        else:
            super().add_after_evaluation_notification(fn)

    def notify_before_evaluation(self):
        if self._recovering:
            for notification_receiver in self._before_evaluation_notification:
                notification_receiver()
            self._before_evaluation_notification.clear()
        else:
            super().notify_before_evaluation()

    def notify_after_evaluation(self):
        if self._recovering:
            for notification_receiver in reversed(self._after_evaluation_notification):
                notification_receiver()
            self._after_evaluation_notification.clear()
        else:
            super().notify_after_evaluation()


class PythonComponentNodeImpl(PythonNestedNodeImpl, ComponentNode):

    def __init__(
        self,
        node_ndx: int,
        owning_graph_id: tuple[int, ...],
        signature: NodeSignature,
        scalars: Mapping[str, Any],
        nested_graph_builder: GraphBuilder = None,
        input_node_ids: Mapping[str, int] = None,
        output_node_id: int = None,
    ):
        super().__init__(node_ndx, owning_graph_id, signature, scalars)
        self.nested_graph_builder: GraphBuilder = nested_graph_builder
        self.input_node_ids: Mapping[str, int] = input_node_ids
        self.output_node_id: int = output_node_id
        self._active_graph: Graph | None = None
        self._last_evaluation_time: datetime | None = None
        from hgraph import RecordReplayEnum
        from hgraph import RecordReplayContext
        self._recover: bool = RecordReplayEnum.RECOVER in RecordReplayContext.instance().mode

    def _wire_graph(self):
        """Connect inputs and outputs to the nodes inputs and outputs"""
        if self._active_graph:
            return  # Already wired in
        id_, ready = self.recordable_id()
        if not ready:
            return
        if (gs := GlobalState.instance()).get(k := f"component::{id_}", None) is not None:
            raise RuntimeError(f"Component[{id_}] {self.signature.signature} already exists in graph")
        else:
            gs[k] = True  # Just write a marker for now

        self._active_graph = self.nested_graph_builder.make_instance(self.node_id, self, label=id_)
        self._active_graph.traits.set_traits(recordable_id=id_)
        self._active_graph.evaluation_engine = PythonComponentEvaluationEngine(
            self.graph.evaluation_engine, NestedEngineEvaluationClock(self.graph.engine_evaluation_clock, self)
        )
        self._active_graph.initialise()

        # Start, then when in recovery, this will cause the graph to recover at one engine tick prior to the desired
        # start time, after which we can wire in the new inputs and connect the output up.
        self._start_active_graph()

        for arg, node_ndx in self.input_node_ids.items():
            node: NodeImpl = self._active_graph.nodes[node_ndx]
            node.notify()
            ts = self.input[arg]
            node.input = node.input.copy_with(__init_args__=dict(owning_node=node), ts=ts)
            # Now we need to re-parent the pruned ts input.
            ts.re_parent(node.input)

        if self.output_node_id:
            node: Node = self._active_graph.nodes[self.output_node_id]
            # Replace the nodes output with the map node's output
            node.output = self.output

    def _start_active_graph(self):
        tm = self.graph.evaluation_clock.evaluation_time
        if tm == MIN_ST:
            self._recover = False
        if self._recover:
            # Set the current time back by one tick to support recovery
            self._active_graph.engine_evaluation_clock.evaluation_time = tm - MIN_TD
            self._active_graph.evaluation_engine.mark_recovering()
        self._active_graph.start()
        if self._recover:
            self.recover()
            self._active_graph.evaluation_engine.mark_recovered()
            self._active_graph.engine_evaluation_clock.evaluation_time = tm
            self.notify()

    def do_start(self):
        self._wire_graph()
        if not self._active_graph:
            self.graph.schedule_node(self.node_ndx, self.graph.evaluation_clock.evaluation_time)

    def do_stop(self):
        if self._active_graph:
            self._active_graph.stop()

    def dispose(self):
        if self._active_graph:
            GlobalState.instance().pop(f"component::{self._active_graph.label}", None)
            self._active_graph.dispose()
            self._active_graph = None

    def eval(self):
        if self._active_graph is None:
            self._wire_graph()
            if self._active_graph is None:
                # Still pending
                return

        self.mark_evaluated()
        self._active_graph.evaluation_clock.reset_next_scheduled_evaluation_time()
        self._active_graph.evaluate_graph()
        self._active_graph.evaluation_clock.reset_next_scheduled_evaluation_time()

    def nested_graphs(self):
        if self._active_graph:
            return {0: self._active_graph}
        else:
            return {}

    def recover(self):
        """
        Recover by loading inputs, then set the time to be the most recent time that the last value was processed.
        Then step through at that time, ensure the state can be recovered.
        """
        traits = self._active_graph.traits
        ec_clock = self.graph.engine_evaluation_clock
        def _set_engine_time(dt):
            ec_clock.evaluation_time = dt
        tm = ec_clock.evaluation_time

        for node in self._active_graph.nodes:
            if node.signature.name == "replay_stub":
                output = node.output
                key = node.scalars["key"]
                recordable_id, _ready = self.recordable_id()
                recordable_id = get_fq_recordable_id(self.graph.traits, recordable_id)
                recover_ts(
                    key,
                    recordable_id,
                    tm,
                    get_as_of(self.graph.evaluation_clock),
                    _set_engine_time,
                    lambda: output,
                ).value
                _set_engine_time(tm)

        times = sorted(set(self._active_graph.schedule))
        max_time = times[-1]
        if times[0] == MIN_DT:
            times = times[1:]
        for time_ in times:
            _set_engine_time(time_)
            self._active_graph.evaluation_engine.notify_before_evaluation()
            for s_time, node in zip(self._active_graph.schedule, self._active_graph.nodes):
                if s_time == time_ or (force_ := time_ == max_time):
                    if node.signature.uses_recordable_state:
                        eval_node = s_time == time_
                        if (state_time := s_time-MIN_TD) > MIN_DT or force_:
                            output = node.recordable_state
                            key = "__state__"
                            recordable_id = get_fq_recordable_id(traits, node.signature.record_replay_id)
                            recover_ts(
                                key,
                                recordable_id,
                                state_time if eval_node else time_,
                                get_as_of(self.graph.evaluation_clock),
                                _set_engine_time,
                                lambda: output,
                            ).value
                        if eval_node:
                            node.eval()
                    elif type(node.signature.time_series_output) is HgTSWTypeMetaData:
                        eval_node = s_time == time_
                        if (state_time := s_time - MIN_TD) > MIN_DT or force_:
                            output = node.output
                            key = "__OUT__"
                            recordable_id = get_fq_recordable_id(traits, node.signature.record_replay_id)
                            recover_ts(
                                key,
                                recordable_id,
                                state_time if eval_node else time_,
                                get_as_of(self.graph.evaluation_clock),
                                _set_engine_time,
                                lambda: output,
                            ).value
                        if eval_node: # Set the time to be the most recent input time
                            node.eval()
                    elif node.signature.is_compute_node or node.signature.is_sink_node:
                        # This needs a bit of work since for some sink nodes we do want to re-inflate (for example, feedbacks)
                        # For some we do not.
                        if s_time == time_:
                           node.eval()
                    else:
                        if s_time == time_:
                            node.eval()
            self._active_graph.evaluation_engine.notify_after_evaluation()
        _set_engine_time(tm)

    def recordable_id(self) -> tuple[str, bool]:
        """The id and True or no id and False if required inputs are not ready yet"""
        outer_id = self.graph.traits.get_trait_or("recordable_id")
        id_ = (
            f"{'' if outer_id is None else outer_id}{'' if outer_id is None else '::'}{self.signature.record_replay_id}"
        )
        dependencies = [k for _, k, _, _ in Formatter().parse(id_) if k is not None]
        if any(k == "" for k in dependencies):
            raise RuntimeError(
                f"recordable_id: {id_} in signature: {self.signature.signature} has non-labeled format descriptors"
            )
        if dependencies:
            ts_values = [k for k in dependencies if k not in self.scalars]
            if ts_values and (
                not (self.is_started or self.is_starting) or not all(self.inputs[k].valid for k in ts_values)
            ):
                return id_, False
            args = {k: self.scalars[k] if k in self.scalars else self.inputs[k].value for k in dependencies}
            return id_.format(**args), True
        else:
            return id_, True
