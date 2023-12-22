import functools
import math
import operator
from collections import deque
from datetime import datetime
from typing import Mapping, Any, Callable, cast, Iterable, Sequence

from more_itertools import take

from hgraph import start_guard, stop_guard
from hgraph._builder._graph_builder import GraphBuilder
from hgraph._impl._runtime._graph import PythonGraph
from hgraph._impl._runtime._node import NodeImpl
from hgraph._runtime._constants import MIN_DT
from hgraph._runtime._evaluation_clock import EngineEvaluationClock
from hgraph._runtime._evaluation_clock import EvaluationClock, EngineEvaluationClockDelegate
from hgraph._runtime._evaluation_engine import EvaluationEngine, EvaluationEngineDelegate
from hgraph._runtime._graph import Graph
from hgraph._runtime._map import KEYS_ARG
from hgraph._runtime._node import Node, NodeSignature
from hgraph._types._scalar_types import SCALAR
from hgraph._types._time_series_types import TIME_SERIES_TYPE, TimeSeriesInput
from hgraph._types._tsd_type import TSD, TSD_OUT
from hgraph._types._tss_type import TSS

__all__ = ("PythonMapNodeImpl",)


class NestedEngineEvaluationClock(EngineEvaluationClockDelegate):

    def __init__(self, engine_evaluation_clock: EngineEvaluationClock, nested_node: "PythonNestedNodeImpl"):
        super().__init__(engine_evaluation_clock)
        self._nested_node = nested_node

    def update_next_scheduled_evaluation_time(self, next_time: datetime):
        # NOTE: We only need to schedule if the next time is after the current evaluation time (or if the map_node has
        # not yet been evaluated).
        if next_time < self.evaluation_time or self._nested_node.last_evaluation_time == self.evaluation_time:
            # No point doing anything as we are already scheduled to run.
            return
        self._nested_node.graph.schedule_node(self._nested_node.node_ndx, next_time)


class MapNestedEngineEvaluationClock(NestedEngineEvaluationClock):

    def __init__(self, engine_evaluation_clock: EngineEvaluationClock, key: SCALAR, nested_node: "PythonMapNodeImpl"):
        super().__init__(engine_evaluation_clock, nested_node)
        self._key = key

    def update_next_scheduled_evaluation_time(self, next_time: datetime):
        # First we make sure the key is correctly scheduled, then we call super, which will ensure the
        # node is scheduled if required.
        if next_time <= self._nested_node.last_evaluation_time:
            return
        tm = self._nested_node._scheduled_keys.get(self._key)
        if tm is None or tm > next_time:
            self._nested_node._scheduled_keys[self._key] = next_time
        super().update_next_scheduled_evaluation_time(next_time)


class NestedEvaluationEngine(EvaluationEngineDelegate):
    """

    Requesting a stop of the engine will stop the outer engine.
    Stopping an inner graph is a source of bugs and confusion. Instead, the user should create a mechanism to
    remove the key used to create the graph.
    """

    def __init__(self, engine: EvaluationEngine, evaluation_clock: EngineEvaluationClock):
        super().__init__(engine)
        self._engine_evaluation_clock = evaluation_clock

    @property
    def evaluation_clock(self) -> "EvaluationClock":
        return self._engine_evaluation_clock

    @property
    def engine_evaluation_clock(self) -> "EngineEvaluationClock":
        return self._engine_evaluation_clock


class PythonNestedNodeImpl(NodeImpl):

    def __init__(self,
                 node_ndx: int,
                 owning_graph_id: tuple[int, ...],
                 signature: NodeSignature,
                 scalars: Mapping[str, Any],
                 eval_fn: Callable = None,
                 start_fn: Callable = None,
                 stop_fn: Callable = None,
                 ):
        super().__init__(node_ndx, owning_graph_id, signature, scalars, eval_fn, start_fn, stop_fn)
        self._last_evaluation_time = MIN_DT

    @property
    def last_evaluation_time(self) -> datetime:
        return self._last_evaluation_time

    def mark_evaluated(self):
        self._last_evaluation_time = self.graph.evaluation_clock.evaluation_time


class PythonMapNodeImpl(PythonNestedNodeImpl):

    def __init__(self,
                 node_ndx: int,
                 owning_graph_id: tuple[int, ...],
                 signature: NodeSignature,
                 scalars: Mapping[str, Any],
                 eval_fn: Callable = None,
                 start_fn: Callable = None,
                 stop_fn: Callable = None,
                 nested_graph_builder: GraphBuilder = None,
                 input_node_ids: Mapping[str, int] = None,
                 output_node_id: int = None,
                 multiplexed_args: frozenset[str] = None
                 ):
        super().__init__(node_ndx, owning_graph_id, signature, scalars, eval_fn, start_fn, stop_fn)
        self.nested_graph_builder: GraphBuilder = nested_graph_builder
        self.input_node_ids: Mapping[str, int] = input_node_ids
        self.output_node_id: int = output_node_id
        self.multiplexed_args: frozenset[str] = multiplexed_args
        self._scheduled_keys: dict[SCALAR, datetime] = {}
        self._active_graphs: dict[SCALAR, Graph] = {}
        self._count = 0

    def eval(self):
        self.mark_evaluated()
        # 1. All inputs should be reference, and we should only be active on the KEYS_ARG input
        keys: TSS[SCALAR] = self._kwargs[KEYS_ARG]
        if keys.modified:
            for k in keys.added():
                self._create_new_graph(k)
            for k in keys.removed():
                self._remove_graph(k)
        # 2. or one of the nested graphs has been scheduled for evaluation.
        scheduled_keys = self._scheduled_keys
        self._scheduled_keys = {}
        for k, dt in scheduled_keys.items():
            if dt == self.last_evaluation_time:
                self._evaluate_graph(k)
            elif dt < self.last_evaluation_time:
                raise RuntimeError("Scheduled time is in the past")
            else:
                # Re-schedule for the next time.
                self._scheduled_keys[k] = dt
                self.graph.schedule_node(self.node_ndx, dt)

    def _create_new_graph(self, key: SCALAR):
        """Create new graph instance and wire it into the node"""
        graph: Graph = self.nested_graph_builder.make_instance(self.graph.graph_id + (self._count,), self)
        self._count += 1
        self._active_graphs[key] = graph
        graph.evaluation_engine = NestedEvaluationEngine(self.graph.evaluation_engine, MapNestedEngineEvaluationClock(
            self.graph.evaluation_engine.engine_evaluation_clock, key, self))
        graph.initialise()
        self._wire_graph(key, graph)
        graph.start()
        self._evaluate_graph(key)

    def _remove_graph(self, key: SCALAR):
        """Un-wire graph and schedule for removal"""
        graph: Graph = self._active_graphs.pop(key)
        graph.stop()
        graph.dispose()

    def _evaluate_graph(self, key: SCALAR):
        """Evaluate the graph for this key"""
        graph: Graph = self._active_graphs[key]
        graph.evaluate_graph()

    def _wire_graph(self, key: SCALAR, graph: Graph):
        """Connect inputs and outputs to the nodes inputs and outputs"""
        for arg, node_ndx in self.input_node_ids.items():
            node: NodeImpl = graph.nodes[node_ndx]
            node.notify()
            if arg == 'key':
                # The key should be a const node, then we can adjust the scalar values.
                from hgraph._wiring._stub_wiring_node import KeyStubEvalFn
                cast(KeyStubEvalFn, node.eval_fn).key = key
            else:
                if arg in self.multiplexed_args:  # Is this a multiplexed input?
                    # This should create a phantom input if one does not exist.
                    ts = cast(TSD[str, TIME_SERIES_TYPE], self.input[arg]).get_or_create(key)
                else:
                    ts = self.input[arg]
                node.input = node.input.copy_with(__init_args__=dict(owning_node=node), ts=ts)
                # Now we need to re-parent the pruned ts input.
                ts.re_parent(node.input)

        if self.output_node_id:
            node: Node = graph.nodes[self.output_node_id]
            # Replace the nodes output with the map node's output for the key
            node.output = cast(TSD_OUT[str, TIME_SERIES_TYPE], self.output).get_or_create(key)


class PythonReduceNodeImpl(PythonNestedNodeImpl):
    """
    This implements the TSD reduction. The solution uses an inverted binary tree with inputs at the leaves and the
    result at the root. The inputs bound to the leaves can be moved as nodes come and go.

    Follow a similar pattern to a list where we grow the tree with additional capacity, but also support the
    reduction of the tree when the tree has shrunk sufficiently.
    """

    def __init__(self,
                 node_ndx: int,
                 owning_graph_id: tuple[int, ...],
                 signature: NodeSignature,
                 scalars: Mapping[str, Any],
                 eval_fn: Callable = None,
                 start_fn: Callable = None,
                 stop_fn: Callable = None,
                 nested_graph_builder: GraphBuilder = None,
                 input_node_ids: tuple[int, int] = None,
                 output_node_id: int = None,
                 ):
        super().__init__(node_ndx, owning_graph_id, signature, scalars, eval_fn, start_fn, stop_fn)
        self._nested_graph: PythonGraph = PythonGraph(self.owning_graph_id + (self.node_ndx,), nodes=[],
                                                      parent_node=self)

        self.nested_graph_builder: GraphBuilder = nested_graph_builder
        self.input_node_ids: tuple[int, int] = input_node_ids  # LHS index, RHS index
        self.output_node_id: int = output_node_id

        self._bound_node_indexes: dict[SCALAR, tuple[int, int]] = {}
        self._free_node_indexes: list[tuple[int, int]] = []  # This is a list of (ndx, 0(lhs)|1(rhs)) tuples.

    def initialise(self):
        self._nested_graph.evaluation_engine = NestedEvaluationEngine(
            self.graph.evaluation_engine,
            NestedEngineEvaluationClock(self.graph.engine_evaluation_clock, self)
        )

    @start_guard
    def start(self):
        super().start()
        keys = set(self._tsd.keys()) - set(self._tsd.added_keys())
        if len(keys) > 0:
            self._add_nodes(keys)  # If there are already inputs, then add the keys.
        else:
            self._grow_tree()
        self._nested_graph.start()

    @stop_guard
    def stop(self):
        self._nested_graph.stop()
        super().stop()

    def eval(self):
        # Process additions and removals (do in order remove then add to reduce the possibility of growing
        # The tree just to tear it down again
        self._remove_nodes(self._tsd.removed_keys())
        self._add_nodes(self._tsd.added_keys())

        # Now we can re-balance the tree if required.
        self._re_balance_nodes()

        self._nested_graph.evaluate_graph()

        # Now we just need to detect the change in graph shape, so we can propagate it on.
        # The output as well as the last_output are reference time-series so this should
        # not change very frequently
        if (o := self.output).value != (v := self._last_output.value):
            o.value = v

    @property
    def _last_output(self):
        sub_graph = self._get_node(self._node_count - 1)
        out_node: Node = sub_graph[self.output_node_id]
        return out_node.output

    @property
    def _zero(self) -> TIME_SERIES_TYPE:
        return self._input['zero']

    @property
    def _tsd(self) -> TSD[SCALAR, TIME_SERIES_TYPE]:
        # noinspection PyTypeChecker
        return self._input['ts']

    def _add_nodes(self, keys: Iterable[SCALAR]):
        """
        Add nodes to the tree, when the tree is full we grow the tree by doubling the capacity.
        This adds 2n+1 nodes to the tree where n is the current number of nodes in the graph (not the number of inputs).
        There are more efficient ways to do this, but this is the simplest.
        """
        for key in keys:
            if not self._free_node_indexes:
                # We need to grow the tree.
                self._grow_tree()
            # We have free nodes, so we can just re-use them.
            ndx = self._free_node_indexes.pop()
            self._bind_key_to_node(key, ndx)

    def _remove_nodes(self, keys: Iterable[SCALAR]):
        """Remove nodes from the tree"""
        for key in keys:
            ndx = self._bound_node_indexes.pop(key)
            if self._bound_node_indexes:
                next_largest = max(self._bound_node_indexes.items(), key=operator.itemgetter(1))
                if next_largest[1][0] > ndx[0]:  # Make sure that we weren't deleting the last element
                    self._swap_node(ndx, next_largest[1])
                    self._bound_node_indexes[next_largest[0]] = ndx
                    ndx = next_largest[1]
            self._free_node_indexes.append(ndx)
            self._zero_node(ndx)

    def _swap_node(self, src_ndx: tuple[int, int], dst_ndx: tuple[int, int]):
        """Swap two nodes in the tree"""
        src_node_id, src_side = src_ndx
        dst_node_id, dst_side = dst_ndx
        src_node = self._get_node(src_node_id)[src_side]
        dst_node = self._get_node(dst_node_id)[dst_side]
        # The previously bound time-series can be dropped as it would have been removed and is going away.
        src_input = src_node.input[0]
        dst_input = dst_node.input[0]
        src_node.input = src_node.input.copy_with(__init_args__=dict(owning_node=src_node), ts=dst_input)
        dst_node.input = dst_node.input.copy_with(__init_args__=dict(owning_node=dst_node), ts=src_input)
        src_node.notify()
        dst_node.notify()

    def _re_balance_nodes(self):
        if self._node_count > 8 and (len(self._free_node_indexes) * .75) > len(self._bound_node_indexes):
            # We can shrink the tree.
            self._shrink_tree()

    def _evaluate_graph(self):
        """Evaluate the graph for this key"""
        self._nested_graph.evaluate_graph()

    @functools.cached_property
    def _node_size(self):
        """Return the number of nodes in the tree"""
        return len(self.nested_graph_builder.node_builders)

    @property
    def _node_count(self) -> int:
        """Return the number of nodes in the tree"""
        return len(self._nested_graph.nodes) // self._node_size

    def _get_node(self, ndx: int) -> Sequence[Node]:
        """
        Returns a view of the nodes at the level and column.
        """
        return self._nested_graph.nodes[ndx * self._node_size: (ndx + 1) * self._node_size]

    def _bind_key_to_node(self, key: SCALAR, ndx: tuple[int, int]):
        """Bind a key to a node"""
        self._bound_node_indexes[key] = ndx
        node_id, side = ndx
        node: NodeImpl = self._get_node(node_id)[side]
        ts = self._tsd[key]  # The key must exist.
        node.input = node.input.copy_with(__init_args__=dict(owning_node=node), ts=ts)
        node.notify()

    def _zero_node(self, ndx: tuple[int, int]):
        """Unbind a key from a node"""
        node_id, side = ndx
        node = self._get_node(node_id)[side]
        # The previously bound time-series can be dropped as it would have been removed and is going away.
        node.input = node.input.copy_with(__init_args__=dict(owning_node=node), ts=self._zero)
        node.notify()

    def _grow_tree(self):
        """Grow the tree by doubling the capacity"""
        # The tree will double in size, so we need to add 2n+1 nodes where n is the current number of nodes.
        count = self._node_count
        end = (2 * count + 1) # Not inclusive
        top_layer_length = int((end+1)/4)  # The half-length of the full top row
        top_layer_end = max(count + top_layer_length, 1)
        last_node = end - 1
        un_bound_outputs = deque(maxlen=end - count)
        for i in range(count, end):
            un_bound_outputs.append(i)
            self._nested_graph.extend_graph(self.nested_graph_builder, True)
            if i < top_layer_end:
                ndx = (i, 0)
                self._free_node_indexes.append(ndx)
                self._zero_node(ndx)
                ndx = (i, 1)
                self._free_node_indexes.append(ndx)
                self._zero_node(ndx)
            else:
                if i < last_node:
                    # Connect the new nodes together
                    left_parent = self._get_node(un_bound_outputs.popleft())[self.output_node_id].output
                    right_parent= self._get_node(un_bound_outputs.popleft())[self.output_node_id].output
                else:
                    old_root = self._get_node(count - 1)[self.output_node_id]
                    left_parent = old_root.output  # The last of the old series
                    new_root = self._get_node(un_bound_outputs.popleft())[self.output_node_id]
                    right_parent = new_root.output
                sub_graph = self._get_node(i)
                lhs_input: Node = sub_graph[self.input_node_ids[0]]
                rhs_input: Node = sub_graph[self.input_node_ids[1]]
                cast(TimeSeriesInput, lhs_input.input[0]).bind_output(left_parent)
                cast(TimeSeriesInput, rhs_input.input[0]).bind_output(right_parent)
                lhs_input.notify()
                rhs_input.notify()

        # The newly created last node should tick on first evaluation with the new output binding.
        # Evaluation should pick this up and ensure we forward the new output on.

        if self._nested_graph.is_started or self._nested_graph.is_starting:
            self._nested_graph.start_subgraph(count * self._node_size, len(self._nested_graph.nodes))

    def _shrink_tree(self):
        """Shrink the tree by halving the capacity"""
        # The nodes are expected to remain left based by ensuring we switch out with the outermost node
        # when deleting
        capacity = (active_count := len(self._bound_node_indexes)) + len(self._free_node_indexes)
        if capacity <= 8:
            return
        halved_capacity = capacity // 2  # Halved capacity gives number of top nodes
        if halved_capacity < active_count:
            return  # Should not be, but best to ensure
        last_node = (self._node_count - 1) // 2 # Reverse out the size calc to get correct starting point for deletion
        start = last_node
        self._nested_graph.reduce_graph(start*self._node_size)
        # Now remove from free list
        free_nodes = list(take((halved_capacity-active_count), sorted(self._free_node_indexes)))
        self._free_node_indexes = sorted(free_nodes, reverse=True)

