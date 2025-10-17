import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta

import perspective
import psutil
from sqlalchemy.testing.util import total_size

from hgraph import MIN_DT, Node, Graph
from hgraph.debug._inspector_item_id import InspectorItemId, InspectorItemType
from hgraph.debug._inspector_state import InspectorState
from hgraph.debug._inspector_util import (
    format_value,
    format_modified,
    format_scheduled,
    estimate_size,
    estimate_value_size,
)

logger = logging.getLogger(__name__)


def process_tick(state: InspectorState, node: Node):
    start = time.perf_counter_ns()

    state.tick_node_ids.add(node.node_id)
    if item_id := state.node_subscriptions.get(node.node_id):
        v = item_id.find_item_on_graph(node.graph)
        str_id = item_id.to_str()
        state.tick_data[str_id] = dict(
            id=str_id,
            value=format_value(v),
            modified=format_modified(v),
            scheduled=format_scheduled(v),
        )

    for item_id in state.node_item_subscriptions.get(node.node_id, ()):
        v = item_id.find_item_on_graph(node.graph)
        str_id = item_id.to_str()
        state.tick_data[str_id] = dict(
            id=str_id,
            value=format_value(v),
            modified=format_modified(v),
            scheduled=format_scheduled(v),
            # size=estimate_value_size(v),
            # total_size=estimate_size(v)
        )

    state.inspector_time += (time.perf_counter_ns() - start) / 1_000_000_000


def process_item_stats(state, item_id: InspectorItemId):
    v = item_id.find_item_on_graph(state.observer.get_graph_info(item_id.graph).graph)
    str_id = item_id.to_str()
    state.value_data.setdefault(str_id, dict(id=str_id)).update(value_size=estimate_value_size(v), size=estimate_size(v))


def process_node_stats(state, node_id, item_id, load_detailed = False):
    root_graph = state.observer.get_graph_info(())
    gi = state.observer.get_graph_info(node_id[:-1])
    if gi is not None:
        node_ndx = node_id[-1]
        str_id = item_id.to_str()
        state.perf_data.setdefault(str_id, dict(id=str_id)).update(
                evals=gi.node_eval_counts[node_ndx],
                time=gi.node_eval_times[node_ndx] / 1_000_000_000,
                of_graph=gi.node_eval_times[node_ndx] / gi.eval_time if gi.eval_time else None,
                of_total=gi.node_eval_times[node_ndx] / root_graph.eval_time if root_graph.eval_time else None,
                value_size=(v_s := gi.node_value_sizes[node_ndx]),
                size=(n_s := gi.node_sizes[node_ndx]),
                total_value_size=(
                    gi.node_total_value_sizes[node_ndx] + gi.node_value_sizes[node_ndx] if v_s is not None else None
                ),
                total_size=gi.node_total_sizes[node_ndx] + gi.node_sizes[node_ndx] if n_s is not None else None,
                subgraphs=gi.node_total_subgraph_counts[node_ndx],
                nodes=gi.node_total_node_counts[node_ndx],
            )
        
    if load_detailed:
        last = state.detailed_perf_data_node_times.get(node_id)
        perf_data = state.observer.get_recent_node_performance(node_id, last)
        if perf_data:
            state.detailed_perf_data_node_times[node_id] = perf_data[0][0]  # these come in reverse order
            state.detailed_perf_data[str_id].extend(perf_data)


def process_graph_stats(state, graph_id, item_id, load_detailed = False):
    root_graph = state.observer.get_graph_info(())
    gi = state.observer.get_graph_info(graph_id)
    parent_time = state.observer.get_graph_info(gi.graph.parent_node.graph.graph_id).eval_time
    str_id = item_id.to_str()
    state.perf_data.setdefault(str_id, dict(id=str_id)).update(
            evals=gi.eval_count,
            time=gi.eval_time / 1_000_000_000,
            of_graph=gi.eval_time / parent_time if parent_time else None,
            of_total=gi.eval_time / root_graph.eval_time if root_graph.eval_time else None,
            size=gi.size,
            total_size=gi.total_size,
            value_size=gi.total_value_size,
            subgraphs=gi.total_subgraph_count,
            nodes=gi.total_node_count,
        )
    
    if load_detailed:
        last = state.detailed_perf_data_graph_times.get(graph_id)
        perf_data = state.observer.get_recent_graph_performance(graph_id, last)
        if perf_data:
            state.detailed_perf_data_graph_times[graph_id] = perf_data[0][0]  # these come in reverse order
            state.detailed_perf_data[str_id].extend(perf_data)


def process_graph(state: InspectorState, graph: Graph, publish_interval: float):
    start = time.perf_counter_ns()

    root_graph = state.observer.get_graph_info(())

    if graph.graph_id == ():
        # publish node stats
        for node_id in state.tick_node_ids:
            if item_id := state.node_subscriptions.get(node_id):
                process_node_stats(state, node_id, item_id)

        detailed = state.track_detailed_performance
        full_stats_republish = state.observer.recent_performance_batch > state.detailed_perf_data_time \
            if detailed \
            else state.detailed_perf_data_time + timedelta(seconds=60) < datetime.utcnow()
            
        if full_stats_republish:
            for graph_id, item_id in state.graph_subscriptions.items():
                process_graph_stats(state, graph_id, item_id, detailed)
            for node_id, item_id in state.node_subscriptions.items():
                process_node_stats(state, node_id, item_id, detailed)
            state.detailed_perf_data_time = state.observer.recent_performance_batch if detailed else datetime.utcnow()

        state.total_data["time"].append(datetime.utcnow())
        state.total_data["evaluation_time"].append(root_graph.graph.evaluation_clock.evaluation_time)
        state.total_data["cycles"].append(root_graph.eval_count)
        state.total_data["cycle_time"].append(root_graph.cycle_time)
        state.total_data["os_cycle_time"].append(root_graph.os_cycle_time)
        state.total_data["graph_time"].append(root_graph.eval_time)
        state.total_data["os_graph_time"].append(root_graph.os_eval_time)
        state.total_data["total_size"].append(root_graph.total_size)

        state.inspector_time += root_graph.observation_time / 1_000_000_000
        start = check_requests_and_publish(state, start)
    else:
        # not a root graph
        if item_id := state.graph_subscriptions.get(graph.graph_id):
            process_graph_stats(state, graph.graph_id, item_id)
            state.tick_node_ids.add(graph.parent_node.node_id)

    state.inspector_time += (time.perf_counter_ns() - start) / 1_000_000_000


def check_requests_and_publish(state: InspectorState, start: int = None, stats_period=2.5):
    if start is None:
        start_ = time.perf_counter_ns()

    from hgraph.debug._inspector_handler import handle_requests

    if (
        state.last_request_process_time is None
        or (datetime.utcnow() - state.last_request_process_time).total_seconds() > 0.1
    ):
        publish_values = handle_requests(state)
        state.last_request_process_time = datetime.utcnow()

        publish = (
            state.last_publish_time is None
            or (datetime.utcnow() - state.last_publish_time).total_seconds() > stats_period
        )
        if publish_values or publish:
            if start is not None:
                state.inspector_time += (time.perf_counter_ns() - start) / 1_000_000_000
                start = time.perf_counter_ns()
            publish_tables(state, include_stats=publish)
            state.inspector_time = 0.0

    if start is None:
        state.inspector_time += (time.perf_counter_ns() - start_) / 1_000_000_000

    return start


def publish_tables(state: InspectorState, include_stats=True):
    state.manager.update_table(
        "inspector", [i for i in state.value_data.values() if i["id"] not in state.value_removals], state.value_removals
    )
    state.value_data.clear()

    state.manager.update_table("inspector", [i for i in state.perf_data.values() if i["id"] not in state.value_removals])
    state.perf_data.clear()

    state.manager.update_table(
        "inspector", [i for i in state.tick_data.values() if i["id"] not in state.value_removals]
    )
    state.tick_data.clear()

    state.manager.update_table(
        "recent_performance",
        [
            dict(id=k, time=t, **i)
            for k, v in state.detailed_perf_data.items()
            if k not in state.value_removals
            for t, i in v
        ],
    )
    state.detailed_perf_data.clear()

    state.value_removals.clear()
    state.tick_node_ids.clear()

    data = state.total_data
    if data["time"]:
        total_time = max(
            (state.total_data["time"][-1] - state.total_data_prev.get("time", datetime.min)).total_seconds(), 1e-6
        )  # in case we went below system clock resolution
        total_graph_time = (data["graph_time"][-1] - state.total_data_prev.get("graph_time", 0)) / 1_000_000_000
        total_os_graph_time = data["os_graph_time"][-1] - state.total_data_prev.get("os_graph_time", 0)
        lags = [(data["time"][i] - data["evaluation_time"][i]).total_seconds() for i in range(len(data["time"]))]
        
        psp_stats = state.manager.get_stats()
        data["psp_polls"] = [psp_stats.get('polling', 0)]
        data["psp_updates"] = [psp_stats.get('updates', 0)]
        data["psp_batches"] = [psp_stats.get('batches', 0)]
        data["psp_rows"] = [psp_stats.get('rows', 0)]

        meminfo = state.process.memory_info()
        readings = dict(
            time=data["time"][-1],
            evaluation_time=data["evaluation_time"][-1],
            cycles=(data["cycles"][-1] - state.total_data_prev.get("cycles", 0)) / total_time,
            avg_cycle=sum(data["cycle_time"]) / (len(data["time"]) * 1_000_000_000),
            avg_os_cycle=sum(data["os_cycle_time"]) / len(data["time"]),
            max_cycle=max(data["cycle_time"]) / 1_000_000_000,
            graph_time=total_graph_time,
            os_graph_time=total_os_graph_time,
            graph_load=total_graph_time / total_time,
            avg_lag=sum(lags) / len(data["time"]),
            max_lag=max(lags),
            inspection_time=state.inspector_time / total_graph_time,
            memory=meminfo.rss / (1024 * 1024),
            virt_memory=meminfo.vms / (1024 * 1024),
            graph_memory=(
                (state.total_data["total_size"][-1] / (1024 * 1024))
                if state.total_data["total_size"][-1] is not None
                else None
            ),
            psp_polls=(data["psp_polls"][-1] - state.total_data_prev.get("psp_polls", 0)) / total_time if "psp_polls" in data else None,
            psp_updates=(data["psp_updates"][-1] - state.total_data_prev.get("psp_updates", 0)) / total_time if "psp_updates" in data else None,
            psp_batches=(data["psp_batches"][-1] - state.total_data_prev.get("psp_batches", 0)) / total_time if "psp_batches" in data else None,
            psp_rows=(data["psp_rows"][-1] - state.total_data_prev.get("psp_rows", 0)) / total_time if "psp_rows" in data else None,
        )

        state.manager.update_table("graph_performance", [readings])
        logger.info(f"performance: {readings}")

        state.total_data_prev = {k: v[-1] for k, v in data.items()}
        state.total_data = defaultdict(list)

    state.last_publish_time = datetime.utcnow()
