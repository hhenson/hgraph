import asyncio
import os
import tempfile
import threading
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime

from _socket import gethostname
from frozendict import frozendict
from perspective import Table
import tornado.web
import pyarrow as pa

from hgraph import graph, TS, STATE, TSD, CompoundScalar, \
    TimeSeries, PythonNestedNodeImpl, Node, Graph, PythonTimeSeriesReference, sink_node, PythonTimeSeriesReferenceOutput
from hgraph._impl._runtime._node import _SenderReceiverState
from hgraph.adaptors.perspective import PerspectiveTablesManager
from hgraph.adaptors.tornado.http_server_adaptor import HttpRequest, HttpResponse, \
    HttpGetRequest
from hgraph.debug._inspector_observer import InspectionObserver
from hgraph.debug._inspector_util import node_id_from_str, str_node_id, format_value, format_timestamp, enum_items, \
    format_type, name, inspect_item


@graph
def inspector_controller(port: int = 8080):
    @dataclass
    class GraphInspectorData(CompoundScalar):
        id: str
        X: str = "+"
        name: str = None
        type: str = None
        value: str = None
        timestamp: datetime = None
        evals: int = None
        time: float = None
        of_graph: float = None
        of_total: float = None


    @dataclass
    class InspectorState(CompoundScalar):
        observer: InspectionObserver = None
        manager: PerspectiveTablesManager = None
        table: Table = None
        total_cycle_table: Table = None

        requests: _SenderReceiverState = field(default_factory=_SenderReceiverState)

        row_ids: dict = field(default_factory=dict)

        key_ids: dict = field(default_factory=dict)
        id_keys: dict = field(default_factory=dict)

        node_subscriptions: dict = field(default_factory=lambda: defaultdict(dict))  # node_id -> [(item path, row_id)]

        value_data: list = field(default_factory=list)
        perf_data: list = field(default_factory=list)
        total_data_prev: list = field(default_factory=dict)
        total_data: list = field(default_factory=lambda: defaultdict(list))
        last_publish_time: datetime = None

        def id_for_key(self, key):
            i = self.key_ids.setdefault(key, len(self.key_ids))
            self.id_keys[i] = key
            return i

        def key_for_id(self, i):
            return self.id_keys[i]

    INPUTS = "zzz001"
    OUTPUT = "zzz002"
    SUBGRAPHS = "zzz003"
    SCALARS = "zzz004"
    MORE = "zzy"

    def process_tick(state: STATE, node: Node):
        for row_id, path in state.node_subscriptions.get(node.node_id, {}).items():
            if row_id is not None:
                if path:
                    match path[0]:
                        case "inputs":
                            v = node.input
                        case "output":
                            v = node.output
                        case "nested":
                            v = node
                        case _:
                            continue

                    for item in path[1:]:
                        try:
                            v = inspect_item(v, item)
                        except:
                            v = "This value could not be retrieved"
                else:
                    v = node

                if isinstance(v, (TimeSeries, Node)):
                    state.value_data.append(dict(
                        id=row_id,
                        value=format_value(v),
                        timestamp=format_timestamp(v),
                    ))
                else:
                    state.value_data.append(dict(
                        id=row_id,
                        value=format_value(v),
                    ))

    def process_graph(state: STATE, graph: Graph):
        root_graph = state.observer.get_graph_info(())
        for node_id, items in state.node_subscriptions.items():
            if node_row := items.get(None, None):
                gi = state.observer.get_graph_info(node_id[:-1])
                if gi is not None:
                    node_ndx = node_id[-1]
                    state.perf_data.append(dict(
                        id=node_row,
                        evals=gi.node_eval_counts[node_ndx],
                        time=gi.node_eval_times[node_ndx] / 1_000_000_000,
                        of_graph=gi.node_eval_times[node_ndx] / gi.eval_time if gi.eval_time else None,
                        of_total=gi.node_eval_times[node_ndx] / root_graph.eval_time if root_graph.eval_time else None
                    ))

        if graph.graph_id == ():
            gi = state.observer.get_graph_info(())
            state.total_data['time'].append(datetime.utcnow())
            state.total_data['evaluation_time'].append(graph.evaluation_clock.evaluation_time)
            state.total_data['cycles'].append(gi.eval_count)
            state.total_data['graph_time'].append(gi.eval_time)

            if state.last_publish_time is None or (datetime.now() - state.last_publish_time).total_seconds() > 2.5:
                publish_stats(state)

            handle_request(state)

        else:
            gi = state.observer.get_graph_info(graph.graph_id)
            for k, row_id in state.node_subscriptions.get(graph.graph_id, {}).items():
                if k is None:
                    parent_time = state.observer.get_graph_info(graph.parent_node.graph.graph_id).eval_time
                    state.perf_data.append(dict(
                        id=row_id,
                        timestamp=graph.parent_node.last_evaluation_time if graph.parent_node else None,
                        evals=gi.eval_count,
                        time=gi.eval_time / 1_000_000_000,
                        of_graph=gi.eval_time / parent_time if parent_time else None,
                        of_total=gi.eval_time / root_graph.eval_time if root_graph.eval_time else None
                    ))


    def publish_stats(state: STATE):
        state.manager.update_table("inspector", [i for i in state.value_data if i["id"] in state.row_ids])
        state.value_data = []

        state.manager.update_table("inspector", [i for i in state.perf_data if i["id"] in state.row_ids])
        state.perf_data = []

        data = state.total_data
        if data["time"]:
            total_time = (state.total_data["time"][-1] - state.total_data_prev.get("time", datetime.min)).total_seconds()
            total_graph_time = (data["graph_time"][-1] - state.total_data_prev.get("graph_time", 0)) / 1_000_000_000
            lags = [(data["time"][i] - data["evaluation_time"][i]).total_seconds() for i in range(len(data["time"]))]

            state.total_cycle_table.update([dict(
                time=data["time"][-1],
                evaluation_time=data["evaluation_time"][-1],
                cycles=(data["cycles"][-1] - state.total_data_prev.get("cycles", 0)) / total_time,
                graph_time=total_graph_time,
                graph_load=total_graph_time / total_time,
                avg_lag=sum(lags) / len(data["time"]),
                max_lag=max(lags)
            )])

            state.total_data_prev = {k: v[-1] for k, v in data.items()}
            state.total_data = defaultdict(list)

        state.last_publish_time = datetime.now()

    @sink_node
    def inspector(start: TS[bool] = True, port: int = 8080, _state: STATE[InspectorState] = None):
        ...

    def set_result(f, r):
        from hgraph.adaptors.tornado._tornado_web import TornadoWeb
        TornadoWeb.get_loop().add_callback(lambda f, r: f.set_result(r), f, r)

    def handle_request(_state):
        data = []  # to be published to the table
        remove = set()  # to be removed from the table

        observer = _state.observer

        while f_r := _state.requests.dequeue():
            f: asyncio.Future
            r: HttpRequest
            f, r = f_r

            command = r.url_parsed_args[0]
            id_str = r.url_parsed_args[1]

            commands = deque()
            commands.append((command, id_str))

            while commands:
                command, id_str = commands.popleft()
                level = "graph"

                if id_str[-6:-3] == MORE:
                    more = node_id_from_str(id_str[-3:])[0]
                    id_str = id_str[:-6]
                else:
                    more = None

                if id_str.endswith(INPUTS):
                    level = "inputs"
                    id_str_node = id_str[:-6]
                elif id_str.endswith(OUTPUT):
                    level = "output"
                    id_str_node = id_str[:-6]
                elif id_str.endswith(SUBGRAPHS):
                    level = "nested"
                    id_str_node = id_str[:-6]
                elif id_str.endswith(SCALARS):
                    level = "scalars"
                    id_str_node = id_str[:-6]
                else:
                    id_str_node = id_str

                if id_str_node == '':
                    graph_id = ()
                    level = "graph"
                    path = None
                else:
                    graph_id = _state.row_ids[id_str_node]
                    path = _state.node_subscriptions[graph_id][id_str]

                if (gi := observer.get_graph_info(graph_id)) is None:
                    level = "node" if level == "graph" else level
                    node_id = graph_id[-1]
                    graph_id = graph_id[:-1]
                    if (gi := observer.get_graph_info(graph_id)) is None:
                        set_result(f, HttpResponse(status_code=500, body=f"Graph {graph_id} was not found"))
                        continue
                    node = gi.graph.nodes[node_id]
                else:
                    node = None

                if path is not None:
                    level = path[0]
                    if len(path) == 2 and level == "nested":
                        try:
                            graph = inspect_item(node, path[1])
                            graph_id = graph.graph_id
                            gi = observer.get_graph_info(graph_id)
                            level = "graph"
                        except:
                            set_result(f, HttpResponse(status_code=500, body=f"Graph {graph_id}, {path[1]} was not found"))
                            continue

                if level == "output":
                    root_item = node.output
                    level = "value"
                elif level == "inputs":
                    root_item = node.input
                    level = "value"
                elif level == "nested":
                    root_item = node
                    level = "value"
                elif level == "scalars":
                    root_item = node.scalars
                    level = "value"
                else:
                    root_item = node

                match command:
                    case "expand":
                        tab = "\u00A0\u00A0" * (len(id_str) // 3)

                        if id_str:
                            data.append(dict(id=id_str, X="-"))

                        match level:
                            case "graph":
                                for i, n in enumerate(gi.graph.nodes):
                                    row_id = id_str + str_node_id((i,))
                                    data.append(dict(
                                        id=row_id,
                                        X="+",
                                        name=tab + name(gi.graph.nodes[i]),
                                        type=format_type(n),
                                        value=format_value(n),
                                        timestamp=format_timestamp(n)))

                                    _state.node_subscriptions[n.node_id][row_id] = None
                                    _state.node_subscriptions[n.node_id][None] = row_id
                                    _state.observer.subscribe(n.node_id)
                                    _state.row_ids[row_id] = n.node_id

                            case "node":
                                if node.input is not None:
                                    row_id = id_str + INPUTS
                                    data.append(dict(
                                        id=row_id,
                                        X="+",
                                        name=tab + "INPUTS",
                                        type=format_type(node.input),
                                        value=format_value(node.input),
                                        timestamp=format_timestamp(node.input)))

                                    _state.node_subscriptions[node.node_id][row_id] = ["inputs"]
                                    _state.row_ids[row_id] = node.node_id

                                if node.output is not None:
                                    row_id = id_str + OUTPUT
                                    data.append(dict(
                                        id=row_id,
                                        X="+",
                                        name=tab + "OUTPUT",
                                        type=format_type(node.output),
                                        value=format_value(node.output),
                                        timestamp=format_timestamp(node.output)))

                                    _state.node_subscriptions[node.node_id][row_id] = ["output"]
                                    _state.row_ids[row_id] = node.node_id

                                if isinstance(node, PythonNestedNodeImpl):
                                    row_id = id_str + SUBGRAPHS
                                    data.append(dict(
                                        id=row_id,
                                        X="+",
                                        name=tab + "GRAPHS",
                                        type="",
                                        value=format_value(node.input),
                                        timestamp=format_timestamp(node.input)))

                                    _state.row_ids[row_id] = None
                                    _state.node_subscriptions[node.node_id][row_id] = ["nested"]

                                if node.scalars:
                                    row_id = id_str + SCALARS
                                    data.append(dict(
                                        id=row_id,
                                        X="+",
                                        name=tab + "SCALARS",
                                        type="",
                                        value=format_value(node.scalars),
                                        timestamp=format_timestamp(node.scalars)))

                                    _state.row_ids[row_id] = None
                                    _state.node_subscriptions[node.node_id][row_id] = ["scalars"]

                            case "value":
                                start = 0
                                path = [] if path is None else path
                                for key in path[1:]:
                                    try:
                                        root_item = inspect_item(root_item, key)
                                    except:
                                        root_item = None
                                        set_result(f, HttpResponse(status_code=500, body=f"Item cannot be inspected"))
                                        break

                                if more is not None:
                                    start = more
                                    remove.add(id_str + MORE + str_node_id((start,)))

                                if root_item:
                                    for i, (k, v) in enumerate(enum_items(root_item)):
                                        if i < start:
                                            continue
                                        if i >= start + 10 and not 'all' in r.query:
                                            row_id = id_str + MORE + str_node_id((i,))
                                            data.append(dict(
                                                id=row_id,
                                                X="+",
                                                name=tab + "...",
                                                type="",
                                            ))
                                            _state.row_ids[row_id] = None
                                            break

                                        row_id = id_str + str_node_id((_state._value.id_for_key(k),))
                                        data.append(dict(
                                            id=row_id,
                                            X="+",
                                            name=tab + str(k),
                                            type=format_type(v),
                                            value=format_value(v),
                                            timestamp=format_timestamp(v)
                                        ))

                                        if isinstance(v, Graph):
                                            _state.row_ids[row_id] = v.graph_id
                                            _state.node_subscriptions[v.graph_id][None] = row_id
                                            _state.observer.subscribe(v.graph_id)
                                        else:
                                            _state.node_subscriptions[node.node_id][row_id] = path + [k]
                                            _state.row_ids[row_id] = node.node_id

                                    remove.add(id_str + MORE + str_node_id((start,)))

                    case "collapse":
                        remove |= {i for i in _state.row_ids if i.startswith(id_str)} - {id_str}
                        for i in remove:
                            if node_id := _state.row_ids.get(i):
                                (subs := _state.node_subscriptions[node_id]).pop(i, None)
                                if len(subs) == 0:
                                    _state.observer.unsubscribe(node_id)

                        data.append(dict(id=id_str, X="+"))

                    case "ref":
                        path = [] if path is None else path
                        for key in path[1:]:
                            try:
                                root_item = inspect_item(root_item, key)
                                path.append(key)
                            except:
                                root_item = None
                                set_result(f, HttpResponse(status_code=500, body=f"Item cannot be inspected"))
                                break

                        if node is None:
                            root_item = gi.graph.parent_node

                        if isinstance(root_item, Node):
                            root_item = root_item.output

                        if isinstance(root_item, PythonTimeSeriesReferenceOutput):
                            root_item = root_item.value

                        if isinstance(root_item, PythonTimeSeriesReference):
                            if (o := root_item.output) is not None:
                                ref_node = o.owning_node
                                path = []
                                while o.parent_output:
                                    path.append(o.parent_output.key_from_value(o))
                                    o = o.parent_output

                                ref_id = ''
                                i = 1
                                while i < len(ref_node.node_id):
                                    graph_id = ref_node.node_id[:i]
                                    ref_id += str_node_id((graph_id[-1],))
                                    commands.append(("expand", ref_id))
                                    if observer.get_graph_info(graph_id) is None:
                                        ref_id += SUBGRAPHS
                                        commands.append(("expand", ref_id))
                                        gi = observer.get_graph_info(ref_node.node_id[:i + 1])
                                        ref_id += str_node_id((_state._value.id_for_key(gi.graph.label),))
                                        commands.append(("expand", ref_id))
                                        i += 2
                                    else:
                                        i += 1

                                ref_id += str_node_id((ref_node.node_id[-1],))
                                commands.append(("expand", ref_id))
                                ref_id += OUTPUT
                                commands.append(("expand", ref_id))

                                for i in path:
                                    ref_id += str_node_id((_state._value.id_for_key(i),))
                                    commands.append(("expand", ref_id))

                                set_result(f, HttpResponse(status_code=200, body=ref_id))

            if data or remove:
                _state.manager.update_table("inspector", data, remove)
                [_state.row_ids.pop(i, None) for i in remove]

            set_result(f, HttpResponse(status_code=200, body=id_str))

    @inspector.start
    def start_inspector(start: TS[bool], port: int, _state: STATE[InspectorState]):
        from hgraph.adaptors.perspective import PerspectiveTablesManager
        from hgraph.adaptors.tornado._tornado_web import TornadoWeb
        from hgraph.adaptors.perspective._perspective import IndexPageHandler
        from perspective import Table

        _state.requests.evaluation_clock = start.owning_graph.evaluation_clock

        _state.observer = InspectionObserver(
            start.owning_graph,
            callback_node=lambda n: process_tick(_state, n),
            callback_graph=lambda n: process_graph(_state, n)
        )
        _state.observer.on_before_node_evaluation(start.owning_node)
        start.owning_graph.evaluation_engine.add_life_cycle_observer(_state.observer)
        _state.observer.subscribe(())

        _state.manager = PerspectiveTablesManager.current()
        _state.table = Table({"id": str, **{k: v.py_type for k, v in GraphInspectorData.__meta_data_schema__.items()}}, index="id")
        _state.manager.add_table("inspector", _state.table)

        _state.total_cycle_table = Table({
            "time": datetime, "evaluation_time": datetime, "cycles": float, "graph_time": float, "graph_load": float, "avg_lag": float, "max_lag": float
        }, limit=24*3600)
        _state.manager.add_table("graph_performance", _state.total_cycle_table)

        _state.total_data_prev = dict(
            time=datetime.utcnow(),
            evaluation_time=start.owning_graph.evaluation_clock.evaluation_time,
            cycles=0,
            graph_time=0.
        )

        tempfile.gettempdir()
        layouts_dir = os.path.join(tempfile.tempdir, "inspector_layouts")

        app = TornadoWeb.instance(port)
        app.add_handlers(
            [
                (
                    r"/inspector/(.*)",
                    IndexPageHandler,
                    {
                        "mgr": _state.manager,
                        "layouts_path": layouts_dir,
                        "index_template": os.path.join(os.path.dirname(__file__), "inspector_template.html"),
                        "host": gethostname(),
                        "port": port,
                    },
                ),
                (
                    r"/inspect(?:/([^/]*))?(?:/([^/]*))?(/([^/]*))?",
                    InspectorHttpHandler,
                    {
                        "queue": _state.requests,
                    }
                )
            ]
        )

        app.start()

    inspector()

class InspectorHttpHandler(tornado.web.RequestHandler):
    def initialize(self, queue):
        self.queue = queue

    async def get(self, *args):
        request = HttpGetRequest(
                url=self.request.uri,
                url_parsed_args=args,
                headers=self.request.headers,
                query=frozendict({k: ''.join(i.decode() for i in v) for k, v in self.request.query_arguments.items()}),
                cookies=frozendict(self.request.cookies))

        future = asyncio.Future()
        self.queue((future, request))

        response = await future

        self.set_status(response.status_code)

        if response.headers:
            for k, v in response.headers.items():
                self.set_header(k, v)

        await self.finish(response.body)
