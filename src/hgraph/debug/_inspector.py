import os
import tempfile
from datetime import datetime

from _socket import gethostname

from hgraph._wiring._decorators import sink_node
from hgraph._types import TS, STATE

from hgraph.debug._inspector_http_handler import InspectorHttpHandler
from hgraph.debug._inspector_publish import process_tick, process_graph
from hgraph.debug._inspector_state import InspectorState


@sink_node
def inspector(port: int = 8080, publish_interval: float = 2.5, start: TS[bool] = True, _state: STATE[InspectorState] = None):
    ...


@inspector.start
def start_inspector(port: int, publish_interval: float, start: TS[bool], _state: STATE[InspectorState]):
    from perspective import Table

    from hgraph.adaptors.tornado._tornado_web import TornadoWeb
    from hgraph.adaptors.perspective._perspective import IndexPageHandler
    from hgraph.adaptors.perspective import PerspectiveTablesManager
    from hgraph.debug._inspector_observer import InspectionObserver

    _state.requests.evaluation_clock = start.owning_graph.evaluation_clock

    _state.observer = InspectionObserver(
        start.owning_graph,
        callback_node=lambda n: process_tick(_state._value, n),
        callback_graph=lambda n: process_graph(_state._value, n, publish_interval)
    )
    _state.observer.on_before_node_evaluation(start.owning_node)
    start.owning_graph.evaluation_engine.add_life_cycle_observer(_state.observer)
    _state.observer.subscribe_graph(())

    _state.manager = PerspectiveTablesManager.current()

    _state.table = Table(
        {
            "X": str,
            "name": str,
            "type": str,
            "value": str,
            "timestamp": datetime,
            "evals": int,
            "time": float,
            "of_graph": float,
            "of_total": float,
            "id": str,
            "ord": str,
        }, index="id")

    _state.manager.add_table("inspector", _state.table)

    _state.total_cycle_table = Table({
        "time": datetime,
        "evaluation_time": datetime,
        "cycles": float,
        "graph_time": float,
        "graph_load": float,
        "avg_lag": float,
        "max_lag": float,
        "inspection_time": float
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
                r"/inspect(?:/([^/]*))?(?:/(.*))?",
                InspectorHttpHandler,
                {
                    "queue": _state.requests,
                }
            )
        ]
    )

    print(f"Inspector running on http://{gethostname()}:{port}/inspector/view")

    app.start()
