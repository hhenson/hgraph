import asyncio
import concurrent.futures
import json
import logging
import operator
import os
import tempfile
import time
from collections import defaultdict
from datetime import datetime
from functools import reduce
from glob import glob
from pathlib import Path
from threading import Thread
from typing import Dict, Optional, Callable, List, Awaitable

import pyarrow
import tornado
import perspective

if perspective.__version__ == "2.10.1":
    from perspective import PerspectiveManager, Table, View
    from perspective import PerspectiveTornadoHandler
    table = Table
    psp_new_api = False
else:
    from perspective import Server, table, Table, View
    from perspective.handlers.tornado import PerspectiveTornadoHandler
    psp_new_api = True

from hgraph import sink_node, GlobalState, TS, STATE

__all__ = ["perspective_web", "PerspectiveTablesManager"]

from hgraph.adaptors.tornado._tornado_web import TornadoWeb


logger = logging.getLogger(__name__)


class PerspectiveTableUpdatesHandler:
    _table: Table
    _view: View
    _updates_table: Table
    _queue: Optional[Callable]

    def __init__(self, allow_self_updates: bool):
        self._allow_self_updates = allow_self_updates

    @property
    def table(self):
        return self._table

    @table.setter
    def table(self, value):
        self._table = value
        self._view = value.view()
        self._updates_table = table(value.view().to_arrow())
        self._view.on_update(self.on_update, mode="row")
        # self._view.on_remove(self.on_remove)

    @property
    def queue(self):
        return self._queue

    @queue.setter
    def queue(self, value):
        self._queue = value

    def on_update(self, x, y):
        if (x != 0) ^ self._allow_self_updates:
            self._updates_table.replace(y)
            if self._queue:
                self._queue(self._updates_table.view())

            # import pyarrow
            #
            # print(pyarrow.ipc.RecordBatchStreamReader(y).read_all().__repr__())


class PerspectiveTablesManager:
    def __init__(self, host_server_tables=True, table_config_file=(), **kwargs):
        self._host_server_tables = host_server_tables
        self._table_config_files = table_config_file if isinstance(table_config_file, (list, tuple)) else [table_config_file]
        self._started = False
        self._tables = {}
        self._updaters = defaultdict(set)

        self.options = kwargs

        if psp_new_api:
            self._server = perspective.GLOBAL_SERVER
            self._client = perspective.GLOBAL_CLIENT
            n = {"name": "index"}
        else:
            n = {}

        self._index_table = table({
            "name": str,
            "type": str,  # 'table', 'client_table', 'view', 'join'
            "editable": bool,
            "url": str,
            "schema": str,
            "index": str,
            "description": str,
        }, index="name", **n)

        self._tables["index"] = [self._index_table, False]

        for c in self._table_config_files:
            with open(c, "r") as f:
                json.load(f)  # check if it is valid json

    def is_new_api(self):
        return psp_new_api

    @classmethod
    def set_current(cls, self):
        assert GlobalState.instance().get("perspective_manager") is None
        GlobalState.instance()["perspective_manager"] = self

    @classmethod
    def current(cls) -> "PerspectiveTablesManager":
        self = GlobalState.instance().get("perspective_manager")
        if not self:
            self = PerspectiveTablesManager()
            GlobalState.instance()["perspective_manager"] = self

        return self

    @property
    def server_tables(self):
        return self._host_server_tables

    def create_table(self, *args, name, editable=False, user=True, **kwargs):
        tbl = table(*args, **kwargs, **({"name": name} if psp_new_api else {}))
        self.add_table(name, tbl, editable=editable, user=user)
        return tbl

    def add_table(self, name, table, editable=False, user=True):
        self._tables[name] = [table, editable]

        if user:
            self._index_table.update([{
                "name": name,
                "type": "table" if self.server_tables else "client_table",
                "editable": editable,
                "url": f"",
                "schema": json.dumps({k: v if type(v) is str else v.__name__
                                      for k, v in table.schema().items()}),
                "index": table.get_index(),
                "description": ""
            }])

        if self._started:
            self._start_table(name)

        if table.get_index() and not self.server_tables:
            # client tables need removes sent separately (because bug in perspective)
            self.create_table({"i": table.schema()[table.get_index()]}, name=name + "_removes", user=False)

    def add_join(self, name, schema, index, description):
        self._index_table.update([{
            "name": name,
            "type": "join",
            "editable": False,
            "url": f"",
            "schema": json.dumps({k: v.__name__ for k, v in schema.items()}),
            "index": index,
            "description": json.dumps(description)
        }])

    def subscribe_table_updates(self, name, cb, self_updates: bool = False):
        if (table := self._tables.get(name)) and not table[1]:
            raise ValueError(f"Table '{name}' is not editable")

        updater = PerspectiveTableUpdatesHandler(self_updates)
        self._updaters[name].add(updater)
        updater.queue = cb
        if self._started and table:
            updater.table = table[0]

        return updater

    def unsubscribe_table_updates(self, name, updater):
        self._updaters[name].remove(updater)

    def start(self):
        self._start_manager()
        self._started = True


    def _start_manager(self):
        if not psp_new_api:
            self._manager = PerspectiveManager(lock=True)
            self._editable_manager = PerspectiveManager(lock=False)

        for name in self._tables:
            self._start_table(name)


    def _manager_for_table(self, name):
        if psp_new_api:
            return self._client
        else:
            _, editable = self._tables[name]
            return self._manager if not editable else self._editable_manager

    def _start_table(self, name):
        if not psp_new_api:
            table, editable = self._tables[name]
            self._manager_for_table(name).host_table(name, table)

        if name in self._updaters:
            for u in self._updaters[name]:
                u.table = self._tables[name][0]

    @staticmethod
    def _table_update(table, update, data):
        try:
            table.update(update)
        except Exception as e:
            print(f"Error updating table {table} with {data}: {e}")
            raise

    def update_table(self, name, data, removals=None):
        table = self._tables[name][0]

        if data:
            if isinstance(data, list):
                if self.is_new_api():
                    prev = {}
                    batches = []
                    value_count = []
                    for i, r in enumerate(data):
                        if r.keys() != prev.keys():
                            batches.append({k: [] for k in r.keys()})
                            value_count.append({k: 0 for k in r.keys()})
                            prev = r
                        for k, v in r.items():
                            batches[-1][k].append(v)
                            value_count[-1][k] += 0 if v is None else 1

                    for b, c in zip(batches, value_count):
                        for k, cv in c.items():
                            if cv == 0:
                                b.pop(k)

                    data = batches
                else:
                    d0 = {}
                    d1 = defaultdict(lambda: [None] * len(data))
                    for i, r in enumerate(data):
                        for k, v in r.items():
                            if v is not None:
                                d1[k][i] = v
                                d0[k] = True
                    data = {k: v for k, v in d1.items() if k in d0}

            if not isinstance(data, list):
                data = [data]

            for d in data:
                try:
                    batch = pyarrow.record_batch(d)
                except Exception as e:
                    logger.error(f"Error creating record batch for {d}: {e}")
                    continue

                stream = pyarrow.BufferOutputStream()

                with pyarrow.ipc.new_stream(stream, batch.schema) as writer:
                    writer.write_batch(batch)

                arrow = stream.getvalue().to_pybytes()

                if psp_new_api:
                    self._callback(lambda: PerspectiveTablesManager._table_update(table, arrow, data))
                else:
                    self._manager_for_table(name).call_loop(lambda: PerspectiveTablesManager._table_update(table, arrow, data))

        if removals:
            if table.get_index() and not self.server_tables:
                self.update_table(name + "_removes", {"i": list(removals)},
                                  removals=reduce(operator.add, (d[table.get_index()] for d in data)) if data else [])

            if table.get_index():
                if psp_new_api:
                    self._callback(lambda: table.remove(list(removals)))
                else:
                    self._manager_for_table(name).call_loop(lambda: table.remove(removals))

        # table.update(data)

    def get_table_names(self):
        return list(self._tables.keys())

    def get_table(self, name):
        return self._tables[name][0]

    def get_table_config_files(self):
        return self._table_config_files

    def read_table_config(self):
        config = {}
        for file in self._table_config_files:
            with open(file, "r") as f:
                config |= json.load(f)
        return config

    def tornado_config(self):
        if psp_new_api:
            return [
                (
                    r"/websocket",
                    PerspectiveTornadoHandlerWithLogNewApi,
                    {"perspective_server": self._server, "manager": self},
                ),
            ]
        else:
            return [
                (
                    r"/websocket_readonly",
                    PerspectiveTornadoHandlerWithLog,
                    {"manager": self._manager, "check_origin": True},
                ),
                (
                    r"/websocket_editable",
                    PerspectiveTornadoHandlerWithLog,
                    {"manager": self._editable_manager, "check_origin": True},
                ),
            ]

    def set_loop_callback(self, cb, *args):
        if psp_new_api:
            self._client.set_loop_callback(cb, *args)
        else:
            self._manager.set_loop_callback(cb, *args)
            self._editable_manager.set_loop_callback(cb, *args)

        self._callback = lambda *margs, **kwargs: cb(*args, *margs, **kwargs)

        if self._started:
            self._publish_heartbeat_table()

    def get_loop_callback(self):
        return self._callback

    def is_table_editable(self, name):
        return self._tables[name][1]

    async def _publish_heartbeat(self):
        counter = 0
        while True:
            self.update_table("heartbeat", [{"name": "heartbeat", "time": datetime.utcnow(), "sequence": counter}])
            counter += 1
            await asyncio.sleep(15)

    def _publish_heartbeat_table(self):
        self.create_table({"name": str, "time": datetime, "sequence": int}, index="name", name="heartbeat")
        self._callback(self._publish_heartbeat)


class PerspectiveTornadoHandlerWithLog(PerspectiveTornadoHandler):
    def _log_websocket_event(self, event: str) -> None:
        logger.info(f"Websocket from {self.request.remote_ip} to {self.request.path} "
                    f"with id {self.request.headers['Sec-Websocket-Key']}: {event}")

    def open(self, *args: str, **kwargs: str) -> Optional[Awaitable[None]]:
        self._log_websocket_event("opened")
        return super().open(*args, **kwargs)

    def on_close(self) -> None:
        self._log_websocket_event(f"closed with {self.close_code}: {self.close_reason}")
        super().on_close()

    def get_compression_options(self) -> Dict[str, str]:
        return {"compression_level": 9, "mem_level": 9}


class PerspectiveTornadoHandlerWithLogNewApi(PerspectiveTornadoHandler):
    def _log_websocket_event(self, event: str) -> None:
        logger.info(f"Websocket from {self.request.remote_ip} to {self.request.path} "
                    f"with id {self.request.headers['Sec-Websocket-Key']}: {event}")

    def initialize(self, perspective_server=None, manager=None):
        self.server = perspective_server or perspective.GLOBAL_SERVER
        self.callback = manager.get_loop_callback()
        self.tornado_loop = TornadoWeb.get_loop()

    def open(self, *args: str, **kwargs: str) -> Optional[Awaitable[None]]:
        self._log_websocket_event("opened")

        def inner(msg):
            self.tornado_loop.add_callback(lambda: self.write_message(msg, binary=True))

        self.session = self.server.new_session(inner)

    def on_close(self) -> None:
        self._log_websocket_event(f"closed with {self.close_code}: {self.close_reason}")
        self.session.close()
        del self.session

    def on_message(self, msg: bytes):
        if not isinstance(msg, bytes):
            return

        self.callback(self.session.handle_request, msg)
        self.callback(self.session.poll)


@sink_node
def perspective_web(
    host: str,
    port: int,
    static: Dict[str, str] = None,
    table_template: str = "table_template.html",
    index_template: str = "index_template.html",
    workspace_template: str = "workspace_template.html",
    layouts_path: str = None,
    _sig: TS[bool] = True,
    logger: logging.Logger = None,
):
    perspective_manager = PerspectiveTablesManager.current()
    app = GlobalState.instance()["perspective_tornado_web"]

    if _sig.value:
        app.start()

        started = False

        def started_cb():
            nonlocal started
            started = True

        thread = Thread(target=_perspective_thread, kwargs=dict(manager=perspective_manager, cb=started_cb))
        thread.daemon = True
        thread.start()

        while not started:
            time.sleep(0.1)

        logger.info(f"Perspective server started at http://{host}:{port}")
    else:
        # we want the table publishing nodes to work but do not want the web server to run (for testing for ex)
        perspective_manager.set_loop_callback(lambda *args, **kwargs: None)
        logger.info(f"Perspective server not started")


def _get_node_location():
    """Assuming node is installed this will retrieve the global local for npm modules"""
    import subprocess

    result = subprocess.run(["npm", "root", "-g", "for", "npm"], shell=os.name == 'nt', capture_output=True, text=True)
    node_path = result.stdout.rstrip()
    print(f"NPM found at '{node_path}'")
    return node_path


class NoCache_StaticFileHandler(tornado.web.StaticFileHandler):
    def set_extra_headers(self, path):
        self.set_header("Cache-Control", "no-cache")


@perspective_web.start
def perspective_web_start(
    host: str,
    port: int,
    static: Dict[str, Dict[str, str]],
    table_template: str = "table_template.html",
    index_template: str = "index_template.html",
    workspace_template: str = "workspace_template.html",
    layouts_path: str = None,
):

    if "/" not in table_template:
        table_template = os.path.join(os.path.dirname(__file__), table_template)
    if "/" not in index_template:
        index_template = os.path.join(os.path.dirname(__file__), index_template)
    if "/" not in workspace_template:
        workspace_template = os.path.join(os.path.dirname(__file__), workspace_template)

    perspective_manager = PerspectiveTablesManager.current()
    perspective_manager.start()

    tempfile.gettempdir()
    layouts_dir = layouts_path or os.path.join(tempfile.tempdir, "psp_layouts")

    app = TornadoWeb.instance(port)
    app.add_handlers(
        [
            (
                r"/table/(.*)",
                TablePageHandler,
                {"mgr": perspective_manager, "template": table_template, "host": host, "port": port},
            ),
            (
                r"/workspace/(.*)",
                IndexPageHandler,
                {
                    "mgr": perspective_manager,
                    "layouts_path": layouts_dir,
                    "index_template": workspace_template,
                    "host": host,
                    "port": port,
                },
            ),
            (r"/layout/(.*)", WorkspacePageHandler, {"path": layouts_dir}),
            (
                r"/node_modules/(.*)",
                tornado.web.StaticFileHandler,
                {"path": Path(_get_node_location())},
            ),
            (
                r"/workspace_code/(.*)",
                NoCache_StaticFileHandler,
                {"path": os.path.dirname(__file__)},
            ),
        ]
        + perspective_manager.tornado_config()
        + ([(k, tornado.web.StaticFileHandler, v) for k, v in static.items()] if static else [])
        + (
            [(
                r"/",
                IndexPageHandler,
                {
                    "mgr": perspective_manager,
                    "layouts_path": layouts_dir,
                    "index_template": index_template,
                    "host": host,
                    "port": port,
                },
            ),(
                r"/versions/(.*)",
                IndexPageHandler,
                {
                    "mgr": perspective_manager,
                    "layouts_path": layouts_dir,
                    "index_template": index_template,
                    "host": host,
                    "port": port,
                },
            )]
            if index_template
            else []
        ),
    )

    GlobalState.instance()["perspective_tornado_web"] = app


def _perspective_thread(manager, cb):
    if manager.server_tables:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            loop = tornado.ioloop.IOLoop()
            manager.set_loop_callback(loop.run_in_executor, executor)
            cb()
            loop.start()
    else:
        loop = tornado.ioloop.IOLoop()
        manager.set_loop_callback(loop.add_callback)
        cb()
        loop.start()


class TablePageHandler(tornado.web.RequestHandler):
    def initialize(self, mgr: PerspectiveTablesManager, template: str, host: str, port: int):
        self.mgr = mgr
        self.template = template
        self.host = host
        self.port = port

    def get(self, table_name):
        tornado.log.app_log.info(f"requesting table {table_name}")
        if table_name in self.mgr.get_table_names():
            self.render(
                self.template,
                table_name=table_name,
                is_new_api=self.mgr.is_new_api(),
                table=self.mgr.get_table(table_name),
                editable=self.mgr.is_table_editable(table_name),
                host=self.host,
                port=self.port,
            )
        else:
            self.set_status(404)
            self.write("Table not found")


class IndexPageHandler(tornado.web.RequestHandler):
    def initialize(self, mgr: PerspectiveTablesManager, layouts_path: str, index_template: str, host: str, port: int):
        self.index_template = index_template
        self.layouts_path = layouts_path
        self.mgr = mgr
        self.host = host
        self.port = port

    def get(self, url = ""):
        tornado.log.app_log.info(f"requesting url {url} for template {self.index_template}")
        if url == "" or True:
            layouts = glob(os.path.join(self.layouts_path, f"{url or '*'}.json"))
            layouts.sort()
            layouts = [os.path.basename(f).split(".json")[0] for f in layouts]

            if url:
                versions = glob(os.path.join(self.layouts_path, f"{url}.*.version"))
                sizes = [os.path.getsize(f) for f in versions]
                vs = [(os.path.basename(f).split(".")[1], f"{s/1024:0.2}k") for f, s in zip(versions, sizes)]
                version_size = sorted(vs, key=lambda x: x[0], reverse=True)
            else:
                version_size = []

            self.render(self.index_template, url=url, **self.__dict__, layouts=layouts, versions=version_size)
        else:
            self.set_status(404)
            self.write("not found")


class WorkspacePageHandler(tornado.web.RequestHandler):
    def initialize(self, path: str):
        self.path = path

    def get(self, url):
        tornado.log.app_log.info(f"requesting workspace {url}")
        if url != "":
            try:
                if "." in url and os.path.isfile(v_path := os.path.join(self.path, f"{url}.version")):
                    with open(v_path, "r") as f:
                        json = f.read().encode("utf-8")
                        tornado.log.app_log.info(f"loaded layout {url} returning {json}")
                        self.finish(json)
                        return

                layout_file = os.path.join(self.path, f"{url}.json")
                with open(layout_file, "r") as f:
                    json = f.read().encode("utf-8")
                    tornado.log.app_log.info(f"loaded layout {url} from {layout_file} returned {json}")
                    self.finish(json)
            except:
                files = glob(os.path.join(self.path, f"{url}.*.version"))
                if files:
                    files.sort()
                    with open(files[-1], "r") as f:
                        json = f.read().encode("utf-8")
                        tornado.log.app_log.info(f"loaded layout {url} from {files[-1]} returning {json}")
                        self.finish(json)
                else:
                    self.finish("{}")
                    tornado.log.app_log.warn(
                        f"failed to open file {os.path.join(self.path, url)} and no found no versions, returning {{}}"
                    )
        else:
            self.set_status(404)
            self.finish("not found")

    def post(self, url):
        json = self.request.body.decode("utf-8")

        layout_path = os.path.join(self.path, f"{url}.json")
        layout_path_hist = os.path.join(
            self.path, f"{url}.{datetime.now().isoformat(timespec='seconds').replace(':', '-')}.version"
        )

        if os.path.isfile(layout_path):
            with open(layout_path, "r") as f:
                if f.read() == json:
                    self.finish("ok")
                    return

        tornado.log.app_log.info(f"posting workspace {url} at {layout_path} with {json}")
        with open(layout_path, "w") as f:
            f.write(json)
        with open(layout_path_hist, "w") as f:
            f.write(json)
        self.finish("ok")

    def delete(self, url):
        layout_path = os.path.join(self.path, f"{url}.json")
        layout_path_hist = os.path.join(
            self.path, f"{url}.{datetime.now().isoformat(timespec='seconds').replace(':', '-')}.version"
        )

        if os.path.isfile(layout_path):
            os.rename(layout_path, layout_path_hist)
            self.finish("ok")
        else:
            self.set_status(404)
            self.finish("not found")
