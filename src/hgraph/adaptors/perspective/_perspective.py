import logging
import os
import sys
import tempfile
import time
import concurrent.futures
from datetime import datetime
from glob import glob
from threading import Thread
from typing import Dict, Optional, Callable, List

import tornado
from perspective import PerspectiveManager, PerspectiveTornadoHandler, Table, View

from hgraph import sink_node, GlobalState, TS

__all__ = ["perspective_web", "PerspectiveTablesManager"]


class PerspectiveTableUpdatesHandler:
    _table: Table
    _view: View
    _updates_table: Table
    _queue: Optional[Callable] = None
    _expected_updates: List[int]

    @property
    def table(self):
        return self._table

    @table.setter
    def table(self, value):
        self._table = value
        self._view = value.view()
        self._updates_table = Table(value.view().to_arrow())
        self._view.on_update(self.on_update, mode="row")
        # self._view.on_remove(self.on_remove)

    @property
    def queue(self):
        return self._queue

    @queue.setter
    def queue(self, value):
        self._queue = value
        self._expected_updates = []

    def on_update(self, x, y):
        if x != 0:
            self._updates_table.replace(y)
            if self._queue:
                self._queue(self._updates_table.view())

            import pyarrow

            print(pyarrow.ipc.RecordBatchStreamReader(y).read_all().__repr__())


class PerspectiveTablesManager:
    def __init__(self):
        self._started = False
        self._tables = {}
        self._updaters = {}

    @classmethod
    def current(cls) -> "PerspectiveTablesManager":
        self = GlobalState.instance().get("perspective_manager")
        if not self:
            self = PerspectiveTablesManager()
            GlobalState.instance()["perspective_manager"] = self

        return self

    def add_table(self, name, table, editable=False):
        self._tables[name] = [table, editable]
        if self._started:
            self._start_table(name)

    def subscribe_table_updates(self, name, cb):
        if (table := self._tables.get(name)) and not table[1]:
            raise ValueError(f"Table '{name}' is not editable")

        updater = self._updaters.setdefault(name, PerspectiveTableUpdatesHandler())
        updater.queue = cb
        if self._started and table:
            updater.table = table[0]

    def start(self):
        self._start_manager()
        self._start_editable_manager()
        self._started = True

    def _start_manager(self):
        self._manager = PerspectiveManager(lock=True)
        for name, (table, editable) in self._tables.items():
            if editable is False:
                self._start_table(name)

    def _start_editable_manager(self):
        self._editable_manager = PerspectiveManager(lock=False)
        for name, (table, editable) in self._tables.items():
            if editable is True:
                self._start_table(name)

    def _manager_for_table(self, name):
        _, editable = self._tables[name]
        return self._manager if not editable else self._editable_manager

    def _start_table(self, name):
        table, editable = self._tables[name]
        self._manager_for_table(name).host_table(name, table)
        if name in self._updaters:
            self._updaters[name].table = self._tables[name][0]

    def update_table(self, name, data, removals=None):
        table = self._tables[name][0]
        self._manager_for_table(name).call_loop(lambda: table.update(data))
        if removals:
            self._manager_for_table(name).call_loop(lambda: table.remove(removals))
        # table.update(data)

    def get_table_names(self):
        return list(self._tables.keys())

    def get_table(self, name):
        return self._tables[name][0]

    def tornado_config(self):
        return [
            (r"/websocket_readonly", PerspectiveTornadoHandler, {"manager": self._manager, "check_origin": True}),
            (
                r"/websocket_editable",
                PerspectiveTornadoHandler,
                {"manager": self._editable_manager, "check_origin": True},
            ),
        ]

    def set_loop_callback(self, cb, *args):
        self._manager.set_loop_callback(cb, *args)
        self._editable_manager.set_loop_callback(cb, *args)

    def is_table_editable(self, name):
        return self._tables[name][1]


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
    app = GlobalState.instance()["tornado_app"]
    started = False

    def started_cb():
        nonlocal started
        started = True

    if _sig.value:
        thread = Thread(target=_tornado_thread, kwargs=dict(app=app, port=port, cb=started_cb))
        thread.daemon = True
        thread.start()

        thread = Thread(target=_perspective_thread, kwargs=dict(manager=perspective_manager))
        thread.daemon = True
        thread.start()

        while not started:
            time.sleep(0.1)

        logger.info(f"Perspective server started at http://{host}:{port}")
    else:
        # we want the table publishing nodes to work but do not want the web server to run (for testing for ex)
        perspective_manager.set_loop_callback(lambda *args, **kwargs: None)
        logger.info(f"Perspective server not started")


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

    from tornado.log import enable_pretty_logging

    enable_pretty_logging()

    perspective_manager = PerspectiveTablesManager.current()
    perspective_manager.start()

    tempfile.gettempdir()
    layouts_dir = layouts_path or os.path.join(tempfile.tempdir, "psp_layouts")

    app = tornado.web.Application(
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
                {"path": os.path.join(sys.prefix, "node_modules")},
            ),
        ]
        + perspective_manager.tornado_config()
        + ([(k, tornado.web.StaticFileHandler, v) for k, v in static.items()] if static else [])
        + (
            [
                (
                    r"/(.*)",
                    IndexPageHandler,
                    {
                        "mgr": perspective_manager,
                        "layouts_path": layouts_dir,
                        "index_template": index_template,
                        "host": host,
                        "port": port,
                    },
                )
            ]
            if index_template
            else []
        ),
        websocket_ping_interval=1,
    )

    GlobalState.instance()["tornado_app"] = app


def _tornado_thread(app, port, cb):
    app.listen(port)
    loop = tornado.ioloop.IOLoop.current()
    cb()
    loop.start()


def _perspective_thread(manager):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        loop = tornado.ioloop.IOLoop()
        manager.set_loop_callback(loop.run_in_executor, executor)
        loop.start()


class TablePageHandler(tornado.web.RequestHandler):
    def initialize(self, mgr: PerspectiveTablesManager, template: str, host: str, port: int):
        self.mgr = mgr
        self.template = template
        self.host = host
        self.port = port

    def get(self, table_name):
        tornado.log.app_log.info("requesting table", table_name)
        if table_name in self.mgr.get_table_names():
            self.render(
                self.template,
                table_name=table_name,
                table=self.mgr.get_table(table_name),
                editable=self.mgr.is_table_editable(table_name),
                host=self.host,
                port=self.port,
            )
        else:
            self.set_status(404)
            self.write("Table not found")


class IndexPageHandler(tornado.web.RequestHandler):
    def initialize(self, mgr: PerspectiveManager, layouts_path: str, index_template: str, host: str, port: int):
        self.index_template = index_template
        self.layouts_path = layouts_path
        self.mgr = mgr
        self.host = host
        self.port = port

    def get(self, url):
        tornado.log.app_log.info(f"requesting url {url} for template {self.index_template}")
        if url == "" or True:
            layouts = glob(os.path.join(self.layouts_path, f"{url or '*'}.json"))
            layouts = [os.path.basename(f).split(".json")[0] for f in layouts]

            if url:
                versions = glob(os.path.join(self.layouts_path, f"{url}.*.version"))
                versions = sorted([os.path.basename(f).split(".")[1] for f in versions], reverse=True)
            else:
                versions = []

            self.render(self.index_template, url=url, **self.__dict__, layouts=layouts, versions=versions)
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
                        tornado.log.app_log.info("loaded layout", url, "returning", json)
                        self.finish(json)
                        return

                layout_file = os.path.join(self.path, f"{url}.json")
                with open(layout_file, "r") as f:
                    json = f.read().encode("utf-8")
                    tornado.log.app_log.info("loaded layout", url, "from", layout_file, "returning", json)
                    self.finish(json)
            except:
                files = glob(os.path.join(self.path, f"{url}.*.version"))
                if files:
                    files.sort()
                    with open(files[-1], "r") as f:
                        json = f.read().encode("utf-8")
                        tornado.log.app_log.info("loaded layout", url, "from", files[-1], "returning", json)
                        self.finish(json)
                else:
                    self.finish("{}")
                    tornado.log.app_log.warn(
                        "failed to open file", os.path.join(self.path, url), "and no found no versions, returning {}"
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
