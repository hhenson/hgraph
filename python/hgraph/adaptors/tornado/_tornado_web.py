"""Shared Tornado I/O-loop and web-server ownership."""

from __future__ import annotations

import threading

import tornado.ioloop
import tornado.web

__all__ = ("TornadoWeb", "BaseHandler")


class TornadoWeb:
    """Share one Tornado loop and one server instance per listening port."""

    _instances: dict[int, "TornadoWeb"] = {}
    _instances_lock = threading.Lock()
    _loop_lock = threading.Lock()
    _loop: tornado.ioloop.IOLoop | None = None
    _loop_thread: threading.Thread | None = None
    _loop_users = 0

    @classmethod
    def instance(cls, port: int = 80) -> "TornadoWeb":
        with cls._instances_lock:
            instance = cls._instances.get(port)
            if instance is None:
                instance = cls(port)
                cls._instances[port] = instance
            return instance

    def __init__(self, port: int):
        self._port = port
        self._app = tornado.web.Application(websocket_ping_interval=1)
        self._server = None
        self._users = 0
        self._lock = threading.Lock()

    @classmethod
    def start_loop(cls) -> tornado.ioloop.IOLoop:
        """Acquire the shared loop, starting its daemon thread if necessary."""
        with cls._loop_lock:
            cls._loop_users += 1
            if cls._loop is not None:
                return cls._loop

            ready = threading.Event()

            def run_loop():
                loop = tornado.ioloop.IOLoop()
                with cls._loop_lock:
                    cls._loop = loop
                    ready.set()
                loop.start()
                loop.close(all_fds=True)

            cls._loop_thread = threading.Thread(
                target=run_loop,
                name="hgraph-tornado",
                daemon=True,
            )
            cls._loop_thread.start()

        if not ready.wait(timeout=5.0):
            with cls._loop_lock:
                cls._loop_users -= 1
            raise RuntimeError("Tornado I/O loop did not start")
        return cls.get_loop()

    @classmethod
    def stop_loop(cls) -> None:
        """Release the shared loop and stop it after the final user exits."""
        with cls._loop_lock:
            if cls._loop_users == 0:
                return
            cls._loop_users -= 1
            if cls._loop_users != 0:
                return
            loop = cls._loop
            thread = cls._loop_thread

        if loop is not None:
            loop.add_callback(loop.stop)
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=5.0)
            if thread.is_alive():
                raise RuntimeError("Tornado I/O loop did not stop")

        with cls._loop_lock:
            if cls._loop_thread is thread:
                cls._loop = None
                cls._loop_thread = None

    @classmethod
    def get_loop(cls) -> tornado.ioloop.IOLoop:
        with cls._loop_lock:
            if cls._loop is None:
                raise RuntimeError("Tornado I/O loop is not running")
            return cls._loop

    def start(self) -> None:
        with self._lock:
            self._users += 1
            if self._server is not None:
                return

        loop = self.start_loop()
        ready = threading.Event()
        failure = []

        def listen():
            try:
                self._server = self._app.listen(self._port)
            except BaseException as error:
                failure.append(error)
            finally:
                ready.set()

        loop.add_callback(listen)
        if not ready.wait(timeout=5.0):
            with self._lock:
                self._users -= 1
            self.stop_loop()
            raise RuntimeError(f"Tornado server on port {self._port} did not start")
        if failure:
            with self._lock:
                self._users -= 1
            self.stop_loop()
            raise RuntimeError(f"Tornado server on port {self._port} failed to start") from failure[0]

    def stop(self) -> None:
        with self._lock:
            if self._users == 0:
                return
            self._users -= 1
            if self._users != 0:
                return
            server, self._server = self._server, None

        if server is not None:
            stopped = threading.Event()

            def stop_server():
                try:
                    server.stop()
                finally:
                    stopped.set()

            self.get_loop().add_callback(stop_server)
            try:
                if not stopped.wait(timeout=5.0):
                    raise RuntimeError(f"Tornado server on port {self._port} did not stop")
            finally:
                self.stop_loop()
        else:
            self.stop_loop()

    def add_handler(self, path, handler, options=None) -> None:
        self.add_handlers([(path, handler, options or {})])

    def add_handlers(self, handlers) -> None:
        with self._lock:
            running = self._server is not None
        if not running:
            self._app.add_handlers(".*$", handlers)
            return

        added = threading.Event()
        failure = []

        def add():
            try:
                self._app.add_handlers(".*$", handlers)
            except BaseException as error:
                failure.append(error)
            finally:
                added.set()

        self.get_loop().add_callback(add)
        if not added.wait(timeout=5.0):
            raise RuntimeError(f"Tornado handler registration on port {self._port} timed out")
        if failure:
            raise RuntimeError(f"Tornado handler registration on port {self._port} failed") from failure[0]


class BaseHandler(tornado.web.RequestHandler):
    """Common authentication hook used by HTTP and WebSocket handlers."""

    _auth_callback = None
    _auth_callback_async = None

    @classmethod
    def set_auth_callback(cls, func) -> None:
        cls._auth_callback = func

    @classmethod
    def set_auth_callback_async(cls, func) -> None:
        cls._auth_callback_async = func

    async def prepare(self) -> None:
        if BaseHandler._auth_callback is not None:
            self.current_user = BaseHandler._auth_callback(self.request)
        elif BaseHandler._auth_callback_async is not None:
            self.current_user = await BaseHandler._auth_callback_async(self.request)
        else:
            self.current_user = ("Anonymous", "Anonymous")

        if self.current_user is None:
            self.set_status(401)
            self.finish()
