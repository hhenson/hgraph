import time
from threading import Thread

import tornado
from tornado.web import Application


class TornadoWeb:
    """Tornado web server. This class allows for a shared instances of the web server across multiple adaptors"""

    _instances: dict[int, "TornadoWeb"] = {}
    _started = 0
    _loop = None

    @classmethod
    def instance(cls, port: int = 80) -> "TornadoWeb":
        if port not in cls._instances:
            cls._instances[port] = TornadoWeb(port)

        return cls._instances[port]

    def __init__(self, port):
        self._port = port
        self._app = Application(websocket_ping_interval=1, debug=True)
        self._started = 0

    @classmethod
    def start_loop(cls):
        if cls._started == 0:
            from tornado.log import enable_pretty_logging

            enable_pretty_logging()

            def started_cb():
                cls._started += 1

            thread = Thread(target=_tornado_thread, kwargs=dict(cb=started_cb))
            thread.daemon = True
            thread.start()

            while not cls._started:
                time.sleep(0.1)

    def start(self):
        if self._started:
            self._started += 1
            return

        self.start_loop()

        TornadoWeb._loop.add_callback(lambda: self._listen())

        while not self._started:
            time.sleep(0.1)

    def _listen(self):
        self._server = self._app.listen(self._port)
        self._started += 1

    def stop(self):
        self._started -= 1
        if self._started == 0:
            self._server.stop()

    @classmethod
    def stop_loop(cls):
        cls._started -= 1
        if cls._started == 0:
            ioloop = TornadoWeb._loop
            ioloop.add_callback(ioloop.stop)

    def add_handler(self, path, handler, options):
        self._app.add_handlers(".*$", [(path, handler, options)])

    def add_handlers(self, handlers):
        self._app.add_handlers(".*$", handlers)

    @classmethod
    def get_loop(cls):
        return cls._loop


def _tornado_thread(cb):
    TornadoWeb._loop = tornado.ioloop.IOLoop()
    cb()
    TornadoWeb._loop.start()
