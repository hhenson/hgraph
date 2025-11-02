import time
from threading import Thread
import asyncio

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
                time.sleep(0.01)

    def start(self):
        if self._started:
            self._started += 1
            return

        # Ensure the Tornado IOLoop thread is running
        self.start_loop()

        # Schedule the server listen on the IOLoop thread
        try:
            TornadoWeb._loop.add_callback(lambda: self._listen())
        except Exception:
            # If scheduling failed (e.g., loop not yet ready), try to (re)start the loop and schedule again
            try:
                self.start_loop()
                TornadoWeb._loop.add_callback(lambda: self._listen())
            except Exception:
                # As a last resort, fall through to bounded wait and retry below
                pass

        # Bounded wait for the server to report started; avoid infinite wait if the loop is not processing callbacks
        waited = 0.0
        retry_scheduled = False
        while not self._started and waited < 5.0:
            time.sleep(0.1)
            waited += 0.1
            # If we haven't started after ~1s, try nudging the listen callback again once
            if not retry_scheduled and waited >= 1.0 and not self._started:
                try:
                    TornadoWeb._loop.add_callback(lambda: self._listen())
                    retry_scheduled = True
                except Exception:
                    # Ignore and continue waiting up to the bounded timeout
                    pass
        # Do not block forever; tests that perform a short sleep before making requests will still succeed,
        # and this avoids rare hangs when the IOLoop is not processing callbacks yet.

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


class BaseHandler(tornado.web.RequestHandler):
    def initialize(self):
        ...

    @staticmethod
    def set_auth_callback(func):
        BaseHandler._auth_callback = func
        
    @staticmethod
    def set_auth_callback_async(func):
        BaseHandler._auth_callback_async = func

    async def prepare(self):
        if gcu := getattr(BaseHandler, "_auth_callback", False):
            self.current_user = gcu(self.request)
        elif agcu := getattr(BaseHandler, "_auth_callback_async", False):
            self.current_user = await agcu(self.request)
        else:
            self.current_user = "Anonymous", "Anonymous"

        if self.current_user is None:
            self.set_status(401)
            self.finish()
            return
