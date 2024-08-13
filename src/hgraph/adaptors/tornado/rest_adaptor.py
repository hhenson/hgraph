import asyncio
import concurrent.futures
from dataclasses import dataclass
from typing import Callable

from hgraph import (
    CompoundScalar,
    graph,
    push_queue,
    TSD,
    TS,
    GlobalState,
    sink_node,
    STATE,
    partition,
    map_,
    unpartition,
    adaptor,
    combine,
    adaptor_impl,
    merge,
    register_adaptor,
    register_service,
)
import tornado
from hgraph.adaptors.tornado._tordano_web import TornadoWeb


@dataclass(frozen=True)
class RestRequest(CompoundScalar):
    url: str
    url_parsed_args: tuple[str]
    headers: dict[str, str]


@dataclass(frozen=True)
class RestResponse(CompoundScalar):
    status_code: int
    body: str
    headers: dict[str, str] = None


@dataclass(frozen=True)
class RestGetRequest(RestRequest):
    pass


@dataclass(frozen=True)
class RestPostRequest(RestRequest):
    body: str


class RestAdaptorManager:
    handlers: dict[str, Callable | str]

    def __init__(self):
        self.handlers = {}
        self.requests = {}

    @classmethod
    def instance(cls):
        if not hasattr(cls, "_instance"):
            cls._instance = cls()
        return cls._instance

    def set_queue(self, queue):
        self.queue = queue

    def start(self, port):
        self.tornado_web = TornadoWeb.instance(port)

        for path in self.handlers.keys():
            self.tornado_web.add_handler(path, RestHandler, {"path": path, "mgr": self})

        self.tornado_web.start()

    def stop(self):
        self.tornado_web.stop()

    def add_handler(self, path, handler):
        self.handlers[path] = handler

    def add_request(self, request_id, request):
        try:
            future = asyncio.Future()
        except Exception as e:
            print(f"Error creating future: {e}")
            raise e
        self.requests[request_id] = future
        self.queue({request_id: request})
        return future

    def complete_request(self, request_id, response):
        self.requests[request_id].set_result(response)
        print(f"Completed request {request_id} with response {response}")


class RestHandler(tornado.web.RequestHandler):
    def initialize(self, path, mgr):
        self.path = path
        self.mgr = mgr

    async def get(self, *args):
        request_obj = object()
        request_id = id(request_obj)

        response = await self.mgr.add_request(
            request_id, RestGetRequest(url=self.path, url_parsed_args=args, headers=self.request.headers)
        )

        self.set_status(response.status_code)

        if response.headers:
            for k, v in response.headers.items():
                self.set_header(k, v)

        await self.finish(response.body)

    async def post(self, *args):
        request_obj = object()
        request_id = id(request_obj)

        response = await self.mgr.add_request(
            request_id,
            RestPostRequest(url=self.path, url_parsed_args=args, headers=self.request.headers, body=self.request.body),
        )

        self.set_status(response.status_code)
        for k, v in response.headers.items():
            self.set_header(k, v)
        await self.finish(response.body)


def rest_handler(fn: Callable = None, *, url: str):
    if fn is None:
        return lambda fn: rest_handler(fn, url=url)

    from hgraph import WiringNodeClass

    if not isinstance(fn, WiringNodeClass):
        fn = graph(fn)

    assert "request" in fn.signature.time_series_inputs.keys(), "Rest graph must have an input named 'request'"
    assert all(
        k in fn.signature.defaults for k in fn.signature.time_series_inputs.keys() if k != "request"
    ), "Rest graph must have default values for all inputs except 'request'"
    assert fn.signature.time_series_inputs["request"].matches_type(TS[RestRequest]) or fn.signature.time_series_inputs[
        "request"
    ].matches_type(
        TSD[int, TS[RestRequest]]
    ), "Rest graph must have a single input named 'request' of type TS[RestRequest] or TSD[int, TS[RestRequest]]"
    assert fn.signature.output_type.matches_type(TS[RestResponse]) or fn.signature.output_type.matches_type(
        TSD[int, TS[RestResponse]]
    ), "Rest graph must have a single output of type TS[RestResponse] or TSD[int, TS[RestResponse]]"

    mgr = RestAdaptorManager.instance()
    mgr.add_handler(url, fn)

    return fn


@adaptor
def rest_adaptor(response: TSD[int, TS[RestResponse]], path: str) -> TSD[int, TS[RestRequest]]: ...


@adaptor_impl(interfaces=(rest_adaptor, rest_adaptor))
def rest_adaptor_helper(path: str, port: int):
    register_service("rest_adaptor", rest_adaptor_impl, port=port)


@adaptor_impl(interfaces=())
def rest_adaptor_impl(path: str, port: int):
    from hgraph import WiringNodeClass
    from hgraph import WiringGraphContext

    @push_queue(TSD[int, TS[RestGetRequest]])
    def from_web(sender, path: str = "tornado_rest_adaptor") -> TSD[int, TS[RestGetRequest]]:
        GlobalState.instance()[f"rest_adaptor://{path}/queue"] = sender
        return None

    @sink_node
    def to_web(
        responses: TSD[int, TS[RestResponse]], port: int, path: str = "tornado_rest_adaptor", _state: STATE = None
    ):
        for response_id, response in responses.modified_items():
            TornadoWeb.get_loop().add_callback(_state.mgr.complete_request, response_id, response.value)

    @to_web.start
    def to_web_start(port: int, path: str, _state: STATE):
        _state.mgr = RestAdaptorManager.instance()
        _state.mgr.set_queue(queue=GlobalState.instance()[f"rest_adaptor://{path}/queue"])
        _state.mgr.start(port)

    @to_web.stop
    def to_web_stop(_state: STATE):
        _state.mgr.tornado_web.stop()

    requests = from_web()
    requests_by_url = partition(requests, requests.url)

    responses = {}
    for url, handler in RestAdaptorManager.instance().handlers.items():
        if isinstance(handler, WiringNodeClass):
            if handler.signature.time_series_inputs["request"].matches_type(TS[RestGetRequest]):
                responses[url] = map_(handler, request=requests_by_url[url])
            elif handler.signature.time_series_inputs["request"].matches_type(TSD[int, TS[RestGetRequest]]):
                responses[url] = handler(request=requests_by_url[url])
        else:
            raise ValueError(f"Invalid REST handler type for the rest adaptor: {handler}")

    adaptors_dedup = set()
    adaptors = set()
    for path, type_map, node, receive in WiringGraphContext.__stack__[0].registered_service_clients(rest_adaptor):
        assert type_map == {}, "Rest adaptor does not support type generics"
        if (path, receive) in adaptors_dedup:
            raise ValueError(f"Duplicate rest adaptor client for path {path}: only one client is allowed")
        adaptors_dedup.add((path, receive))
        adaptors.add(path.replace("/from_graph", "").replace("/to_graph", ""))

    for path in adaptors:
        url = rest_adaptor.path_from_full_path(path)

        mgr = RestAdaptorManager.instance()
        mgr.add_handler(url, None)

        responses[url] = rest_adaptor.wire_impl_inputs_stub(path).response
        rest_adaptor.wire_impl_out_stub(path, requests_by_url[url])

    to_web(merge(*responses.values()), port)
