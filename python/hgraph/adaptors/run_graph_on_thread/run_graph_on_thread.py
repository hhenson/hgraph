import inspect
import logging
import threading
import traceback
import types
from dataclasses import dataclass
from datetime import date, datetime, time

from hgraph import (
    OUT,
    REMOVE,
    TIME_SERIES_TYPE,
    CompoundScalar,
    EvaluationMode,
    GraphConfiguration,
    GlobalContext,
    GlobalState,
    TS,
    TSB,
    TSD,
    TimeSeriesSchema,
    WiringPort,
    compute_node,
    const,
    push_queue,
    evaluate_graph,
    service_adaptor,
    service_adaptor_impl,
    sink_node,
)

logger = logging.getLogger(__name__)

__all__ = (
    "RunGraphOutput",
    "publish_output",
    "run_graph_on_thread",
    "run_graph_on_thread_impl",
)


_OUTPUT_CALLBACK = ":adaptors:run_graph_on_thread:output"
_RUN_OUTPUT_SCHEMAS = {}


class RunGraphOutput(TimeSeriesSchema):
    """The typed result bundle returned by ``run_graph_on_thread[OUT]``."""

    def __class_getitem__(cls, output_type):
        schema = _RUN_OUTPUT_SCHEMAS.get(output_type)
        if schema is not None:
            return schema
        name = f"RunGraphOutput[{output_type!r}]"
        schema = types.new_class(name, (TimeSeriesSchema,))
        schema.__module__ = __name__
        schema.__annotations__ = {
            "out": output_type,
            "started": TS[bool],
            "finished": TS[bool],
            "status": TS[str],
        }
        _RUN_OUTPUT_SCHEMAS[output_type] = schema
        return schema


@dataclass(frozen=True)
class _RunGraphRequest(CompoundScalar):
    fn: object
    global_state: dict[str, object]
    params: dict[str, object]


class _RawRunGraphOutput(TimeSeriesSchema):
    out: TS[object]
    started: TS[bool]
    finished: TS[bool]
    status: TS[str]


_RAW_OUTPUT = TSB[_RawRunGraphOutput]


@sink_node
def publish_output(
    ts: TIME_SERIES_TYPE,
    delta: bool = True,
    _global_state: GlobalState = None,
):
    """Publish a child graph value to its ``run_graph_on_thread`` client."""
    callback = _global_state.get(_OUTPUT_CALLBACK)
    if callback is None:
        raise RuntimeError("publish_output must run inside run_graph_on_thread")
    callback(ts.delta_value if delta else ts.value)


@compute_node
def _make_request(
    fn: TS[object],
    global_state: TS[dict[str, object]],
    params: TS[dict[str, object]],
) -> TS[_RunGraphRequest]:
    return _RunGraphRequest(fn.value, dict(global_state.value), dict(params.value))


@compute_node
def _typed_output(value: TS[object]) -> OUT:
    return value.value


@service_adaptor
def _run_graph_on_thread(request: TS[_RunGraphRequest], path: str = "thread_graph_runner") -> _RAW_OUTPUT:
    ...


def _as_port(value, output_type):
    if isinstance(value, WiringPort):
        return value
    return const(value, tp=output_type)


class _RunGraphOnThread:
    """Run each client graph on its own worker thread.

    Publications retain their order within a client. Relative timing and tick
    alignment between independently scheduled clients are intentionally not
    defined.
    """

    __name__ = "run_graph_on_thread"

    def __init__(self, output_type=None):
        self.output_type = output_type

    def __getitem__(self, output_type):
        return _RunGraphOnThread(output_type)

    def __call__(
        self,
        fn,
        global_state=None,
        params=None,
        out_=None,
        path="thread_graph_runner",
    ):
        output_type = self.output_type
        if output_type is None:
            output_type = out_
        elif out_ is not None and out_ != output_type:
            raise TypeError(
                "out_ does not match the bracket-specialized output type")
        if output_type is None:
            raise TypeError("run_graph_on_thread must be specialized with an output type")
        request = _make_request(
            _as_port(fn, TS[object]),
            _as_port(
                global_state if global_state is not None else {},
                TS[dict[str, object]],
            ),
            _as_port(
                params if params is not None else {},
                TS[dict[str, object]],
            ),
        )
        raw = _run_graph_on_thread(request, path=path)
        output_schema = RunGraphOutput[output_type]
        return TSB[output_schema].from_ts(
            out=_typed_output[OUT : output_type](raw.out),
            started=raw.started,
            finished=raw.finished,
            status=raw.status,
        )


run_graph_on_thread = _RunGraphOnThread()


class _ResponseState:
    def __init__(self):
        self.lock = threading.Lock()
        self.sender = None
        self.pending = []
        self.generations = {}
        self.published = set()
        self.threads = []
        self.active = True

    def attach(self, sender):
        with self.lock:
            if not self.active:
                return
            self.sender = sender
            pending, self.pending = self.pending, []
        for value in pending:
            sender(value)

    def begin(self, request_id):
        with self.lock:
            generation = self.generations.get(request_id, 0) + 1
            self.generations[request_id] = generation
            return generation

    def add_thread(self, thread):
        with self.lock:
            self.threads.append(thread)

    def publish(self, request_id, generation, delta):
        with self.lock:
            if not self.active or self.generations.get(request_id) != generation:
                return
            self.published.add(request_id)
            value = {request_id: delta}
            if self.sender is None:
                self.pending.append(value)
                return
            sender = self.sender
        sender(value)

    def cancel(self, request_id):
        with self.lock:
            self.generations[request_id] = self.generations.get(request_id, 0) + 1
            if request_id not in self.published:
                return
            self.published.remove(request_id)
            value = {request_id: REMOVE}
            if self.sender is None:
                self.pending.append(value)
                return
            sender = self.sender
        sender(value)

    def close(self):
        with self.lock:
            self.active = False
            self.sender = None
            self.pending.clear()
            threads, self.threads = self.threads, []
        for thread in threads:
            thread.join()


def _graph_parameters(fn, params):
    target = getattr(fn, "fn", fn)
    signature = inspect.signature(target, eval_str=True)
    return {
        name: value
        for name, value in params.items()
        if name in signature.parameters
    }


def _wire_graph_parameters(fn, params):
    from hgraph._types import _TsExpr

    target = getattr(fn, "fn", fn)
    signature = inspect.signature(target, eval_str=True)
    return {
        name: const(value, tp=signature.parameters[name].annotation)
        if isinstance(signature.parameters[name].annotation, _TsExpr)
        else value
        for name, value in _graph_parameters(fn, params).items()
    }


def _date_at_midnight(value):
    return datetime.combine(value, time()) if isinstance(value, date) and not isinstance(value, datetime) else value


def _run_child(request: _RunGraphRequest, publish):
    params = dict(request.params)
    run_mode = params.pop("run_mode", EvaluationMode.SIMULATION)
    # Time bounds configure the child engine, but remain available to child
    # graphs that declare parameters with these names.  This matches the
    # established Python adaptor contract.
    start_time = params.get("start_time")
    end_time = params.get("end_time")
    if start_time is None:
        start_time = _date_at_midnight(params.get("start_date"))
    if end_time is None:
        end_time = _date_at_midnight(params.get("end_date"))
    config_options = {}
    for name in (
        "graph_logger",
        "capture_values",
        "cleanup_on_error",
        "trace_back_depth",
    ):
        value = params.pop(name, None)
        if value is not None:
            config_options[name] = value

    state = GlobalState()
    for key, value in request.global_state.items():
        state[key] = value
    state[_OUTPUT_CALLBACK] = lambda value: publish({"out": value})

    publish({"started": True})
    with GlobalContext(state):
        def child():
            request.fn(**_wire_graph_parameters(request.fn, params))

        evaluate_graph(
            child,
            GraphConfiguration(
                run_mode=run_mode,
                start_time=start_time,
                end_time=end_time,
                **config_options,
            ),
        )
    publish({"finished": True, "status": "OK"})


@service_adaptor_impl(interfaces=_run_graph_on_thread)
def run_graph_on_thread_impl(
    requests: TSD[int, TS[_RunGraphRequest]],
    path: str = "thread_graph_runner",
) -> TSD[int, _RAW_OUTPUT]:
    state = _ResponseState()

    @push_queue(TSD[int, _RAW_OUTPUT])
    def responses(sender):
        state.attach(sender)

    @sink_node
    def submit(requests: TSD[int, TS[_RunGraphRequest]]):
        for request_id in requests.removed_keys():
            state.cancel(request_id)
        for request_id, request in requests.modified_items():
            generation = state.begin(request_id)

            def publish(delta, *, request_id=request_id, generation=generation):
                state.publish(request_id, generation, delta)

            def run(request=request.value, publish=publish):
                try:
                    _run_child(request, publish)
                except Exception as error:
                    logger.exception("thread graph failed", exc_info=error)
                    publish(
                        {
                            "finished": True,
                            "status": "ERROR:"
                            + str(error)
                            + "\n"
                            + traceback.format_exc(),
                        }
                    )

            thread = threading.Thread(
                target=run,
                name=f"hgraph-child-{request_id}",
                daemon=False,
            )
            state.add_thread(thread)
            thread.start()

    @submit.stop
    def stop():
        state.close()

    submit(requests)
    return responses()
