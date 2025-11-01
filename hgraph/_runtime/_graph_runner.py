import gc
import sys
import warnings
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from logging import Logger, getLogger, DEBUG, StreamHandler, Formatter
from typing import Callable, Any

from typing_extensions import deprecated

from hgraph._runtime._constants import MIN_ST, MAX_ET, MIN_DT
from hgraph._runtime._evaluation_engine import EvaluationMode, EvaluationLifeCycleObserver
from hgraph._runtime._global_state import GlobalState
from hgraph._runtime._graph_executor import GraphEngineFactory
from hgraph._wiring._wiring_observer import WiringObserver

__all__ = ("run_graph", "evaluate_graph", "GraphConfiguration", "node_path_log_formatter")


def _default_logger() -> Logger:
    logger = getLogger("hgraph")
    if not logger.handlers:
        # If no handler exists, assume we need to create one.
        logger.setLevel(DEBUG)
        # create console handler and set level to debug
        ch = StreamHandler(sys.stdout)
        ch.setLevel(DEBUG)
        # create formatter
        formatter = Formatter("%(asctime)s [%(name)s][%(levelname)s] %(message)s")
        # add formatter to ch
        ch.setFormatter(formatter)
        # add ch to logger
        logger.addHandler(ch)
    warnings.showwarning = warn_with_log
    warnings.filterwarnings("once", category=DeprecationWarning)
    return logger


def node_path_log_formatter(
    level, msg, args, exc_info=None, extra=None, stack_info=False, stacklevel=1, node_path=None, __orig_log__=None
):
    """A formatter that prints out the node path in addition to the log message."""
    return __orig_log__(level, f"{node_path}:\n{msg}", args, exc_info, extra, stack_info, stacklevel)


def warn_with_log(message, category, filename, lineno, file=None, line=None):
    log = getLogger("hgraph")
    log.warning(f"{filename}:{lineno}: {category.__name__}: {message}")


@dataclass
class GraphConfiguration:
    """
    The configuration to be supplied to ``evaluate_graph``.
    The following properties are defined:

    run_mode
        Either ``REAL_TIME`` or ``SIMULATION``.

    start_time
        The first time to evaluate the engine for, this cannot be earlier than MIN_ST.

    end_time
        The last time to evaluate the engine for (inclusive), this cannot be later than MAX_ET.

    trace
        Turn on tracing by setting this to ``True``. It is also possible to be selective with tracing by
        setting this to a dict of the form ``{"start": False, "stop": False, "eval": False}``. Setting the value
        to be ``True`` will turn on tracing of this element, ``False`` will turn off tracing of the particular
        life-cycle. For more information on available options see: :class:`hgraph.test.EvaluationTrace`

    profile
        Similar to tracing, except setting this will turn on profiling of the graph.
        See :class:`hgraph.test.EvaluationProfiler` for more information as to the options.

    life_cycle_observers
        This allows for additional life-cycle observers to be registered. This should be supplied as a tuple of
        :class:`hgraph.EvaluationLifeCycleObserver` instances.

    trace_wiring
        This indicates an interest in observing the wiring choices made during the wiring stage of the graph.
        As with tracing and profiling, a dictionary of options can also be supplied see :class:`hgraph.test.WiringTracer`
        for more information on the options.

    wiring_observers
        This allows for custom wiring observers to be registered. This should be supplied as a tuple of
        :class:`hgraph.WiringObserver` instances.

    graph_logger
        The instance of the Python ``Logger`` to use for graph logging, by default an instance of the logger
        will be setup and registered under the 'hgraph' name. (Can be retrieved using ``getLogger('hgraph')``)

    trace_back_depth
        Used as a parameter to the error handling logic to determine the depth of traceback to capture.

    capture_values
        capture the values of the inputs to the trace-back (default is False)

    default_log_level
        The default log level to use, the default is DEBUG.

    logger_formatter
        Use to provide a custom formatter to override the default logger._log method.
        The node_path is supplied as an extra argument to the formatter as well as the original log method
        (as ``__orig_log__``).
        An example formatter is provided as ``node_path_log_formatter``.

    """

    run_mode: EvaluationMode = EvaluationMode.SIMULATION
    start_time: datetime = MIN_DT
    end_time: datetime = MAX_ET
    trace: bool | dict = False
    profile: bool | dict = False
    life_cycle_observers: tuple[EvaluationLifeCycleObserver, ...] = tuple()
    trace_wiring: bool | dict = False
    wiring_observers: tuple[WiringObserver, ...] = tuple()
    graph_logger: Logger = field(default_factory=_default_logger)
    trace_back_depth: int = 1
    capture_values: bool = False
    default_log_level: int = DEBUG
    logger_formatter: Callable = None

    def __post_init__(self):
        if self.start_time is MIN_DT:
            self.start_time = MIN_ST if self.run_mode is EvaluationMode.SIMULATION else datetime.utcnow()

        if self.start_time < MIN_ST:
            raise RuntimeError(f"Start time '{self.start_time}' is less than minimum time '{MIN_ST}'")

        if isinstance(self.end_time, timedelta):
            if self.run_mode is EvaluationMode.SIMULATION:
                self.end_time = self.start_time + self.end_time
            elif self.run_mode is EvaluationMode.REAL_TIME:
                self.end_time = datetime.utcnow() + self.end_time

        if self.end_time > MAX_ET:
            raise RuntimeError(f"End time '{self.end_time}' is greater than maximum time '{MAX_ET}'")

        if self.trace:
            from hgraph.test import EvaluationTrace

            self.life_cycle_observers = self.life_cycle_observers + (
                EvaluationTrace(**(self.trace if type(self.trace) is dict else {})),
            )

        if self.profile:
            from hgraph.test import EvaluationProfiler

            self.life_cycle_observers = self.life_cycle_observers + (
                EvaluationProfiler(**(self.profile if type(self.profile) is dict else {})),
            )

        if self.trace_wiring:
            from hgraph.test import WiringTracer

            self.wiring_observers = self.wiring_observers + (
                WiringTracer(**(self.trace_wiring if type(self.trace_wiring) is dict else {})),
            )

        if self.default_log_level != DEBUG:
            self.graph_logger.setLevel(self.default_log_level)

    @property
    def error_capture_options(self):
        return {"trace_back_depth": self.trace_back_depth, "capture_values": self.capture_values}


def evaluate_graph(graph: Callable, config: GraphConfiguration, *args, **kwargs) -> list[tuple[datetime, Any]] | None:
    """
    Wires the ``graph`` supplied, constructs the evaluation engine using the configuration supplied and starts the
    engines' run loop. If the ``graph`` has an ouput, this will collect the results in memory and return them
    as the end, once the run loop exists.

    .. note: Recording results can be memory intensive, don't use top level graphs with outputs unless you intend
             on using the results.

    :param graph: The graph to evaluate.
    :param config: The configuration used to construct the evaluation engine.
    :param args:  Any arguments to supply to the graph.
    :param kwargs: Any kwargs to supply to the graph.
    :return: The list of engine time and value for each tick returned by the graph if an output is present, None otherwise.
    """
    from hgraph._builder._graph_builder import GraphBuilder
    from hgraph._wiring._wiring_node_signature import WiringNodeSignature
    from hgraph._impl._operators._record_replay_in_memory import get_recorded_value

    with GlobalState() if not GlobalState.has_instance() else nullcontext():
        signature: WiringNodeSignature = None
        if not isinstance(graph, GraphBuilder):
            graph_builder, signature = _build_main_graph(graph, config, args, kwargs)
        else:
            graph_builder = graph

        config.graph_logger.debug("Creating graph engine: %s", config.run_mode)
        engine = GraphEngineFactory.make(
            graph=graph_builder.make_instance(tuple()), run_mode=config.run_mode, observers=config.life_cycle_observers
        )

        gc.collect()  # Clean up any garbage from wiring
        gc.freeze()  # Freeze the graph memory

        config.graph_logger.debug("Starting to run graph from: %s to %s", config.start_time, config.end_time)
        try:
            GlobalState.instance()["__graph_logger__"] = config.graph_logger
            GlobalState.instance()["__graph_custom_formatter__"] = config.logger_formatter
            engine.run(config.start_time, config.end_time)
            if signature is not None and signature.output_type:
                return get_recorded_value("__out__")
        except Exception as e:
            config.graph_logger.exception("Graph failed", exc_info=True)
            raise e
        finally:
            config.graph_logger.debug("Finished running graph")
            graph_builder.release_instance(engine.graph)


def _build_main_graph(graph, config, args, kwargs):
    from hgraph._wiring._wiring_node_instance import WiringNodeInstanceContext
    from hgraph._wiring._graph_builder import wire_graph
    from hgraph._operators._record_replay import record
    from hgraph._builder._graph_builder import GraphBuilder

    config.graph_logger.debug("Wiring graph: %s", graph.signature.signature)
    signature = graph.signature
    if signature.output_type:
        graph_ = graph

        def _record(*args, **kwargs):
            out = graph_(*args, **kwargs)
            record(out, "__out__")

        graph = _record

    with WiringNodeInstanceContext(error_capture_options=config.error_capture_options):
        from hgraph._wiring._wiring_observer import WiringObserverContext

        with WiringObserverContext() as wiring_observer_context:
            for observer in config.wiring_observers:
                wiring_observer_context.add_wiring_observer(observer)

            graph_builder: GraphBuilder = wire_graph(graph, *args, **kwargs)

    return graph_builder, signature


@deprecated("Use evaluate_graph instead")
def run_graph(
    graph: Callable,
    *args,
    run_mode: EvaluationMode = EvaluationMode.SIMULATION,
    start_time: datetime = None,
    end_time: datetime | timedelta = None,
    print_progress: bool = True,
    life_cycle_observers: [EvaluationLifeCycleObserver] = None,
    __trace__: bool | dict = False,
    __profile__: bool | dict = False,
    __trace_wiring__: bool | dict = False,
    __logger__: Logger = None,
    __trace_back_depth__: int = 1,
    __capture_values__: bool = False,
    **kwargs,
):
    """
    Use this to initiate the graph engine run loop.

    The run_mode indicates how the graph engine should evaluate the graph, in EvaluationMode.REAL_TIME the graph will be
    evaluated using the system clock, in EvaluationMode.SIMULATION the graph will be evaluated using a simulated clock.
    The simulated clock is advanced as fast as possible without following the system clock timings. This allows a
    back-test to be evaluated as fast as possible.

    :param graph: The graph to evaluate
    :param args: Any arguments to pass to the graph
    :param run_mode: The mode to evaluate the graph in
    :param start_time: The time to start the graph
    :param end_time: The time to end the graph (this is exclusive)
    :param print_progress: If true, print the progress of the graph (will go away and be replaced with logging later)
    :param life_cycle_observers: A list of observers to register with the runtime engine prior to evaluation.
    :param kwargs: Any additional kwargs to pass to the graph.
    """
    kwargs_ = {
        "run_mode": run_mode,
        "trace": __trace__,
        "profile": __profile__,
        "trace_wiring": __trace_wiring__,
    }

    if __logger__ is not None:
        kwargs_["graph_logger"] = __logger__
    if start_time is not None:
        kwargs_["start_time"] = start_time
    if end_time is not None:
        kwargs_["end_time"] = end_time
    if life_cycle_observers is not None:
        kwargs_["life_cycle_observers"] = life_cycle_observers
    config = GraphConfiguration(**kwargs_, trace_back_depth=__trace_back_depth__, capture_values=__capture_values__)
    return evaluate_graph(graph, config, *args, **kwargs)
