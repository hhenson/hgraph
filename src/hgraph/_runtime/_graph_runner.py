from dataclasses import dataclass, field
from datetime import datetime
from logging import Logger, getLogger, DEBUG, StreamHandler, Formatter
from typing import Callable, Any, Dict

from hgraph._runtime._constants import MIN_ST, MAX_ET, MIN_DT
from hgraph._runtime._evaluation_engine import EvaluationMode, EvaluationLifeCycleObserver
from hgraph._runtime._graph_executor import GraphEngineFactory
import warnings

__all__ = ("run_graph", "evaluate_graph", "GraphConfiguration")

from hgraph._runtime._graph_recorder import GraphRecorder

def _default_logger() -> Logger:
    logger = getLogger("hgraph")
    if not logger.handlers:
        # If no handler exists, assume we need to create one.
        logger.setLevel(DEBUG)
        # create console handler and set level to debug
        ch = StreamHandler()
        ch.setLevel(DEBUG)
        # create formatter
        formatter = Formatter('%(asctime)s [%(name)s][%(levelname)s] %(message)s')
        # add formatter to ch
        ch.setFormatter(formatter)
        # add ch to logger
        logger.addHandler(ch)
    warnings.showwarning = warn_with_log
    warnings.filterwarnings("once", category=DeprecationWarning)
    return logger


def warn_with_log(message, category, filename, lineno, file=None, line=None):
    log = getLogger("hgraph")
    log.warning(f"{filename}:{lineno}: {category.__name__}: {message}")


@dataclass
class GraphConfiguration:
    run_mode: EvaluationMode = EvaluationMode.SIMULATION
    start_time: datetime = MIN_DT
    end_time: datetime = MAX_ET
    trace: bool | dict = False
    profile: bool | dict = False
    trace_mode: Dict[str, bool] = None
    life_cycle_observers: tuple[EvaluationLifeCycleObserver, ...] = tuple()
    graph_logger: Logger = field(default_factory=_default_logger)
    recorder: GraphRecorder | None = None

    def __post_init__(self):
        if self.start_time is MIN_DT:
            self.start_time = MIN_ST if self.run_mode is EvaluationMode.SIMULATION else datetime.utcnow()

        if self.start_time < MIN_ST:
            raise RuntimeError(f"Start time '{self.start_time}' is less than minimum time '{MIN_ST}'")

        if self.end_time > MAX_ET:
            raise RuntimeError(f"End time '{self.end_time}' is greater than maximum time '{MAX_ET}'")

        if self.trace:
            from hgraph.test import EvaluationTrace
            self.life_cycle_observers = self.life_cycle_observers + (EvaluationTrace(**(self.trace if type(self.trace) is dict else {})),)

        if self.profile:
            from hgraph.test import EvaluationProfiler
            self.life_cycle_observers = self.life_cycle_observers + (EvaluationProfiler(**(self.profile if type(self.profile) is dict else {})),)


def evaluate_graph(graph: Callable, config: GraphConfiguration, *args, **kwargs) -> list[tuple[datetime, Any]] | None:
    from hgraph._builder._graph_builder import GraphBuilder
    from hgraph._wiring._graph_builder import wire_graph
    from hgraph._wiring._wiring_node_instance import WiringNodeInstanceContext
    from hgraph._wiring._wiring_node_signature import WiringNodeSignature
    from hgraph.nodes import get_recorded_value, record

    signature: WiringNodeSignature = None
    if not isinstance(graph, GraphBuilder):
        config.graph_logger.debug("Wiring graph: %s", graph.signature.signature)
        signature = graph.signature
        if signature.output_type:
            graph_ = graph
            def _record(*args, **kwargs):
                out = graph_(*args, **kwargs)
                record(out, "__out__")
            graph = _record
        with WiringNodeInstanceContext():
            graph_builder: GraphBuilder = wire_graph(graph, *args, **kwargs)
    else:
        graph_builder = graph

    config.graph_logger.debug("Creating graph engine: %s", config.run_mode)
    engine = GraphEngineFactory.make(graph=graph_builder.make_instance(tuple()), run_mode=config.run_mode,
                                     observers=config.life_cycle_observers)
    config.graph_logger.debug("Starting to run graph from: %s to %s", config.start_time,
                              config.end_time)
    try:
        engine.run(config.start_time, config.end_time)
        if signature is not None and signature.output_type:
            return get_recorded_value("__out__")
    except Exception as e:
        config.graph_logger.exception("Graph failed", exc_info=True)
        raise e
    finally:
        config.graph_logger.debug("Finished running graph")


def run_graph(graph: Callable, *args,
              run_mode: EvaluationMode = EvaluationMode.SIMULATION,
              start_time: datetime = None,
              end_time: datetime = None,
              print_progress: bool = True,
              life_cycle_observers: [EvaluationLifeCycleObserver] = None,
              __trace__: bool | dict = False,
              __profile__: bool | dict = False,
              __trace_mode__: Dict[str, bool] = None,
              **kwargs):
    """
    Use this to initiate the graph engine run loop.

    The run_mode indicates how the graph engine should evaluate the graph, in RunMOde.REAL_TIME the graph will be
    evaluated using the system clock, in RunMode.BACK_TEST the graph will be evaluated using a simulated clock.
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
    kwargs_ = {"run_mode": run_mode, "trace": __trace__, "profile": __profile__}
    if start_time is not None:
        kwargs_["start_time"] = start_time
    if end_time is not None:
        kwargs_["end_time"] = end_time
    if life_cycle_observers is not None:
        kwargs_["life_cycle_observers"] = life_cycle_observers
    config = GraphConfiguration(**kwargs_)
    evaluate_graph(graph, config, *args, **kwargs)