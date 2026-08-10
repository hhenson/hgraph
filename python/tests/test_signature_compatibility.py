import inspect
import logging
from datetime import datetime, timedelta, timezone
from typing import TypeVar

import pytest

import hgraph as hg


def _parameters(callable_):
    return tuple(inspect.signature(callable_).parameters)


def test_shared_callable_signatures_match_python_authoring_surface():
    assert _parameters(hg.GraphConfiguration) == (
        "run_mode", "start_time", "end_time", "trace", "profile",
        "life_cycle_observers", "trace_wiring", "wiring_observers",
        "graph_logger", "trace_back_depth", "capture_values",
        "default_log_level", "logger_formatter", "cleanup_on_error",
    )
    assert _parameters(hg.compute_node) == (
        "fn", "node_impl", "active", "valid", "all_valid", "overloads",
        "resolvers", "requires", "label", "deprecated",
    )
    assert _parameters(hg.sink_node) == _parameters(hg.compute_node)
    assert _parameters(hg.graph) == (
        "fn", "overloads", "resolvers", "requires", "label", "deprecated",
    )
    assert _parameters(hg.generator) == _parameters(hg.graph)
    assert _parameters(hg.component) == (
        "fn", "recordable_id", "resolvers", "label", "deprecated",
    )
    assert _parameters(hg.push_queue) == (
        "tp", "overloads", "resolvers", "requires", "label", "deprecated",
        "conflate",
    )
    assert _parameters(hg.feedback) == ("tp_or_wp", "default")
    assert tuple(inspect.signature(hg.dispatch_).parameters)[:2] == (
        "overloaded", "args")
    assert {"__trace__", "__trace_wiring__", "__observers__"} <= set(
        inspect.signature(hg.eval_node).parameters)
    for decorator in (
            hg.reference_service, hg.subscription_service,
            hg.request_reply_service, hg.adaptor, hg.service_adaptor):
        assert _parameters(decorator) == ("fn", "resolvers")
    assert _parameters(hg.register_service)[:3] == (
        "path", "implementation", "resolution_dict")
    assert _parameters(hg.register_adaptor)[:3] == (
        "path", "implementation", "resolution_dict")


def test_lazy_operator_runtime_signatures_use_native_registry_metadata():
    assert _parameters(hg.filter_) == ("condition", "ts")
    signature = inspect.signature(hg.filter_)
    assert all(
        parameter.default is inspect.Parameter.empty
        for parameter in signature.parameters.values()
    )
    assert signature.return_annotation is hg.WiringPort

    # A Python inspect.Signature cannot represent an overload set whose call
    # structures differ. Those operators retain a truthful catch-all runtime
    # signature and document every concrete overload instead.
    assert _parameters(hg.add_) == ("args", "kwargs")
    assert "add_(lhs: TS[int], rhs: TS[int]) -> TS[int]" in inspect.getdoc(hg.add_)


def test_compute_node_all_valid_uses_native_structural_validity():
    @hg.compute_node(all_valid=("tsl",))
    def all_children(tsl: hg.TSL[hg.TS[int], hg.Size[2]]) -> hg.TS[bool]:
        return True

    assert hg.eval_node(all_children, [{0: 1}, {1: 2}]) == [None, True]


def test_graph_configuration_honours_run_mode_and_logger(caplog):
    observed = []

    @hg.compute_node
    def inspect_run(
            ts: hg.TS[int],
            engine: hg.EvaluationEngineApi = None,
            logger: hg.LOGGER = None) -> hg.TS[str]:
        observed.append(logger)
        return engine.evaluation_mode

    @hg.graph
    def app() -> hg.TS[str]:
        value = hg.const(1)
        hg.log_("native run logger {}", value, level=logging.WARNING)
        return inspect_run(value)

    logger = logging.getLogger("hgraph.signature-compatibility")
    start_time = datetime.now(timezone.utc).replace(tzinfo=None)
    end_time = start_time + timedelta(seconds=0.25)
    with caplog.at_level(logging.WARNING):
        result = hg.evaluate_graph(
            app,
            hg.GraphConfiguration(
                run_mode=hg.EvaluationMode.REAL_TIME,
                start_time=start_time,
                end_time=end_time,
                graph_logger=logger,
                default_log_level=logging.WARNING,
            ),
        )

    assert [value for _, value in result] == [hg.EvaluationMode.REAL_TIME]
    assert observed == [logger]
    assert logger.level == logging.WARNING
    assert "native run logger 1" in caplog.text


def test_graph_configuration_resolves_relative_end_time():
    simulation_start = hg.MIN_ST + timedelta(seconds=1)
    simulation = hg.GraphConfiguration(
        start_time=simulation_start,
        end_time=timedelta(seconds=2),
    )
    assert simulation.end_time == simulation_start + timedelta(seconds=2)

    before = hg.utc_now()
    real_time = hg.GraphConfiguration(
        run_mode=hg.EvaluationMode.REAL_TIME,
        end_time=timedelta(seconds=2),
    )
    after = hg.utc_now()
    assert before + timedelta(seconds=2) <= real_time.end_time
    assert real_time.end_time <= after + timedelta(seconds=2)


def test_graph_configuration_applies_logger_formatter_to_python_and_native_nodes(caplog):
    formatted = []

    def formatter(level, message, args, exc_info=None, extra=None,
                  stack_info=False, stacklevel=1, node_path=None,
                  __orig_log__=None):
        formatted.append((node_path, message))
        return __orig_log__(
            level, f"{node_path}: {message}", args, exc_info, extra,
            stack_info, stacklevel)

    @hg.compute_node
    def python_log(value: hg.TS[int], logger: hg.LOGGER = None) -> hg.TS[int]:
        logger.warning("python contextual log")
        return value.value

    @hg.graph
    def app() -> hg.TS[int]:
        value = python_log(hg.const(1))
        hg.log_("native contextual log {}", value, level=logging.WARNING)
        return value

    logger = logging.getLogger("hgraph.contextual-logger")
    with caplog.at_level(logging.WARNING, logger=logger.name):
        result = hg.evaluate_graph(
            app,
            hg.GraphConfiguration(
                graph_logger=logger,
                default_log_level=logging.WARNING,
                logger_formatter=formatter,
            ),
        )

    assert [value for _, value in result] == [1]
    assert any(path == "python_log" and "python contextual" in message
               for path, message in formatted)
    assert any(path != "python_log" and "native contextual" in message
               for path, message in formatted)
    assert "python contextual log" in caplog.text
    assert "native contextual log 1" in caplog.text


def test_graph_configuration_rejects_unimplemented_options_explicitly():
    @hg.graph
    def app() -> hg.TS[int]:
        return hg.const(1)

    assert [value for _, value in hg.evaluate_graph(
        app, hg.GraphConfiguration(trace_wiring={"graph": True, "node": False})
    )] == [1]
    with pytest.raises(TypeError):
        hg.GraphConfiguration(unknown_option=True)


def test_graph_configuration_uses_the_native_evaluation_profiler(caplog):
    from hgraph.test import EvaluationProfiler

    @hg.compute_node
    def add_one(value: hg.TS[int]) -> hg.TS[int]:
        return value.value + 1

    @hg.graph
    def app() -> hg.TS[int]:
        return add_one(hg.const(1))

    logger = logging.getLogger("hgraph.profile-test")
    profiler = EvaluationProfiler(recent_window=4)
    with caplog.at_level(logging.DEBUG, logger=logger.name):
        result = hg.evaluate_graph(
            app,
            hg.GraphConfiguration(profile=profiler, graph_logger=logger),
        )

    assert [value for _, value in result] == [2]
    snapshot = profiler.snapshot()
    assert snapshot.graph_cycles == 1
    assert snapshot.wall_time >= timedelta(0)
    assert snapshot.root_evaluation_time >= timedelta(0)
    assert any("__py_compute" in entry.path and entry.evaluation.count == 1
               for entry in snapshot.entries)
    assert "Graph profile: cycles=1" in caplog.text


def test_graph_configuration_accepts_boolean_and_dict_profile_options():
    @hg.graph
    def app() -> hg.TS[int]:
        return hg.const(1)

    assert [value for _, value in hg.evaluate_graph(
        app, hg.GraphConfiguration(profile=True)
    )] == [1]
    assert [value for _, value in hg.evaluate_graph(
        app, hg.GraphConfiguration(profile={"start": False, "stop": False})
    )] == [1]


def test_graph_generator_and_component_resolvers():
    @hg.graph(resolvers={hg.SIZE: lambda mapping, width: width})
    def resolved_graph(
            width: int,
            size: type[hg.SIZE] = hg.AUTO_RESOLVE) -> hg.TS[int]:
        return hg.const(size.SIZE)

    @hg.generator(resolvers={hg.SCALAR: lambda mapping, scalar_type: scalar_type})
    def resolved_source(scalar_type: type) -> hg.TS[hg.SCALAR]:
        yield hg.MIN_ST, 7

    @hg.component(resolvers={hg.SIZE: lambda mapping, width: width})
    def resolved_component(
            ts: hg.TS[int],
            width: int,
            size: type[hg.SIZE] = hg.AUTO_RESOLVE) -> hg.TS[int]:
        return ts + hg.const(size.SIZE)

    @hg.graph
    def source_graph() -> hg.TS[int]:
        return resolved_source(int)

    assert hg.eval_node(resolved_graph, width=3) == [3]
    assert hg.eval_node(source_graph) == [7]
    assert hg.eval_node(resolved_component, [1, 2], width=3) == [4, 5]


def test_resolvers_only_run_for_unresolved_targets_across_wiring_surfaces():
    calls = []

    def unexpected(surface):
        def resolver(mapping):
            calls.append(surface)
            raise AssertionError(f"{surface} resolver must not run")
        return resolver

    @hg.compute_node(resolvers={hg.SCALAR: unexpected("node inference")})
    def inferred_node(ts: hg.TS[hg.SCALAR]) -> hg.TS[hg.SCALAR]:
        return ts.value

    @hg.compute_node(resolvers={hg.SCALAR_1: unexpected("node pin")})
    def pinned_node(ts: hg.TS[hg.SCALAR]) -> hg.TS[hg.SCALAR_1]:
        return str(ts.value)

    @hg.graph(resolvers={hg.SCALAR: unexpected("graph")})
    def inferred_graph(ts: hg.TS[hg.SCALAR]) -> hg.TS[hg.SCALAR]:
        return ts

    @hg.operator
    def inferred_operator(ts: hg.TS[hg.SCALAR]) -> hg.TS[hg.SCALAR]: ...

    @hg.compute_node(
        overloads=inferred_operator,
        resolvers={hg.SCALAR: unexpected("operator")},
    )
    def inferred_operator_impl(ts: hg.TS[hg.SCALAR]) -> hg.TS[hg.SCALAR]:
        return ts.value

    @hg.graph
    def operator_graph(ts: hg.TS[int]) -> hg.TS[int]:
        return inferred_operator(ts)

    assert hg.eval_node(inferred_node, [1, 2]) == [1, 2]
    assert hg.eval_node(
        pinned_node[hg.SCALAR:int, hg.SCALAR_1:str], [1, 2]) == ["1", "2"]
    assert hg.eval_node(inferred_graph, [1, 2]) == [1, 2]
    assert hg.eval_node(operator_graph, [1, 2]) == [1, 2]
    assert calls == []


def test_resolution_callables_ignore_postponed_local_annotations():
    class LocalAnnotation:
        pass

    def operator_resolver(
            mapping: "LocalAnnotation",
            scalar_type: "LocalAnnotation") -> "LocalAnnotation":
        return scalar_type

    def operator_requires(
            mapping: "LocalAnnotation",
            scalar_type: "LocalAnnotation") -> "LocalAnnotation":
        return scalar_type is int

    def service_resolver(
            mapping: "LocalAnnotation") -> "LocalAnnotation":
        return int

    @hg.operator
    def annotated_operator(
            ts: hg.TS[int], scalar_type: type) -> hg.TS[hg.SCALAR]: ...

    @hg.compute_node(
        overloads=annotated_operator,
        resolvers={hg.SCALAR: operator_resolver},
        requires=operator_requires,
    )
    def annotated_operator_impl(
            ts: hg.TS[int], scalar_type: type) -> hg.TS[hg.SCALAR]:
        return ts.value

    @hg.request_reply_service(resolvers={hg.SCALAR: service_resolver})
    def annotated_service(request: hg.TS[int]) -> hg.TS[hg.SCALAR]: ...

    @hg.service_impl(interfaces=annotated_service)
    def annotated_service_impl(
            requests: hg.TSD[int, hg.TS[int]],
    ) -> hg.TSD[int, hg.TS[int]]:
        return requests

    @hg.graph
    def operator_app(ts: hg.TS[int]) -> hg.TS[int]:
        return annotated_operator(ts, int)

    @hg.graph
    def service_app(ts: hg.TS[int]) -> hg.TS[int]:
        hg.register_service("annotated", annotated_service_impl)
        return annotated_service(ts, path="annotated")

    assert hg.eval_node(operator_app, [1, 2]) == [1, 2]
    assert hg.eval_node(
        service_app,
        [3],
        __end_time__=hg.MIN_ST + 4 * hg.MIN_TD,
    ) == [None, 3]


def test_deprecated_node_warns_at_wiring_and_node_impl_is_explicitly_rejected():
    @hg.compute_node(deprecated="use replacement")
    def old_node(ts: hg.TS[int]) -> hg.TS[int]:
        return ts.value

    with pytest.warns(DeprecationWarning, match="use replacement"):
        assert hg.eval_node(old_node, [1]) == [1]

    with pytest.raises(NotImplementedError, match="legacy Python-runtime"):
        hg.compute_node(None, object())


def test_service_resolvers_and_registration_resolution_dict():
    request_type = TypeVar("request_type", int, str)
    response_type = TypeVar("response_type", int, float)

    @hg.request_reply_service(resolvers={response_type: lambda mapping: int})
    def resolved_service(
            request: hg.TS[request_type]) -> hg.TS[response_type]: ...

    concrete_service = resolved_service[
        request_type:int, response_type:int]

    @hg.service_impl(interfaces=concrete_service)
    def resolved_impl(
            requests: hg.TSD[int, hg.TS[int]]) -> hg.TSD[int, hg.TS[int]]:
        return requests

    payload = TypeVar("payload", int, str)

    @hg.request_reply_service
    def registered_service(request: hg.TS[payload]) -> hg.TS[payload]: ...

    @hg.service_impl(interfaces=registered_service)
    def registered_impl(
            requests: hg.TSD[int, hg.TS[payload]],
    ) -> hg.TSD[int, hg.TS[payload]]:
        return requests

    @hg.service_adaptor
    def registered_adaptor(request: hg.TS[payload]) -> hg.TS[payload]: ...

    @hg.service_adaptor_impl(interfaces=registered_adaptor)
    def registered_adaptor_impl(
            requests: hg.TSD[int, hg.TS[payload]],
    ) -> hg.TSD[int, hg.TS[payload]]:
        return requests

    @hg.graph
    def resolved_app(value: hg.TS[int]) -> hg.TS[int]:
        hg.register_service("resolved", resolved_impl)
        return resolved_service(value, path="resolved")

    @hg.graph
    def registered_app(value: hg.TS[int]) -> hg.TS[int]:
        hg.register_service(
            "registered", registered_impl, resolution_dict={payload: int})
        return registered_service[payload:int](value, path="registered")

    @hg.graph
    def registered_adaptor_app(value: hg.TS[int]) -> hg.TS[int]:
        hg.register_adaptor(
            "registered-adaptor",
            registered_adaptor_impl,
            resolution_dict={payload: int},
        )
        return registered_adaptor[payload:int](
            value, path="registered-adaptor")

    end_time = hg.MIN_ST + 4 * hg.MIN_TD
    assert hg.eval_node(resolved_app, [1], __end_time__=end_time) == [None, 1]
    assert hg.eval_node(registered_app, [2], __end_time__=end_time) == [None, 2]
    assert hg.eval_node(
        registered_adaptor_app, [3], __end_time__=end_time) == [3]


def test_push_queue_options_feedback_keyword_and_eval_node_trace_defaults(capfd):
    @hg.operator
    def queued_value(scalar_type: type) -> hg.TS[hg.SCALAR]: ...

    @hg.push_queue(
        hg.TS[hg.SCALAR],
        overloads=queued_value,
        resolvers={hg.SCALAR: lambda mapping, scalar_type: scalar_type},
        requires=lambda mapping, scalar_type: scalar_type is int,
        label="typed queue",
    )
    def source(sender, scalar_type: type):
        sender(5)

    @hg.graph
    def queue_graph() -> hg.TS[int]:
        return queued_value(int)

    @hg.graph
    def delayed(ts: hg.TS[int]) -> hg.TS[int]:
        return hg.feedback(tp_or_wp=ts, default=None)()

    start_time = datetime.now(timezone.utc).replace(tzinfo=None)
    end_time = start_time + timedelta(seconds=0.25)
    queued = hg.run_graph(
        queue_graph,
        run_mode=hg.EvaluationMode.REAL_TIME,
        start_time=start_time,
        end_time=end_time,
    )
    assert [value for _, value in queued] == [5]
    assert hg.eval_node(
        delayed,
        [1, None],
        __trace__=False,
        __trace_wiring__=False,
        __observers__=[],
        __end_time__=hg.MIN_ST + 3 * hg.MIN_TD,
    ) == [None, 1]
    from hgraph.test import EvaluationTrace

    EvaluationTrace.set_use_logger(False)
    try:
        assert hg.eval_node(
            delayed,
            [1, None],
            __trace__={"start": False, "stop": False},
            __end_time__=hg.MIN_ST + 3 * hg.MIN_TD,
        ) == [None, 1]
    finally:
        EvaluationTrace.set_use_logger(True)

    trace = capfd.readouterr().out
    assert "Eval Start" in trace
    assert "[IN]" in trace
    assert "[OUT]" in trace
    assert "Eval Done" in trace
    assert "Starting Graph" not in trace


def test_graph_configuration_trace_uses_native_observer(capfd):
    from hgraph.test import EvaluationTrace

    @hg.graph
    def app() -> hg.TS[int]:
        return hg.const(7)

    EvaluationTrace.set_use_logger(False)
    try:
        result = hg.evaluate_graph(
            app,
            hg.GraphConfiguration(trace={"start": False, "stop": False}),
        )
    finally:
        EvaluationTrace.set_use_logger(True)

    assert [value for _, value in result] == [7]
    trace = capfd.readouterr().out
    assert "Eval Start" in trace
    assert " *->* 7 [OUT]" in trace
    assert "Eval Done" in trace
    assert "Starting Graph" not in trace
