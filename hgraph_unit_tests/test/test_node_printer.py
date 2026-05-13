import logging

from hgraph import not_
from hgraph.test import EvaluationTrace, eval_node


def _trace_messages(caplog) -> list[str]:
    return [record.getMessage() for record in caplog.records if record.name == "hgraph.test._node_printer"]


def test_eval_node_trace_parameter_registers_trace_observer(caplog):
    old_use_logger = getattr(EvaluationTrace, "_USE_LOGGER", True)
    old_print_all_values = getattr(EvaluationTrace, "_PRINT_ALL_VALUES", False)

    EvaluationTrace.set_use_logger(True)
    EvaluationTrace.set_print_all_values(False)
    try:
        with caplog.at_level(logging.INFO, logger="hgraph.test._node_printer"):
            assert eval_node(not_, [True], __trace__=False) == [False]
        assert _trace_messages(caplog) == []

        caplog.clear()

        with caplog.at_level(logging.INFO, logger="hgraph.test._node_printer"):
            assert eval_node(not_, [True], __trace__=True) == [False]

        trace_messages = _trace_messages(caplog)
        assert any("Starting Graph" in message for message in trace_messages)
        assert any("Eval Start" in message for message in trace_messages)
        assert any("[IN]" in message for message in trace_messages)
        assert any("[OUT]" in message for message in trace_messages)
        assert any("Stopped node" in message for message in trace_messages)
        assert any("Graph stopped" in message for message in trace_messages)
    finally:
        EvaluationTrace.set_use_logger(old_use_logger)
        EvaluationTrace.set_print_all_values(old_print_all_values)
