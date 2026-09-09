"""``log_`` must actually produce output.

The operator exists to emit, and a ``logging.Logger`` with no handler emits
nothing below WARNING, so a missing handler makes it a silent no-op rather than
a failure. Released hgraph attaches a stdout handler from its own
``GraphConfiguration`` default, which is what these tests pin.
"""
import logging
import sys

import pytest

import hgraph as hg
from hgraph.test import eval_node

from hgraph._wiring._runner import _default_graph_logger


@pytest.fixture
def hgraph_logger_restored():
    """Restore the process-wide ``hgraph`` logger around a test."""
    logger = logging.getLogger("hgraph")
    handlers = list(logger.handlers)
    level = logger.level
    yield logger
    logger.handlers = handlers
    logger.setLevel(level)


@pytest.fixture
def root_logger_restored():
    """Restore the root logger's handlers around a test."""
    root = logging.getLogger()
    handlers = list(root.handlers)
    yield root
    root.handlers = handlers


def test_default_logger_gains_a_stdout_handler(hgraph_logger_restored, root_logger_restored):
    logger = hgraph_logger_restored
    logger.handlers = []
    logger.setLevel(logging.NOTSET)
    root_logger_restored.handlers = []

    resolved = _default_graph_logger()

    assert resolved is logger
    assert [h for h in logger.handlers
            if isinstance(h, logging.StreamHandler) and h.stream is sys.stdout]
    # A bare logger inherits the root's WARNING and would drop log_'s INFO.
    assert logger.level == logging.DEBUG


def test_default_logger_defers_to_a_handler_on_the_root(
    hgraph_logger_restored, root_logger_restored
):
    """An application that configured logging through the root already has a
    destination for these records.

    Adding one here would emit every record twice, once to stdout and once to
    the application's own handler, and leak graph records into a destination it
    did not choose for them. This is stricter than released hgraph, which tests
    ``logger.handlers`` and would duplicate.
    """
    logger = hgraph_logger_restored
    logger.handlers = []
    logger.setLevel(logging.CRITICAL)
    root_logger_restored.handlers = [logging.NullHandler()]

    _default_graph_logger()

    assert logger.handlers == []
    # Its level is the application's choice too.
    assert logger.level == logging.CRITICAL


def test_default_logger_does_not_displace_an_existing_handler(hgraph_logger_restored):
    logger = hgraph_logger_restored
    existing = logging.NullHandler()
    logger.handlers = [existing]
    logger.setLevel(logging.CRITICAL)

    _default_graph_logger()

    # An application that configured its own logging keeps it, level included.
    assert logger.handlers == [existing]
    assert logger.level == logging.CRITICAL


def test_log_emits_a_record_per_tick(caplog):
    @hg.graph
    def g(ts: hg.TS[int]) -> None:
        hg.log_("v={}", ts)

    with caplog.at_level(logging.INFO, logger="hgraph"):
        eval_node(g, [1, 2, 3])

    messages = [r.getMessage() for r in caplog.records if r.name.startswith("hgraph")]
    assert [m for m in messages if m.endswith("v=1")]
    assert [m for m in messages if m.endswith("v=2")]
    assert [m for m in messages if m.endswith("v=3")]


def test_log_record_carries_the_engine_time(caplog):
    # Released hgraph logs "[%s] %s" % (last_modified_time, value): the tick a
    # record belongs to is part of the record, not of the handler's format.
    @hg.graph
    def g(ts: hg.TS[int]) -> None:
        hg.log_("v={}", ts)

    with caplog.at_level(logging.INFO, logger="hgraph"):
        eval_node(g, [1])

    messages = [r.getMessage() for r in caplog.records if r.getMessage().endswith("v=1")]
    assert messages, "log_ produced no record"
    assert messages[0].startswith("[1970-01-01 00:00:00.000001] ")


def test_log_respects_its_level(caplog):
    @hg.graph
    def g(ts: hg.TS[int]) -> None:
        hg.log_("quiet={}", ts, level=logging.DEBUG)

    with caplog.at_level(logging.INFO, logger="hgraph"):
        eval_node(g, [1])

    assert not [r for r in caplog.records if "quiet" in r.getMessage()]
