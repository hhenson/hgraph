"""The two read-only level queries on the LOGGER facade.

``if logger.isEnabledFor(DEBUG):`` around an expensive message is the standard
guard, and it raised ``AttributeError`` inside a node until issue #810 item 3.4
was settled. ``getEffectiveLevel`` answers on the same scale.

Deliberately still absent: ``setLevel``, because a node reconfiguring the run's
logging is not something to enable from inside the graph; and the deprecated
``warn``/``fatal`` aliases.
"""
import logging

import pytest

import hgraph as hg
from hgraph import LOGGER, TS, graph, sink_node
from hgraph.test import eval_node


@sink_node
def _record_level_queries(ts: TS[int], logger: LOGGER = None):
    logger.info("debug=%s info=%s critical=%s effective=%s",
                logger.isEnabledFor(logging.DEBUG),
                logger.isEnabledFor(logging.INFO),
                logger.isEnabledFor(logging.CRITICAL),
                logger.getEffectiveLevel())


def test_is_enabled_for_answers_against_the_run_logger(caplog):
    @graph
    def g(ts: TS[int]) -> None:
        _record_level_queries(ts)

    with caplog.at_level(logging.INFO, logger="hgraph"):
        eval_node(g, [1])

    messages = [r.getMessage() for r in caplog.records if "effective=" in r.getMessage()]
    assert messages, "the guard query produced no record"
    # The run logger is at DEBUG by default, so every standard level is enabled.
    assert "debug=True" in messages[0]
    assert "info=True" in messages[0]
    assert "critical=True" in messages[0]


def test_get_effective_level_uses_the_python_scale():
    """The value must be comparable with logging.DEBUG and friends."""
    seen = []

    @sink_node
    def capture(ts: TS[int], logger: LOGGER = None):
        seen.append(logger.getEffectiveLevel())

    @graph
    def g(ts: TS[int]) -> None:
        capture(ts)

    eval_node(g, [1])
    assert seen, "no level was captured"
    assert seen[0] in (
        logging.NOTSET, logging.DEBUG, logging.INFO,
        logging.WARNING, logging.ERROR, logging.CRITICAL, 60,
    ), f"{seen[0]} is not a standard Python logging level"


def test_setlevel_and_deprecated_aliases_stay_absent():
    """Pinned so a later 'completeness' change has to argue with a test.

    ``warn`` and ``fatal`` are deprecated in Python's own logging, and
    ``setLevel`` would let a node reconfigure the run it is part of.
    """
    for name in ("warn", "fatal", "setLevel"):
        assert not hasattr(LOGGER, name), f"LOGGER unexpectedly exposes {name}"
    for name in ("debug", "info", "warning", "error", "critical", "exception",
                 "log", "isEnabledFor", "getEffectiveLevel"):
        assert hasattr(LOGGER, name), f"LOGGER is missing {name}"
