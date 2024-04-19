from hgraph import compute_node, TIME_SERIES_TYPE, LOGGER
from hgraph.test import eval_node


def test_logger_injectable(capsys):
    @compute_node
    def log_and_pass_through(ts: TIME_SERIES_TYPE, logger: LOGGER = None) -> TIME_SERIES_TYPE:
        logger.info(f"Tick: {ts.delta_value}")
        return ts.delta_value

    assert eval_node(log_and_pass_through, [1, None, 2]) == [1, None, 2]

    read = capsys.readouterr()
    log = read.err
    if log != '':
        assert "Tick: 1" in log
        assert "Tick: 2" in log