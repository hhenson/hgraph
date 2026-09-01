import runpy
from pathlib import Path

import numpy as np
import pytest

EXAMPLES = Path(__file__).parents[1] / "examples"


def _example(name):
    return runpy.run_path(EXAMPLES / name)


def test_price_pipeline_example():
    result = _example("price_pipeline.py")["run_example"]()

    assert result[0] is None
    assert result[-1]["change"] == pytest.approx(4.0 / 101.0)
    assert result[-1]["moving_average"] == pytest.approx(308.0 / 3.0)


def test_window_statistics_example():
    result = _example("window_statistics.py")["run_example"]()

    assert result[0] is None
    assert result[-1] == pytest.approx({"mean": 5.0, "sample_std": 3.0, "median": 5.0})


def test_array_statistics_example():
    [result] = _example("array_statistics.py")["run_example"]()

    np.testing.assert_array_equal(result["cumulative"], [1.0, 3.0, 6.0, 10.0])
    assert result["median"] == 2.5
    assert result["correlation"] == 1.0
