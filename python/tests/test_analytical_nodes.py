import subprocess
import sys

import hgraph
from hgraph.nodes import (
    np_quantile,
    np_rolling_window,
    np_std,
    pct_change,
    rolling_average,
    rolling_window,
)
from hgraph.test import eval_node


def test_migrated_analytics_names_are_deprecated_compatibility_graphs():
    root_names = (
        "as_array",
        "clip",
        "corrcoef",
        "count",
        "cumsum",
        "diff",
        "ewma",
        "get_item",
        "np_std",
        "pct_change",
        "quantile",
        "resample",
        "rolling_average",
        "std",
        "var",
    )
    node_aliases = (
        np_quantile,
        np_rolling_window,
        np_std,
        pct_change,
        rolling_average,
    )

    assert all(getattr(hgraph, name)._deprecated for name in root_names)
    assert all(alias._deprecated for alias in node_aliases)


def test_importing_core_does_not_import_optional_analytics_distribution():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import hgraph; "
            "assert 'hgraph_analytics' not in sys.modules",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_public_window_alias_remains_wirable():
    result = eval_node(rolling_window, [1, 2, 3], 2)
    assert result[0] is None
    assert result[1]["buffer"] == (1, 2)
    assert result[2]["buffer"] == (2, 3)
