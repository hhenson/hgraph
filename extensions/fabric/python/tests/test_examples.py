import runpy
from pathlib import Path

import _hgraph
import pytest

from hgraph.test import eval_node


EXAMPLES = Path(__file__).resolve().parents[1] / "examples"


@pytest.mark.parametrize(
    ("module", "graph_name"),
    (
        ("publish_once.py", "local_app"),
        ("subscription_modes.py", "local_live_app"),
        ("subscription_modes.py", "local_replay_app"),
        ("derived_dataset.py", "local_automatic_app"),
        ("derived_dataset.py", "local_explicit_app"),
    ),
)
def test_python_fabric_example_wires_through_public_api(module, graph_name):
    namespace = runpy.run_path(EXAMPLES / module)
    example = namespace[graph_name]

    assert eval_node(example) is None


def test_python_load_as_of_example_runs_through_public_api():
    namespace = runpy.run_path(EXAMPLES / "load_as_of.py")

    previous = _hgraph.polars_frames()
    _hgraph.set_polars_frames(False)
    try:
        loaded = namespace["local_load_example"]()
    finally:
        _hgraph.set_polars_frames(previous)

    assert loaded.to_pydict() == {
        "symbol": ["AAPL", "MSFT"],
        "price": [201.5, 415.0],
    }
