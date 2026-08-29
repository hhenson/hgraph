import runpy
from pathlib import Path

import pytest

from hgraph.test import eval_node


EXAMPLES = Path(__file__).resolve().parents[1] / "examples"


@pytest.mark.parametrize(
    ("module", "graph_name"),
    (
        ("publish_once.py", "local_app"),
        ("subscription_modes.py", "local_live_app"),
        ("subscription_modes.py", "local_replay_app"),
        ("subscription_modes.py", "local_snapshot_app"),
        ("derived_dataset.py", "local_automatic_app"),
        ("derived_dataset.py", "local_explicit_app"),
    ),
)
def test_python_fabric_example_wires_through_public_api(module, graph_name):
    namespace = runpy.run_path(EXAMPLES / module)
    example = namespace[graph_name]

    assert eval_node(example) is None
