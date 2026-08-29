import runpy
from pathlib import Path

import _hgraph
import pytest

from hgraph.test import use_wiring


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

    wiring = _hgraph.Wiring()
    with use_wiring(wiring):
        example()
    wiring.run()
