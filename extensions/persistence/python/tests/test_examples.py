import runpy
from pathlib import Path

EXAMPLES = Path(__file__).parents[1] / "examples"


def _example(name):
    return runpy.run_path(EXAMPLES / name)


def test_record_and_replay_example():
    frame, replayed = _example("record_and_replay.py")["run_example"]()

    assert frame.column("value").to_pylist() == [10, 20, 30]
    assert replayed == [10, 20, 30]


def test_keyed_recording_example():
    frame, replayed = _example("keyed_recording.py")["run_example"]()

    assert frame.column("__key_1__").to_pylist() == ["AAPL", "MSFT", "AAPL"]
    assert replayed == [{"AAPL": 10.0}, {"MSFT": 5.0}, {"AAPL": 12.0}]


def test_component_modes_example():
    result = _example("component_modes.py")["run_example"]()

    assert result == {
        "recorded": [11, 21, 23],
        "replayed": [11, 21, 23],
        "comparison": (3, 0),
        "recovered": [11, 110],
    }
