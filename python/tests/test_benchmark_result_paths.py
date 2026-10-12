"""The benchmark orchestrator confines result artefacts to its results tree."""
import importlib.util
import os
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("benchmark_orchestrate", ROOT / "benchmarks/orchestrate.py")
orchestrate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(orchestrate)


@pytest.fixture
def results_root(tmp_path, monkeypatch):
    root = tmp_path / "results"
    root.mkdir()
    monkeypatch.setattr(orchestrate, "RESULTS_DIR", root)
    return root


def test_result_path_keeps_the_callers_spelling(results_root):
    resolved = orchestrate._validated_result_path(results_root / "MyCache.json")
    assert resolved.name == "MyCache.json"
    assert resolved.parent == Path(os.path.realpath(results_root))


def test_result_path_rejects_the_root_its_siblings_and_symlink_escapes(results_root, tmp_path):
    (tmp_path / "results-private").mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    rejected = [results_root, tmp_path / "results-private" / "x.json", tmp_path / "x.json"]
    escape = results_root / "escape"
    try:
        escape.symlink_to(outside, target_is_directory=True)
    except OSError:
        pass  # symbolic links need a privilege on some Windows accounts
    else:
        rejected.append(escape / "x.json")
    for path in rejected:
        with pytest.raises(ValueError):
            orchestrate._validated_result_path(path)


@pytest.mark.skipif(sys.platform != "win32", reason="realpath spells existing components as the file system does only on Windows")
def test_result_path_accepts_another_spelling_of_an_existing_results_directory(results_root):
    other_case = results_root.parent / results_root.name.upper() / "x.json"
    resolved = orchestrate._validated_result_path(other_case)
    assert resolved.name == "x.json"
    assert resolved.parent == Path(os.path.realpath(results_root))
