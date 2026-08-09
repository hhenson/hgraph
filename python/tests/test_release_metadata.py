from pathlib import Path

import pytest

from tools.validate_release import parse_release_tag, validate_release


def _cmake_project(tmp_path: Path, version: str = "0.8.0") -> Path:
    path = tmp_path / "CMakeLists.txt"
    path.write_text(f"project(\n    hgraph\n    VERSION {version}\n)\n")
    return path


def test_core_release_requires_matching_native_version(tmp_path: Path):
    with pytest.raises(ValueError, match="bump project\\(VERSION\\)"):
        validate_release(
            "v_0.8.1",
            cmake_path=_cmake_project(tmp_path),
            release_exists=lambda _package, _version: False,
        )


def test_core_prerelease_uses_matching_numeric_native_core(tmp_path: Path):
    release = validate_release(
        "v_0.8.0rc1",
        cmake_path=_cmake_project(tmp_path),
        release_exists=lambda _package, _version: False,
    )

    assert release.package == "hgraph"
    assert release.version == "0.8.0rc1"
    assert release.core == (0, 8, 0)


def test_kafka_release_has_an_independent_version(tmp_path: Path):
    release = validate_release(
        "hgraph-kafka-v_1.2.3",
        cmake_path=tmp_path / "missing-CMakeLists.txt",
        release_exists=lambda _package, _version: False,
    )

    assert release.package == "hgraph-kafka"
    assert release.version == "1.2.3"


def test_release_rejects_existing_pypi_version(tmp_path: Path):
    with pytest.raises(ValueError, match="already exists on PyPI"):
        validate_release(
            "v_0.8.0",
            cmake_path=_cmake_project(tmp_path),
            release_exists=lambda _package, _version: True,
        )


@pytest.mark.parametrize("tag", ["v_0.7.9", "v0.8.0", "v_0.8", "not-a-tag"])
def test_invalid_or_pre_port_core_tags_are_rejected(tmp_path: Path, tag: str):
    if tag == "v_0.7.9":
        with pytest.raises(ValueError, match="starts at 0.8.0"):
            validate_release(
                tag,
                cmake_path=_cmake_project(tmp_path, "0.7.9"),
                release_exists=lambda _package, _version: False,
            )
    else:
        with pytest.raises(ValueError, match="release tag must match"):
            parse_release_tag(tag)
