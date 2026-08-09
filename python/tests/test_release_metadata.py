from pathlib import Path

import pytest

from tools.validate_release import parse_release_tag, validate_release


def _cmake_projects(
    tmp_path: Path,
    *,
    core_version: str = "0.8.0",
    kafka_version: str = "0.8.0",
) -> tuple[Path, Path]:
    core_path = tmp_path / "CMakeLists.txt"
    core_path.write_text(
        f"project(\n    hgraph\n    VERSION {core_version}\n)\n"
    )
    kafka_path = tmp_path / "kafka-CMakeLists.txt"
    kafka_path.write_text(
        f"project(hgraph_kafka VERSION {kafka_version} LANGUAGES CXX)\n"
    )
    return core_path, kafka_path


def test_shared_release_requires_matching_core_native_version(tmp_path: Path):
    core_path, kafka_path = _cmake_projects(tmp_path)
    with pytest.raises(ValueError, match="bump project\\(VERSION\\)"):
        validate_release(
            "0.8.1",
            cmake_path=core_path,
            kafka_cmake_path=kafka_path,
            release_exists=lambda _package, _version: False,
        )


def test_shared_release_requires_matching_kafka_native_version(tmp_path: Path):
    core_path, kafka_path = _cmake_projects(tmp_path, kafka_version="0.8.1")
    with pytest.raises(ValueError, match="hgraph-kafka.*bump project\\(VERSION\\)"):
        validate_release(
            "0.8.0",
            cmake_path=core_path,
            kafka_cmake_path=kafka_path,
            release_exists=lambda _package, _version: False,
        )


def test_shared_prerelease_uses_matching_numeric_native_core(tmp_path: Path):
    core_path, kafka_path = _cmake_projects(tmp_path)
    release = validate_release(
        "0.8.0rc1",
        cmake_path=core_path,
        kafka_cmake_path=kafka_path,
        release_exists=lambda _package, _version: False,
    )

    assert release.packages == ("hgraph", "hgraph-kafka")
    assert release.version == "0.8.0rc1"
    assert release.core == (0, 8, 0)


def test_shared_release_checks_both_pypi_packages(tmp_path: Path):
    core_path, kafka_path = _cmake_projects(tmp_path)
    checked: list[tuple[str, str]] = []

    validate_release(
        "0.8.0",
        cmake_path=core_path,
        kafka_cmake_path=kafka_path,
        release_exists=lambda package, version: checked.append((package, version))
        or False,
    )

    assert checked == [("hgraph", "0.8.0"), ("hgraph-kafka", "0.8.0")]


def test_release_rejects_existing_pypi_version(tmp_path: Path):
    core_path, kafka_path = _cmake_projects(tmp_path)
    with pytest.raises(ValueError, match="hgraph-kafka 0.8.0 already exists on PyPI"):
        validate_release(
            "0.8.0",
            cmake_path=core_path,
            kafka_cmake_path=kafka_path,
            release_exists=lambda package, _version: package == "hgraph-kafka",
        )


@pytest.mark.parametrize(
    "tag",
    ["0.7.9", "v_0.8.0", "hgraph-kafka-v_0.8.0", "0.8", "not-a-tag"],
)
def test_invalid_or_pre_port_core_tags_are_rejected(tmp_path: Path, tag: str):
    if tag == "0.7.9":
        core_path, kafka_path = _cmake_projects(
            tmp_path, core_version="0.7.9", kafka_version="0.7.9"
        )
        with pytest.raises(ValueError, match="starts at 0.8.0"):
            validate_release(
                tag,
                cmake_path=core_path,
                kafka_cmake_path=kafka_path,
                release_exists=lambda _package, _version: False,
            )
    else:
        with pytest.raises(ValueError, match="release tag must be a bare version"):
            parse_release_tag(tag)
