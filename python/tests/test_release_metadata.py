from pathlib import Path

import pytest

from tools.validate_release import parse_release_tag, validate_release


def _cmake_projects(
    tmp_path: Path,
    *,
    core_version: str = "0.8.0",
    kafka_version: str = "0.8.0",
    analytics_version: str = "0.8.0",
) -> tuple[Path, Path, Path]:
    core_path = tmp_path / "CMakeLists.txt"
    core_path.write_text(
        f"project(\n    hgraph\n    VERSION {core_version}\n)\n"
    )
    kafka_path = tmp_path / "kafka-CMakeLists.txt"
    kafka_path.write_text(
        f"project(hgraph_kafka VERSION {kafka_version} LANGUAGES CXX)\n"
    )
    analytics_path = tmp_path / "analytics-CMakeLists.txt"
    analytics_path.write_text(
        f"project(hgraph_analytics VERSION {analytics_version} LANGUAGES CXX)\n"
    )
    return core_path, kafka_path, analytics_path


def _next_patch(version: str) -> str:
    major, minor, patch = map(int, version.split("."))
    return f"{major}.{minor}.{patch + 1}"


def test_shared_release_can_advance_without_bumping_native_api_version(tmp_path: Path):
    native_version = "1.2.3"
    release_version = _next_patch(native_version)
    core_path, kafka_path, analytics_path = _cmake_projects(
        tmp_path,
        core_version=native_version,
        kafka_version=native_version,
    )
    release = validate_release(
        release_version,
        cmake_path=core_path,
        kafka_cmake_path=kafka_path,
        analytics_cmake_path=analytics_path,
        release_exists=lambda _package, _version: False,
    )

    assert release.version == release_version


def test_shared_release_cannot_predate_native_api_version(tmp_path: Path):
    release_version = "1.2.3"
    core_path, kafka_path, analytics_path = _cmake_projects(
        tmp_path,
        core_version=release_version,
        kafka_version=_next_patch(release_version),
    )

    with pytest.raises(ValueError, match="hgraph-kafka.*predates native API version"):
        validate_release(
            release_version,
            cmake_path=core_path,
            kafka_cmake_path=kafka_path,
            analytics_cmake_path=analytics_path,
            release_exists=lambda _package, _version: False,
        )


def test_shared_prerelease_uses_numeric_release_core():
    release = validate_release(
        "0.8.0rc1",
        release_exists=lambda _package, _version: False,
    )

    assert release.packages == ("hgraph", "hgraph-kafka", "hgraph-analytics")
    assert release.version == "0.8.0rc1"
    assert release.core == (0, 8, 0)


def test_shared_release_checks_all_pypi_packages():
    checked: list[tuple[str, str]] = []

    validate_release(
        "0.8.0",
        release_exists=lambda package, version: checked.append((package, version))
        or False,
    )

    assert checked == [
        ("hgraph", "0.8.0"),
        ("hgraph-kafka", "0.8.0"),
        ("hgraph-analytics", "0.8.0"),
    ]


def test_release_rejects_existing_pypi_version():
    with pytest.raises(ValueError, match="hgraph-kafka 0.8.0 already exists on PyPI"):
        validate_release(
            "0.8.0",
            release_exists=lambda package, _version: package == "hgraph-kafka",
        )


@pytest.mark.parametrize(
    "tag",
    ["0.7.9", "v_0.8.0", "hgraph-kafka-v_0.8.0", "0.8", "not-a-tag"],
)
def test_invalid_or_pre_port_core_tags_are_rejected(tag: str):
    if tag == "0.7.9":
        with pytest.raises(ValueError, match="starts at 0.8.0"):
            validate_release(
                tag,
                release_exists=lambda _package, _version: False,
            )
    else:
        with pytest.raises(ValueError, match="release tag must be a bare version"):
            parse_release_tag(tag)
