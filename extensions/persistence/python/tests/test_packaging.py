import importlib.util
import re
import tomllib
from pathlib import Path

EXTENSION_ROOT = Path(__file__).resolve().parents[2]
REPOSITORY_ROOT = EXTENSION_ROOT.parents[1]
CORE_SDK_REQUIREMENT = "hgraph>=0.8.0"

AUDIT_SPEC = importlib.util.spec_from_file_location(
    "hgraph_persistence_audit_distribution",
    EXTENSION_ROOT / "tools" / "audit_distribution.py",
)
assert AUDIT_SPEC is not None and AUDIT_SPEC.loader is not None
AUDIT_MODULE = importlib.util.module_from_spec(AUDIT_SPEC)
AUDIT_SPEC.loader.exec_module(AUDIT_MODULE)


def _load(path: Path) -> dict:
    return tomllib.loads(path.read_text())


def test_persistence_is_a_separate_workspace_distribution():
    root = _load(REPOSITORY_ROOT / "pyproject.toml")
    project = _load(EXTENSION_ROOT / "pyproject.toml")

    assert root["tool"]["uv"]["workspace"]["members"] == ["extensions/*"]
    assert root["tool"]["uv"]["sources"]["hgraph"] == {"workspace": True}
    # hgraph[dataframe] installs the durable record/replay implementation
    # (RFC 0025 checkpoint 5).
    assert "hgraph-persistence>=0.8.0" in root["project"]["optional-dependencies"]["dataframe"]
    assert root["tool"]["uv"]["sources"]["hgraph-persistence"] == {"workspace": True}
    assert project["project"]["name"] == "hgraph-persistence"
    assert "uv" not in project.get("tool", {})
    assert project["tool"]["scikit-build"]["wheel"]["py-api"] == "cp312"
    assert project["tool"]["scikit-build"]["wheel"]["packages"] == [
        "python/hgraph_persistence"
    ]
    assert not any((EXTENSION_ROOT / "python" / "hgraph").rglob("*.py"))
    # The released import paths ride hgraph.adaptors.data_frame's guarded
    # lazy re-export (RFC 0025), not a dedicated core shim package.
    assert (
        REPOSITORY_ROOT
        / "python" / "hgraph" / "adaptors" / "data_frame" / "__init__.py"
    ).is_file()
    assert CORE_SDK_REQUIREMENT in project["project"]["dependencies"]
    assert CORE_SDK_REQUIREMENT in project["build-system"]["requires"]
    assert "extensions/**" in root["tool"]["scikit-build"]["sdist"]["exclude"]


def test_native_extension_is_opt_in_and_standalone_buildable():
    root_cmake = (REPOSITORY_ROOT / "CMakeLists.txt").read_text()
    extension_cmake = (EXTENSION_ROOT / "CMakeLists.txt").read_text()

    assert (
        'option(HGRAPH_BUILD_PERSISTENCE_EXTENSION "Build the first-party persistence extension" OFF)'
        in root_cmake
    )
    assert "add_subdirectory(extensions/persistence)" in root_cmake
    assert "if(NOT TARGET hgraph::core)" in extension_cmake
    assert "find_package(hgraph CONFIG REQUIRED)" in extension_cmake
    assert "add_library(hgraph::persistence ALIAS hgraph_persistence)" in extension_cmake
    assert "install(EXPORT hgraphPersistenceTargets" in extension_cmake
    assert "hgraph_persistence_arrow.cmake" in extension_cmake


def test_persistence_uses_the_shared_release_version_contract():
    project_version = _load(EXTENSION_ROOT / "pyproject.toml")["project"]["version"]
    persistence_cmake = (EXTENSION_ROOT / "CMakeLists.txt").read_text()
    persistence_match = re.search(
        r"project\(hgraph_persistence VERSION ([^ ]+)", persistence_cmake
    )
    core_cmake = (REPOSITORY_ROOT / "CMakeLists.txt").read_text()
    core_match = re.search(
        r"project\(\s*hgraph\s+VERSION\s+(\d+\.\d+\.\d+)", core_cmake
    )

    assert project_version == "0.0.0"
    assert persistence_match is not None
    assert core_match is not None
    assert persistence_match.group(1) == core_match.group(1)


def test_ci_builds_and_tests_separate_persistence_artifacts():
    release_workflow = (
        REPOSITORY_ROOT / ".github/workflows/release-wheels.yml"
    ).read_text()
    wheel_workflows = "\n".join(
        (
            release_workflow,
            (
                REPOSITORY_ROOT / ".github/workflows/release-platform-wheel.yml"
            ).read_text(),
            (
                REPOSITORY_ROOT / ".github/workflows/test-platform-wheel.yml"
            ).read_text(),
        )
    )
    native_workflow = (
        REPOSITORY_ROOT / ".github/workflows/native-cpp.yml"
    ).read_text()

    for artifact in (
        "persistence-distribution-sdist",
        "persistence-distribution-wheel-macos-26",
        "persistence-distribution-wheel-ubuntu-latest",
        "persistence-distribution-wheel-windows-latest",
    ):
        assert artifact in wheel_workflows
    assert "python -m pytest extensions/persistence/python/tests -q" in wheel_workflows
    assert "-DHGRAPH_BUILD_PERSISTENCE_EXTENSION=ON" in native_workflow
    assert "Build and test installed persistence consumer" in native_workflow
    assert "hgraph-persistence-v_" not in release_workflow
    assert "pattern: persistence-distribution-*" in wheel_workflows
    assert release_workflow.count(
        'python tools/restamp_distribution.py dist "$RELEASE_TAG"'
    ) == 5
    for dependency in (
        '"scikit-build-core==1.0.3"',
        '"nanobind==2.13.0"',
        '"ninja==1.13.0"',
        '"pyarrow==25.0.0"',
    ):
        assert dependency in wheel_workflows
    assert "publish-persistence" in release_workflow
    assert "https://pypi.org/p/hgraph-persistence" in release_workflow
