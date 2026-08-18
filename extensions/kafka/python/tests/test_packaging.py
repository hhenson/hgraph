import importlib.util
import re
import tomllib
from pathlib import Path

EXTENSION_ROOT = Path(__file__).resolve().parents[2]
REPOSITORY_ROOT = EXTENSION_ROOT.parents[1]
CORE_SDK_REQUIREMENT = "hgraph>=0.8.0"

AUDIT_SPEC = importlib.util.spec_from_file_location(
    "hgraph_kafka_audit_distribution",
    EXTENSION_ROOT / "tools" / "audit_distribution.py",
)
assert AUDIT_SPEC is not None and AUDIT_SPEC.loader is not None
AUDIT_MODULE = importlib.util.module_from_spec(AUDIT_SPEC)
AUDIT_SPEC.loader.exec_module(AUDIT_MODULE)


def _load(path: Path) -> dict:
    return tomllib.loads(path.read_text())


def test_kafka_is_a_separate_workspace_distribution():
    root = _load(REPOSITORY_ROOT / "pyproject.toml")
    project = _load(EXTENSION_ROOT / "pyproject.toml")

    assert root["tool"]["uv"]["workspace"]["members"] == ["extensions/*"]
    assert root["tool"]["uv"]["sources"]["hgraph"] == {"workspace": True}
    assert project["project"]["name"] == "hgraph-kafka"
    assert "uv" not in project.get("tool", {})
    assert project["tool"]["scikit-build"]["wheel"]["py-api"] == "cp312"
    assert project["tool"]["scikit-build"]["wheel"]["packages"] == [
        "python/hgraph_kafka"
    ]
    assert not any((EXTENSION_ROOT / "python" / "hgraph").rglob("*.py"))
    assert (
        REPOSITORY_ROOT / "python" / "hgraph" / "adaptors" / "kafka" / "__init__.py"
    ).is_file()
    assert CORE_SDK_REQUIREMENT in project["project"]["dependencies"]
    assert CORE_SDK_REQUIREMENT in project["build-system"]["requires"]
    assert "extensions/**" in root["tool"]["scikit-build"]["sdist"]["exclude"]


def test_native_extension_is_opt_in_and_standalone_buildable():
    root_cmake = (REPOSITORY_ROOT / "CMakeLists.txt").read_text()
    extension_cmake = (EXTENSION_ROOT / "CMakeLists.txt").read_text()

    assert (
        'option(HGRAPH_BUILD_KAFKA_EXTENSION "Build the first-party Kafka extension" OFF)'
        in root_cmake
    )
    assert "add_subdirectory(extensions/kafka)" in root_cmake
    assert "if(NOT TARGET hgraph::core)" in extension_cmake
    assert "find_package(hgraph CONFIG REQUIRED)" in extension_cmake
    assert "add_library(hgraph::kafka ALIAS hgraph_kafka)" in extension_cmake
    assert "target_compile_definitions(hgraph_kafka PRIVATE NOMINMAX)" in extension_cmake
    assert "install(EXPORT hgraphKafkaTargets" in extension_cmake


def test_windows_wheel_statically_links_non_system_dependencies():
    extension_cmake = (EXTENSION_ROOT / "CMakeLists.txt").read_text()

    assert "HGRAPH_KAFKA_BUILD_PYTHON AND (APPLE OR WIN32)" in extension_cmake
    assert "set(OPENSSL_USE_STATIC_LIBS TRUE)" in extension_cmake
    assert "MSVC AND OPENSSL_USE_STATIC_LIBS" in extension_cmake
    assert "/NODEFAULTLIB:libcrypto.lib" in extension_cmake
    assert "/NODEFAULTLIB:libssl.lib" in extension_cmake
    assert AUDIT_MODULE._unexpected_windows_dependencies(
        {
            "hgraph_stdlib.dll",
            "nanobind-abi3.dll",
            "python3.dll",
            "KERNEL32.dll",
            "VCRUNTIME140.dll",
            "api-ms-win-crt-runtime-l1-1-0.dll",
        }
    ) == []
    assert AUDIT_MODULE._unexpected_windows_dependencies(
        {"libssl-3-x64.dll", "libcrypto-3-x64.dll"}
    ) == ["libcrypto-3-x64.dll", "libssl-3-x64.dll"]
    assert AUDIT_MODULE._windows_dependencies_from_output(
        "  KERNEL32.dll\n    libssl-3-x64.dll\n"
    ) == {"KERNEL32.dll", "libssl-3-x64.dll"}
    assert AUDIT_MODULE._windows_dependencies_from_output(
        "    DLL Name: hgraph_stdlib.dll\n    DLL Name: libcrypto-3-x64.dll\n"
    ) == {"hgraph_stdlib.dll", "libcrypto-3-x64.dll"}


def test_kafka_uses_the_shared_release_version_contract():
    project_version = _load(EXTENSION_ROOT / "pyproject.toml")["project"]["version"]
    kafka_cmake = (EXTENSION_ROOT / "CMakeLists.txt").read_text()
    kafka_match = re.search(r"project\(hgraph_kafka VERSION ([^ ]+)", kafka_cmake)
    core_cmake = (REPOSITORY_ROOT / "CMakeLists.txt").read_text()
    core_match = re.search(
        r"project\(\s*hgraph\s+VERSION\s+(\d+\.\d+\.\d+)", core_cmake
    )

    assert project_version == "0.0.0"
    assert kafka_match is not None
    assert core_match is not None
    assert kafka_match.group(1) == core_match.group(1)


def test_ci_builds_and_tests_separate_kafka_artifacts():
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
        "kafka-distribution-sdist",
        "kafka-distribution-wheel-macos-26",
        "kafka-distribution-wheel-ubuntu-latest",
        "kafka-distribution-wheel-windows-latest",
    ):
        assert artifact in wheel_workflows
    assert "python -m pytest extensions/kafka/python/tests -q" in wheel_workflows
    assert "-DHGRAPH_BUILD_KAFKA_EXTENSION=ON" in native_workflow
    assert "Build and test installed Kafka consumer" in native_workflow
    assert '      - "*.*.*"' in release_workflow
    assert "hgraph-kafka-v_" not in release_workflow
    assert "pattern: kafka-distribution-*" in wheel_workflows
    assert release_workflow.count(
        'python tools/restamp_distribution.py dist "$RELEASE_TAG"'
    ) == 5
    assert "--wheel --no-isolation" in wheel_workflows
    assert "--sdist --no-isolation" in wheel_workflows
    assert "--skip-dependency-check" in wheel_workflows
    for dependency in (
        '"scikit-build-core==1.0.3"',
        '"nanobind==2.13.0"',
        '"ninja==1.13.0"',
        '"pyarrow==25.0.0"',
    ):
        assert dependency in wheel_workflows
    assert '"scikit-build-core>=0.11"' not in wheel_workflows
    assert '"pyarrow>=25,<26"' not in wheel_workflows
