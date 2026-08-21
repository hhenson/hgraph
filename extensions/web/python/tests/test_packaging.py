import importlib.util
import re
import tomllib
from pathlib import Path

EXTENSION_ROOT = Path(__file__).resolve().parents[2]
REPOSITORY_ROOT = EXTENSION_ROOT.parents[1]
CORE_SDK_REQUIREMENT = "hgraph>=0.8.0"

AUDIT_SPEC = importlib.util.spec_from_file_location(
    "hgraph_web_audit_distribution",
    EXTENSION_ROOT / "tools" / "audit_distribution.py",
)
assert AUDIT_SPEC is not None and AUDIT_SPEC.loader is not None
AUDIT_MODULE = importlib.util.module_from_spec(AUDIT_SPEC)
AUDIT_SPEC.loader.exec_module(AUDIT_MODULE)


def _load(path: Path) -> dict:
    return tomllib.loads(path.read_text())


def test_web_is_a_separate_workspace_distribution():
    root = _load(REPOSITORY_ROOT / "pyproject.toml")
    project = _load(EXTENSION_ROOT / "pyproject.toml")

    assert root["tool"]["uv"]["workspace"]["members"] == ["extensions/*"]
    assert root["tool"]["uv"]["sources"]["hgraph"] == {"workspace": True}
    assert project["project"]["name"] == "hgraph-web"
    assert "uv" not in project.get("tool", {})
    assert project["tool"]["scikit-build"]["wheel"]["py-api"] == "cp312"
    assert project["tool"]["scikit-build"]["wheel"]["packages"] == [
        "python/hgraph_web"
    ]
    assert not any((EXTENSION_ROOT / "python" / "hgraph").rglob("*.py"))
    assert (
        REPOSITORY_ROOT / "python" / "hgraph" / "adaptors" / "web" / "__init__.py"
    ).is_file()
    assert CORE_SDK_REQUIREMENT in project["project"]["dependencies"]
    assert CORE_SDK_REQUIREMENT in project["build-system"]["requires"]
    assert "extensions/**" in root["tool"]["scikit-build"]["sdist"]["exclude"]


def test_the_core_tornado_adaptor_is_left_in_place():
    tornado_package = (
        REPOSITORY_ROOT / "python" / "hgraph" / "adaptors" / "tornado" / "__init__.py"
    )
    shim = (
        REPOSITORY_ROOT / "python" / "hgraph" / "adaptors" / "web" / "__init__.py"
    ).read_text()

    assert tornado_package.is_file()
    assert "hgraph_web" in shim
    assert "pip install hgraph-web" in shim


def test_native_extension_is_opt_in_and_standalone_buildable():
    root_cmake = (REPOSITORY_ROOT / "CMakeLists.txt").read_text()
    extension_cmake = (EXTENSION_ROOT / "CMakeLists.txt").read_text()

    assert (
        'option(HGRAPH_BUILD_WEB_EXTENSION "Build the first-party web extension" OFF)'
        in root_cmake
    )
    assert "add_subdirectory(extensions/web)" in root_cmake
    assert "if(NOT TARGET hgraph::core)" in extension_cmake
    assert "find_package(hgraph CONFIG REQUIRED)" in extension_cmake
    assert "add_library(hgraph::web ALIAS hgraph_web)" in extension_cmake
    assert (
        "target_compile_definitions(hgraph_web PRIVATE NOMINMAX WIN32_LEAN_AND_MEAN)"
        in extension_cmake
    )
    assert "install(EXPORT hgraphWebTargets" in extension_cmake


def test_third_party_libraries_stay_private_to_the_extension():
    core_cmake = (REPOSITORY_ROOT / "CMakeLists.txt").read_text()
    public_headers = list((EXTENSION_ROOT / "include").rglob("*.h"))

    # RFC 0024: core builds without curl, Boost, nghttp2, or OpenSSL, and the
    # public headers carry only schemas, descriptors, and wiring helpers.
    for dependency in ("CURL", "Boost", "nghttp2"):
        assert dependency not in core_cmake
    assert public_headers
    for header in public_headers:
        contents = header.read_text()
        for third_party in ("<boost/", "<curl/", "<nghttp2", "<openssl/"):
            assert third_party not in contents, (header, third_party)


def test_windows_wheel_imports_only_system_and_core_libraries():
    # The CMake-side static-OpenSSL guards (OPENSSL_USE_STATIC_LIBS, the MSVC
    # /NODEFAULTLIB:libcrypto.lib and /NODEFAULTLIB:libssl.lib exclusions) land
    # with the TLS transports that link OpenSSL; the skeleton links only
    # hgraph::core. The audit's allowlist is the invariant they must satisfy:
    # nothing beyond the core runtime and Windows system libraries may be
    # imported, so a shared curl or OpenSSL is rejected either way.
    assert AUDIT_MODULE._unexpected_windows_dependencies(
        {
            "hgraph_stdlib.dll",
            "nanobind-abi3.dll",
            "python3.dll",
            "KERNEL32.dll",
            "WS2_32.dll",
            "MSWSOCK.dll",
            "VCRUNTIME140.dll",
            "api-ms-win-crt-runtime-l1-1-0.dll",
        }
    ) == []
    assert AUDIT_MODULE._unexpected_windows_dependencies(
        {"libssl-3-x64.dll", "libcrypto-3-x64.dll", "libcurl.dll"}
    ) == ["libcrypto-3-x64.dll", "libcurl.dll", "libssl-3-x64.dll"]
    assert AUDIT_MODULE._windows_dependencies_from_output(
        "  KERNEL32.dll\n    libssl-3-x64.dll\n"
    ) == {"KERNEL32.dll", "libssl-3-x64.dll"}
    assert AUDIT_MODULE._windows_dependencies_from_output(
        "    DLL Name: hgraph_stdlib.dll\n    DLL Name: libcurl.dll\n"
    ) == {"hgraph_stdlib.dll", "libcurl.dll"}


def test_web_uses_the_shared_release_version_contract():
    project_version = _load(EXTENSION_ROOT / "pyproject.toml")["project"]["version"]
    web_cmake = (EXTENSION_ROOT / "CMakeLists.txt").read_text()
    web_match = re.search(r"project\(hgraph_web VERSION ([^ ]+)", web_cmake)
    core_cmake = (REPOSITORY_ROOT / "CMakeLists.txt").read_text()
    core_match = re.search(
        r"project\(\s*hgraph\s+VERSION\s+(\d+\.\d+\.\d+)", core_cmake
    )

    assert project_version == "0.0.0"
    assert web_match is not None
    assert core_match is not None
    assert web_match.group(1) == core_match.group(1)


def test_ci_builds_and_tests_separate_web_artifacts():
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
        "web-distribution-sdist",
        "web-distribution-wheel-macos-26",
        "web-distribution-wheel-ubuntu-latest",
        "web-distribution-wheel-windows-latest",
    ):
        assert artifact in wheel_workflows
    assert "python -m pytest extensions/web/python/tests -q" in wheel_workflows
    assert "-DHGRAPH_BUILD_WEB_EXTENSION=ON" in native_workflow
    assert "Build and test installed web consumer" in native_workflow
    assert '      - "*.*.*"' in release_workflow
    assert "hgraph-web-v_" not in release_workflow
    assert "pattern: web-distribution-*" in wheel_workflows
    assert release_workflow.count(
        'python tools/restamp_distribution.py dist "$RELEASE_TAG"'
    ) == 6
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
