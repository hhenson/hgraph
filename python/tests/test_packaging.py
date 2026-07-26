"""Packaging contract checks for the optional Python bridge."""

from pathlib import Path
import re
import tomllib

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
from packaging.version import Version
from trove_classifiers import classifiers as valid_classifiers


ROOT = Path(__file__).resolve().parents[2]
PYARROW_REQUIREMENT = "pyarrow>=24,<25"
NANOBIND_REQUIREMENT = "nanobind==2.13.0"
SUPPORTED_PYTHON_MINIMUM = ">=3.12"
STABLE_ABI_TAG = "cp312"
DISTRIBUTION_NAME = "hg_cpp"
RELEASE_CANDIDATE = Version("0.4.0rc1")


def load_project():
    return tomllib.loads((ROOT / "pyproject.toml").read_text())


def test_pyarrow_build_and_runtime_requirements_share_the_supported_abi():
    project = load_project()

    build_requires = project["build-system"]["requires"]
    runtime_requires = project["project"]["dependencies"]

    assert PYARROW_REQUIREMENT in build_requires
    assert PYARROW_REQUIREMENT in runtime_requires


def test_nanobind_build_and_sdk_headers_use_one_exact_runtime_abi():
    project = load_project()

    assert NANOBIND_REQUIREMENT in project["build-system"]["requires"]
    assert NANOBIND_REQUIREMENT in project["project"]["optional-dependencies"]["python"]
    assert NANOBIND_REQUIREMENT in project["project"]["optional-dependencies"]["dev"]

    cmake = (ROOT / "CMakeLists.txt").read_text()
    assert "find_dependency(nanobind ${nanobind_VERSION} EXACT CONFIG)" in cmake


def test_windows_wheel_installs_all_linked_pyarrow_runtimes():
    python_cmake = (ROOT / "python/CMakeLists.txt").read_text()

    assert '"${HGRAPH_PYARROW_ARROW_RUNTIME}"' in python_cmake
    assert '"${HGRAPH_PYARROW_COMPUTE_RUNTIME}"' in python_cmake
    assert '"${HGRAPH_PYARROW_ACERO_RUNTIME}"' in python_cmake


def test_supported_python_versions_are_declared():
    project = load_project()["project"]

    assert project["requires-python"] == SUPPORTED_PYTHON_MINIMUM
    assert "Programming Language :: Python :: 3.12" in project["classifiers"]
    assert "Programming Language :: Python :: 3.13" in project["classifiers"]
    assert "Programming Language :: Python :: 3.14" in project["classifiers"]


def test_pypi_classifiers_are_valid():
    declared_classifiers = set(load_project()["project"]["classifiers"])
    invalid_classifiers = declared_classifiers - valid_classifiers

    assert not invalid_classifiers, f"invalid PyPI classifiers: {sorted(invalid_classifiers)}"


def test_release_metadata_is_consistent():
    project = load_project()["project"]
    version = Version(project["version"])

    assert project["name"] == DISTRIBUTION_NAME
    assert version == RELEASE_CANDIDATE
    assert version.is_prerelease

    cmake = (ROOT / "CMakeLists.txt").read_text()
    cmake_version = re.search(r"project\(\s*hgraph\s+VERSION\s+(\d+\.\d+\.\d+)", cmake)
    assert cmake_version is not None
    assert Version(cmake_version.group(1)) == Version(version.base_version)

    sphinx = (ROOT / "docs/source/conf.py").read_text()
    sphinx_version = re.search(r'^release = "([^"]+)"$', sphinx, re.MULTILINE)
    assert sphinx_version is not None
    assert Version(sphinx_version.group(1)) == version


def test_wheel_targets_the_python_312_stable_abi():
    scikit_build = load_project()["tool"]["scikit-build"]

    assert scikit_build["wheel"]["py-api"] == STABLE_ABI_TAG


def test_wheel_uses_shared_runtime_for_downstream_native_extensions():
    cmake_defines = load_project()["tool"]["scikit-build"]["cmake"]["define"]

    assert cmake_defines["HGRAPH_BUILD_SHARED"] == "ON"


def test_source_distribution_excludes_private_release_evidence():
    scikit_build = load_project()["tool"]["scikit-build"]
    excluded = scikit_build["sdist"]["exclude"]

    assert "reports/**" in excluded
    assert "ext/**" in excluded
    assert "benchmarks/.venv*/**" in excluded
    assert "benchmarks/results/**" in excluded


def test_full_suite_dependencies_include_the_dataframe_runtime():
    test_dependencies = load_project()["project"]["optional-dependencies"]["test"]

    assert "polars[rtcompat]>=1.32" in test_dependencies


def _dependency_names(requirements):
    return {
        canonicalize_name(Requirement(requirement).name)
        for requirement in requirements
    }


def test_adaptor_extras_include_their_runtime_dependencies():
    extras = load_project()["project"]["optional-dependencies"]

    assert "polars>=1.32" in extras["sql"]
    assert "polars>=1.32" in extras["delta"]
    assert (
        _dependency_names(extras["sql"]) | _dependency_names(extras["delta"])
    ) <= _dependency_names(extras["test"])


def test_full_suite_gate_installs_the_wheel_test_extra():
    instructions = (ROOT / "AGENTS.md").read_text()

    assert 'wheel_path="$(find "$wheel_dir"' in instructions
    assert (
        'uv pip install --python "$test_env/bin/python" '
        '"${wheel_path}[test]"'
    ) in instructions


def test_release_workflow_targets_supported_platforms():
    workflow = (ROOT / ".github/workflows/build.yml").read_text()

    assert "macos-15-intel" not in workflow
    assert "          - os: macos-26" in workflow
    assert "          - macos-26" in workflow
    assert "CMAKE_OSX_DEPLOYMENT_TARGET=15.0" in workflow
    assert "quay.io/pypa/manylinux_2_28_x86_64:latest" in workflow
    assert "Build manylinux 2.28 / GCC 14 wheel" in workflow
    assert "--plat manylinux_2_28_x86_64" in workflow
    assert "--exclude libarrow.so.2400" in workflow
    assert "--exclude libarrow_compute.so.2400" in workflow
    assert "--exclude libarrow_acero.so.2400" in workflow
    assert "--exclude libhgraph_runtime.so" in workflow
    assert "--exclude libhgraph_wiring.so" in workflow
    assert "--exclude libhgraph_stdlib.so" in workflow
    assert "--exclude libnanobind-abi3.so" in workflow
    assert "tests/python_extension_consumer/check.py" in workflow
    assert "libarrow-acero=24" in workflow
    assert "g++-13 --version" in workflow
    assert "runs-on: ubuntu-24.04" in workflow
    # Windows builds with Ninja over cl (vcvars via msvc-dev-cmd): the
    # Visual Studio generator ignores CMAKE_CXX_COMPILER_LAUNCHER, so the
    # shared compiler cache never fires under MSBuild.
    assert "Visual Studio 18 2026" not in workflow
    assert "ilammy/msvc-dev-cmd@v1" in workflow
    # Embedded (/Z7) debug info only for debug-carrying configurations: the
    # Release wheel emits no debug info, and sccache cannot cache /Zi.
    assert (
        "CMAKE_MSVC_DEBUG_INFORMATION_FORMAT="
        "$<$<CONFIG:Debug,RelWithDebInfo>:Embedded>" in workflow
    )
    assert 'python-version: "3.12"' in workflow
    assert '- "3.13"' in workflow
    assert '- "3.14"' in workflow


def test_release_workflow_reuses_tested_commit_artifacts():
    workflow = (ROOT / ".github/workflows/build.yml").read_text()

    assert "Find tested distributions for this commit" in workflow
    assert "head_sha: context.sha" in workflow
    assert "run.head_sha !== context.sha" in workflow
    assert "github.rest.actions.listWorkflowRunArtifacts" in workflow
    assert "needs.reuse-build.outputs.run-id == ''" in workflow
    assert "needs.reuse-build.outputs.run-id || github.run_id" in workflow


def test_release_workflow_audits_distribution_contents():
    workflow = (ROOT / ".github/workflows/build.yml").read_text()

    assert workflow.count("tools/audit_distribution.py") == 3
    assert '"dist/*.whl"' in workflow
    assert '"dist/*.tar.gz"' in workflow


def main():
    test_pyarrow_build_and_runtime_requirements_share_the_supported_abi()
    test_windows_wheel_installs_all_linked_pyarrow_runtimes()
    test_supported_python_versions_are_declared()
    test_pypi_classifiers_are_valid()
    test_release_metadata_is_consistent()
    test_wheel_targets_the_python_312_stable_abi()
    test_wheel_uses_shared_runtime_for_downstream_native_extensions()
    test_source_distribution_excludes_private_release_evidence()
    test_full_suite_dependencies_include_the_dataframe_runtime()
    test_adaptor_extras_include_their_runtime_dependencies()
    test_full_suite_gate_installs_the_wheel_test_extra()
    test_release_workflow_targets_supported_platforms()
    test_release_workflow_reuses_tested_commit_artifacts()
    test_release_workflow_audits_distribution_contents()
    print("PASS test_pyarrow_build_and_runtime_requirements_share_the_supported_abi")
    print("PASS test_windows_wheel_installs_all_linked_pyarrow_runtimes")
    print("PASS test_supported_python_versions_are_declared")
    print("PASS test_pypi_classifiers_are_valid")
    print("PASS test_release_metadata_is_consistent")
    print("PASS test_wheel_targets_the_python_312_stable_abi")
    print("PASS test_wheel_uses_shared_runtime_for_downstream_native_extensions")
    print("PASS test_source_distribution_excludes_private_release_evidence")
    print("PASS test_full_suite_dependencies_include_the_dataframe_runtime")
    print("PASS test_adaptor_extras_include_their_runtime_dependencies")
    print("PASS test_full_suite_gate_installs_the_wheel_test_extra")
    print("PASS test_release_workflow_targets_supported_platforms")
    print("PASS test_release_workflow_reuses_tested_commit_artifacts")
    print("PASS test_release_workflow_audits_distribution_contents")


if __name__ == "__main__":
    main()
