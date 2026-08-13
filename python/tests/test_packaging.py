"""Packaging contract checks for the optional Python bridge."""

import re
import tomllib
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
from packaging.version import Version
from trove_classifiers import classifiers as valid_classifiers

ROOT = Path(__file__).resolve().parents[2]
PYARROW_REQUIREMENT = "pyarrow>=25,<26"
NANOBIND_REQUIREMENT = "nanobind==2.13.0"
SUPPORTED_PYTHON_MINIMUM = ">=3.12"
STABLE_ABI_TAG = "cp312"
DISTRIBUTION_NAME = "hgraph"
UNTAGGED_VERSION = Version("0.0.0")
PORT_VERSION = Version("0.8.0")


def load_project():
    return tomllib.loads((ROOT / "pyproject.toml").read_text())


def workflow_job(workflow, name):
    match = re.search(
        rf"(?ms)^  {re.escape(name)}:\n.*?(?=^  [A-Za-z0-9_-]+:\n|\Z)",
        workflow,
    )
    assert match is not None, name
    return match.group(0)


def test_pyarrow_build_and_runtime_requirements_share_the_supported_abi():
    project = load_project()

    build_requires = project["build-system"]["requires"]
    runtime_requires = project["project"]["dependencies"]

    assert PYARROW_REQUIREMENT in build_requires
    assert PYARROW_REQUIREMENT in runtime_requires

    cmake = (ROOT / "CMakeLists.txt").read_text()
    conan = (ROOT / "conanfile.py").read_text()
    assert 'set(HGRAPH_PYARROW_ABI_MAJOR "25"' in cmake
    assert 'self.requires("arrow/25.0.0")' in conan


def test_nanobind_build_and_sdk_headers_use_one_exact_runtime_abi():
    project = load_project()

    assert NANOBIND_REQUIREMENT in project["build-system"]["requires"]
    assert NANOBIND_REQUIREMENT in project["project"]["optional-dependencies"]["python"]
    assert NANOBIND_REQUIREMENT in project["project"]["optional-dependencies"]["dev"]

    cmake = (ROOT / "CMakeLists.txt").read_text()
    assert "find_dependency(nanobind ${nanobind_VERSION} EXACT CONFIG)" in cmake


def test_installed_sdk_exports_required_msvc_source_contract():
    cmake = (ROOT / "CMakeLists.txt").read_text()
    graph_wiring = (ROOT / "include/hgraph/types/graph_wiring.h").read_text()

    assert "target_compile_options(hgraph_options INTERFACE /W4 /permissive- /utf-8)" in cmake
    assert "$<INSTALL_INTERFACE:FMT_HEADER_ONLY>" in cmake
    assert "FMT_LIB_EXPORT" not in cmake
    assert "FMT_SHARED" not in cmake
    assert "class HGRAPH_EXPORT ServiceImplementationScope" in graph_wiring


def test_windows_wheel_installs_all_linked_pyarrow_runtimes():
    python_cmake = (ROOT / "python/CMakeLists.txt").read_text()

    assert '"${HGRAPH_PYARROW_ARROW_RUNTIME}"' in python_cmake
    assert '"${HGRAPH_PYARROW_COMPUTE_RUNTIME}"' in python_cmake
    assert '"${HGRAPH_PYARROW_ACERO_RUNTIME}"' in python_cmake
    # RFC 0016 links Parquet; a linked DLL left out of the wheel is an
    # ImportError on the user's machine, not a build failure here.
    assert '"${HGRAPH_PYARROW_PARQUET_RUNTIME}"' in python_cmake


def test_wheel_installs_generated_native_stub_and_typed_package_marker():
    python_cmake = (ROOT / "python/CMakeLists.txt").read_text()

    assert "nanobind_add_stub(hgraph_python_native_stub" in python_cmake
    assert 'MODULE _hgraph' in python_cmake
    assert 'install(FILES "${_hgraph_stub}" DESTINATION .)' in python_cmake
    assert 'hgraph/py.typed" DESTINATION hgraph' in python_cmake
    assert (ROOT / "python/hgraph/py.typed").is_file()
    assert (ROOT / "python/hgraph/_operator_typing.pyi").is_file()


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


def test_release_metadata_uses_untagged_sentinel():
    project = load_project()["project"]
    version = Version(project["version"])

    assert project["name"] == DISTRIBUTION_NAME
    assert version == UNTAGGED_VERSION

    cmake = (ROOT / "CMakeLists.txt").read_text()
    cmake_version = re.search(r"project\(\s*hgraph\s+VERSION\s+(\d+\.\d+\.\d+)", cmake)
    assert cmake_version is not None
    assert Version(cmake_version.group(1)) == PORT_VERSION

    conan = (ROOT / "conanfile.py").read_text()
    assert "MINIMUM_RELEASE_VERSION = (0, 8, 0)" in conan

    sphinx = (ROOT / "docs/source/conf.py").read_text()
    sphinx_version = re.search(r'^release = "([^"]+)"$', sphinx, re.MULTILINE)
    assert sphinx_version is not None
    assert Version(sphinx_version.group(1)) == Version(cmake_version.group(1))

    workflow = (ROOT / ".github/workflows/release-wheels.yml").read_text()
    assert "Restamp distributions to the tag version" in workflow
    assert '      - "*.*.*"' in workflow
    assert "hgraph-kafka-v_" not in workflow
    assert "v_*.*.*" not in workflow
    assert workflow.count(
        'python tools/restamp_distribution.py dist "$RELEASE_TAG"'
    ) == 3
    assert 'python tools/validate_release.py "$RELEASE_TAG"' in workflow


def test_wheel_targets_the_python_312_stable_abi():
    scikit_build = load_project()["tool"]["scikit-build"]

    assert scikit_build["wheel"]["py-api"] == STABLE_ABI_TAG


def test_repository_enables_polars_compatibility_without_a_cpp_switch():
    feature_config = (ROOT / "hgraph_features.yaml").read_text().splitlines()
    runtime_dependencies = load_project()["project"]["dependencies"]
    runtime_dependency_names = {
        canonicalize_name(Requirement(requirement).name)
        for requirement in runtime_dependencies
    }

    assert feature_config == ["features:", "  polars_frames: true"]
    assert "polars>=1.32" in runtime_dependencies
    assert {"polars", "pyyaml"} <= runtime_dependency_names


def test_wheel_uses_shared_runtime_for_downstream_native_extensions():
    cmake_defines = load_project()["tool"]["scikit-build"]["cmake"]["define"]

    assert cmake_defines["HGRAPH_BUILD_SHARED"] == "ON"


def test_source_distribution_excludes_private_release_evidence():
    scikit_build = load_project()["tool"]["scikit-build"]
    excluded = scikit_build["sdist"]["exclude"]

    assert "reports/**" in excluded
    assert "ext/**" in excluded
    assert "extensions/**" in excluded
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
    workflow = (ROOT / ".github/workflows/release-wheels.yml").read_text()
    platform_workflow = (
        ROOT / ".github/workflows/release-platform-wheel.yml"
    ).read_text()
    test_workflow = (
        ROOT / ".github/workflows/test-platform-wheel.yml"
    ).read_text()
    native_workflow = (ROOT / ".github/workflows/native-cpp.yml").read_text()
    combined_workflow = "\n".join(
        (workflow, platform_workflow, test_workflow, native_workflow)
    )
    nightly_workflow = (
        ROOT / ".github/workflows/parity-nightly.yml"
    ).read_text()

    assert "macos-15-intel" not in combined_workflow
    assert "      os: macos-26" in workflow
    assert "CMAKE_OSX_DEPLOYMENT_TARGET=15.0" in combined_workflow
    assert "quay.io/pypa/manylinux_2_28_x86_64:latest" in workflow
    assert "Build manylinux 2.28 / GCC 14 wheel" in workflow
    assert "--plat manylinux_2_28_x86_64" in workflow
    assert "--exclude libarrow.so.2500" in workflow
    assert "--exclude libarrow_compute.so.2500" in workflow
    assert "--exclude libarrow_acero.so.2500" in workflow
    assert "--exclude libhgraph_runtime.so" in workflow
    assert "--exclude libhgraph_wiring.so" in workflow
    assert "--exclude libhgraph_stdlib.so" in workflow
    assert "--exclude libnanobind-abi3.so" in workflow
    assert "tests/python_extension_consumer/check.py" in combined_workflow
    assert "libarrow-acero=25" in native_workflow
    for linux_workflow in (nightly_workflow, native_workflow):
        assert 'GCC_VERSION: "14"' in linux_workflow
        assert 'command -v "gcc-$GCC_VERSION"' in linux_workflow
        assert 'command -v "g++-$GCC_VERSION"' in linux_workflow
        assert '-eq "$GCC_VERSION"' in linux_workflow
        assert "gcc-13" not in linux_workflow
        assert "g++-13" not in linux_workflow
    assert "runs-on: ubuntu-24.04" in workflow
    # Windows builds with Ninja over cl (vcvars via msvc-dev-cmd): the
    # Visual Studio generator ignores CMAKE_CXX_COMPILER_LAUNCHER, so the
    # shared compiler cache never fires under MSBuild.
    assert "Visual Studio 18 2026" not in combined_workflow
    assert (
        "ilammy/msvc-dev-cmd@"
        "0b201ec74fa43914dc39ae48a89fd1d8cb592756 # v1"
        in platform_workflow
    )
    # Embedded (/Z7) debug info only for debug-carrying configurations: the
    # Release wheel emits no debug info, and sccache cannot cache /Zi.
    assert (
        "CMAKE_MSVC_DEBUG_INFORMATION_FORMAT="
        "$<$<CONFIG:Debug,RelWithDebInfo>:Embedded>" in workflow
    )
    assert '- "3.12"' in test_workflow
    assert '- "3.13"' in test_workflow
    assert '- "3.14"' in test_workflow
    assert test_workflow.count("uv run --no-sync --no-build") == 5


def test_platform_wheel_builds_use_stable_cacheable_paths():
    platform_workflow = (
        ROOT / ".github/workflows/release-platform-wheel.yml"
    ).read_text()

    assert "--wheel --no-isolation --skip-dependency-check" in platform_workflow
    assert '"-Cbuild-dir=.ci-build/core/{wheel_tag}"' in platform_workflow
    assert '"-Cbuild-dir=.ci-build/kafka/{wheel_tag}"' in platform_workflow
    assert "Install Kafka wheel build tools" not in platform_workflow
    for dependency in (
        '"scikit-build-core==1.0.3"',
        '"nanobind==2.13.0"',
        '"pyarrow==25.0.0"',
    ):
        assert platform_workflow.index(dependency) < platform_workflow.index(
            "Build stable ABI wheel"
        )


def test_platform_wheel_tests_start_after_only_their_matching_build():
    workflow = (ROOT / ".github/workflows/release-wheels.yml").read_text()
    dependencies = {
        "test-windows-wheel": "build-windows-wheel",
        "test-macos-wheel": "build-macos-wheel",
        "test-linux-wheel": "build-linux-wheel",
    }

    assert "  test-wheel:\n" not in workflow
    for test_name, matching_build in dependencies.items():
        test_job = workflow_job(workflow, test_name)
        assert "uses: ./.github/workflows/test-platform-wheel.yml" in test_job
        assert f"      - {matching_build}\n" in test_job
        for other_build in set(dependencies.values()) - {matching_build}:
            assert other_build not in test_job

    for build_name in ("build-windows-wheel", "build-macos-wheel"):
        build_job = workflow_job(workflow, build_name)
        assert "uses: ./.github/workflows/release-platform-wheel.yml" in build_job

    for publish_name in ("publish", "publish-kafka"):
        publish_job = workflow_job(workflow, publish_name)
        for test_name in dependencies:
            assert f"needs.{test_name}.result == 'success'" in publish_job
            assert f"      - {test_name}\n" in publish_job


def test_native_cpp_validation_is_independent_of_wheel_publication():
    release_workflow = (ROOT / ".github/workflows/release-wheels.yml").read_text()
    native_workflow = (ROOT / ".github/workflows/native-cpp.yml").read_text()

    assert "  native-cpp:\n" not in release_workflow
    assert "  native-install:\n" not in release_workflow
    assert "needs.native-cpp.result" not in release_workflow
    assert "needs.native-install.result" not in release_workflow
    assert "  native-cpp:\n" in native_workflow
    assert "  native-shared-install:\n" in native_workflow

    shared_job = workflow_job(native_workflow, "native-shared-install")
    assert "-DHGRAPH_BUILD_SHARED=ON" in shared_job
    assert "Verify IPO is enabled" in shared_job
    assert "-flto(=auto)?" in shared_job
    assert "Run optimized shared C++ tests" in shared_job
    assert "Build and test installed-package consumer" in shared_job
    assert "Build and test installed Kafka consumer" in shared_job
    assert "Build and test installed analytics consumer" in shared_job

    for publish_name in ("publish", "publish-kafka", "publish-analytics"):
        publish_job = workflow_job(release_workflow, publish_name)
        assert "native-cpp" not in publish_job
        assert "native-install" not in publish_job
        assert "native-shared-install" not in publish_job


def test_workflow_actions_are_pinned_to_immutable_commits():
    workflows = "\n".join(
        path.read_text() for path in (ROOT / ".github/workflows").glob("*.yml")
    )
    action_refs = re.findall(r"(?m)^\s*(?:-\s*)?uses:\s*[^@\s]+@([^\s#]+)", workflows)

    assert action_refs
    assert all(re.fullmatch(r"[0-9a-f]{40}", ref) for ref in action_refs)


def test_release_workflow_reuses_tested_commit_artifacts():
    workflow = (ROOT / ".github/workflows/release-wheels.yml").read_text()

    assert "Find tested distributions for this commit" in workflow
    assert "head_sha: context.sha" in workflow
    assert "run.head_sha !== context.sha" in workflow
    assert "github.rest.actions.listWorkflowRunArtifacts" in workflow
    assert "needs.reuse-build.outputs.run-id == ''" in workflow
    assert "needs.reuse-build.outputs.run-id || github.run_id" in workflow


def test_release_workflow_audits_distribution_contents():
    workflow = (ROOT / ".github/workflows/release-wheels.yml").read_text()
    workflow += (
        ROOT / ".github/workflows/release-platform-wheel.yml"
    ).read_text()

    assert workflow.count(" tools/audit_distribution.py") == 3
    assert workflow.count("extensions/kafka/tools/audit_distribution.py") == 3
    assert '"dist/*.whl"' in workflow
    assert '"dist/*.tar.gz"' in workflow
    assert '"kafka-dist/*.whl"' in workflow
    assert '"kafka-dist/*.tar.gz"' in workflow


def test_release_workflow_locates_installed_sdk_from_distribution():
    workflow = (ROOT / ".github/workflows/release-wheels.yml").read_text()
    workflow += (
        ROOT / ".github/workflows/release-platform-wheel.yml"
    ).read_text()

    assert 'metadata.distribution("hgraph")' in workflow
    assert 'distribution.locate_file("")' in workflow
    assert '"hgraphConfig.cmake"' in workflow
    assert "HGRAPH_KAFKA_SDK_PREFIX={site.getsitepackages()[0]}" not in workflow


def main():
    test_pyarrow_build_and_runtime_requirements_share_the_supported_abi()
    test_windows_wheel_installs_all_linked_pyarrow_runtimes()
    test_supported_python_versions_are_declared()
    test_pypi_classifiers_are_valid()
    test_release_metadata_uses_untagged_sentinel()
    test_wheel_targets_the_python_312_stable_abi()
    test_repository_enables_polars_compatibility_without_a_cpp_switch()
    test_wheel_uses_shared_runtime_for_downstream_native_extensions()
    test_source_distribution_excludes_private_release_evidence()
    test_full_suite_dependencies_include_the_dataframe_runtime()
    test_adaptor_extras_include_their_runtime_dependencies()
    test_full_suite_gate_installs_the_wheel_test_extra()
    test_release_workflow_targets_supported_platforms()
    test_platform_wheel_builds_use_stable_cacheable_paths()
    test_platform_wheel_tests_start_after_only_their_matching_build()
    test_release_workflow_reuses_tested_commit_artifacts()
    test_release_workflow_audits_distribution_contents()
    test_release_workflow_locates_installed_sdk_from_distribution()
    print("PASS test_pyarrow_build_and_runtime_requirements_share_the_supported_abi")
    print("PASS test_windows_wheel_installs_all_linked_pyarrow_runtimes")
    print("PASS test_supported_python_versions_are_declared")
    print("PASS test_pypi_classifiers_are_valid")
    print("PASS test_release_metadata_uses_untagged_sentinel")
    print("PASS test_wheel_targets_the_python_312_stable_abi")
    print("PASS test_repository_enables_polars_compatibility_without_a_cpp_switch")
    print("PASS test_wheel_uses_shared_runtime_for_downstream_native_extensions")
    print("PASS test_source_distribution_excludes_private_release_evidence")
    print("PASS test_full_suite_dependencies_include_the_dataframe_runtime")
    print("PASS test_adaptor_extras_include_their_runtime_dependencies")
    print("PASS test_full_suite_gate_installs_the_wheel_test_extra")
    print("PASS test_release_workflow_targets_supported_platforms")
    print("PASS test_platform_wheel_builds_use_stable_cacheable_paths")
    print("PASS test_platform_wheel_tests_start_after_only_their_matching_build")
    print("PASS test_release_workflow_reuses_tested_commit_artifacts")
    print("PASS test_release_workflow_audits_distribution_contents")
    print("PASS test_release_workflow_locates_installed_sdk_from_distribution")


if __name__ == "__main__":
    main()
