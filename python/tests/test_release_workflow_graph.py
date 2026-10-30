"""Release builds expose independent work without weakening artifact gates."""

import importlib.util
from pathlib import Path
import re
from types import SimpleNamespace

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "extension_wheel_builder", ROOT / "tools/build_extension_wheel.py"
)
BUILDER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BUILDER)


def workflow(name):
    return yaml.load((ROOT / ".github/workflows" / name).read_text(),
                     Loader=yaml.BaseLoader)


def needs(job):
    values = job.get("needs", [])
    return {values} if isinstance(values, str) else set(values)


def test_core_tests_and_extensions_start_after_only_the_matching_core():
    jobs = workflow("release-wheels.yml")["jobs"]
    for platform in ("windows", "macos", "linux"):
        core = f"build-{platform}-wheel"
        early = jobs[f"test-{platform}-core-wheel"]
        extensions = jobs[f"build-{platform}-extensions"]
        assert needs(early) == {"reuse-build", core}
        assert early["with"]["suite"] == "core"
        assert needs(extensions) == {"reuse-build", core}
        assert needs(jobs[f"test-{platform}-wheel"]) == {
            "reuse-build", core, f"build-{platform}-extensions",
        }


def test_only_fabric_waits_for_persistence():
    jobs = workflow("release-extension-wheels.yml")["jobs"]
    assert set(jobs) == set(BUILDER.PACKAGES)
    for package, job in jobs.items():
        assert job["with"]["package"] == package
        assert job["uses"] == "./.github/workflows/build-extension-wheel.yml"
        assert needs(job) == ({"persistence"} if package == "fabric" else set())


def test_each_extension_is_tested_before_its_artifact_is_uploaded():
    for job in workflow("build-extension-wheel.yml")["jobs"].values():
        steps = job["steps"]
        build = next(i for i, step in enumerate(steps)
                     if step.get("name") == "Build, audit, and test selected extension")
        upload = next(i for i, step in enumerate(steps)
                      if step.get("uses", "").startswith("actions/upload-artifact@"))
        assert build < upload
        assert "if" not in steps[upload]  # Failed tests must not publish a wheel.


def test_extension_read_permissions_are_granted_to_each_job():
    builder = workflow("build-extension-wheel.yml")
    assert builder["permissions"] == {}
    for job in builder["jobs"].values():
        assert job["permissions"] == {"actions": "read", "contents": "read"}


def test_core_and_extensions_use_one_immutable_manylinux_image():
    core = workflow("release-wheels.yml")["jobs"]["build-linux-wheel"]["container"]["image"]
    extension = workflow("build-extension-wheel.yml")["jobs"]["linux"]["container"]["image"]
    assert core == extension
    assert re.fullmatch(r"quay\.io/pypa/manylinux_2_28_x86_64:[\d.-]+@sha256:[a-f0-9]{64}", core)


@pytest.mark.parametrize("package", BUILDER.PACKAGES)
def test_repair_keeps_arrow_and_sdk_libraries_with_their_owners(package):
    excluded = BUILDER.manylinux_excludes(package, 26)
    assert {"libarrow.so.2600", "libarrow_compute.so.2600",
            "libarrow_acero.so.2600", "libhgraph_runtime.so",
            "libhgraph_wiring.so", "libhgraph_stdlib.so",
            "libnanobind-abi3.so"} <= set(excluded)
    assert ("libarrow_s3.so.2600" in excluded) == (package in ("persistence", "fabric"))
    assert ("libparquet.so.2600" in excluded) == (package in ("persistence", "fabric"))
    assert ("libhgraph_persistence.so" in excluded) == (package == "fabric")


def test_unrepaired_linux_wheel_is_not_accepted():
    with pytest.raises(AssertionError):
        BUILDER.verify_wheel(
            Path("hgraph_kafka-0.0.0-cp312-abi3-linux_x86_64.whl"), "kafka", True
        )


def test_extension_build_rejects_a_core_wheel_without_its_sdk(monkeypatch, tmp_path):
    monkeypatch.setattr(BUILDER.metadata, "distribution",
                        lambda name: SimpleNamespace(locate_file=lambda path: tmp_path))
    with pytest.raises(FileNotFoundError, match="matching core wheel"):
        BUILDER.build_extension("kafka", manylinux=True)


@pytest.mark.parametrize("package", ["--help", "../web", "kafka -Cbad=value", "web/../../"])
def test_untrusted_package_selection_never_reaches_a_build_command(monkeypatch, package):
    def unexpected(*args, **kwargs):
        pytest.fail("invalid selection reached SDK discovery or a command")

    monkeypatch.setattr(BUILDER.metadata, "distribution", unexpected)
    monkeypatch.setattr(BUILDER.subprocess, "check_call", unexpected)
    with pytest.raises(ValueError, match="unsupported extension package"):
        BUILDER.build_extension(package, manylinux=True)


@pytest.mark.parametrize("library_dir", ["lib", "lib64"])
def test_extension_tests_use_the_repaired_artifact_and_matching_sdk(
        monkeypatch, tmp_path, library_dir):
    sdk = tmp_path / "sdk"
    config = sdk / library_dir / "cmake/hgraph/hgraphConfig.cmake"
    config.parent.mkdir(parents=True)
    config.touch()
    monkeypatch.setattr(BUILDER, "ROOT", tmp_path)
    monkeypatch.setattr(BUILDER.metadata, "distribution",
                        lambda name: SimpleNamespace(locate_file=lambda path: sdk))
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        if command[1:3] == ["-m", "build"]:
            output = Path(command[command.index("--outdir") + 1])
            output.mkdir()
            (output / "hgraph_kafka-0.0.0-cp312-abi3-linux_x86_64.whl").touch()
        elif command[0] == "auditwheel":
            output = Path(command[command.index("--wheel-dir") + 1])
            (output / "hgraph_kafka-0.0.0-cp312-abi3-manylinux_2_28_x86_64.whl").touch()

    monkeypatch.setattr(BUILDER.subprocess, "check_call", run)
    BUILDER.build_extension("kafka", manylinux=True)
    assert "-Cbuild-dir=.ci-build/kafka/{wheel_tag}" in calls[0][0]
    assert calls[0][1]["env"]["CMAKE_PREFIX_PATH"].startswith(str(sdk))
    commands = [command for command, _ in calls]
    install = next(command for command in commands if command[1:4] == ["-m", "pip", "install"])
    assert "--no-deps" in install
    assert "--force-reinstall" in install
    assert Path(install[-1]).parent == tmp_path / "kafka-dist"
    assert "manylinux_2_28_x86_64" in install[-1]
    assert commands[-1][1:3] == ["-m", "pytest"]
    assert str(tmp_path / "extensions/kafka/python/tests") in commands[-1]
