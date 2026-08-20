#!/usr/bin/env python3
"""Run the hidden Fabric conformance case against a restartable real broker."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile
import time


DEFAULT_IMAGE = (
    "docker.redpanda.com/redpandadata/redpanda@"
    "sha256:9a47c1f8d6736f98fa2616f6f0b715c051cb0bdac1a1176e38321bf45a5b572d"
)


def run(*command: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=check, text=True, capture_output=True)


def wait_until(predicate, process: subprocess.Popen[str] | None, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        if process is not None and process.poll() is not None:
            raise RuntimeError(f"conformance executable exited with {process.returncode}")
        time.sleep(0.1)
    raise TimeoutError("timed out waiting for broker conformance phase")


def broker_ready(container: str) -> bool:
    result = run(
        "docker",
        "exec",
        container,
        "rpk",
        "topic",
        "list",
        "--brokers",
        "127.0.0.1:9092",
        check=False,
    )
    return result.returncode == 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--test-executable", type=Path, required=True)
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    parser.add_argument("--port", type=int, default=19092)
    args = parser.parse_args()

    executable = args.test_executable.resolve()
    if not executable.is_file():
        parser.error(f"test executable does not exist: {executable}")

    suffix = secrets.token_hex(4)
    container = f"hgraph-fabric-redpanda-{suffix}"
    topic = f"hgraph-fabric-conformance-{suffix}"
    with tempfile.TemporaryDirectory(prefix="hgraph-fabric-kafka-") as directory:
        control = Path(directory)
        output_path = control / "test-output.log"
        test_process: subprocess.Popen[str] | None = None
        try:
            run(
                "docker",
                "run",
                "--detach",
                "--name",
                container,
                "--hostname",
                container,
                "--publish",
                f"{args.port}:{args.port}",
                args.image,
                "redpanda",
                "start",
                "--mode",
                "dev-container",
                "--smp",
                "1",
                "--memory",
                "512M",
                "--reserve-memory",
                "0M",
                "--check=false",
                "--kafka-addr",
                f"internal://0.0.0.0:9092,external://0.0.0.0:{args.port}",
                "--advertise-kafka-addr",
                f"internal://{container}:9092,external://127.0.0.1:{args.port}",
            )
            wait_until(lambda: broker_ready(container), None, 45.0)
            run(
                "docker",
                "exec",
                container,
                "rpk",
                "topic",
                "create",
                topic,
                "--partitions",
                "3",
                "--brokers",
                "127.0.0.1:9092",
            )

            environment = os.environ.copy()
            environment.update(
                {
                    "HGRAPH_FABRIC_KAFKA_INTEGRATION_BOOTSTRAP": f"127.0.0.1:{args.port}",
                    "HGRAPH_FABRIC_KAFKA_INTEGRATION_TOPIC": topic,
                    "HGRAPH_FABRIC_KAFKA_INTEGRATION_CONTROL_DIR": str(control),
                }
            )
            with output_path.open("w", encoding="utf-8") as output:
                test_process = subprocess.Popen(
                    [str(executable), "[kafka-broker]"],
                    env=environment,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    text=True,
                )

                wait_until(lambda: (control / "initial-ready").exists(), test_process, 30.0)
                run("docker", "stop", "--time", "2", container)
                (control / "broker-stopped").write_text("stopped\n", encoding="utf-8")

                wait_until(
                    lambda: (control / "publication-durable").exists(),
                    test_process,
                    30.0,
                )
                wait_until(
                    lambda: (control / "delivery-failed-retriable").exists(),
                    test_process,
                    30.0,
                )

                run("docker", "start", container)
                wait_until(lambda: broker_ready(container), test_process, 45.0)
                (control / "broker-restarted").write_text("restarted\n", encoding="utf-8")
                try:
                    return_code = test_process.wait(timeout=45.0)
                except subprocess.TimeoutExpired:
                    test_process.terminate()
                    test_process.wait(timeout=10.0)
                    raise TimeoutError("Fabric conformance executable did not finish")

            if return_code != 0:
                raise RuntimeError(f"conformance executable exited with {return_code}")
            print(output_path.read_text(encoding="utf-8"), end="")
            return 0
        except Exception as error:
            print(f"Fabric Kafka broker conformance failed: {error}", file=sys.stderr)
            if output_path.exists():
                print(output_path.read_text(encoding="utf-8"), file=sys.stderr)
            logs = run("docker", "logs", "--tail", "200", container, check=False)
            if logs.stdout:
                print(logs.stdout, file=sys.stderr)
            if logs.stderr:
                print(logs.stderr, file=sys.stderr)
            return 1
        finally:
            if test_process is not None and test_process.poll() is None:
                test_process.terminate()
                try:
                    test_process.wait(timeout=10.0)
                except subprocess.TimeoutExpired:
                    test_process.kill()
                    test_process.wait()
            run("docker", "rm", "--force", container, check=False)


if __name__ == "__main__":
    raise SystemExit(main())
