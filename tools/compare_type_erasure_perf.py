#!/usr/bin/env python3
"""Compare two format-2 hgraph type-erasure benchmark reports."""

from __future__ import annotations

import argparse
import math
import shlex
import sys
from dataclasses import dataclass
from pathlib import Path


EXACT_FIELDS = ("iterations", "median_allocations", "median_bytes", "checksum")
IDENTITY_FIELDS = ("host", "compiler", "compiler_version", "architecture")


@dataclass(frozen=True)
class Report:
    metadata: dict[str, str]
    benchmarks: dict[str, dict[str, str]]


def parse_fields(line: str) -> tuple[str, dict[str, str]]:
    tokens = shlex.split(line)
    if not tokens:
        raise ValueError("empty report line")

    fields: dict[str, str] = {}
    for token in tokens[1:]:
        if "=" not in token:
            raise ValueError(f"malformed field {token!r}")
        name, value = token.split("=", 1)
        if not name or not value or name in fields:
            raise ValueError(f"invalid field {token!r}")
        fields[name] = value
    return tokens[0], fields


def read_report(path: Path) -> Report:
    metadata: dict[str, str] | None = None
    benchmarks: dict[str, dict[str, str]] = {}
    report_text = sys.stdin.read() if str(path) == "-" else path.read_text(encoding="utf-8")
    for line_number, raw_line in enumerate(report_text.splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            record_type, fields = parse_fields(line)
        except ValueError as exc:
            raise ValueError(f"{path}:{line_number}: {exc}") from exc

        if record_type == "type_erasure_perf":
            if metadata is not None:
                raise ValueError(f"{path}:{line_number}: duplicate report header")
            metadata = fields
        elif record_type == "benchmark":
            name = fields.get("name")
            if name is None:
                raise ValueError(f"{path}:{line_number}: benchmark has no name")
            if name in benchmarks:
                raise ValueError(f"{path}:{line_number}: duplicate benchmark {name!r}")
            benchmarks[name] = fields
        else:
            raise ValueError(f"{path}:{line_number}: unknown record type {record_type!r}")

    if metadata is None:
        raise ValueError(f"{path}: missing report header")
    if metadata.get("format") != "2":
        raise ValueError(f"{path}: expected benchmark format 2")
    if not benchmarks:
        raise ValueError(f"{path}: report contains no benchmarks")
    return Report(metadata, benchmarks)


def finite_float(fields: dict[str, str], name: str, context: str) -> float:
    try:
        value = float(fields[name])
    except (KeyError, ValueError) as exc:
        raise ValueError(f"{context}: missing or invalid {name}") from exc
    if not math.isfinite(value):
        raise ValueError(f"{context}: non-finite {name}")
    return value


def compare_reports(
    baseline: Report,
    current: Report,
    timing_threshold_percent: float,
    mad_multiplier: float,
    skip_timing: bool,
) -> list[str]:
    failures: list[str] = []
    for field in IDENTITY_FIELDS:
        if baseline.metadata.get(field) != current.metadata.get(field):
            failures.append(
                f"metadata {field}: expected {baseline.metadata.get(field)!r}, "
                f"got {current.metadata.get(field)!r}"
            )

    baseline_names = set(baseline.benchmarks)
    current_names = set(current.benchmarks)
    for name in sorted(baseline_names - current_names):
        failures.append(f"benchmark {name}: missing from current report")
    for name in sorted(current_names - baseline_names):
        failures.append(f"benchmark {name}: missing from baseline report")

    print("benchmark                                         baseline       current      change   result")
    for name in sorted(baseline_names & current_names):
        expected = baseline.benchmarks[name]
        actual = current.benchmarks[name]
        benchmark_failures: list[str] = []
        for field in EXACT_FIELDS:
            if expected.get(field) != actual.get(field):
                benchmark_failures.append(
                    f"{field} expected {expected.get(field)!r}, got {actual.get(field)!r}"
                )

        baseline_median = finite_float(expected, "median_ns_per_op", f"baseline {name}")
        current_median = finite_float(actual, "median_ns_per_op", f"current {name}")
        change_percent = (
            0.0 if baseline_median == 0.0 else (current_median - baseline_median) * 100.0 / baseline_median
        )
        if not skip_timing and current_median > baseline_median:
            baseline_mad = finite_float(expected, "mad_ns_per_op", f"baseline {name}")
            current_mad = finite_float(actual, "mad_ns_per_op", f"current {name}")
            regression = current_median - baseline_median
            percentage_allowance = baseline_median * timing_threshold_percent / 100.0
            noise_allowance = mad_multiplier * max(baseline_mad, current_mad)
            if regression > max(percentage_allowance, noise_allowance):
                benchmark_failures.append(
                    f"timing regression {change_percent:.2f}% exceeds both "
                    f"{timing_threshold_percent:.2f}% and {mad_multiplier:.2f}x MAD"
                )

        result = "FAIL" if benchmark_failures else "ok"
        print(f"{name:48} {baseline_median:10.3f} {current_median:13.3f} {change_percent:9.2f}% {result}")
        failures.extend(f"benchmark {name}: {failure}" for failure in benchmark_failures)

    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path, help="accepted format-2 benchmark report")
    parser.add_argument("current", type=Path, help="new format-2 benchmark report, or - for standard input")
    parser.add_argument("--timing-threshold-percent", type=float, default=5.0)
    parser.add_argument("--mad-multiplier", type=float, default=3.0)
    parser.add_argument(
        "--skip-timing",
        action="store_true",
        help="check benchmark inventory, identity, allocation counts, bytes, and checksums only",
    )
    args = parser.parse_args()
    if args.timing_threshold_percent < 0.0 or args.mad_multiplier < 0.0:
        parser.error("timing thresholds must be non-negative")

    try:
        failures = compare_reports(
            read_report(args.baseline),
            read_report(args.current),
            args.timing_threshold_percent,
            args.mad_multiplier,
            args.skip_timing,
        )
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if failures:
        print("\ncomparison failed:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1
    print("\ncomparison passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
