#!/usr/bin/env python3
"""
Analyze state traces and generate state flow documentation.

Usage:
    HGRAPH_TRACE_STATES=1 uv run pytest hgraph_unit_tests/ts_tests/ -v
    python hgraph_unit_tests/ts_tests/analyze_state_flows.py state_trace.json

This script analyzes the state trace JSON file and generates:
1. State models for each time-series type
2. State transition diagrams (as text/mermaid)
3. Test coverage analysis
"""
import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional


@dataclass
class StateModel:
    """Represents the state model for a time-series type."""
    ts_type: str
    states: set[str]
    transitions: list[tuple[str, str, str]]  # (from_state, to_state, trigger)
    properties: set[str]
    methods: set[str]
    test_coverage: dict[str, set[str]]  # transition -> tests that exercise it


def load_trace(filename: str) -> dict:
    """Load trace data from JSON file."""
    with open(filename) as f:
        return json.load(f)


def analyze_state_changes(transitions: list[dict]) -> dict[str, StateModel]:
    """Analyze transitions and build state models."""
    models: dict[str, StateModel] = {}

    for t in transitions:
        ts_type = t['ts_type']
        if ts_type not in models:
            models[ts_type] = StateModel(
                ts_type=ts_type,
                states=set(),
                transitions=[],
                properties=set(),
                methods=set(),
                test_coverage=defaultdict(set),
            )

        model = models[ts_type]
        model.methods.add(t['method'])

        # Extract state from changes
        changes = t.get('changes', {})
        for prop, change in changes.items():
            model.properties.add(prop)

            before = change.get('before')
            after = change.get('after')

            # Create state identifiers
            if prop in ('valid', 'modified', 'active', 'bound', 'has_peer'):
                from_state = f"{prop}={before}"
                to_state = f"{prop}={after}"
                trigger = t['method']

                model.states.add(from_state)
                model.states.add(to_state)

                trans_key = f"{from_state} -> {to_state} [{trigger}]"
                model.transitions.append((from_state, to_state, trigger))
                if t.get('test'):
                    model.test_coverage[trans_key].add(t['test'])


    return models


def generate_state_diagram(model: StateModel) -> str:
    """Generate a Mermaid state diagram for a type."""
    lines = [
        "```mermaid",
        "stateDiagram-v2",
        "    direction LR",
    ]

    # Group transitions by property
    prop_transitions = defaultdict(set)
    for from_s, to_s, trigger in model.transitions:
        prop = from_s.split('=')[0]
        prop_transitions[prop].add((from_s, to_s, trigger))

    def clean_state(s: str) -> str:
        """Clean state name for valid Mermaid identifier."""
        # Replace = with _ and handle None/True/False
        s = s.replace('=', '_').replace('<', '').replace('>', '')
        # Wrap in quotes if contains special values
        if 'None' in s or s.endswith('_True') or s.endswith('_False'):
            return f'"{s}"'
        return s

    for prop, trans in sorted(prop_transitions.items()):
        lines.append(f"")
        lines.append(f"    %% {prop} states")
        seen_transitions = set()
        for from_s, to_s, trigger in sorted(trans):
            from_clean = clean_state(from_s)
            to_clean = clean_state(to_s)
            key = (from_clean, to_clean, trigger)
            if key not in seen_transitions:
                seen_transitions.add(key)
                lines.append(f"    {from_clean} --> {to_clean}: {trigger}")

    lines.append("```")
    return "\n".join(lines)


def generate_transition_table(model: StateModel) -> str:
    """Generate a transition table for a type."""
    lines = [
        f"| From State | To State | Trigger | Tests |",
        f"|------------|----------|---------|-------|",
    ]

    seen = set()
    for from_s, to_s, trigger in sorted(model.transitions):
        key = (from_s, to_s, trigger)
        if key in seen:
            continue
        seen.add(key)

        trans_key = f"{from_s} -> {to_s} [{trigger}]"
        tests = model.test_coverage.get(trans_key, set())
        test_count = f"{len(tests)} tests"

        lines.append(f"| {from_s} | {to_s} | {trigger} | {test_count} |")

    return "\n".join(lines)


def generate_coverage_report(models: dict[str, StateModel]) -> str:
    """Generate a test coverage report."""
    lines = [
        "# Test Coverage Report",
        "",
        "## Summary by Type",
        "",
    ]

    for ts_type, model in sorted(models.items()):
        total_transitions = len(set((f, t, tr) for f, t, tr in model.transitions))
        covered_transitions = len([k for k, v in model.test_coverage.items() if v])

        lines.append(f"### {ts_type}")
        lines.append(f"- Properties tracked: {', '.join(sorted(model.properties))}")
        lines.append(f"- Methods instrumented: {', '.join(sorted(model.methods))}")
        lines.append(f"- Unique transitions: {total_transitions}")
        lines.append(f"- Transitions with test coverage: {covered_transitions}")
        lines.append("")

        # Show transitions without coverage
        uncovered = [k for k, v in model.test_coverage.items() if not v]
        if uncovered:
            lines.append("**Uncovered transitions:**")
            for t in sorted(uncovered):
                lines.append(f"- {t}")
            lines.append("")

    return "\n".join(lines)


def generate_state_model_doc(model: StateModel) -> str:
    """Generate complete documentation for a state model."""
    lines = [
        f"## {model.ts_type} State Model",
        "",
        "### Properties",
        "",
    ]

    for prop in sorted(model.properties):
        lines.append(f"- `{prop}`")

    lines.append("")
    lines.append("### State Diagram")
    lines.append("")
    lines.append(generate_state_diagram(model))
    lines.append("")
    lines.append("### Transition Table")
    lines.append("")
    lines.append(generate_transition_table(model))
    lines.append("")

    return "\n".join(lines)


def main(trace_file: str):
    """Main analysis function."""
    print(f"Loading trace from {trace_file}...")
    data = load_trace(trace_file)

    print(f"Analyzing {len(data['transitions'])} transitions...")
    models = analyze_state_changes(data['transitions'])

    # Generate documentation
    output_lines = [
        "# Time-Series State Models",
        "",
        "Auto-generated from test execution traces.",
        "",
        f"Total transitions analyzed: {len(data['transitions'])}",
        "",
        "## Overview",
        "",
        "| Type | Properties | Methods | Transitions |",
        "|------|------------|---------|-------------|",
    ]

    for ts_type, model in sorted(models.items()):
        output_lines.append(
            f"| {ts_type} | {len(model.properties)} | {len(model.methods)} | {len(set(model.transitions))} |"
        )

    output_lines.append("")

    # Generate per-type documentation
    for ts_type, model in sorted(models.items()):
        output_lines.append(generate_state_model_doc(model))

    # Add coverage report
    output_lines.append(generate_coverage_report(models))

    # Write output
    output_file = trace_file.replace('.json', '_analysis.md')
    with open(output_file, 'w') as f:
        f.write("\n".join(output_lines))

    print(f"Analysis written to {output_file}")

    # Print summary
    print("\n" + "=" * 60)
    print("ANALYSIS SUMMARY")
    print("=" * 60)
    for ts_type, model in sorted(models.items()):
        print(f"\n{ts_type}:")
        print(f"  States: {len(model.states)}")
        print(f"  Transitions: {len(set(model.transitions))}")
        print(f"  Properties: {sorted(model.properties)}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python analyze_state_flows.py <trace_file.json>")
        sys.exit(1)

    main(sys.argv[1])
